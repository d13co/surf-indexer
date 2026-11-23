package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha512"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/duckdb/duckdb-go/v2"
)

// specsDir is the sigreg export directory (SPECS_DIR) holding specs.jsonl and trees.jsonl; "" turns
// the feature off. It is an external dataset, the same for every network, so it isn't part of config:
// its tables are reloaded from the files, never synced from blocks.
var specsDir string

// specTables are created with the feature on and dropped with it off: they hold nothing the files
// can't rebuild. Each load replaces every row. No row is read from the files as is: every field is
// checked, and selectors are computed here (specSelector). What a route would have to count over
// many rows is counted on load (the counts in specs, spec_programs.owners, spec_sigs), so each
// route reads a few rows by an index.
const specTables = `CREATE TABLE IF NOT EXISTS specs(
	blob VARCHAR PRIMARY KEY, -- git blob SHA-1 of the spec file, which the browser checks the fetched bytes against
	kind VARCHAR NOT NULL,    -- arc56, arc32 or arc4
	name VARCHAR,             -- the contract's name, as its author claims; NULL if absent or refused
	methods UINTEGER NOT NULL, events UINTEGER NOT NULL, seen UINTEGER NOT NULL, -- its rows in spec_methods, spec_events and spec_seen
	owners UINTEGER NOT NULL  -- distinct owners of its non-mirror rows in spec_seen, case-insensitive as GitHub's
);
CREATE TABLE IF NOT EXISTS spec_programs(
	hash BLOB NOT NULL, -- SHA-512/256 of an ARC-56 spec's byteCode
	prog VARCHAR NOT NULL, blob VARCHAR NOT NULL,
	owners UINTEGER NOT NULL -- the spec's, to rank by
);
CREATE TABLE IF NOT EXISTS spec_seen(
	blob VARCHAR NOT NULL, owner VARCHAR NOT NULL, repo VARCHAR NOT NULL, path VARCHAR NOT NULL,
	ref VARCHAR NOT NULL, ref_type VARCHAR NOT NULL, -- branch or tag
	commit VARCHAR,                                  -- NULL when the harvester didn't record one
	mirror BOOLEAN NOT NULL                          -- a registry repo holding copies of others' specs
);
CREATE TABLE IF NOT EXISTS spec_methods(blob VARCHAR NOT NULL, selector UINTEGER NOT NULL, sig VARCHAR NOT NULL, args VARCHAR[] NOT NULL, readonly BOOLEAN NOT NULL);
CREATE TABLE IF NOT EXISTS spec_events(blob VARCHAR NOT NULL, selector UINTEGER NOT NULL, sig VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS spec_files(commit VARCHAR NOT NULL, path VARCHAR NOT NULL); -- .ts, .mts and .py files of a scanned commit
-- Each method and event signature, by selector, counted over every spec that has it (specSigsSQL).
CREATE TABLE IF NOT EXISTS spec_sigs(
	selector UINTEGER NOT NULL, event BOOLEAN NOT NULL, sig VARCHAR NOT NULL,
	args VARCHAR[],           -- a method's: the arg names of the most owners, then specs; NULL for an event
	owners UINTEGER NOT NULL, -- distinct owners, case-insensitive, of the non-mirror sources of its specs
	specs UINTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS spec_meta(loaded_at TIMESTAMP NOT NULL, specs UINTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS spec_programs_hash ON spec_programs(hash);
CREATE INDEX IF NOT EXISTS spec_programs_blob ON spec_programs(blob);
CREATE INDEX IF NOT EXISTS spec_seen_blob ON spec_seen(blob);
CREATE INDEX IF NOT EXISTS spec_methods_blob ON spec_methods(blob);
CREATE INDEX IF NOT EXISTS spec_events_blob ON spec_events(blob);
CREATE INDEX IF NOT EXISTS spec_files_commit ON spec_files(commit);
CREATE INDEX IF NOT EXISTS spec_sigs_selector ON spec_sigs(selector)`

// specTableNames are specTables but spec_meta, in load order: those appended to, then spec_sigs,
// computed from them.
var specTableNames = []string{"specs", "spec_programs", "spec_seen", "spec_methods", "spec_events", "spec_files", "spec_sigs"}

// specOrder is the key each table is looked up by. Rows are inserted in its order, so a key's rows
// sit together: a lookup reads few row groups, by index or by their min/max.
var specOrder = map[string]string{"specs": "blob", "spec_programs": "hash", "spec_seen": "blob", "spec_methods": "blob",
	"spec_events": "blob", "spec_files": "commit", "spec_sigs": "selector"}

const dropSpecTables = `DROP TABLE IF EXISTS specs; DROP TABLE IF EXISTS spec_programs; DROP TABLE IF EXISTS spec_seen;
DROP TABLE IF EXISTS spec_methods; DROP TABLE IF EXISTS spec_events; DROP TABLE IF EXISTS spec_files; DROP TABLE IF EXISTS spec_sigs;
DROP TABLE IF EXISTS spec_meta`

// maxSigOwners is how many of a spec's owners count toward its signatures' owners: the first, by
// name. Every owner would make the count's input a spec's methods times its owners, which a hostile
// set of accounts can blow up (512 methods x 200 owners a spec).
const maxSigOwners = 10

// specSigsSQL fills stage_spec_sigs from the other staged tables. A signature's owners are counted
// once across its specs, so they can't be summed from the specs' own counts. A spec has a signature
// once, with one args, so a signature's specs are its args' specs summed.
var specSigsSQL = fmt.Sprintf(`INSERT INTO temp.stage_spec_sigs
WITH o AS (SELECT blob, owner FROM (SELECT DISTINCT blob, lower(owner) AS owner FROM temp.stage_spec_seen WHERE NOT mirror)
		QUALIFY row_number() OVER (PARTITION BY blob ORDER BY owner) <= %d),
	a AS (SELECT selector, sig, args, coalesce(list(DISTINCT owner) FILTER (WHERE owner IS NOT NULL), []) AS owners, count(DISTINCT blob) AS specs
		FROM temp.stage_spec_methods LEFT JOIN o USING (blob) GROUP BY ALL)
SELECT selector, false, sig, first(args ORDER BY len(owners) DESC, specs DESC, args), len(list_distinct(flatten(list(owners)))), sum(specs)
	FROM a GROUP BY selector, sig
UNION ALL
SELECT selector, true, sig, NULL, count(DISTINCT o.owner), count(DISTINCT blob) FROM temp.stage_spec_events LEFT JOIN o USING (blob) GROUP BY ALL`, maxSigOwners)

// specShapeSQL is the spec tables' columns and indexes in one string, to tell a DB's tables from
// those specTables makes.
const specShapeSQL = `SELECT coalesce(string_agg(s, '; ' ORDER BY s), '') FROM (
	SELECT table_name || '.' || column_name || ' ' || data_type || if(is_nullable, '', ' NOT NULL') AS s FROM duckdb_columns()
		WHERE database_name = current_database() AND schema_name = 'main' AND table_name LIKE 'spec%'
	UNION ALL SELECT table_name || ' index ' || index_name FROM duckdb_indexes()
		WHERE database_name = current_database() AND schema_name = 'main' AND table_name LIKE 'spec%')`

// specShape is the shape of the tables specTables makes, read from an in-memory DB.
var specShape = sync.OnceValues(func() (string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", err
	}
	defer db.Close()
	var shape string
	if _, err = db.Exec(specTables); err == nil {
		err = db.QueryRow(specShapeSQL).Scan(&shape)
	}
	return shape, err
})

// setupSpecTables creates the spec tables, first dropping those of another shape: a DB from another
// build has them, and CREATE TABLE IF NOT EXISTS would keep them, failing every load. They hold
// nothing the files can't rebuild.
func setupSpecTables(tx *sql.Tx) error {
	want, err := specShape()
	if err != nil {
		return err
	}
	var have string
	if err = tx.QueryRow(specShapeSQL).Scan(&have); err != nil {
		return err
	}
	if have != "" && have != want {
		slog.Info("specs: recreating the spec tables, which are of another build's shape")
		if _, err = tx.Exec(dropSpecTables); err != nil {
			return err
		}
	}
	_, err = tx.Exec(specTables)
	return err
}

// The files come from GitHub repos read by the harvester, any of which may be hostile, so every
// count is capped: a line, the entries of a row, the rows of a file, and the entries of a load.
const (
	maxSpecLine    = 4 << 20 // bytes per JSONL line, as the exporter's per-spec cap
	maxSpecEntries = 512     // methods, and events, per spec, as the exporter
	maxSpecSeen    = 200     // seen per spec, non-mirror and with a commit kept first
	maxSpecArgs    = 64      // arg names per method
	maxSpecName    = 256     // characters of a spec's name
	maxSpecSig     = 1024    // characters of a signature
	maxTreeFiles   = 3000    // paths per commit, as the exporter
	maxSpecFiles   = 5_000_000
	maxSpecPath    = 512 // characters
	maxSpecRef     = 255 // bytes, git's limit
)

// Rows of each file, and entries per load, past which they are dropped: ~20x today's (~8.5k specs,
// 53k methods, 1.6k events, 31k sources), but specs and commits ~25x. Programs need no cap: a spec
// has at most 2. Vars for tests.
var (
	maxSpecs, maxTreeRows                                = 200_000, 200_000 // rows of specs.jsonl, trees.jsonl
	maxSpecMethods, maxSpecEvents, maxSpecSeenRows int64 = 1_000_000, 200_000, 500_000
)

// specCheckEvery is how often the files are checked; a change is loaded once they have stayed the
// same for specSettleAfter, as they may still be being written. Vars for tests.
var specCheckEvery, specSettleAfter = 10 * time.Minute, 30 * time.Second

var (
	ownerRe = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}$`)
	repoRe  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
	argRe   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
	sigName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// isHex reports whether s is n hex digits.
func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// shown is whether r can be shown as is: no control, format (zero-width, bidi), separator-line or
// replacement character, nor one that renders blank or not at all though graphic (Hangul fillers,
// the blank Braille pattern, variation selectors). A path or name with one could render as another.
func shown(r rune) bool {
	switch {
	case r == 0x115f, r == 0x1160, r == 0x2800, r == 0x3164, r == 0xffa0,
		r >= 0xfe00 && r <= 0xfe0f, r >= 0xe0100 && r <= 0xe01ef:
		return false
	}
	return unicode.IsGraphic(r) && r != utf8.RuneError
}

func validOwner(s string) bool { return ownerRe.MatchString(s) }

// validRepo is a GitHub repo name; "." and ".." would step out of the owner in a URL.
func validRepo(s string) bool { return repoRe.MatchString(s) && s != "." && s != ".." }

// validPath is a repo-relative file path that a URL can carry segment by segment: no empty, "."
// or ".." segment, no leading or trailing "/", no "\", nothing that can't be shown.
func validPath(s string) bool {
	if s == "" || !utf8.ValidString(s) || utf8.RuneCountInString(s) > maxSpecPath || strings.ContainsFunc(s, func(r rune) bool { return r == '\\' || !shown(r) }) {
		return false
	}
	for seg := range strings.SplitSeq(s, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// validRef follows git check-ref-format, and refuses what can't be shown.
func validRef(s string) bool {
	if s == "" || len(s) > maxSpecRef || !utf8.ValidString(s) || s == "@" ||
		strings.ContainsFunc(s, func(r rune) bool { return r == ' ' || strings.ContainsRune("~^:?*[\\", r) || !shown(r) }) ||
		strings.Contains(s, "..") || strings.Contains(s, "@{") || strings.HasSuffix(s, ".") {
		return false
	}
	for seg := range strings.SplitSeq(s, "/") {
		if seg == "" || seg[0] == '.' || strings.HasSuffix(seg, ".lock") {
			return false
		}
	}
	return true
}

// maxSigDepth is how deep a signature's tuples may nest.
const maxSigDepth = 16

// validSig is an ARC-4 method signature (method) or an ARC-28 event signature of at most maxSpecSig
// characters: a name, its arg types in parentheses, then for a method its return type or void.
func validSig(s string, method bool) bool {
	name, _, ok := strings.Cut(s, "(")
	if len(s) > maxSpecSig || !ok || !sigName.MatchString(name) {
		return false
	}
	p := sigParser{s: s, i: len(name)}
	if !p.tuple(0, method) {
		return false
	}
	switch {
	case !method:
		return p.i == len(s)
	case s[p.i:] == "void":
		return true
	}
	return p.typ(0) && p.i == len(s)
}

// sigParser reads ARC-4 types from s at i.
type sigParser struct {
	s string
	i int
}

// argTypes are the reference and transaction types: a method's args only, not in a tuple or array.
var argTypes = map[string]bool{"account": true, "asset": true, "application": true,
	"txn": true, "pay": true, "keyreg": true, "acfg": true, "axfer": true, "afrz": true, "appl": true}

func (p *sigParser) at(c byte) bool { return p.i < len(p.s) && p.s[p.i] == c }

func (p *sigParser) eat(c byte) bool {
	ok := p.at(c)
	if ok {
		p.i++
	}
	return ok
}

// word reads lowercase letters and digits.
func (p *sigParser) word() string {
	j := p.i
	for p.i < len(p.s) && ('a' <= p.s[p.i] && p.s[p.i] <= 'z' || '0' <= p.s[p.i] && p.s[p.i] <= '9') {
		p.i++
	}
	return p.s[j:p.i]
}

// tuple reads "(T,…)", possibly empty; args allows argTypes, as a method's arg list does.
func (p *sigParser) tuple(depth int, args bool) bool {
	if depth > maxSigDepth || !p.eat('(') {
		return false
	}
	if p.eat(')') {
		return true
	}
	for {
		j := p.i
		if !args || !argTypes[p.word()] || !(p.at(',') || p.at(')')) {
			p.i = j
			if !p.typ(depth) {
				return false
			}
		}
		if p.eat(')') {
			return true
		}
		if !p.eat(',') {
			return false
		}
	}
}

// typ reads an ARC-4 type: a tuple or a base type, then any [] or [N].
func (p *sigParser) typ(depth int) bool {
	if p.at('(') {
		if !p.tuple(depth+1, false) {
			return false
		}
	} else {
		w := p.word()
		if n, ok := strings.CutPrefix(w, "uint"); ok {
			if !abiBits(n) {
				return false
			}
		} else if nm, ok := strings.CutPrefix(w, "ufixed"); ok {
			n, m, ok := strings.Cut(nm, "x")
			if !ok || !abiBits(n) || !abiNum(m, 1, 160) {
				return false
			}
		} else if w != "byte" && w != "bool" && w != "address" && w != "string" {
			return false
		}
	}
	for p.eat('[') {
		j := p.i
		for p.i < len(p.s) && '0' <= p.s[p.i] && p.s[p.i] <= '9' {
			p.i++
		}
		if p.i > j && !abiNum(p.s[j:p.i], 0, math.MaxUint32) || !p.eat(']') {
			return false
		}
	}
	return true
}

// abiNum is whether s is a decimal from lo to hi, without leading zeros.
func abiNum(s string, lo, hi int) bool {
	n, err := strconv.Atoi(s)
	return err == nil && s[0] != '+' && s[0] != '-' && (s == "0" || s[0] != '0') && lo <= n && n <= hi
}

// abiBits is a uint's or ufixed's bit size: 8 to 512, a multiple of 8.
func abiBits(s string) bool {
	n, _ := strconv.Atoi(s)
	return abiNum(s, 8, 512) && n%8 == 0
}

// validName is a spec's name: 1 to maxSpecName characters, each shown as is.
func validName(s string) bool {
	return s != "" && utf8.ValidString(s) && utf8.RuneCountInString(s) <= maxSpecName && !strings.ContainsFunc(s, func(r rune) bool { return !shown(r) })
}

// specSelector is the ARC-4 method (or ARC-28 event) selector of sig. Never read from the file:
// a row could pair any selector with any signature.
func specSelector(sig string) uint32 {
	h := sha512.Sum512_256([]byte(sig))
	return binary.BigEndian.Uint32(h[:4])
}

// specRow is a specs.jsonl line. Fields not listed (such as a selector) are ignored.
type specRow struct {
	Blob     string `json:"blob"`
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Programs []struct {
		Prog string `json:"prog"`
		Hash string `json:"hash"`
	} `json:"programs"`
	Methods []struct {
		Sig      string   `json:"sig"`
		Args     []string `json:"args"`
		Readonly bool     `json:"readonly"`
	} `json:"methods"`
	Events []struct {
		Sig string `json:"sig"`
	} `json:"events"`
	Seen []specSeen `json:"seen"`
}

type specSeen struct {
	Owner   string  `json:"owner"`
	Repo    string  `json:"repo"`
	Path    string  `json:"path"`
	Ref     string  `json:"ref"`
	RefType string  `json:"refType"`
	Commit  *string `json:"commit"`
	Mirror  bool    `json:"mirror"`
}

// treeRow is a trees.jsonl line: the source files of a scanned commit.
type treeRow struct {
	Repo   string   `json:"repo"` // owner/name
	Commit string   `json:"commit"`
	Files  []string `json:"files"`
}

// specCounts is what a load kept and dropped, for its log line. Rows are whole lines; the rest are
// entries dropped from rows that were kept.
type specCounts struct {
	Specs, Programs, Methods, Events, Seen, Commits, Files int64

	BadRows, LongRows, DupRows, OverRows     int64 // specs.jsonl: unparsable or invalid, over maxSpecLine, a blob again, past maxSpecs
	BadTrees, LongTrees, DupTrees, OverTrees int64 // trees.jsonl, likewise; a commit again; past maxTreeRows
	BadPrograms, BadMethods, BadEvents       int64 // invalid, repeated or past the cap (programs: or not arc56's)
	BadSeen, BadArgs, BadNames, BadFiles     int64
	OverMethods, OverEvents, OverSeen        int64 // past the load's maxSpecMethods, maxSpecEvents, maxSpecSeenRows
	OverFiles                                int64 // paths past maxSpecFiles
}

func (c specCounts) attrs() []any {
	return []any{"specs", c.Specs, "programs", c.Programs, "methods", c.Methods, "events", c.Events, "seen", c.Seen,
		"commits", c.Commits, "files", c.Files,
		"badRows", c.BadRows, "longRows", c.LongRows, "dupRows", c.DupRows, "overRows", c.OverRows,
		"badTrees", c.BadTrees, "longTrees", c.LongTrees, "dupTrees", c.DupTrees, "overTrees", c.OverTrees,
		"badPrograms", c.BadPrograms, "badMethods", c.BadMethods, "badEvents", c.BadEvents,
		"badSeen", c.BadSeen, "badArgs", c.BadArgs, "badNames", c.BadNames, "badFiles", c.BadFiles,
		"overMethods", c.OverMethods, "overEvents", c.OverEvents, "overSeen", c.OverSeen, "overFiles", c.OverFiles}
}

// eachLine calls fn with each line of r (without its newline). A line over maxSpecLine is skipped
// and counted in long, not fatal: one bad row mustn't stop the load. fn returning false stops.
func eachLine(ctx context.Context, r io.Reader, long *int64, fn func([]byte) bool) error {
	br := bufio.NewReaderSize(r, maxSpecLine+1)
	for n := 0; ; n++ {
		if n%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		line, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			*long++
			for errors.Is(err, bufio.ErrBufferFull) {
				_, err = br.ReadSlice('\n')
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if l := bytes.TrimSpace(line); len(l) > 0 && !fn(l) {
			return nil
		}
		if err == io.EOF {
			return nil
		}
	}
}

// specAppenders are a load's appenders, one per staging table appended to, in specTableNames order.
type specAppenders [6]*duckdb.Appender

// appendSpec checks row and appends what passes. A row with a bad blob or kind is dropped whole;
// otherwise a bad entry is dropped alone, as is a bad name or arg name (left out: NULL, "").
func appendSpec(a *specAppenders, row *specRow, seen map[string]bool, c *specCounts) error {
	blob := strings.ToLower(row.Blob)
	if !isHex(blob, 40) || (row.Kind != "arc56" && row.Kind != "arc32" && row.Kind != "arc4") {
		c.BadRows++
		return nil
	}
	if seen[blob] {
		c.DupRows++
		return nil
	}
	seen[blob] = true
	var name any
	if validName(row.Name) {
		name = row.Name
	} else if row.Name != "" {
		c.BadNames++
	}

	// Non-mirror sources with a commit first, so the cap drops the least useful. GitHub's owner and
	// repo names are case-insensitive.
	type key struct{ owner, repo, path, ref, refType, commit string }
	keys := map[key]bool{}
	var kept []specSeen
	for _, s := range row.Seen {
		commit := ""
		if s.Commit != nil {
			commit = strings.ToLower(*s.Commit)
		}
		k := key{strings.ToLower(s.Owner), strings.ToLower(s.Repo), s.Path, s.Ref, s.RefType, commit}
		if !validOwner(s.Owner) || !validRepo(s.Repo) || !validPath(s.Path) || !validRef(s.Ref) ||
			(s.RefType != "branch" && s.RefType != "tag") || (s.Commit != nil && !isHex(commit, 40)) || keys[k] {
			c.BadSeen++
			continue
		}
		keys[k] = true
		if s.Commit != nil {
			s.Commit = &commit
		}
		kept = append(kept, s)
	}
	slices.SortStableFunc(kept, func(x, y specSeen) int {
		rank := func(s specSeen) int {
			r := 0
			if s.Mirror {
				r += 2
			}
			if s.Commit == nil {
				r++
			}
			return r
		}
		return rank(x) - rank(y)
	})
	if len(kept) > maxSpecSeen {
		c.BadSeen += int64(len(kept) - maxSpecSeen)
		kept = kept[:maxSpecSeen]
	}
	owners, seenRows := map[string]bool{}, 0
	for _, s := range kept {
		if c.Seen >= maxSpecSeenRows {
			c.OverSeen++
			continue
		}
		var commit any
		if s.Commit != nil {
			commit = *s.Commit
		}
		if err := a[2].AppendRow(blob, s.Owner, s.Repo, s.Path, s.Ref, s.RefType, commit, s.Mirror); err != nil {
			return err
		}
		c.Seen++
		seenRows++
		if !s.Mirror {
			owners[strings.ToLower(s.Owner)] = true
		}
	}

	// Only an ARC-56 spec has byteCode, which the hashes are of.
	progs := map[string]bool{}
	for _, p := range row.Programs {
		h, err := hex.DecodeString(p.Hash)
		if row.Kind != "arc56" || (p.Prog != "approval" && p.Prog != "clear") || len(h) != 32 || err != nil || progs[p.Prog] {
			c.BadPrograms++
			continue
		}
		progs[p.Prog] = true
		if err := a[1].AppendRow(h, p.Prog, blob, uint32(len(owners))); err != nil {
			return err
		}
		c.Programs++
	}

	sigs := map[string]bool{}
	for _, m := range row.Methods {
		switch {
		case !validSig(m.Sig, true) || sigs[m.Sig] || len(sigs) >= maxSpecEntries:
			c.BadMethods++
			continue
		case c.Methods >= maxSpecMethods:
			c.OverMethods++
			continue
		}
		sigs[m.Sig] = true
		args := make([]string, 0, min(len(m.Args), maxSpecArgs))
		if len(m.Args) <= maxSpecArgs {
			for _, n := range m.Args {
				if !argRe.MatchString(n) { // kept as "" so later names keep their position
					n = ""
					c.BadArgs++
				}
				args = append(args, n)
			}
		} else {
			c.BadArgs += int64(len(m.Args))
		}
		if err := a[3].AppendRow(blob, specSelector(m.Sig), m.Sig, args, m.Readonly); err != nil {
			return err
		}
		c.Methods++
	}
	methods := len(sigs)
	clear(sigs)
	for _, e := range row.Events {
		switch {
		case !validSig(e.Sig, false) || sigs[e.Sig] || len(sigs) >= maxSpecEntries:
			c.BadEvents++
			continue
		case c.Events >= maxSpecEvents:
			c.OverEvents++
			continue
		}
		sigs[e.Sig] = true
		if err := a[4].AppendRow(blob, specSelector(e.Sig), e.Sig); err != nil {
			return err
		}
		c.Events++
	}

	if err := a[0].AppendRow(blob, row.Kind, name, uint32(methods), uint32(len(sigs)), uint32(seenRows), uint32(len(owners))); err != nil {
		return err
	}
	c.Specs++
	return nil
}

// validTreeRepo is trees.jsonl's repo: owner/name, as the harvester names repos.
func validTreeRepo(s string) bool {
	owner, name, ok := strings.Cut(s, "/")
	return ok && validOwner(owner) && validRepo(name)
}

// appendTree checks row and appends its valid paths. One commit's tree is the same in every fork,
// so a commit seen again is skipped.
func appendTree(a *specAppenders, row *treeRow, seen map[string]bool, c *specCounts) error {
	commit := strings.ToLower(row.Commit)
	if !isHex(commit, 40) || !validTreeRepo(row.Repo) {
		c.BadTrees++
		return nil
	}
	if seen[commit] {
		c.DupTrees++
		return nil
	}
	seen[commit] = true
	c.Commits++
	paths := map[string]bool{}
	for _, p := range row.Files {
		switch {
		case !validPath(p) || paths[p] || len(paths) >= maxTreeFiles:
			c.BadFiles++
		case c.Files >= maxSpecFiles:
			c.OverFiles++
		default:
			paths[p] = true
			if err := a[5].AppendRow(commit, p); err != nil {
				return err
			}
			c.Files++
		}
	}
	return nil
}

// specFiles is the mtime and size of specs.jsonl and trees.jsonl (zero if absent): a load is due
// when either changes.
type specFiles struct {
	specsMtime, treesMtime time.Time
	specsSize, treesSize   int64
}

func (f specFiles) same(g specFiles) bool {
	return f.specsMtime.Equal(g.specsMtime) && f.treesMtime.Equal(g.treesMtime) && f.specsSize == g.specsSize && f.treesSize == g.treesSize
}

func statSpecFiles(dir string) (f specFiles, err error) {
	st, err := os.Stat(filepath.Join(dir, "specs.jsonl"))
	if err != nil {
		return f, err
	}
	f.specsMtime, f.specsSize = st.ModTime(), st.Size()
	if st, err := os.Stat(filepath.Join(dir, "trees.jsonl")); err == nil {
		f.treesMtime, f.treesSize = st.ModTime(), st.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return f, err
	}
	return f, nil
}

// specStatus is what /v0/status reports of the loaded specs; loadedAt is unix seconds, 0 for never.
type specStatus struct {
	Count    int64 `json:"count"`
	LoadedAt int64 `json:"loadedAt"`
}

var specsLoaded atomic.Pointer[specStatus]

// loadSpecStatus reads the last load's status from spec_meta, so /v0/status has it before this
// process's first load ends.
func loadSpecStatus(ctx context.Context, db *sql.DB) error {
	var s specStatus
	err := db.QueryRowContext(ctx, "SELECT specs, epoch(loaded_at)::BIGINT FROM spec_meta").Scan(&s.Count, &s.LoadedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	specsLoaded.Store(&s)
	return nil
}

var specLoadMu sync.Mutex // one load at a time: they share the staging tables' names

// loadSpecs replaces the spec tables with dir's files, returning what it kept and the files it read
// (their stat once open: a file renamed into place meanwhile isn't the one read). Rows are appended
// to temp staging tables on one connection, spec_sigs is computed from them, then all are copied over
// the live ones in one transaction, so readers see the old load or the new one, never part of
// either. A missing trees.jsonl loads no files.
// ingest's writeMu isn't taken: no other writer touches these tables, and ingest needn't wait.
func loadSpecs(ctx context.Context, db *sql.DB, dir string) (c specCounts, f specFiles, err error) {
	specLoadMu.Lock()
	defer specLoadMu.Unlock()
	sf, err := os.Open(filepath.Join(dir, "specs.jsonl"))
	if err != nil {
		return c, f, err
	}
	defer sf.Close()
	st, err := sf.Stat()
	if err != nil {
		return c, f, err
	}
	f.specsMtime, f.specsSize = st.ModTime(), st.Size()
	tf, err := os.Open(filepath.Join(dir, "trees.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		slog.Warn("specs: no trees.jsonl, loading no source files", "dir", dir)
	} else if err != nil {
		return c, f, err
	} else {
		defer tf.Close()
		if st, err = tf.Stat(); err != nil {
			return c, f, err
		}
		f.treesMtime, f.treesSize = st.ModTime(), st.Size()
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return c, f, err
	}
	defer conn.Close()
	// Temp tables live with the connection, which goes back to the pool: drop them on the way out.
	defer func() {
		for _, t := range specTableNames {
			conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS temp.stage_"+t)
		}
	}()
	for _, t := range specTableNames {
		if _, err = conn.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE stage_"+t+" AS FROM "+t+" LIMIT 0"); err != nil {
			return c, f, err
		}
	}
	err = conn.Raw(func(dc any) (err error) {
		var a specAppenders
		defer func() { // Close flushes: its error fails the load
			for _, ap := range a {
				if ap != nil {
					err = errors.Join(err, ap.Close())
				}
			}
		}()
		conn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("specs: %T is not a driver.Conn", dc)
		}
		for i, t := range specTableNames[:len(a)] {
			if a[i], err = duckdb.NewAppender(conn, "temp", "main", "stage_"+t); err != nil {
				return err
			}
		}
		blobs := map[string]bool{}
		var aerr error
		err = eachLine(ctx, sf, &c.LongRows, func(line []byte) bool {
			if len(blobs) >= maxSpecs {
				c.OverRows++
				return true
			}
			var row specRow
			if json.Unmarshal(line, &row) != nil {
				c.BadRows++
				return true
			}
			aerr = appendSpec(&a, &row, blobs, &c)
			return aerr == nil
		})
		if err == nil && aerr == nil && tf != nil {
			commits := map[string]bool{}
			err = eachLine(ctx, tf, &c.LongTrees, func(line []byte) bool {
				if len(commits) >= maxTreeRows {
					c.OverTrees++
					return true
				}
				var row treeRow
				if json.Unmarshal(line, &row) != nil {
					c.BadTrees++
					return true
				}
				aerr = appendTree(&a, &row, commits, &c)
				return aerr == nil
			})
		}
		return errors.Join(err, aerr)
	})
	if err == nil {
		_, err = conn.ExecContext(ctx, specSigsSQL)
	}
	if err != nil {
		return c, f, err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return c, f, err
	}
	defer tx.Rollback()
	for _, t := range specTableNames {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+t); err != nil {
			return c, f, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO "+t+" FROM temp.stage_"+t+" ORDER BY "+specOrder[t]); err != nil {
			return c, f, err
		}
	}
	now := time.Now().UTC()
	if _, err = tx.ExecContext(ctx, "DELETE FROM spec_meta"); err == nil {
		_, err = tx.ExecContext(ctx, "INSERT INTO spec_meta VALUES (?, ?)", now, c.Specs)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err == nil {
		specsLoaded.Store(&specStatus{c.Specs, now.Unix()})
	}
	return c, f, err
}

// watchSpecs loads dir's files now, then whenever they change, checking every specCheckEvery. A file
// may still be being written, so a change is loaded once the files' mtimes and sizes are the same at
// two checks specSettleAfter apart (whatever the mtimes say: a clock may be off). What was loaded is
// the files loadSpecs read, so one renamed into place meanwhile is loaded next. A failed load keeps
// the previous one and is retried.
func watchSpecs(ctx context.Context, db *sql.DB, dir string) {
	var loaded, pending specFiles
	var since time.Time // when pending was first seen
	for {
		wait := specCheckEvery
		f, err := statSpecFiles(dir)
		switch {
		case err != nil:
			slog.Error("specs", "err", err)
		case f.same(loaded):
			pending = specFiles{}
		case !f.same(pending):
			if pending.same(specFiles{}) {
				slog.Info("specs changed, loading once they settle", "dir", dir, "after", specSettleAfter)
			}
			pending, since, wait = f, time.Now(), specSettleAfter
		case time.Since(since) < specSettleAfter:
			wait = specSettleAfter - time.Since(since)
		default:
			start := time.Now()
			c, read, err := loadSpecs(ctx, db, dir)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("specs load", "err", err)
				}
				break
			}
			loaded, pending = read, specFiles{}
			slog.Info("specs loaded", append(c.attrs(), "ms", time.Since(start).Milliseconds())...)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
