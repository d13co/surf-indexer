package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpecValidators(t *testing.T) {
	long := func(n int) string { return strings.Repeat("a", n) }
	for _, c := range []struct {
		name  string
		valid func(string) bool
		good  []string
		bad   []string
	}{
		{"owner", validOwner, []string{"alice", "a-b", "A1", long(39)}, []string{"", "a b", "a_b", "a/b", "a.b", long(40), "ä"}},
		{"repo", validRepo, []string{"x", "a.b", "a_b-c", ".github", "...", long(100)}, []string{"", ".", "..", "a/b", "a b", long(101), "a\\b"}},
		{"path", validPath, []string{"a.ts", "src/a.py", "a..b.ts", "dir with space/x.ts", "日本/a.ts", strings.Repeat("é", 512)},
			[]string{"", "/a", "a/", "a//b", "..", "../a", "a/../b", "a/..", "./a", "a/./b", "a\\b", "a\x00b", "a\nb", "a\x7fb", "a\u0085b",
				"a\u202eb", "a\u2066b", "a\u200bb", "a\u061cb", "a\ufeffb", "a\u2028b", "a\ufffdb", "a\xffb", strings.Repeat("é", 513),
				"a\u3164b", "a\u115fb", "a\u1160b", "a\uffa0b", "a\u2800b", "a\ufe0fb", "a\U000e0100b"}},
		{"ref", validRef, []string{"main", "v1.2.0", "feature/x", "release-1_2", "日本", long(255)},
			[]string{"", "@", "a..b", "a b", "a~1", "a^", "a:b", "a?", "a*", "a[b", "a\\b", "/a", "a/", "a//b", ".a", "a/.b", "a.lock", "a/b.lock",
				"a.", "a@{1}", "a\x01", "a\x7f", "a\u202e", "a\u200f", "a\xff", long(256), "a\u3164b", "a\u115fb", "a\u1160b", "a\uffa0b", "a\u2800b", "a\ufe0fb", "a\U000e0100b"}},
		{"method", func(s string) bool { return validSig(s, true) }, []string{"add(uint64,uint64)uint128", "f()void", "f()(uint64,bool)",
			"_x((uint64,byte[]),address[2])(bool,string)", "f(uint8,uint512,ufixed64x2,ufixed512x160,byte,bool,address,string)void",
			"pay(pay,axfer,account,asset,application,appl,txn,keyreg,acfg,afrz)void", "f(((uint64,bool),uint64)[])void", "f(()[],uint64[0][2][])()",
			"f(" + strings.Repeat("(", maxSigDepth) + "bool" + strings.Repeat(")", maxSigDepth) + ")void"},
			[]string{"", "(uint64)void", "1f()void", "f", "f()", "f (uint64)void", "f(uint64)void;", "f(Uint64)void", "f(uint64))(", "f(uint64]void",
				"f(uint64", "f(uint64)\u202evoid", "f(a)b", "f(,)void", "f(uint64,,)void", "f(uint64,)void", "f(,uint64)void", "f(uint7)void",
				"f(uint520)void", "f(uint0)void", "f(uint064)void", "f(uint)void", "f(ufixed64x0)void", "f(ufixed64x161)void", "f(ufixed63x2)void",
				"f(ufixed64)void", "f(void)void", "f()pay", "f()account", "f(pay[])void", "f((pay))void", "f(byte[01])void", "f(byte[-1])void",
				"f(byte[)void", "f(byte])void", "f()voidx", "f()uint64x", "f(bool)void\n",
				"f(" + strings.Repeat("(", maxSigDepth+1) + "bool" + strings.Repeat(")", maxSigDepth+1) + ")void",
				"f(" + strings.Repeat("bool,", maxSpecSig/5) + "bool)void"}},
		{"event", func(s string) bool { return validSig(s, false) }, []string{"Registered(byte[4],string)", "E()", "E((uint64,address)[])"},
			[]string{"E", "E()void", "E()uint64", "E(pay)", "E(account)", "E(void)", "E(uint64", "E(uint64))"}},
		{"name", validName, []string{"TimeSeek", "My App 2", "日本", "café", "e\u0301", long(maxSpecName)},
			[]string{"", "a\nb", "a\u202eb", "a\u200bb", long(maxSpecName + 1), "\u3164", "a\u3164b", "a\u115fb", "a\u1160b", "a\uffa0b", "a\u2800b", "a\ufe0fb", "a\U000e0100b"}},
	} {
		for _, s := range c.good {
			if !c.valid(s) {
				t.Errorf("%s %q: want valid", c.name, s)
			}
		}
		for _, s := range c.bad {
			if c.valid(s) {
				t.Errorf("%s %q: want invalid", c.name, s)
			}
		}
	}
}

// Known ARC-4 selectors (Python's hashlib sha512_256 agrees).
func TestSpecSelector(t *testing.T) {
	for sig, want := range map[string]uint32{"add(uint64,uint64)uint128": 0x8aa3b61f, "hello(string)string": 0x02bece11, "Registered(byte[4],string)": 0xdb2d669a} {
		if got := specSelector(sig); got != want {
			t.Errorf("%s: %08x, want %08x", sig, got, want)
		}
	}
}

var (
	blobA, blobB, blobC, blobD = strings.Repeat("aa", 20), strings.Repeat("bb", 20), strings.Repeat("cc", 20), strings.Repeat("dd", 20)
	hash1, hash2, hash3        = strings.Repeat("11", 32), strings.Repeat("22", 32), strings.Repeat("33", 32)
	commit1, commit2           = strings.Repeat("c1", 20), strings.Repeat("c2", 20)
)

// specLines are the fixture's specs.jsonl rows; bad ones are marked.
func specLines() []string {
	j := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	type m = map[string]any
	seen := func(owner, repo, path, ref, refType string, commit any, mirror bool) m {
		return m{"owner": owner, "repo": repo, "path": path, "ref": ref, "refType": refType, "commit": commit, "mirror": mirror}
	}
	return []string{
		j(m{"blob": blobA, "kind": "arc56", "name": "Alpha", "selector": "02bece11",
			"programs": []m{{"prog": "approval", "hash": hash1}, {"prog": "clear", "hash": hash2}},
			// A selector of the file's is ignored: add's is 8aa3b61f, so hello is not listed there.
			"methods": []m{{"sig": "add(uint64,uint64)uint128", "args": []string{"a", "b"}, "readonly": true, "selector": "00000000"},
				{"sig": "hello(string)string", "args": []string{"name"}, "selector": "8aa3b61f"}},
			"events": []m{{"sig": "Registered(byte[4],string)"}},
			"seen": []m{seen("scholtz", "ARC56Registry", "clients/Alpha.arc56.json", "main", "branch", nil, true),
				seen("alice", "alpha", "contracts/Alpha.arc56.json", "main", "branch", commit1, false)}}),
		"",
		j(m{"blob": blobB, "kind": "arc56", "name": "AlphaCopy", "programs": []m{{"prog": "approval", "hash": hash1}},
			"methods": []m{{"sig": "add(uint64,uint64)uint128", "args": []string{"x", "y"}}},
			"seen":    []m{seen("nullun", "ARC56Registry", "clients/Alpha.arc56.json", "main", "branch", commit2, true)}}),
		// An ARC-32 spec has no byteCode: its programs are dropped.
		j(m{"blob": strings.ToUpper(blobC), "kind": "arc32", "programs": []m{{"prog": "approval", "hash": hash3}},
			"methods": []m{{"sig": "add(uint64,uint64)uint128", "args": []string{"a", "b"}}}}),
		// Kept, with its bad entries dropped, and its programs: an ARC-4 spec has no byteCode either.
		j(m{"blob": blobD, "kind": "arc4", "name": "Bad\u202eName",
			"programs": []m{{"prog": "approval", "hash": "12"}, {"prog": "logic", "hash": hash3}, {"prog": "clear", "hash": hash3}, {"prog": "clear", "hash": hash1}},
			"methods": []m{{"sig": "bad sig(uint64)void"}, {"sig": "ok(uint64)void", "args": []string{"good", "1bad"}}, {"sig": "ok(uint64)void"},
				{"sig": "x(uint64"}, {"sig": "many()void", "args": make([]string, maxSpecArgs+1)}},
			"events": []m{{"sig": "E(uint64)"}, {"sig": "E(uint64)"}},
			"seen": []m{seen("bad owner", "r", "a.json", "main", "branch", nil, false), seen("o", "..", "a.json", "main", "branch", nil, false),
				seen("o", "r", "../etc/passwd", "main", "branch", nil, false), seen("o", "r", "/abs.json", "main", "branch", nil, false),
				seen("o", "r", "a\\b.json", "main", "branch", nil, false), seen("o", "r", "a/\u202e/b.json", "main", "branch", nil, false),
				seen("o", "r", "a.json", "a..b", "branch", nil, false), seen("o", "r", "a.json", "x:y", "branch", nil, false),
				seen("o", "r", "a.json", "main", "pr", nil, false), seen("o", "r", "a.json", "main", "branch", "123", false),
				seen("dave", "d.repo", "src/D.arc56.json", "v1.2.0", "tag", strings.ToUpper(commit2), false),
				seen("dave", "d.repo", "src/D.arc56.json", "v1.2.0", "tag", commit2, false),    // the same again
				seen("Dave", "D.Repo", "src/D.arc56.json", "v1.2.0", "tag", commit2, false)}}), // and again: GitHub's names are case-insensitive
		`not json`,                     // bad
		`{"blob": 5, "kind": "arc56"}`, // bad: a number
		j(m{"blob": "zz" + blobA[2:], "kind": "arc56"}),                                                   // bad blob
		j(m{"blob": blobA[2:], "kind": "arc56"}),                                                          // bad blob: short
		j(m{"blob": strings.Repeat("ee", 20), "kind": "arc99"}),                                           // bad kind
		j(m{"blob": blobA, "kind": "arc56", "name": "Evil"}),                                              // a blob again
		j(m{"blob": strings.Repeat("ff", 20), "kind": "arc56", "name": strings.Repeat("x", maxSpecLine)}), // too long
	}
}

func treeLines() []string {
	return []string{
		`{"repo":"alice/alpha","commit":"` + commit1 + `","files":["contracts/b.py","contracts/a.ts","../x.ts","contracts/a.ts","a\\b.ts"]}`,
		`{"repo":"fork/alpha","commit":"` + commit1 + `","files":["other.ts"]}`, // a commit again
		`{"repo":"..","commit":"` + commit2 + `","files":["x.ts"]}`,             // bad repo
		`{"repo":"dave/d.repo","commit":"bad","files":["x.ts"]}`,                // bad commit
		`{"repo":"dave/d.repo","commit":"` + strings.ToUpper(commit2) + `","files":["src/d.py"]}`,
		`[]`,
	}
}

// writeSpecs writes an export dir and returns it.
func writeSpecs(t *testing.T, dir string, specs, trees []string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "specs.jsonl"), []byte(strings.Join(specs, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if trees != nil {
		if err := os.WriteFile(filepath.Join(dir, "trees.jsonl"), []byte(strings.Join(trees, "\n")), 0o600); err != nil { // no final newline
			t.Fatal(err)
		}
	}
	return dir
}

// specsDB opens an in-memory DB with the specs feature on.
func specsDB(t *testing.T) *sql.DB {
	t.Helper()
	old := specsDir
	specsDir = t.TempDir()
	t.Cleanup(func() { specsDir = old })
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLoadSpecs(t *testing.T) {
	db := specsDB(t)
	if s := specsLoaded.Load(); *s != (specStatus{}) {
		t.Errorf("before a load: %+v", s)
	}
	c, read, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, specLines(), treeLines()))
	if err != nil {
		t.Fatal(err)
	}
	if f, err := statSpecFiles(specsDir); err != nil || read != f || read.treesSize == 0 {
		t.Errorf("read %+v, want %+v (%v)", read, f, err)
	}
	want := specCounts{Specs: 4, Programs: 3, Methods: 6, Events: 2, Seen: 4, Commits: 2, Files: 3,
		BadRows: 5, LongRows: 1, DupRows: 1, BadTrees: 3, DupTrees: 1,
		BadPrograms: 5, BadMethods: 3, BadEvents: 1, BadSeen: 12, BadArgs: 1 + maxSpecArgs + 1, BadNames: 1, BadFiles: 3}
	if c != want {
		t.Errorf("counts\n%+v\nwant\n%+v", c, want)
	}
	for q, want := range map[string][]string{
		"SELECT concat_ws(' ', blob, kind, coalesce(name, 'NULL'), methods, events, owners) FROM specs ORDER BY blob": {
			blobA + " arc56 Alpha 2 1 1", blobB + " arc56 AlphaCopy 1 0 0", blobC + " arc32 NULL 1 0 0", blobD + " arc4 NULL 2 1 1"},
		"SELECT concat_ws(' ', blob, prog, lower(hex(hash))) FROM spec_programs ORDER BY blob, prog": {
			blobA + " approval " + hash1, blobA + " clear " + hash2, blobB + " approval " + hash1},
		"SELECT concat_ws(' ', blob, sig, args::VARCHAR, readonly) FROM spec_methods ORDER BY blob, sig": {
			blobA + " add(uint64,uint64)uint128 [a, b] true", blobA + " hello(string)string [name] false",
			blobB + " add(uint64,uint64)uint128 [x, y] false", blobC + " add(uint64,uint64)uint128 [a, b] false",
			blobD + " many()void [] false", blobD + " ok(uint64)void [good, ''] false"},
		"SELECT concat_ws(' ', blob, owner, repo, ref, ref_type, coalesce(commit, 'NULL'), mirror) FROM spec_seen ORDER BY blob, owner": {
			blobA + " alice alpha main branch " + commit1 + " false", blobA + " scholtz ARC56Registry main branch NULL true",
			blobB + " nullun ARC56Registry main branch " + commit2 + " true", blobD + " dave d.repo v1.2.0 tag " + commit2 + " false"},
		"SELECT concat_ws(' ', commit, path) FROM spec_files ORDER BY commit, path": {
			commit1 + " contracts/a.ts", commit1 + " contracts/b.py", commit2 + " src/d.py"},
	} {
		if got := strs(t, db, q); !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n got %q\nwant %q", q, got, want)
		}
	}
	// Every selector is the signature's, whatever the file said.
	for _, q := range []string{"SELECT sig, selector FROM spec_methods", "SELECT sig, selector FROM spec_events"} {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var sig string
			var sel uint32
			rows.Scan(&sig, &sel)
			if sel != specSelector(sig) {
				t.Errorf("%s: selector %08x, want %08x", sig, sel, specSelector(sig))
			}
		}
		rows.Close()
	}
	checkSpecCounts(t, db)
	if s := specsLoaded.Load(); s.Count != 4 || time.Since(time.Unix(s.LoadedAt, 0)) > time.Minute {
		t.Errorf("status %+v", s)
	}

	// No trees.jsonl: specs load, with no files.
	dir := t.TempDir()
	if c, _, err := loadSpecs(context.Background(), db, writeSpecs(t, dir, specLines()[:1], nil)); err != nil || c.Specs != 1 || c.Files != 0 {
		t.Errorf("without trees.jsonl: %+v, %v", c, err)
	}
	// No specs.jsonl: an error, and the last load stays.
	if _, _, err := loadSpecs(context.Background(), db, t.TempDir()); err == nil {
		t.Error("without specs.jsonl: want an error")
	}
	if got := strs(t, db, "SELECT blob FROM specs"); !reflect.DeepEqual(got, []string{blobA}) {
		t.Errorf("after a failed load: %q", got)
	}
}

// A reload replaces every table in one transaction: a reader sees the old load or the new, whole.
func TestSpecsReloadAtomic(t *testing.T) {
	db := specsDB(t)
	big, small := specLines(), specLines()[:1]
	ctx := context.Background()
	load := func(lines []string) {
		if _, _, err := loadSpecs(ctx, db, writeSpecs(t, specsDir, lines, treeLines())); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := "SELECT concat_ws(' ', (SELECT count(*) FROM specs), (SELECT count(*) FROM spec_methods), (SELECT count(*) FROM spec_seen), (SELECT count(*) FROM spec_meta))"
	load(big)
	wantBig := strs(t, db, snapshot)[0]
	load(small)
	wantSmall := strs(t, db, snapshot)[0]
	if wantBig != "4 6 4 1" || wantSmall != "1 2 2 1" {
		t.Fatalf("snapshots %q, %q", wantBig, wantSmall)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var reads atomic.Int64
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				var s string
				if err := db.QueryRow(snapshot).Scan(&s); err != nil {
					t.Error(err)
					return
				}
				if s != wantBig && s != wantSmall {
					t.Errorf("read a half load: %q", s)
				}
				reads.Add(1)
			}
		})
	}
	for i := range 10 {
		load([][]string{big, small}[i%2])
	}
	stop.Store(true)
	wg.Wait()
	if reads.Load() == 0 {
		t.Error("no reads")
	}

	// A cancelled load leaves the last one (small: the loop ends on it).
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := loadSpecs(cctx, db, writeSpecs(t, specsDir, big, nil)); err == nil {
		t.Error("cancelled load: want an error")
	}
	if got := strs(t, db, snapshot)[0]; got != wantSmall {
		t.Errorf("after a cancelled load %q, want %q", got, wantSmall)
	}
}

// jsonEq reports whether a and b are the same JSON value.
func jsonEq(a, b string) bool {
	var x, y any
	return json.Unmarshal([]byte(a), &x) == nil && json.Unmarshal([]byte(b), &y) == nil && reflect.DeepEqual(x, y)
}

func TestSpecRoutes(t *testing.T) {
	db := specsDB(t)
	if _, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, specLines(), treeLines())); err != nil {
		t.Fatal(err)
	}
	h := routes(db)
	get := func(url string) (int, string, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w.Code, strings.TrimSpace(w.Body.String()), w.Header().Get("Cache-Control")
	}
	seenA := `[{"owner":"alice","repo":"alpha","path":"contracts/Alpha.arc56.json","ref":"main","refType":"branch","commit":"` + commit1 + `","mirror":false},
		{"owner":"scholtz","repo":"ARC56Registry","path":"clients/Alpha.arc56.json","ref":"main","refType":"branch","commit":null,"mirror":true}]`
	seenB := `[{"owner":"nullun","repo":"ARC56Registry","path":"clients/Alpha.arc56.json","ref":"main","refType":"branch","commit":"` + commit2 + `","mirror":true}]`
	for _, c := range []struct {
		url  string
		code int
		want string
	}{
		// A has a non-mirror source, B only a mirror.
		{"/v0/program/" + hash1, 200, `[{"blob":"` + blobA + `","kind":"arc56","name":"Alpha","prog":"approval","methods":2,"events":1,"seenTotal":2,"seen":` + seenA + `},
			{"blob":"` + blobB + `","kind":"arc56","name":"AlphaCopy","prog":"approval","methods":1,"events":0,"seenTotal":1,"seen":` + seenB + `}]`},
		{"/v0/program/" + strings.ToUpper(hash2), 200, `[{"blob":"` + blobA + `","kind":"arc56","name":"Alpha","prog":"clear","methods":2,"events":1,"seenTotal":2,"seen":` + seenA + `}]`},
		{"/v0/program/" + hash3, 200, `[]`}, // C's and D's, dropped
		{"/v0/program/" + strings.Repeat("44", 32), 200, `[]`},
		{"/v0/program/" + hash1[1:], 400, ""},
		{"/v0/program/" + hash1[1:] + "g", 400, ""},
		{"/v0/selector/8aa3b61f", 200, `{"methods":[{"sig":"add(uint64,uint64)uint128","args":["a","b"],"owners":1,"specs":3}],"events":[],"truncated":false}`},
		{"/v0/selector/02BECE11", 200, `{"methods":[{"sig":"hello(string)string","args":["name"],"owners":1,"specs":1}],"events":[],"truncated":false}`},
		{"/v0/selector/db2d669a", 200, `{"methods":[],"events":[{"sig":"Registered(byte[4],string)","owners":1,"specs":1}],"truncated":false}`},
		{"/v0/selector/00000000", 200, `{"methods":[],"events":[],"truncated":false}`},
		{"/v0/selector/8aa3b61", 400, ""},
		{"/v0/selector/8aa3b61f0", 400, ""},
		{"/v0/spec/" + blobA, 200, `{"kind":"arc56","name":"Alpha","methods":[{"sig":"add(uint64,uint64)uint128","args":["a","b"],"readonly":true},
			{"sig":"hello(string)string","args":["name"],"readonly":false}],"events":[{"sig":"Registered(byte[4],string)"}],"seen":` + seenA + `}`},
		{"/v0/spec/" + strings.ToUpper(blobC), 200, `{"kind":"arc32","name":null,"methods":[{"sig":"add(uint64,uint64)uint128","args":["a","b"],"readonly":false}],"events":[],"seen":[]}`},
		{"/v0/spec/" + strings.Repeat("ab", 20), 404, ""},
		{"/v0/spec/" + blobA + "a", 400, ""},
		{"/v0/files/" + commit1, 200, `["contracts/a.ts","contracts/b.py"]`},
		{"/v0/files/" + strings.ToUpper(commit2), 200, `["src/d.py"]`},
		{"/v0/files/" + strings.Repeat("ab", 20), 200, `[]`},
		{"/v0/files/xyz", 400, ""},
	} {
		code, body, cache := get(c.url)
		if code != c.code || (c.code == 200 && !jsonEq(body, c.want)) {
			t.Errorf("%s = %d %s\nwant %d %s", c.url, code, body, c.code, c.want)
		}
		if wantCache := map[bool]string{true: "public, max-age=600", false: "no-store"}[code == 200]; cache != wantCache {
			t.Errorf("%s: Cache-Control %q, want %q", c.url, cache, wantCache)
		}
	}

	_, body, _ := get("/v0/status")
	var st struct{ Specs specStatus }
	if json.Unmarshal([]byte(body), &st); st.Specs.Count != 4 || st.Specs.LoadedAt == 0 {
		t.Errorf("status %s", body)
	}

	specsDir = ""
	h = routes(db)
	for _, url := range []string{"/v0/program/" + hash1, "/v0/selector/8aa3b61f", "/v0/spec/" + blobA, "/v0/files/" + commit1} {
		if code, _, _ := get(url); code != 404 {
			t.Errorf("%s with specs off = %d, want 404", url, code)
		}
	}
	if _, body, _ := get("/v0/status"); !strings.Contains(body, `"specs":false`) {
		t.Errorf("status with specs off: %s", body)
	}
}

// The tables come with SPECS_DIR and go without it, whatever the DB's config, and the status of the
// last load survives a restart.
func TestSpecTablesSetup(t *testing.T) {
	old := specsDir
	defer func() { specsDir = old }()
	path := filepath.Join(t.TempDir(), "x.duckdb")
	tables := func() string {
		db, err := openDB(path, "test-v1")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if specsDir != "" {
			if _, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, specLines()[:1], nil)); err != nil {
				t.Fatal(err)
			}
		}
		return strings.Join(strs(t, db, "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'spec%' ORDER BY ALL"), " ")
	}
	specsDir = t.TempDir()
	if got := tables(); got != "spec_events spec_files spec_meta spec_methods spec_programs spec_seen spec_sigs specs" {
		t.Errorf("on: %q", got)
	}
	specsLoaded.Store(nil)
	db, err := openDB(path, "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	if s := specsLoaded.Load(); s == nil || s.Count != 1 || s.LoadedAt == 0 {
		t.Errorf("status after reopening: %+v", s)
	}
	db.Close()
	specsDir = ""
	if got := tables(); got != "" {
		t.Errorf("off: %q", got)
	}

	// Tables of another build's shape (no counts in specs, file_mtime in spec_meta) are recreated,
	// and load.
	db, err = openDB(path, "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE specs(blob VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, name VARCHAR);
		CREATE TABLE spec_meta(loaded_at TIMESTAMP NOT NULL, file_mtime TIMESTAMP NOT NULL, specs UINTEGER NOT NULL);
		INSERT INTO specs VALUES ('` + blobB + `', 'arc56', NULL)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	specsDir = t.TempDir()
	if got := tables(); got != "spec_events spec_files spec_meta spec_methods spec_programs spec_seen spec_sigs specs" {
		t.Errorf("recreated: %q", got)
	}
	if db, err = openDB(path, "test-v1"); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want, _ := specShape()
	if got := strs(t, db, specShapeSQL); got[0] != want || !strings.Contains(want, "specs.owners UINTEGER NOT NULL") {
		t.Errorf("shape\n%s\nwant\n%s", got[0], want)
	}
	if got := strs(t, db, "SELECT blob FROM specs"); !reflect.DeepEqual(got, []string{blobA}) {
		t.Errorf("after recreating: %q", got)
	}
}

// checkSpecCounts checks the counts made on load against the rows they count.
func checkSpecCounts(t *testing.T, db *sql.DB) {
	t.Helper()
	if bad := strs(t, db, `SELECT blob FROM specs s WHERE methods <> (SELECT count(*) FROM spec_methods x WHERE x.blob = s.blob)
		OR events <> (SELECT count(*) FROM spec_events x WHERE x.blob = s.blob)
		OR seen <> (SELECT count(*) FROM spec_seen x WHERE x.blob = s.blob)
		OR owners <> (SELECT count(DISTINCT lower(owner)) FROM spec_seen x WHERE x.blob = s.blob AND NOT mirror)
		OR EXISTS (FROM spec_programs p WHERE p.blob = s.blob AND p.owners <> s.owners)`); len(bad) > 0 {
		t.Errorf("counts not their rows': %q", bad)
	}
	// Recount spec_sigs from the rows, with each spec's first maxSigOwners owners.
	recount := `WITH o AS (SELECT blob, owner FROM (SELECT DISTINCT blob, lower(owner) AS owner FROM spec_seen WHERE NOT mirror)
			QUALIFY row_number() OVER (PARTITION BY blob ORDER BY owner) <= ` + fmt.Sprint(maxSigOwners) + `),
		x AS (SELECT selector, false AS event, sig, blob FROM spec_methods UNION ALL SELECT selector, true, sig, blob FROM spec_events)
		SELECT concat_ws(' ', selector, event, sig, owners, specs) FROM (
			SELECT selector, event, sig, count(DISTINCT o.owner) AS owners, count(DISTINCT blob) AS specs FROM x LEFT JOIN o USING (blob) GROUP BY ALL) ORDER BY ALL`
	if got, want := strs(t, db, "SELECT concat_ws(' ', selector, event, sig, owners, specs) FROM spec_sigs ORDER BY ALL"), strs(t, db, recount); !reflect.DeepEqual(got, want) {
		t.Errorf("spec_sigs\n%q\nwant\n%q", got, want)
	}
}

// specJSON is a specs.jsonl row of blob n (40 hex digits) with these fields.
func specJSON(n int, fields map[string]any) string {
	v := map[string]any{"blob": fmt.Sprintf("%040x", n), "kind": "arc56"}
	maps.Copy(v, fields)
	b, _ := json.Marshal(v)
	return string(b)
}

func seenBy(owner, repo string, mirror bool) map[string]any {
	return map[string]any{"owner": owner, "repo": repo, "path": "a.arc56.json", "ref": "main", "refType": "branch", "commit": commit1, "mirror": mirror}
}

// getSpec serves url and returns its code, body and X-Truncated.
func getSpec(t *testing.T, db *sql.DB, url string) (int, string, string) {
	t.Helper()
	w := httptest.NewRecorder()
	routes(db).ServeHTTP(w, httptest.NewRequest("GET", url, nil))
	return w.Code, strings.TrimSpace(w.Body.String()), w.Header().Get("X-Truncated")
}

// Signatures rank by distinct non-mirror owners, case-insensitive, then specs: one owner can make
// any number of specs. Each list says when it was cut.
func TestSpecSelectorRanking(t *testing.T) {
	db := specsDB(t)
	method := func(sig string, args ...string) map[string]any {
		return map[string]any{"methods": []map[string]any{{"sig": sig, "args": args}}}
	}
	with := func(f map[string]any, seen ...map[string]any) map[string]any { f["seen"] = seen; return f }
	var lines []string
	for i := range 6 { // one owner, 6 specs
		lines = append(lines, specJSON(i, with(method("one()void"), seenBy("solo", fmt.Sprint("r", i), false))))
	}
	// Two owners, 5 specs; the args of the most owners win over those of the most specs.
	lines = append(lines, specJSON(10, with(method("two(uint64)void", "x"), seenBy("Alice", "a", false))),
		specJSON(11, with(method("two(uint64)void", "x"), seenBy("bob", "b", false))),
		specJSON(12, with(method("two(uint64)void", "y"), seenBy("reg", "ARC56Registry", true))),
		specJSON(13, with(method("two(uint64)void", "y"), seenBy("reg", "ARC56Registry", true))),
		specJSON(14, method("two(uint64)void", "y")))
	// Alice and alice are one owner; a mirror's isn't counted.
	lines = append(lines, specJSON(20, with(method("three()void"), seenBy("Alice", "x", false), seenBy("carol", "ARC56Registry", true))),
		specJSON(21, with(method("three()void"), seenBy("alice", "y", false))))
	// m0()void to m20()void and E0() to E20(), for the cap.
	var ms, es []map[string]any
	for i := range 21 {
		ms, es = append(ms, map[string]any{"sig": fmt.Sprintf("m%d()void", i)}), append(es, map[string]any{"sig": fmt.Sprintf("E%d()", i)})
	}
	lines = append(lines, specJSON(30, map[string]any{"methods": ms, "events": es}))
	if _, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, lines, nil)); err != nil {
		t.Fatal(err)
	}
	checkSpecCounts(t, db)
	exec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	// A selector collision, without grinding 2³² hashes: spec_sigs is what the route reads.
	exec(`UPDATE spec_sigs SET selector = 1 WHERE sig IN ('one()void', 'two(uint64)void', 'three()void')`)
	exec(`UPDATE spec_sigs SET selector = 1 WHERE sig = 'E0()'`)
	want := `{"methods":[{"sig":"two(uint64)void","args":["x"],"owners":2,"specs":5},{"sig":"one()void","args":[],"owners":1,"specs":6},
		{"sig":"three()void","args":[],"owners":1,"specs":2}],"events":[{"sig":"E0()","owners":0,"specs":1}],"truncated":false}`
	if code, body, _ := getSpec(t, db, "/v0/selector/00000001"); code != 200 || !jsonEq(body, want) {
		t.Errorf("collision = %d %s\nwant %s", code, body, want)
	}

	// Lists of 21 are cut to 20, and say so; of 20 they aren't.
	for _, c := range []struct {
		update          string
		methods, events int
		truncated       bool
	}{
		{`UPDATE spec_sigs SET selector = 2 WHERE sig LIKE 'm%'`, 21, 0, true},
		{`UPDATE spec_sigs SET selector = 3 WHERE sig = 'm20()void'`, 20, 0, false},
		{`UPDATE spec_sigs SET selector = 2 WHERE sig LIKE 'E%' AND sig <> 'E20()'`, 20, 20, false},
		{`UPDATE spec_sigs SET selector = 2 WHERE sig = 'E20()'`, 20, 21, true},
	} {
		exec(c.update)
		_, body, _ := getSpec(t, db, "/v0/selector/00000002")
		var v struct {
			Methods, Events []any
			Truncated       bool
		}
		if err := json.Unmarshal([]byte(body), &v); err != nil {
			t.Fatal(err)
		}
		if len(v.Methods) != min(c.methods, 20) || len(v.Events) != min(c.events, 20) || v.Truncated != c.truncated {
			t.Errorf("%d methods, %d events: got %d, %d, truncated %v", c.methods, c.events, len(v.Methods), len(v.Events), v.Truncated)
		}
	}
}

// /v0/program ranks by distinct non-mirror owners, case-insensitive, and lists 20, with X-Truncated
// when there were more.
func TestSpecProgramRanking(t *testing.T) {
	db := specsDB(t)
	hash4, hash5 := strings.Repeat("44", 32), strings.Repeat("55", 32)
	progs := func(hashes ...string) []map[string]any {
		var p []map[string]any
		for i, h := range hashes {
			p = append(p, map[string]any{"prog": []string{"approval", "clear"}[i], "hash": h})
		}
		return p
	}
	lines := []string{
		// Alice and alice are one owner, so 2 ranks first; and one source, as Repo and repo are one repo.
		specJSON(1, map[string]any{"programs": progs(hash1), "seen": []map[string]any{seenBy("Alice", "Repo", false), seenBy("alice", "repo", false),
			seenBy("ALICE", "other", false)}}),
		specJSON(2, map[string]any{"programs": progs(hash1), "seen": []map[string]any{seenBy("bob", "b", false), seenBy("carol", "c", false)}}),
	}
	// 12 sources: 10 listed, non-mirror and with a commit first.
	var many []map[string]any
	for i := range 12 {
		s := seenBy(fmt.Sprintf("o%02d", i), "r", i < 2)
		if i == 11 {
			s["commit"] = nil
		}
		many = append(many, s)
	}
	lines = append(lines, specJSON(3, map[string]any{"programs": progs(hash2), "seen": many}))
	for i := range 21 { // 21 with hash4, 20 of them with hash5
		h := []string{hash4, hash5}
		if i == 20 {
			h = h[:1]
		}
		lines = append(lines, specJSON(100+i, map[string]any{"programs": progs(h...)}))
	}
	c, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, lines, nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Seen != 16 || c.BadSeen != 1 {
		t.Errorf("seen %d, bad %d; want 16, 1", c.Seen, c.BadSeen)
	}
	checkSpecCounts(t, db)

	_, body, _ := getSpec(t, db, "/v0/program/"+hash1)
	var got []struct {
		Blob string
		Seen []any
	}
	json.Unmarshal([]byte(body), &got)
	if len(got) != 2 || got[0].Blob != fmt.Sprintf("%040x", 2) || len(got[1].Seen) != 2 {
		t.Errorf("by owners: %s", body)
	}
	_, body, _ = getSpec(t, db, "/v0/program/"+hash2)
	var cut []struct {
		SeenTotal int
		Seen      []struct{ Owner string }
	}
	json.Unmarshal([]byte(body), &cut)
	var owners []string
	for _, s := range cut[0].Seen {
		owners = append(owners, s.Owner)
	}
	if want := []string{"o02", "o03", "o04", "o05", "o06", "o07", "o08", "o09", "o10", "o11"}; len(cut) != 1 || cut[0].SeenTotal != 12 || !reflect.DeepEqual(owners, want) {
		t.Errorf("seen cut: %s", body)
	}
	if code, body, _ := getSpec(t, db, "/v0/spec/"+fmt.Sprintf("%040x", 3)); code != 200 || strings.Count(body, `"owner"`) != 12 {
		t.Errorf("/v0/spec lists every source: %s", body)
	}
	for _, c := range []struct {
		hash, truncated string
		n               int
	}{{hash4, "20", 20}, {hash5, "", 20}} {
		_, body, truncated := getSpec(t, db, "/v0/program/"+c.hash)
		var got []any
		json.Unmarshal([]byte(body), &got)
		if len(got) != c.n || truncated != c.truncated {
			t.Errorf("%s…: %d specs, X-Truncated %q; want %d, %q", c.hash[:4], len(got), truncated, c.n, c.truncated)
		}
	}
}

// The caps per spec, per commit and per load.
func TestSpecCaps(t *testing.T) {
	db := specsDB(t)
	big := func(n, extra int) string {
		var ms, es, seen []map[string]any
		for i := range maxSpecEntries + extra {
			ms = append(ms, map[string]any{"sig": fmt.Sprintf("m%d()void", i)})
			es = append(es, map[string]any{"sig": fmt.Sprintf("E%d()", i)})
		}
		seen = append(seen, seenBy("reg", "ARC56Registry", true)) // first, but the least useful: dropped
		for i := range maxSpecSeen {
			seen = append(seen, seenBy(fmt.Sprint("o", i), "r", false))
		}
		return specJSON(n, map[string]any{"methods": ms, "events": es, "seen": seen})
	}
	files := make([]string, maxTreeFiles+1)
	for i := range files {
		files[i] = fmt.Sprintf("f%d.ts", i)
	}
	tree, _ := json.Marshal(treeRow{Repo: "a/b", Commit: commit1, Files: files})
	c, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, []string{big(1, 1)}, []string{string(tree)}))
	if err != nil {
		t.Fatal(err)
	}
	want := specCounts{Specs: 1, Methods: maxSpecEntries, Events: maxSpecEntries, Seen: maxSpecSeen, Commits: 1, Files: maxTreeFiles,
		BadMethods: 1, BadEvents: 1, BadSeen: 1, BadFiles: 1}
	if c != want {
		t.Errorf("per spec\n%+v\nwant\n%+v", c, want)
	}
	if n := strs(t, db, "SELECT count(*)::VARCHAR FROM spec_seen WHERE mirror"); n[0] != "0" {
		t.Errorf("kept %s mirrors over non-mirror sources", n[0])
	}
	checkSpecCounts(t, db)

	defer func(m, e, s int64, r, tr int) {
		maxSpecMethods, maxSpecEvents, maxSpecSeenRows, maxSpecs, maxTreeRows = m, e, s, r, tr
	}(maxSpecMethods, maxSpecEvents, maxSpecSeenRows, maxSpecs, maxTreeRows)
	maxSpecMethods, maxSpecEvents, maxSpecSeenRows = 600, 700, 300
	if c, _, err = loadSpecs(context.Background(), db, writeSpecs(t, specsDir, []string{big(1, 0), big(2, 0)}, nil)); err != nil {
		t.Fatal(err)
	}
	want = specCounts{Specs: 2, Methods: 600, Events: 700, Seen: 300, Commits: 1, Files: maxTreeFiles, // trees.jsonl as it was
		OverMethods: 2*maxSpecEntries - 600, OverEvents: 2*maxSpecEntries - 700, OverSeen: 2*maxSpecSeen - 300, BadSeen: 2, BadFiles: 1}
	if c != want {
		t.Errorf("per load\n%+v\nwant\n%+v", c, want)
	}
	checkSpecCounts(t, db)
}

// Lines may end in CRLF, and the last may have no newline: kept if it fits, else counted long.
func TestSpecLines(t *testing.T) {
	db := specsDB(t)
	pad := func(n int) string { return specJSON(n, map[string]any{"pad": strings.Repeat("x", 1<<20)}) }
	huge := specJSON(9, map[string]any{"pad": strings.Repeat("x", maxSpecLine)})
	write := func(name, s string) {
		if err := os.WriteFile(filepath.Join(specsDir, name), []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tree := func(commit string) string {
		return `{"repo":"a/b","commit":"` + commit + `","files":["x.ts"]}`
	}
	for _, c := range []struct {
		specs, trees string
		want         specCounts
	}{
		{specJSON(1, nil) + "\r\n" + specJSON(2, nil) + "\r\n", tree(commit1) + "\r\n" + tree(commit2) + "\r\n",
			specCounts{Specs: 2, Commits: 2, Files: 2}},
		{specJSON(1, nil) + "\n" + pad(2), tree(commit1) + "\n" + tree(commit2), specCounts{Specs: 2, Commits: 2, Files: 2}},
		{specJSON(1, nil) + "\n" + huge, tree(commit1) + "\n" + `{"repo":"a/b","commit":"` + commit2 + `","files":["` + strings.Repeat("x", maxSpecLine) + `"]}`,
			specCounts{Specs: 1, Commits: 1, Files: 1, LongRows: 1, LongTrees: 1}},
	} {
		write("specs.jsonl", c.specs)
		write("trees.jsonl", c.trees)
		if got, _, err := loadSpecs(context.Background(), db, specsDir); err != nil || got != c.want {
			t.Errorf("got %+v, %v\nwant %+v", got, err, c.want)
		}
	}
	if got := strs(t, db, "SELECT path FROM spec_files"); !reflect.DeepEqual(got, []string{"x.ts"}) {
		t.Errorf("files %q", got)
	}
}

func TestWatchSpecs(t *testing.T) {
	db := specsDB(t)
	defer func(e, s time.Duration) { specCheckEvery, specSettleAfter = e, s }(specCheckEvery, specSettleAfter)
	specCheckEvery, specSettleAfter = 10*time.Millisecond, time.Hour
	writeSpecs(t, specsDir, specLines()[:1], nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { watchSpecs(ctx, db, specsDir); close(done) }()
	count := func(table string) string { return strs(t, db, "SELECT count(*)::VARCHAR FROM "+table)[0] }
	time.Sleep(100 * time.Millisecond)
	if n := count("specs"); n != "0" {
		t.Errorf("loaded a file still settling: %s specs", n)
	}
	cancel()
	<-done

	// Stamped an hour ahead, it loads all the same: whether it settled is told by two checks, not
	// by the clock that stamped it.
	specSettleAfter = 200 * time.Millisecond
	path := filepath.Join(specsDir, "specs.jsonl")
	if future := time.Now().Add(time.Hour); os.Chtimes(path, future, future) != nil {
		t.Fatal("chtimes")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go watchSpecs(ctx, db, specsDir)
	wait := func(table, want string) {
		t.Helper()
		for range 300 {
			if count(table) == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s never %s", table, want)
	}
	wait("specs", "1")

	// trees.jsonl changing alone reloads.
	if err := os.WriteFile(filepath.Join(specsDir, "trees.jsonl"), []byte(strings.Join(treeLines(), "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	wait("spec_files", "3")

	// A file changing more often than specSettleAfter isn't loaded until it stops.
	body := strings.Join(specLines()[:4], "\n") // A, a blank line, B, C
	for i, end := 0, time.Now().Add(3*specSettleAfter); time.Now().Before(end); i++ {
		if err := os.WriteFile(path, []byte(body+strings.Repeat("\n", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		if n := count("specs"); n != "1" {
			t.Fatalf("loaded a file still changing: %s specs", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wait("specs", "3")
}

// check fails a SPECS_DIR without specs.jsonl or that isn't absolute, and passes one with it.
func TestCheckSpecsDir(t *testing.T) {
	dir := t.TempDir()
	writeSpecs(t, dir, specLines()[:1], nil)
	for _, c := range []struct {
		dir  string
		want int
	}{{dir, 0}, {t.TempDir(), exitConfig}, {filepath.Join(dir, "nope"), exitConfig}, {"export", exitConfig}, {"", 0}} {
		clearEnv(t)
		t.Setenv("SPECS_DIR", c.dir)
		if got := check([]string{"-offline", "test-v1"}); got != c.want {
			t.Errorf("SPECS_DIR=%q: check = %d, want %d", c.dir, got, c.want)
		}
	}
	for dir, want := range map[string]bool{"/home": true, "/home/u/export": true, "/root/x": true, "/run/user/1000/x": true, "/home/../srv/x": false,
		"/homes/x": false, "/srv/sigreg/export": false, "/var/lib/sigreg": false} {
		if unitHides(dir) != want {
			t.Errorf("unitHides(%q) = %v", dir, !want)
		}
	}
}

// Each kind of repeat is counted where it is dropped.
func TestSpecRepeats(t *testing.T) {
	db := specsDB(t)
	prog := func(p, h string) map[string]any { return map[string]any{"prog": p, "hash": h} }
	lines := []string{
		specJSON(1, map[string]any{"programs": []map[string]any{prog("approval", hash1), prog("approval", hash2), prog("clear", hash2)},
			"methods": []map[string]any{{"sig": "f()void"}, {"sig": "f()void"}}, "events": []map[string]any{{"sig": "E()"}, {"sig": "E()"}},
			"seen": []map[string]any{seenBy("a", "r", false), seenBy("A", "R", false), seenBy("a", "r", true)}}), // the same source, mirror or not
		specJSON(1, nil),
		strings.ToUpper(specJSON(1, nil)), // invalid kind ("ARC56") before it's a repeat
	}
	trees := []string{`{"repo":"a/b","commit":"` + commit1 + `","files":["x.ts","x.ts"]}`, `{"repo":"c/d","commit":"` + strings.ToUpper(commit1) + `","files":["y.ts"]}`}
	c, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, lines, trees))
	if err != nil {
		t.Fatal(err)
	}
	want := specCounts{Specs: 1, Programs: 2, Methods: 1, Events: 1, Seen: 1, Commits: 1, Files: 1,
		BadRows: 1, DupRows: 1, DupTrees: 1, BadPrograms: 1, BadMethods: 1, BadEvents: 1, BadSeen: 2, BadFiles: 1}
	if c != want {
		t.Errorf("got\n%+v\nwant\n%+v", c, want)
	}
}

// Rows of either file past its cap are counted, not loaded.
func TestSpecRowCaps(t *testing.T) {
	db := specsDB(t)
	defer func(r, tr int) { maxSpecs, maxTreeRows = r, tr }(maxSpecs, maxTreeRows)
	maxSpecs, maxTreeRows = 2, 1
	lines := []string{specJSON(1, nil), "not json", specJSON(1, nil), specJSON(2, nil), specJSON(3, nil), specJSON(4, nil)}
	trees := []string{`{"repo":"a/b","commit":"` + commit1 + `","files":["x.ts"]}`, `{"repo":"a/b","commit":"` + commit2 + `","files":["y.ts"]}`, `[]`}
	c, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, lines, trees))
	if err != nil {
		t.Fatal(err)
	}
	// Bad and repeated rows count against nothing: the cap is of specs kept.
	want := specCounts{Specs: 2, Commits: 1, Files: 1, BadRows: 1, DupRows: 1, OverRows: 2, OverTrees: 2}
	if c != want {
		t.Errorf("got\n%+v\nwant\n%+v", c, want)
	}
}

// A spec's signatures count its first maxSigOwners owners, by name; the spec's own count has all.
func TestSpecSigOwnersCap(t *testing.T) {
	db := specsDB(t)
	var seen []map[string]any
	for i := range maxSigOwners + 2 {
		seen = append(seen, seenBy(fmt.Sprintf("O%02d", maxSigOwners+1-i), "r", false)) // in reverse, upper case
	}
	method := []map[string]any{{"sig": "f()void"}}
	lines := []string{specJSON(1, map[string]any{"methods": method, "events": []map[string]any{{"sig": "E()"}}, "seen": seen}),
		// One of the 2 left out: it counts here.
		specJSON(2, map[string]any{"methods": method, "seen": []map[string]any{seenBy(fmt.Sprintf("o%02d", maxSigOwners+1), "r", false)}})}
	if _, _, err := loadSpecs(context.Background(), db, writeSpecs(t, specsDir, lines, nil)); err != nil {
		t.Fatal(err)
	}
	checkSpecCounts(t, db)
	if got := strs(t, db, "SELECT concat_ws(' ', sig, owners, specs) FROM spec_sigs ORDER BY sig"); !reflect.DeepEqual(got,
		[]string{"E() " + fmt.Sprint(maxSigOwners) + " 1", "f()void " + fmt.Sprint(maxSigOwners+1) + " 2"}) {
		t.Errorf("spec_sigs %q", got)
	}
	if got := strs(t, db, "SELECT owners::VARCHAR FROM specs ORDER BY blob"); !reflect.DeepEqual(got, []string{fmt.Sprint(maxSigOwners + 2), "1"}) {
		t.Errorf("specs.owners %q", got)
	}
}
