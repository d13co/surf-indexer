package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"

	_ "github.com/duckdb/duckdb-go/v2"
)

// No PRIMARY KEY on purpose: rows are appended in rnd order and ts/tc never decrease,
// so row-group zonemaps already prune rnd/ts/tc filters. An ART index would dominate disk and RAM.
// Addresses stay VARCHAR: DuckDB dictionary-compresses them, so integer ids would save <= 3%.
const schema = `CREATE TABLE IF NOT EXISTS blocks(
	rnd  UINTEGER NOT NULL,
	ts   UINTEGER NOT NULL,
	tc   UBIGINT  NOT NULL,
	fees UBIGINT  NOT NULL,
	txns UINTEGER NOT NULL, -- the block's own txns while tc is 0 (before the upgrade that added it), else 0
	prop VARCHAR, -- proposer address; NULL for genesis (no cert) or when not tracked
	pp   UBIGINT  -- proposer payout; NULL when not tracked
);
CREATE TABLE IF NOT EXISTS evictions(rnd UINTEGER NOT NULL, acct VARCHAR NOT NULL); -- header partupdabs
CREATE TABLE IF NOT EXISTS voters(rnd UINTEGER NOT NULL, voter VARCHAR NOT NULL);   -- last track.VoterRounds rounds only
CREATE TABLE IF NOT EXISTS config(proposers BOOLEAN NOT NULL, voter_rounds UINTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta(genesis VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS upgrades(
	rnd UINTEGER PRIMARY KEY, -- proposed in this round; a PK is cheap on a table this small (unlike blocks)
	ts UINTEGER NOT NULL, from_proto VARCHAR NOT NULL, proto VARCHAR NOT NULL,
	vote_before UINTEGER NOT NULL, switch_on UINTEGER NOT NULL,
	approvals UINTEGER, passed BOOLEAN, -- NULL while voting
	switch_ts UINTEGER                  -- NULL until applied
);
CREATE TABLE IF NOT EXISTS upgrade_sync(next_sample UINTEGER NOT NULL);
INSERT INTO upgrade_sync SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM upgrade_sync)`

type block struct {
	Rnd  uint64 `json:"rnd"`
	Ts   uint64 `json:"ts"`
	Tc   uint64 `json:"tc"`
	Fees uint64 `json:"fees"`

	txns           uint64 // the block's own txns while Tc is 0, else 0
	prop           string // "" for genesis or when not tracked
	pp             uint64
	absent, voters []string
	lsigs          []lsigUse
	msigs          []msigUse
}

// config is what a DB tracks besides fees. It is fixed once the DB holds a block.
// Each field also gates its routes; /v0/status reports them under the json names.
type config struct {
	Proposers   bool   `json:"proposers"`   // proposer, payout and absent accounts per block
	VoterRounds uint64 `json:"voterRounds"` // cert voters of the last VoterRounds rounds; 0: none
	Lsigs       bool   `json:"lsigs"`       // lsig programs, delegators, rekeyed senders and templates (lsigs, lsig_templates)
	Msigs       bool   `json:"msigs"`       // multisig subsigners (msig_signers)
}

// fullBlocks is whether every round needs its payset.
func (c config) fullBlocks() bool { return c.Lsigs || c.Msigs }

// lsigTables are kept out of schema: the presence of lsigs is the stored Lsigs flag, which an older
// binary ignores (a config column would break its INSERT INTO config), though its ingest leaves a gap.
// An empty string stands for NULL, as duckdb-go can't bind one inside a list and MERGE's ON wants
// plain equality.
const lsigTables = `CREATE TABLE IF NOT EXISTS lsigs(
	prog      VARCHAR  NOT NULL, -- program (contract account) address
	signer    VARCHAR  NOT NULL, -- delegator: the authorizer whose sig/msig/lmsig/pqsig signed prog; '' for an escrow
	sender    VARCHAR  NOT NULL, -- the sender when rekeyed (to prog or to signer), else ''
	tmpl      VARCHAR  NOT NULL, -- template id: hash of prog's logic with constant values left out
	txns      UBIGINT  NOT NULL, -- top-level txns authorized
	first_rnd UINTEGER NOT NULL,
	last_rnd  UINTEGER NOT NULL  -- look a txn up here
);
CREATE TABLE IF NOT EXISTS lsig_templates(
	tmpl  VARCHAR NOT NULL,
	logic VARCHAR NOT NULL -- base64 program of the first instance seen (duckdb-go binds no BLOB in a list)
);
CREATE TABLE IF NOT EXISTS template_names(
	tmpl VARCHAR PRIMARY KEY, -- set by an admin, not ingest; a PK is cheap on a table this small
	name VARCHAR NOT NULL
)`

// msigTable is kept out of schema for the same reason as lsigTables: its presence is the stored Msigs flag.
// A row points at one txn: the first that the signer signed, or while it never has, the first seen.
const msigTable = `CREATE TABLE IF NOT EXISTS msig_signers(
	signer VARCHAR  NOT NULL, -- subsigner address (its pk)
	msig   VARCHAR  NOT NULL, -- multisig address from the preimage: the txn's authorizer, not always its sender
	signed BOOLEAN  NOT NULL, -- ever signed (a txn, or a delegated lsig: before v41 a sig on the program only)
	rnd    UINTEGER NOT NULL, -- the txn's round
	pos    UINTEGER NOT NULL  -- its index in that block's payset (top-level txns)
)`

var track config // from env at startup; openDB refuses a DB synced with another

var dbOptions url.Values // DuckDB config options for openDB, from env at startup (DUCKDB_*)

// openDB opens or creates the DB, refusing one that holds another network's blocks or was
// synced with another config than track. A fees-only DB from before config gains prop/pp.
func openDB(path, genesisID string) (*sql.DB, error) {
	dsn := path
	if len(dbOptions) > 0 {
		dsn += "?" + dbOptions.Encode()
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	if err = setup(db, path, genesisID); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// setup runs in one transaction with every read on tx: another pooled connection can't see
// its uncommitted DDL. A refusal rolls back, leaving the file as it was.
func setup(db *sql.DB, path, genesisID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var stored string
	var legacy, hasBlocks bool
	var have config // a DB without a config row (new, or from before config) tracked nothing
	if _, err = tx.Exec(schema); err == nil {
		_, err = tx.Exec("INSERT INTO meta SELECT ? WHERE NOT EXISTS (SELECT 1 FROM meta)", genesisID)
	}
	if err == nil {
		err = tx.QueryRow("SELECT genesis FROM meta").Scan(&stored)
	}
	if err == nil && stored != genesisID {
		return configErr{fmt.Errorf("%s holds %s blocks, not %s", path, stored, genesisID)}
	}
	if err == nil {
		err = tx.QueryRow(`SELECT count(*) = 0 FROM duckdb_columns() WHERE table_name = 'blocks' AND column_name = 'prop'`).Scan(&legacy)
	}
	if err == nil {
		err = tx.QueryRow("SELECT proposers, voter_rounds FROM config").Scan(&have.Proposers, &have.VoterRounds)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
	}
	if err == nil {
		err = tx.QueryRow("SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'lsigs'").Scan(&have.Lsigs)
	}
	if err == nil {
		err = tx.QueryRow("SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = 'msig_signers'").Scan(&have.Msigs)
	}
	if err == nil {
		err = tx.QueryRow("SELECT count(*) > 0 FROM blocks").Scan(&hasBlocks)
	}
	if err != nil {
		return err
	}
	if hasBlocks && have != track {
		return configErr{fmt.Errorf("%s was synced with PROPOSERS=%t VOTER_ROUNDS=%d LSIGS=%t MSIGS=%t: run with those, or rebuild into a new file (LEDGER_DB) to change them",
			path, have.Proposers, have.VoterRounds, have.Lsigs, have.Msigs)}
	}
	if legacy {
		if _, err = tx.Exec("ALTER TABLE blocks ADD COLUMN prop VARCHAR"); err == nil {
			_, err = tx.Exec("ALTER TABLE blocks ADD COLUMN pp UBIGINT")
		}
	}
	if err == nil { // a DB from before txns counts none while tc is 0: rebuild it to fill them
		_, err = tx.Exec("ALTER TABLE blocks ADD COLUMN IF NOT EXISTS txns UINTEGER DEFAULT 0")
	}
	if err == nil && track.Lsigs { // no-ops on a DB with blocks, where have == track
		_, err = tx.Exec(lsigTables)
	} else if err == nil {
		_, err = tx.Exec("DROP TABLE IF EXISTS lsigs; DROP TABLE IF EXISTS lsig_templates; DROP TABLE IF EXISTS template_names")
	}
	if err == nil && track.Msigs {
		_, err = tx.Exec(msigTable)
	} else if err == nil {
		_, err = tx.Exec("DROP TABLE IF EXISTS msig_signers")
	}
	if err == nil {
		_, err = tx.Exec("DELETE FROM config")
	}
	if err == nil {
		_, err = tx.Exec("INSERT INTO config VALUES (?, ?)", track.Proposers, track.VoterRounds)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// rounds returns the record count and min/max round. hi is -1 when empty.
func rounds(ctx context.Context, db *sql.DB) (n, lo, hi int64, err error) {
	err = db.QueryRowContext(ctx, "SELECT count(*), coalesce(min(rnd), 0), coalesce(max(rnd), -1) FROM blocks").Scan(&n, &lo, &hi)
	return n, lo, hi, err
}

// insertBatch appends rows, their evictions and voters in one transaction, so a batch lands whole
// or not at all, then drops voters that fell out of the window.
// List params + unnest is ~20x faster than a prepared INSERT per row.
// storedTip is the last stored round, for sizing requests; set by openData and insertBatch.
var storedTip atomic.Uint64

func insertBatch(ctx context.Context, db *sql.DB, rows []block) error {
	var rnd, ts, tc, fees, txns, pp, arnd, vrnd []uint64
	var prop, absent, voters []string
	for _, r := range rows {
		rnd, ts, tc, fees, txns = append(rnd, r.Rnd), append(ts, r.Ts), append(tc, r.Tc), append(fees, r.Fees), append(txns, r.txns)
		prop, pp = append(prop, r.prop), append(pp, r.pp)
		for _, a := range r.absent {
			arnd, absent = append(arnd, r.Rnd), append(absent, a)
		}
		for _, v := range r.voters {
			vrnd, voters = append(vrnd, r.Rnd), append(voters, v)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// duckdb-go can't bind a NULL inside a list ([]*uint64{nil} is rejected), so "" stands for one.
	if track.Proposers {
		_, err = tx.ExecContext(ctx, `INSERT INTO blocks (rnd, ts, tc, fees, txns, prop, pp)
			SELECT unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), nullif(unnest($6), ''), unnest($7)`, rnd, ts, tc, fees, txns, prop, pp)
	} else {
		_, err = tx.ExecContext(ctx, "INSERT INTO blocks (rnd, ts, tc, fees, txns) SELECT unnest(?), unnest(?), unnest(?), unnest(?), unnest(?)", rnd, ts, tc, fees, txns)
	}
	if err == nil && len(arnd) > 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO evictions SELECT unnest(?), unnest(?)", arnd, absent)
	}
	if err == nil && len(vrnd) > 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO voters SELECT unnest(?), unnest(?)", vrnd, voters)
	}
	if err == nil && track.Lsigs {
		err = mergeLsigs(ctx, tx, rows)
	}
	if err == nil && track.Msigs {
		err = mergeMsigs(ctx, tx, rows)
	}
	// Rows are in rnd order, so whole row groups empty out and are reclaimed at checkpoint.
	if last := rows[len(rows)-1].Rnd; err == nil && track.VoterRounds > 0 && last >= track.VoterRounds {
		_, err = tx.ExecContext(ctx, "DELETE FROM voters WHERE rnd <= ?", last-track.VoterRounds)
	}
	if err != nil {
		return err
	}
	if err = tx.Commit(); err == nil {
		storedTip.Store(rows[len(rows)-1].Rnd)
	}
	return err
}

// mergeLsigs folds rows' lsig uses into lsigs, and stores the first program seen of each new template.
// There's no PK to upsert on (its ART index would outweigh the table), so uses are deduped here and
// MERGE matches them with a hash join.
func mergeLsigs(ctx context.Context, tx *sql.Tx, rows []block) error {
	var prog, signer, sender, tmpl, ttmpl, tlogic []string
	var txns, first, last []uint64
	seen, tseen := map[[3]string]int{}, map[string]bool{}
	for _, r := range rows {
		for _, u := range r.lsigs {
			k := [3]string{u.prog, u.signer, u.sender}
			i, ok := seen[k]
			if !ok {
				i, seen[k] = len(prog), len(prog)
				prog, signer, sender, tmpl = append(prog, u.prog), append(signer, u.signer), append(sender, u.sender), append(tmpl, u.tmpl)
				txns, first, last = append(txns, 0), append(first, r.Rnd), append(last, 0)
			}
			txns[i] += u.n
			last[i] = r.Rnd // rows are in rnd order
			if u.logic != "" && !tseen[u.tmpl] {
				tseen[u.tmpl] = true
				ttmpl, tlogic = append(ttmpl, u.tmpl), append(tlogic, u.logic)
			}
		}
	}
	if len(prog) == 0 { // an empty list binds as SQLNULL[]
		return nil
	}
	_, err := tx.ExecContext(ctx, `MERGE INTO lsigs t
		USING (SELECT unnest($1) AS prog, unnest($2) AS signer, unnest($3) AS sender, unnest($4) AS tmpl,
			unnest($5) AS txns, unnest($6) AS first_rnd, unnest($7) AS last_rnd) s
		ON t.prog = s.prog AND t.signer = s.signer AND t.sender = s.sender
		WHEN MATCHED THEN UPDATE SET txns = t.txns + s.txns, last_rnd = s.last_rnd
		WHEN NOT MATCHED THEN INSERT VALUES (s.prog, s.signer, s.sender, s.tmpl, s.txns, s.first_rnd, s.last_rnd)`,
		prog, signer, sender, tmpl, txns, first, last)
	if err == nil && len(ttmpl) > 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO lsig_templates SELECT * FROM (SELECT unnest($1) AS tmpl, unnest($2) AS logic) s
			WHERE NOT EXISTS (SELECT 1 FROM lsig_templates t WHERE t.tmpl = s.tmpl)`, ttmpl, tlogic)
	}
	return err
}

// mergeMsigs folds rows' multisig uses into msig_signers, deduped here for MERGE as in mergeLsigs.
// A pair keeps its first use until its first signed one, which then sticks. MERGE keeps the better
// pointer either way (signed, then earlier), so it holds even if batches arrive out of order.
func mergeMsigs(ctx context.Context, tx *sql.Tx, rows []block) error {
	var signer, msig []string
	var signed []bool
	var rnd, pos []uint64
	seen := map[[2]string]int{}
	for _, r := range rows { // in rnd order, and uses in payset order
		for _, u := range r.msigs {
			k := [2]string{u.signer, u.msig}
			i, ok := seen[k]
			if !ok {
				i, seen[k] = len(signer), len(signer)
				signer, msig, signed = append(signer, u.signer), append(msig, u.msig), append(signed, u.signed)
				rnd, pos = append(rnd, r.Rnd), append(pos, uint64(u.pos))
			} else if u.signed && !signed[i] {
				signed[i], rnd[i], pos[i] = true, r.Rnd, uint64(u.pos)
			}
		}
	}
	if len(signer) == 0 { // an empty list binds as SQLNULL[]
		return nil
	}
	_, err := tx.ExecContext(ctx, `MERGE INTO msig_signers t
		USING (SELECT unnest($1) AS signer, unnest($2) AS msig, unnest($3) AS signed, unnest($4) AS rnd, unnest($5) AS pos) s
		ON t.signer = s.signer AND t.msig = s.msig
		WHEN MATCHED AND (s.signed AND NOT t.signed OR s.signed = t.signed AND (s.rnd, s.pos) < (t.rnd, t.pos))
			THEN UPDATE SET signed = s.signed, rnd = s.rnd, pos = s.pos
		WHEN NOT MATCHED THEN INSERT VALUES (s.signer, s.msig, s.signed, s.rnd, s.pos)`,
		signer, msig, signed, rnd, pos)
	return err
}

// ts and tc never decrease, so ordering by them equals ordering by rnd; leading with the
// filtered column lets top-N prune on the same zonemap (~3ms at 65M rows, 2x faster than by rnd).
const (
	cols    = "SELECT rnd, ts, tc, fees FROM blocks "
	qAfter  = cols + "WHERE ts >= ?::UINTEGER ORDER BY ts, rnd LIMIT 1"
	qBefore = cols + "WHERE ts <= ?::UINTEGER ORDER BY ts DESC, rnd DESC LIMIT 1"
	qByTc   = cols + "WHERE tc > ?::UBIGINT ORDER BY tc, rnd LIMIT 1"
)

// queryBlock returns nil if no row matches.
func queryBlock(ctx context.Context, db *sql.DB, q string, arg uint64) (*block, error) {
	var b block
	err := db.QueryRowContext(ctx, q, arg).Scan(&b.Rnd, &b.Ts, &b.Tc, &b.Fees)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// blockAt finds the block at or before/after ts. Closest (any other dir) picks the
// nearer of the two, preferring after: on an exact match, the first block with that ts.
func blockAt(ctx context.Context, db *sql.DB, ts uint64, dir string) (*block, error) {
	switch dir {
	case "after":
		return queryBlock(ctx, db, qAfter, ts)
	case "before":
		return queryBlock(ctx, db, qBefore, ts)
	}
	after, err := queryBlock(ctx, db, qAfter, ts)
	if err != nil {
		return nil, err
	}
	before, err := queryBlock(ctx, db, qBefore, ts)
	if err != nil || before == nil {
		return after, err
	}
	if after == nil || ts-before.Ts < after.Ts-ts {
		return before, nil
	}
	return after, nil
}

// blockByTc finds the block containing txn counter n: tc[r-1] <= n < tc[r].
// A match on round 0 means n is below the genesis counter (genesis has no txns).
func blockByTc(ctx context.Context, db *sql.DB, n uint64) (*block, error) {
	b, err := queryBlock(ctx, db, qByTc, n)
	if b != nil && b.Rnd == 0 {
		return nil, err
	}
	return b, err
}

type feeSum struct {
	Fees       uint64 `json:"fees"`
	Blocks     uint64 `json:"blocks"`
	FirstRound uint64 `json:"firstRound"`
	LastRound  uint64 `json:"lastRound"`
	FirstTs    uint64 `json:"firstTs"`
	LastTs     uint64 `json:"lastTs"`
}

// sumFees sums fees over rounds [minRnd, maxRnd] and timestamps [start, end).
func sumFees(ctx context.Context, db *sql.DB, minRnd, maxRnd, start, end uint64) (s feeSum, err error) {
	err = db.QueryRowContext(ctx, `SELECT coalesce(sum(fees), 0)::UBIGINT, count(*),
		coalesce(min(rnd), 0), coalesce(max(rnd), 0), coalesce(min(ts), 0), coalesce(max(ts), 0)
		FROM blocks WHERE rnd BETWEEN ?::UINTEGER AND ?::UINTEGER AND ts >= ?::UINTEGER AND ts < ?::UINTEGER`,
		minRnd, maxRnd, start, end).Scan(&s.Fees, &s.Blocks, &s.FirstRound, &s.LastRound, &s.FirstTs, &s.LastTs)
	return s, err
}

type interval struct {
	iv    string // DuckDB INTERVAL
	grain uint64 // seconds pre-summed per row
}

// intervals maps the series interval param to a DuckDB INTERVAL. Each is whole UTC minutes or
// hours, so blocks are pre-summed per grain with integer math and only those rows go through
// time_bucket (2x faster at 55M rows than bucketing every block). block has no INTERVAL: qBlocks.
var intervals = map[string]interval{"block": {}, "minute": {"1 minute", 60}, "hour": {"1 hour", 3600},
	"day": {"1 day", 3600}, "week": {"1 week", 3600}, "month": {"1 month", 3600}}

type bucket struct {
	Ts     int64   `json:"ts"` // negative for a week bucket holding a genesis at ts 0 (private nets)
	Fees   uint64  `json:"fees"`
	Txns   uint64  `json:"txns"`
	Tps    float64 `json:"tps"`
	Blocks uint64  `json:"blocks"`
	Rnd    *uint64 `json:"rnd,omitempty"` // block interval only
}

// time_bucket on a naive TIMESTAMP is UTC; weeks start on Monday. txns is the tc delta from the
// previous bucket, or for the first from the last block before start (in the same snapshot, so
// ingest can't slip blocks in between), or with no earlier block from its own first tc (genesis
// has no txns). An empty bucket is skipped, which lag bridges. Blocks from before tc existed
// (tc 0, so no delta) add their own txns.
// ponytail: tps divides by the full bucket length, so a bucket cut by start/end/tip reads low.
const qSeries = `WITH h AS (
	SELECT ts // ?::UINTEGER * ?::UINTEGER AS h, sum(fees) AS fees, count(*) AS blocks, min(tc) AS lo, max(tc) AS hi, sum(txns) AS n
	FROM blocks WHERE ts >= ?::UINTEGER AND ts < ?::UINTEGER GROUP BY h
), b AS (
	SELECT time_bucket(?::INTERVAL, make_timestamp(h::BIGINT * 1000000)) AS t,
		sum(fees)::UBIGINT AS fees, sum(blocks)::UBIGINT AS blocks, min(lo) AS lo, max(hi) AS hi, sum(n)::UBIGINT AS n
	FROM h GROUP BY t
), d AS (
	SELECT t, fees, blocks, hi - coalesce(lag(hi) OVER (ORDER BY t),
		(SELECT tc FROM blocks WHERE ts < ?::UINTEGER ORDER BY ts DESC, rnd DESC LIMIT 1), lo) + n AS txns FROM b
)
SELECT epoch(t)::BIGINT, fees, txns, txns / (epoch(t + ?::INTERVAL) - epoch(t)), blocks, NULL FROM d ORDER BY t LIMIT ?`

// bucketLen is each fixed interval's length in seconds and its boundaries' offset from the epoch
// (1970-01-05 was a Monday). Month has none: 10000 months is 800+ years.
var bucketLen = map[string][2]int64{"minute": {60, 0}, "hour": {3600, 0}, "day": {86400, 0}, "week": {7 * 86400, 4 * 86400}}

// seriesWindows caps the bounded queries one fixed-interval series makes. Each skips the gap
// before its first block, so more are needed only on a chain this sparse (a private net). A var for tests.
var seriesWindows = 4

// qBlocks returns the blocks of rounds [$1, $2] with ts in [$3, $4) as one-block buckets. $1 is the
// round before start's first, riding along so lag gives that block its txns and duration; genesis
// has neither, so 0, and a zero duration gives tps 0. A block from before tc existed adds its own
// txns, as in qSeries. ts never decreases with rnd, so ordering by rnd is ordering by ts.
const qBlocks = `WITH d AS (
	SELECT rnd, ts, fees, tc - lag(tc, 1, tc) OVER w + n AS txns, ts - lag(ts, 1, ts) OVER w AS dt
	FROM (SELECT rnd, ts, tc, fees, txns AS n FROM blocks WHERE rnd BETWEEN ?::UINTEGER AND ?::UINTEGER) WINDOW w AS (ORDER BY rnd)
)
SELECT ts::BIGINT, fees, txns, coalesce(txns / nullif(dt, 0), 0), 1::UBIGINT, rnd FROM d
WHERE ts >= ?::UINTEGER AND ts < ?::UINTEGER ORDER BY rnd`

// maxBuckets caps a series: a year of blocks was ~1 GB of JSON. A var for tests.
var maxBuckets = 10000

// series buckets blocks with ts in [start, end) by interval, a key of intervals: the first maxBuckets,
// truncated if there were more. Each query's scan is bounded too, not just its reply: the window
// and grouping would otherwise read every block in [start, end) first (all blocks: 27 s, 3.4 GB).
func series(ctx context.Context, db *sql.DB, interval string, start, end uint64) ([]bucket, bool, error) {
	out := []bucket{}
	if interval == "block" {
		// Every round is a block and ts never decreases with rnd, so the first maxBuckets+1 are the
		// rounds from the first at or after start.
		first, err := blockAt(ctx, db, start, "after")
		if err != nil || first == nil || first.Ts >= end {
			return out, false, err
		}
		if out, err = scanBuckets(ctx, db, out, qBlocks, max(first.Rnd, 1)-1, first.Rnd+uint64(maxBuckets), start, end); err != nil {
			return nil, false, err
		}
		if len(out) > maxBuckets {
			return out[:maxBuckets], true, nil
		}
		return out, false, nil
	}
	iv := intervals[interval]
	l, fixed := bucketLen[interval]
	for range seriesWindows {
		// A fixed interval's window ends maxBuckets+1 buckets after its first block's.
		cut := end
		if fixed {
			first, err := blockAt(ctx, db, start, "after")
			if err != nil || first == nil || first.Ts >= end {
				return out, false, err
			}
			n, off := l[0], l[1]
			b0 := int64(first.Ts) - ((int64(first.Ts)-off)%n+n)%n
			cut = uint64(min(int64(end), b0+int64(maxBuckets+1-len(out))*n))
		}
		var err error
		if out, err = scanBuckets(ctx, db, out, qSeries, iv.grain, iv.grain, start, cut, iv.iv, start, iv.iv, maxBuckets+1-len(out)); err != nil {
			return nil, false, err
		}
		if len(out) > maxBuckets {
			return out[:maxBuckets], true, nil
		}
		if cut >= end {
			return out, false, nil
		}
		start = cut // the next window's first bucket txns are the tc delta from this one's last block
	}
	// Out of windows with blocks left: truncated, though short.
	next, err := blockAt(ctx, db, start, "after")
	return out, err == nil && next != nil && next.Ts < end, err
}

// scanBuckets appends q's rows to out.
func scanBuckets(ctx context.Context, db *sql.DB, out []bucket, q string, args ...any) ([]bucket, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b bucket
		if err := rows.Scan(&b.Ts, &b.Fees, &b.Txns, &b.Tps, &b.Blocks, &b.Rnd); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// upgrade is a consensus upgrade proposal. Nullable columns are pointers.
type upgrade struct {
	Rnd        uint64  `json:"round"`
	Ts         uint64  `json:"ts"`
	From       string  `json:"from"`
	Proto      string  `json:"proto"`
	VoteBefore uint64  `json:"voteBefore"`
	SwitchOn   uint64  `json:"switchOn"`
	Approvals  *uint64 `json:"approvals"`
	Passed     *bool   `json:"-"`
	Status     string  `json:"status"`
	SwitchTs   *uint64 `json:"switchTs"`
}

func (u upgrade) status() string {
	switch {
	case u.Passed == nil:
		return "voting"
	case !*u.Passed:
		return "rejected"
	case u.SwitchTs == nil:
		return "approved"
	}
	return "applied"
}

const qUpgrades = "SELECT rnd, ts, from_proto, proto, vote_before, switch_on, approvals, passed, switch_ts FROM upgrades "

// queryUpgrades returns the upgrades matching where (a WHERE and/or ORDER BY clause).
func queryUpgrades(ctx context.Context, db *sql.DB, where string) ([]upgrade, error) {
	rows, err := db.QueryContext(ctx, qUpgrades+where)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []upgrade{}
	for rows.Next() {
		var u upgrade
		if err := rows.Scan(&u.Rnd, &u.Ts, &u.From, &u.Proto, &u.VoteBefore, &u.SwitchOn, &u.Approvals, &u.Passed, &u.SwitchTs); err != nil {
			return nil, err
		}
		u.Status = u.status()
		out = append(out, u)
	}
	return out, rows.Err()
}
