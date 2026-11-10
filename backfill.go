package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// A backfillable feature can be turned on for a DB that already holds blocks. Its rounds from before
// then are gaps (backfill_gaps), which the runner fills in the background while live ingest goes on.
// Its routes serve once no gap is left. Its state is derived from the DB, never stored as a flag:
//   - off: no table
//   - backfilling: table present, gaps left
//   - on: table present, no gaps
type job struct {
	feature string
	env     string                                                    // the variable that turns it on
	on      func(c config) bool                                       // whether c has it on
	table   string                                                    // its DDL, run when it is turned on; the table is named feature
	indexes string                                                    // its indexes' DDL (IF NOT EXISTS), run on every start with it on
	drop    string                                                    // drops its tables
	from    func(genesisID string) uint64                             // the first round that can hold its rows
	full    bool                                                      // needs the payset (else header only)
	extract func(b blockCert, blk *block)                             // sets the feature's rows for one round on blk
	merge   func(ctx context.Context, tx *sql.Tx, rows []block) error // same result in any round order
}

// backfillTables are shared by every backfillable feature. Older binaries don't read them.
const backfillTables = `CREATE TABLE IF NOT EXISTS feature_sync(feature VARCHAR PRIMARY KEY, next UINTEGER NOT NULL); -- ingest writes the feature from round next on
CREATE TABLE IF NOT EXISTS backfill_gaps(feature VARCHAR NOT NULL, lo UINTEGER NOT NULL, hi UINTEGER NOT NULL)  -- stored rounds whose rows are missing`

var rekeysJob = job{
	feature: "rekeys",
	env:     "REKEYS",
	on:      func(c config) bool { return c.Rekeys },
	table:   rekeysTable,
	indexes: rekeysIndexes,
	drop:    "DROP TABLE IF EXISTS rekeys",
	from:    func(genesisID string) uint64 { return rekeyFrom[genesisID] },
	full:    true,
	extract: func(b blockCert, blk *block) { blk.rekeys = rekeyUses(b.Block) },
	merge:   mergeRekeys,
}

// allJobs is every backfillable feature, tracked or not.
var allJobs = []job{rekeysJob}

// trackedJobsOf yields the backfillable features c has on.
func trackedJobsOf(c config) iter.Seq[job] {
	return func(yield func(job) bool) {
		for _, j := range allJobs {
			if j.on(c) && !yield(j) {
				return
			}
		}
	}
}

// trackedJobs yields the backfillable features track has on. It reads track on each call, so tests
// that set track see the change.
func trackedJobs() iter.Seq[job] { return trackedJobsOf(track) }

// setupJob brings j's tables in line with track, in setup's transaction. hi is the last stored round
// (-1 when empty), lo the first.
func setupJob(tx *sql.Tx, j job, path, genesisID string, lo, hi int64) error {
	var have bool
	if err := tx.QueryRow("SELECT count(*) > 0 FROM duckdb_tables() WHERE table_name = ?", j.feature).Scan(&have); err != nil {
		return err
	}
	if !j.on(track) {
		if have && hi >= 0 { // its data costs a full backfill: a missing env line must not drop it
			return configErr{fmt.Errorf("%s was synced with %s=true: run with it, or drop it with surf-indexer drop-feature %s %s (service stopped)",
				path, j.env, j.feature, strings.TrimSuffix(filepath.Base(path), ".duckdb"))}
		}
		return dropJob(tx, j)
	}
	// addGap records [max(from, j.from, 1), hi] as missing (genesis has no txns), and ingest as
	// writing from the next round it stores: hi+1, or START_ROUND for an empty DB. Not START_ROUND
	// for a DB with blocks: openData refuses a stale one only after this commits.
	addGap := func(from int64) error {
		from = max(from, int64(j.from(genesisID)), 1)
		if from <= hi {
			if _, err := tx.Exec("INSERT INTO backfill_gaps VALUES (?, ?, ?)", j.feature, from, hi); err != nil {
				return err
			}
		}
		next := hi + 1
		if hi < 0 {
			next = int64(startRound)
		}
		_, err := tx.Exec("INSERT OR REPLACE INTO feature_sync VALUES (?, ?)", j.feature, next)
		return err
	}
	// clearJob empties j's table and gaps, so no round is merged twice: for a table and gaps out of step
	// (only a hand-edited DB), the whole range is backfilled again.
	clearJob := func() error {
		_, err := tx.Exec("DELETE FROM backfill_gaps WHERE feature = ?", j.feature)
		if err == nil {
			_, err = tx.Exec("DELETE FROM " + j.feature)
		}
		return err
	}
	if !have {
		if _, err := tx.Exec(j.table); err != nil {
			return err
		}
	}
	// On every start, so a table made before an index was added gets it.
	if err := buildIndexes(tx, j, have); err != nil {
		return err
	}
	if !have { // being turned on; an empty DB gets no gap, as ingest covers every round
		if err := clearJob(); err != nil {
			return err
		}
		return addGap(lo)
	}
	// An older binary ingests without the feature and leaves next behind: those rounds are a gap.
	next := int64(-1) // no row: every stored round
	if err := tx.QueryRow("SELECT next FROM feature_sync WHERE feature = ?", j.feature).Scan(&next); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if next < 0 {
		if err := clearJob(); err != nil {
			return err
		}
		return addGap(lo)
	}
	if hi+1 > next {
		return addGap(next)
	}
	return nil
}

// buildIndexes creates j's missing indexes. On an existing table (have), that takes a while (some
// 15 s per index per 5M rows) before the service serves, so it is logged before and after.
func buildIndexes(tx *sql.Tx, j job, have bool) error {
	if j.indexes == "" {
		return nil
	}
	const count = "SELECT count(*) FROM duckdb_indexes() WHERE table_name = ?"
	var before, after int
	start := time.Now()
	err := tx.QueryRow(count, j.feature).Scan(&before)
	if want := strings.Count(j.indexes, "CREATE INDEX"); err == nil && have && before < want {
		slog.Info("building indexes", "feature", j.feature, "missing", want-before)
	}
	if err == nil {
		_, err = tx.Exec(j.indexes)
	}
	if err == nil {
		err = tx.QueryRow(count, j.feature).Scan(&after)
	}
	if err == nil && have && after > before {
		slog.Info("built indexes", "feature", j.feature, "indexes", after-before, "took", time.Since(start).Round(time.Millisecond).String())
	}
	return err
}

// dropJob drops j's tables and its rows in the shared ones.
func dropJob(tx *sql.Tx, j job) error {
	_, err := tx.Exec(j.drop)
	for _, t := range []string{"feature_sync", "backfill_gaps"} {
		if err == nil {
			_, err = tx.Exec("DELETE FROM "+t+" WHERE feature = ?", j.feature)
		}
	}
	return err
}

// dropFeature is the drop-feature subcommand: it drops a backfillable feature's data from
// data/<dbID>.duckdb, as main opens it, so the DB can be run with the feature off. DuckDB's file lock
// means the service must be stopped; run it as the service user, in its working directory.
func dropFeature(args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: surf-indexer drop-feature <feature> <dbID>")
		return 2
	}
	if filepath.Base(args[1]) != args[1] || args[1] == ".." { // a dbID, not a path out of data/
		fmt.Fprintf(os.Stderr, "bad dbID %q\n", args[1])
		return 2
	}
	path := filepath.Join("data", args[1]+".duckdb")
	var j *job
	for i := range allJobs {
		if allJobs[i].feature == args[0] {
			j = &allJobs[i]
		}
	}
	if j == nil {
		fmt.Fprintf(os.Stderr, "unknown feature %q\n", args[0])
		return 2
	}
	if _, err := os.Stat(path); err != nil { // sql.Open would create it
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer db.Close()
	tx, err := db.Begin()
	if err == nil {
		defer tx.Rollback()
		if _, err = tx.Exec(backfillTables); err == nil {
			err = dropJob(tx, *j)
		}
	}
	if err == nil {
		err = tx.Commit()
	}
	if err == nil {
		err = db.Close() // checkpoints
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("dropped %s from %s\n", j.feature, path)
	return 0
}

// left is each tracked backfillable feature's rounds left to backfill, for /v0/status and to gate its
// routes. Replaced whole on change; openDB sets it, the runner keeps it in step. A process serves one DB.
var left atomic.Pointer[map[string]uint64]

// loadLeft reads every tracked feature's rounds left from the DB.
func loadLeft(ctx context.Context, db *sql.DB) error {
	m := map[string]uint64{}
	for j := range trackedJobs() {
		n, err := gapRounds(ctx, db, j.feature)
		if err != nil {
			return err
		}
		m[j.feature] = n
	}
	left.Store(&m)
	return nil
}

// gapRounds is feature's rounds left to backfill, read through q (a DB or a transaction).
func gapRounds(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, feature string) (n uint64, err error) {
	err = q.QueryRowContext(ctx, "SELECT coalesce(sum(hi - lo + 1), 0)::UBIGINT FROM backfill_gaps WHERE feature = ?", feature).Scan(&n)
	return n, err
}

var leftMu sync.Mutex // serializes setLeft's copy-on-write

func setLeft(feature string, n uint64) {
	leftMu.Lock()
	defer leftMu.Unlock()
	m := map[string]uint64{feature: n}
	if p := left.Load(); p != nil {
		for k, v := range *p {
			if k != feature {
				m[k] = v
			}
		}
	}
	left.Store(&m)
}

// ready reports whether feature is tracked and fully backfilled, so its routes may serve.
func ready(feature string) bool {
	p := left.Load()
	if p == nil {
		return false
	}
	n, ok := (*p)[feature]
	return ok && n == 0
}

// roundsLeft returns the features still backfilling and their rounds left; nil when none.
func roundsLeft() map[string]uint64 {
	var out map[string]uint64
	if p := left.Load(); p != nil {
		for k, n := range *p {
			if n > 0 {
				if out == nil {
					out = map[string]uint64{}
				}
				out[k] = n
			}
		}
	}
	return out
}

// writeMu serializes the write transactions of live ingest, the backfill and the upgrades sync:
// DuckDB rejects one of two transactions that update the same row, and ingest and the backfill can
// both touch one (acct, auth). Fetches run outside it; each holder keeps it for milliseconds.
var writeMu sync.Mutex

// backfillRetry is how long the runner waits after a failed step. A var for tests.
var backfillRetry = time.Minute

// backfiller fetches backfill rounds from its own source, at most concurrency at once. The source
// sets its own rate limit (algodSource's lim).
type backfiller struct {
	src         source
	concurrency int
	chunk       uint64
}

// newRateLimiter returns a limiter of perSec blocks/s with a burst of concurrency; nil (no limit) for 0.
func newRateLimiter(perSec, concurrency int) *rate.Limiter {
	if perSec == 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSec), concurrency)
}

// errBehind is step's error for a gap that starts past the source's tip: a lagging archival node.
type errBehind struct{ tip, lo uint64 }

func (e errBehind) Error() string {
	return fmt.Sprintf("the backfill source's tip %d is below round %d", e.tip, e.lo)
}

// runBackfill fills every tracked feature's gaps, one feature at a time, lowest gap first, then
// returns. A failed step is logged and retried after backfillRetry; a source behind the gap is
// logged once (not as an error) and checked again as often. Live ingest never waits on it.
func runBackfill(ctx context.Context, db *sql.DB, bf backfiller) {
	for j := range trackedJobs() {
		start, last := time.Now(), time.Now()
		var done, since uint64 // rounds cut, in all and since the last progress log
		behind := false
		for ctx.Err() == nil {
			next, n, rest, err := bf.step(ctx, db, j)
			if b := (errBehind{}); errors.As(err, &b) {
				if !behind {
					slog.Info("backfill waiting", "feature", j.feature, "next", b.lo, "sourceTip", b.tip, "checkEvery", backfillRetry.String())
				}
				behind = true
				sleep(ctx, backfillRetry)
				continue
			}
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("backfill", "feature", j.feature, "next", next, "err", err, "retryIn", backfillRetry.String())
					sleep(ctx, backfillRetry)
				}
				continue
			}
			behind = false
			done, since = done+n, since+n
			if rest == 0 {
				if done > 0 {
					slog.Info("backfill done", "feature", j.feature, "rounds", done, "took", time.Since(start).Round(time.Second).String())
				}
				break
			}
			if d := time.Since(last); d >= time.Minute {
				perSec := float64(since) / d.Seconds()
				attrs := []any{"feature", j.feature, "next", next + n, "roundsLeft", rest, "blkPerSec", math.Round(perSec)}
				if perSec > 0 {
					attrs = append(attrs, "eta", time.Duration(float64(rest)/perSec*float64(time.Second)).Round(time.Minute).String())
				}
				slog.Info("backfill", attrs...)
				last, since = time.Now(), 0
			}
		}
	}
}

// drainBackfill fills every tracked feature's gaps up to the source's tip and returns, stopping at
// the first error. A LEDGER_DB import runs it, from the ledger file: the fast path for a large
// backfill. Rounds past the ledger's tip (a live DB ahead of it, or a short ledger file: the two look
// the same) are left, with a warning, for the service's runner to fill from the BACKFILL_ALGOD_* algods.
func drainBackfill(ctx context.Context, db *sql.DB, bf backfiller) error {
	for j := range trackedJobs() {
		start := time.Now()
		var done uint64
		for {
			lo, n, rest, err := bf.step(ctx, db, j)
			if b := (errBehind{}); errors.As(err, &b) {
				gap, err := gapRounds(ctx, db, j.feature)
				if err != nil {
					return fmt.Errorf("backfill %s: %w", j.feature, err)
				}
				setLeft(j.feature, gap) // for importLedger's last log line
				slog.Warn("backfill left to the service", "feature", j.feature, "next", b.lo, "sourceTip", b.tip, "roundsLeft", gap)
				break
			}
			if err != nil {
				return fmt.Errorf("backfill %s: %w", j.feature, err)
			}
			if done += n; rest == 0 {
				if done > 0 {
					slog.Info("backfill done", "feature", j.feature, "rounds", done, "took", time.Since(start).Round(time.Second).String())
				}
				break
			}
			slog.Info("backfill", "feature", j.feature, "next", lo+n, "roundsLeft", rest)
		}
	}
	return nil
}

// step fills up to bf.chunk rounds from the low end of j's lowest gap, capped at the source's tip:
// it fetches every round, then merges them and cuts them from the gap in one transaction, so a chunk
// is written whole or not at all. It returns the chunk's first round, its length, and the rounds
// left after it; errBehind if the gap starts past the source's tip.
func (bf backfiller) step(ctx context.Context, db *sql.DB, j job) (lo, n, rest uint64, err error) {
	var hi uint64
	err = db.QueryRowContext(ctx, "SELECT lo, hi FROM backfill_gaps WHERE feature = ? ORDER BY lo LIMIT 1", j.feature).Scan(&lo, &hi)
	if errors.Is(err, sql.ErrNoRows) {
		setLeft(j.feature, 0)
		return 0, 0, 0, nil
	}
	if err != nil {
		return 0, 0, 0, err
	}
	// Returns at once while the source holds lo; else algod long-polls for it first.
	tip, err := bf.src.tip(ctx, lo)
	if err != nil {
		return lo, 0, 0, err
	}
	if tip < lo {
		return lo, 0, 0, errBehind{tip, lo}
	}
	end := min(hi, lo+bf.chunk-1, tip)

	blocks := make([]block, end-lo+1)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(bf.concurrency)
	for i := range blocks {
		g.Go(func() error {
			r := lo + uint64(i)
			b, err := bf.src.get(gctx, r, !j.full, false)
			if err != nil {
				return err
			}
			blocks[i].Rnd = r
			j.extract(b, &blocks[i])
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return lo, 0, 0, err
	}

	writeMu.Lock()
	defer writeMu.Unlock()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return lo, 0, 0, err
	}
	defer tx.Rollback()
	if err = j.merge(ctx, tx, blocks); err != nil {
		return lo, 0, 0, err
	}
	var res sql.Result
	if end == hi {
		res, err = tx.ExecContext(ctx, "DELETE FROM backfill_gaps WHERE feature = ? AND lo = ?", j.feature, lo)
	} else {
		res, err = tx.ExecContext(ctx, "UPDATE backfill_gaps SET lo = ? WHERE feature = ? AND lo = ?", end+1, j.feature, lo)
	}
	if err == nil { // a cut that missed would merge these rounds again next step
		if cut, _ := res.RowsAffected(); cut != 1 {
			err = fmt.Errorf("gap %d-%d: its cut matched %d rows", lo, hi, cut)
		}
	}
	if err == nil {
		rest, err = gapRounds(ctx, tx, j.feature)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		return lo, 0, 0, err
	}
	setLeft(j.feature, rest)
	return lo, end - lo + 1, rest, nil
}
