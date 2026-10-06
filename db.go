package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

// No PRIMARY KEY on purpose: rows are appended in rnd order and ts/tc never decrease,
// so row-group zonemaps already prune rnd/ts/tc filters. An ART index would dominate disk and RAM.
const schema = `CREATE TABLE IF NOT EXISTS blocks(
	rnd  UINTEGER NOT NULL,
	ts   UINTEGER NOT NULL,
	tc   UBIGINT  NOT NULL,
	fees UBIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(genesis VARCHAR NOT NULL)`

type block struct {
	Rnd  uint64 `json:"rnd"`
	Ts   uint64 `json:"ts"`
	Tc   uint64 `json:"tc"`
	Fees uint64 `json:"fees"`
}

// openDB opens or creates the DB, refusing one that holds another network's blocks.
func openDB(path, genesisID string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	var stored string
	if _, err = db.Exec(schema); err == nil {
		_, err = db.Exec("INSERT INTO meta SELECT ? WHERE NOT EXISTS (SELECT 1 FROM meta)", genesisID)
	}
	if err == nil {
		err = db.QueryRow("SELECT genesis FROM meta").Scan(&stored)
	}
	if err == nil && stored != genesisID {
		err = fmt.Errorf("%s holds %s blocks, not %s", path, stored, genesisID)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// rounds returns the record count and min/max round. hi is -1 when empty.
func rounds(ctx context.Context, db *sql.DB) (n, lo, hi int64, err error) {
	err = db.QueryRowContext(ctx, "SELECT count(*), coalesce(min(rnd), 0), coalesce(max(rnd), -1) FROM blocks").Scan(&n, &lo, &hi)
	return n, lo, hi, err
}

// insertBatch appends rows in one statement, so a batch lands whole or not at all.
// List params + unnest is ~20x faster than a prepared INSERT per row.
func insertBatch(ctx context.Context, db *sql.DB, rows []block) error {
	var rnd, ts, tc, fees []uint64
	for _, r := range rows {
		rnd, ts, tc, fees = append(rnd, r.Rnd), append(ts, r.Ts), append(tc, r.Tc), append(fees, r.Fees)
	}
	_, err := db.ExecContext(ctx, "INSERT INTO blocks SELECT unnest(?), unnest(?), unnest(?), unnest(?)", rnd, ts, tc, fees)
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

// intervals maps the series interval param to a DuckDB INTERVAL. Each is whole UTC hours, so
// blocks are pre-summed per hour with integer math and only those rows go through time_bucket
// (2x faster at 55M rows than bucketing every block).
var intervals = map[string]string{"hour": "1 hour", "day": "1 day", "week": "1 week", "month": "1 month"}

type bucket struct {
	Ts     uint64  `json:"ts"`
	Fees   uint64  `json:"fees"`
	Txns   uint64  `json:"txns"`
	Tps    float64 `json:"tps"`
	Blocks uint64  `json:"blocks"`
}

// time_bucket on a naive TIMESTAMP is UTC; weeks start on Monday. txns is the tc delta from the
// previous bucket, or for the first from the tc before start (param 4), or with no earlier block
// from its own first tc (genesis has no txns). An empty bucket is skipped, which lag bridges.
// ponytail: tps divides by the full bucket length, so a bucket cut by start/end/tip reads low.
const qSeries = `WITH h AS (
	SELECT ts // 3600 * 3600 AS h, sum(fees) AS fees, count(*) AS blocks, min(tc) AS lo, max(tc) AS hi
	FROM blocks WHERE ts >= ?::UINTEGER AND ts < ?::UINTEGER GROUP BY h
), b AS (
	SELECT time_bucket(?::INTERVAL, make_timestamp(h::BIGINT * 1000000)) AS t,
		sum(fees)::UBIGINT AS fees, sum(blocks)::UBIGINT AS blocks, min(lo) AS lo, max(hi) AS hi
	FROM h GROUP BY t
), d AS (
	SELECT t, fees, blocks, hi - coalesce(lag(hi) OVER (ORDER BY t), ?::UBIGINT, lo) AS txns FROM b
)
SELECT epoch(t)::UBIGINT, fees, txns, txns / (epoch(t + ?::INTERVAL) - epoch(t)), blocks FROM d ORDER BY t`

// series buckets blocks with ts in [start, end) by interval, a value of intervals.
func series(ctx context.Context, db *sql.DB, interval string, start, end uint64) ([]bucket, error) {
	var prev any // tc of the last block before start; nil if none
	if start > 0 {
		b, err := queryBlock(ctx, db, qBefore, start-1)
		if err != nil {
			return nil, err
		}
		if b != nil {
			prev = b.Tc
		}
	}
	rows, err := db.QueryContext(ctx, qSeries, start, end, interval, prev, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []bucket{}
	for rows.Next() {
		var b bucket
		if err := rows.Scan(&b.Ts, &b.Fees, &b.Txns, &b.Tps, &b.Blocks); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
