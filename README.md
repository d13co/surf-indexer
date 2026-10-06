# Algorand Block Fees Service

Stores the timestamp, txn counter and fees of every block of one network in DuckDB, ingested from an archival algod, and serves fee sums and block lookups.

API should be considered **unstable**.

## Getting Started

Requires Go 1.26+ and CGO (DuckDB).

```
go build -o fees-api .
ALGOD_HOST=http://localhost ALGOD_PORT=8080 ALGOD_TOKEN=... ./fees-api mainnet-v1.0
```

The first argument is the expected genesis ID (startup fails if algod reports another). An optional second argument names the DB: `data/<dbID>.duckdb`, default `data/<genesisID>.duckdb`.

Ingestion starts at round 0 and catches up in parallel batches, then follows the chain block by block. Once blocks carry `fc` (the incentives upgrade, mainnet round 46512890), only block headers are fetched.

## Routes

All timestamps are unix seconds. Wherever a time is accepted, `YYYY-MM-DD` (UTC midnight) and RFC3339 are also accepted.

### Status

`/v0/status`

```
{"maxRound":46513899,"ok":1,"records":46513900}
```

Sanity check: `records` should be `maxRound + 1` (round 0 is stored).

### Block lookup

Returns `{"rnd":46512890,"ts":1737640732,"tc":2713939481,"fees":86000}`, or 404. Takes exactly one of:

- `/v0/block?ts=<time>[&dir=before|after]`
  - `after`: the first block with `ts >= time`
  - `before`: the last block with `ts <= time`
  - default (closest): the nearer of the two. On a tie, or an exact match within a run of blocks sharing a timestamp, the `after` block (the first of the run) wins.
- `/v0/block?tc=<n>`: the block containing the txn with counter `n`, i.e. `tc` of the previous block `<= n <` this block's `tc`. The app or asset with ID `X` was created in the block returned for `tc=X-1`.

- `rnd`: round
- `ts`: block timestamp
- `tc`: txn counter after this block (cumulative, inner txns included)
- `fees`: fees paid in this block in microAlgos, inner txns included. This equals the header's `fc`; blocks before the incentives upgrade are computed from their txns using the same rule.

### Fee sums

`/v0/fees?minRound=&maxRound=&start=&end=`

All params are optional and combine. Rounds are inclusive; `start`/`end` are times, `[start, end)`.

`/v0/fees/24h`, `/v0/fees/7d`, `/v0/fees/30d`: shortcuts for `start` = now minus the period.

```
{"fees":1092798728,"blocks":1020,"firstRound":46512880,"lastRound":46513899,"firstTs":1737640704,"lastTs":1737643518}
```

- `fees`: sum of block fees in microAlgos
- `blocks`: number of blocks summed
- `firstRound`, `lastRound`, `firstTs`, `lastTs`: the range actually covered, clipped to indexed data

## Environment Variables

| name        | description                                                    | default_value    |
| ----------- | -------------------------------------------------------------- | ---------------- |
| PORT        | HTTP server port                                               | 8118             |
| ALGOD_HOST  | Algod host; `;`-separate several for failover                  | http://localhost |
| ALGOD_PORT  | Algod port; one value, or one per host                         | 8080             |
| ALGOD_TOKEN | Algod token; one value, or one per host                        |                  |
| CONCURRENCY | Algod block request concurrency                                | 10               |
| DB_CHUNKS   | Blocks per batch; each batch is fetched, then inserted at once | 1000             |
