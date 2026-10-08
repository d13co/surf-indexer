# Algorand Block Fees Service

Stores the timestamp, txn counter and fees of every block of one network in DuckDB, ingested from an archival algod, and serves fee sums and block lookups. Optionally it also stores each block's proposer, payout and evicted (absent) accounts, and the cert voters of recent rounds, serving the [block-proposers](#proposers-and-voters) routes. It can also index every logic sig used, with its delegators and template ([logic sigs](#logic-sigs)).

API should be considered **unstable**.

## Getting Started

Requires Go 1.26+ and CGO (DuckDB).

```
go build -o fees-api .
ALGOD_HOST=http://localhost ALGOD_PORT=8080 ALGOD_TOKEN=... ./fees-api mainnet-v1.0
```

The first argument is the expected genesis ID (startup fails if algod reports another). An optional second argument names the DB: `data/<dbID>.duckdb`, default `data/<genesisID>.duckdb`.

Ingestion starts at round 0 (or `START_ROUND`) and catches up in parallel batches, then follows the chain block by block. Once blocks carry `fc` (the incentives upgrade, mainnet round 46512890), only block headers are fetched, except for rounds in the voter window (`VOTER_ROUNDS`), whose certs need full blocks, and for every round with `LSIGS` on, which needs each block's txns.

Consensus upgrades are found by sampling every 10000th round's header (an upgrade vote lasts exactly 10000 rounds, so none is missed). An existing DB backfills them on first start, a fresh REST ingest once it reaches the tip. This needs an archival algod and a network on standard consensus params; otherwise `upgrades` errors repeat in the log every 10 minutes while fee ingest carries on.

### Bulk load from an archival node's ledger

Catching up from round 0 over REST is slow. On the archival node's machine, read its block DB directly instead, then copy the DuckDB file to the server:

```
# on the archival node, as the algod user: any -shm/-wal files SQLite creates must stay algod's
find <algod datadir> -name ledger.block.sqlite
LEDGER_DB=<path> CONCURRENCY=<cores> DB_CHUNKS=10000 nice -n 19 ionice -c3 ./fees-api mainnet-v1.0 mainnet-v1.0-p
```

What gets imported is set by `PROPOSERS`, `VOTER_ROUNDS` and `LSIGS`, as for the server (see [Environment Variables](#environment-variables)). The command above uses the defaults: proposers on, and voters for the last 100000 rounds. For a fees-only DB, prefix it with `PROPOSERS=0 VOTER_ROUNDS=0` and drop the `-p` DB name.

This ingests blocks and upgrades up to the ledger's tip as of startup, closes the DB and exits; no server is started. If interrupted, rerun it to resume. Then, with the server stopped:

```
rsync -P data/mainnet-v1.0-p.duckdb server:fees-api/data/mainnet-v1.0-p.duckdb.new
# on the server
rm -f data/mainnet-v1.0-p.duckdb.wal   # a stale WAL would be replayed onto the new file
mv data/mainnet-v1.0-p.duckdb.new data/mainnet-v1.0-p.duckdb
```

Start the server as usual (`./fees-api mainnet-v1.0 mainnet-v1.0-p`); it resumes from the last imported round over REST, so its algod must be archival. The import and the server must run with the same `PROPOSERS`, `VOTER_ROUNDS` and `LSIGS`.

With proposers on, rounds before the incentives upgrade read their whole cert from the ledger (~60 KB per round, ~2.8 TB over mainnet's first 46.5M rounds), so the import is I/O-bound.

To try a config on recent rounds before a full import, set `START_ROUND` to start an empty DB there, e.g. for the last 3.5M rounds:

```
L=<path to ledger.block.sqlite>
S=$(sqlite3 -readonly "$L" 'SELECT max(rnd) - 3500000 FROM blocks'); echo $S   # note it: resuming and serving need the same value
START_ROUND=$S LSIGS=1 LEDGER_DB=$L CONCURRENCY=<cores> DB_CHUNKS=10000 nice -n 19 ionice -c3 ./fees-api mainnet-v1.0 lsig-trial
```

Such a DB is partial: fee sums, series and logic sig `txns` and `firstRound`s cover only its rounds, and it opens only with the same `START_ROUND` (and a DB from round 0 only without it).

It is safe to run beside a live algod: the DB is opened read-only, and WAL readers don't block algod's writes. It does compete with algod for disk, page cache and CPU, so watch algod's lag (`goal node status`) and throttle with `CONCURRENCY`, or stop algod for the duration.

## Routes

All timestamps are unix seconds. Wherever a time is accepted, `YYYY-MM-DD` (UTC midnight) and RFC3339 are also accepted.

### Status

`/v0/status`

```
{"maxRound":46513899,"ok":1,"records":46513900}
```

Sanity check: `records` should be `maxRound + 1` (round 0 is stored), or `maxRound + 1 - START_ROUND` for a trial.

### Block lookup

Returns `{"rnd":46512890,"ts":1737640732,"tc":2713939481,"fees":86000}`, or 404. Takes exactly one of:

- `/v0/block?ts=<time>[&dir=before|after]`
  - `after`: the first block with `ts >= time`
  - `before`: the last block with `ts <= time`
  - default (closest): the nearer of the two. On a tie, or an exact match within a run of blocks sharing a timestamp, the `after` block (the first of the run) wins.
- `/v0/block?tc=<n>`: the block containing the txn with counter `n`, i.e. `tc` of the previous block `<= n <` this block's `tc`. The app or asset with ID `X` was created in the block returned for `tc=X-1`.

- `rnd`: round
- `ts`: block timestamp
- `tc`: txn counter after this block (cumulative, inner txns included). It only exists from the upgrade that added it (mainnet round 3317192, 2019-11-26), where it started at 0, so it is 0 before.
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

### Series

`/v0/series?interval=&start=&end=`

Fees and TPS per interval, for charts. `interval` is `block`, `minute`, `hour`, `day` (default), `week` (from Monday) or `month`, in UTC. `start`/`end` are optional times, `[start, end)`.

```
[{"ts":1759708800,"fees":2145000000,"txns":2700864,"tps":31.26,"blocks":30857}, ...]
```

- `ts`: bucket start
- `fees`: sum of block fees in microAlgos
- `txns`: txns in the bucket's blocks, inner txns included (the `tc` delta, or each block's own txns before `tc` existed). A DB built by an older version counts none before mainnet round 3317192: rebuild it to fill them.
- `tps`: `txns` / the bucket's length in seconds
- `blocks`: number of blocks in the bucket
- `rnd`: the block's round, `block` interval only

With `interval=block` each bucket is one block: `ts` is its timestamp and `tps` is `txns` / the seconds since the previous block (0 if none or equal timestamps).

Buckets with no blocks are omitted. A bucket cut by `start`, `end` or the chain tip is partial, and its `tps` is still divided by the full bucket length.

### Upgrades

`/v0/upgrades`

Every consensus upgrade proposal, oldest first.

```
[{"round":46294890,"ts":1737038506,"from":"https://github.com/algorandfoundation/specs/tree/925a464…","proto":"https://github.com/algorandfoundation/specs/tree/236dcc1…","voteBefore":46304890,"switchOn":46512890,"approvals":9157,"status":"applied","switchTs":1737640732}, ...]
```

- `round`, `ts`: when it was proposed
- `from`, `proto`: the protocol it was proposed on and the one it proposes
- `voteBefore`: the round voting ends; approvals count in the rounds before it
- `switchOn`: the round the new protocol takes effect, if approved
- `approvals`: final approval count, `null` while voting
- `status`: `voting`, `rejected`, `approved` (waiting for `switchOn`) or `applied`
- `switchTs`: timestamp of the `switchOn` block once applied, else `null`

### Proposers and voters

Ported from block-proposers, with time filters added. Proposer routes exist only with `PROPOSERS` on, voter routes only with `VOTER_ROUNDS` above 0, and `/v0/exists` with either; otherwise they 404.

Every route below that takes `minRound`/`maxRound` also takes `start`/`end`, as in `/v0/fees`: rounds are inclusive, times are `[start, end)`, and all combine. Addresses must be valid (400 otherwise).

`/v0/proposers`: proposers by blocks proposed, most first.

```
[{"proposer":"FNETJYY6YYB6SUWQDQZ5IDWLXOTOC2TMLW45B4O6MH5LREIBEXPREYSONY","blocks":22890,"payouts":0}, ...]
```

- `blocks`: number of blocks proposed
- `payouts`: sum of proposer payouts in microAlgos

`/v0/proposer/<address>`: the address's blocks, oldest first. `pp` (payout in microAlgos) is omitted when 0.

```
[{"rnd":13,"ts":1718000000},{"rnd":55577,"ts":1718200000,"pp":10000000}, ...]
```

`/v0/proposer/<address>/stats`: aggregate stats instead of the block list.

```
{"blocks":124,"payouts":1240000000,"suspensions":0,"firstRound":13,"lastRound":56546,"avgBlocksPerDay":3261.07,"range":{"firstRound":1,"lastRound":145286,"firstTs":1718000000,"lastTs":1782600000}}
```

- `blocks`, `payouts`: as in `/v0/proposers`
- `suspensions`: number of times the account was evicted (suspended) in the range
- `firstRound`, `lastRound`: the proposer's own first/last block in the range, 0 if none
- `range`: the rounds and times queried, clipped to stored data (genesis excluded); `lastTs - firstTs` is its duration in seconds
- `avgBlocksPerDay`: `blocks` divided by the range duration in days

`/v0/payouts/highest`: the 10 highest payouts, `[{"proposer":"…","rnd":46600000,"ts":1737900000,"pp":8277679}, ...]`.

`/v0/evictions`: accounts marked absent (`partupdabs`), most recently evicted first, each with its rounds newest first: `[{"account":"…","evictions":2,"rounds":[46700000,46650000]}, ...]`.

`/v0/evictions/<address>`: the address's eviction rounds, oldest first: `[46650000,46700000]`.

`/v0/round/<rnd>`: `{"proposer":"…","rnd":46600000,"ts":1737900000,"pp":8277679}`, `pp` omitted when 0, or 404 if the round isn't stored. Round 0 (genesis) has `"proposer":null`.

`/v0/voters`: cert voters by blocks voted, most first: `[{"voter":"…","blocks":99000}, ...]`. Only the last `VOTER_ROUNDS` rounds are kept.

`/v0/voter/<address>`: the rounds the address voted in, oldest first: `[65500001,65500002, ...]`.

`/v0/exists/<address>`: `{"exists":true}` if the address proposed a stored block or voted in the window.

#### Differences from block-proposers

Clients switching from block-proposers should expect:

1. `/v0/status`: `records` is `maxRound + 1`, as round 0 is stored; block-proposers started at round 1.
2. `/v0/round/<rnd>` for a round not stored is a 404, not a 200 with an empty body. `/v0/round/0` returns genesis with `"proposer":null`.
3. Stricter params return 400: `minRound`/`maxRound` must be integers in [0, 2³²) (floats and negatives were coerced), and addresses must pass the checksum (any 58 characters used to return empty results).
4. No CORS preflight: only `Access-Control-Allow-Origin: *` is set, so a cross-origin request that needs a preflight gets 405. Plain `fetch` GETs are unaffected.
5. Error bodies are plain text (`not found`, or the param error), not Fastify's JSON (`{"statusCode":400,"error":"Bad Request","message":…}`), and an unknown route gets a plain-text 404. A client that parses error bodies as JSON will throw.
6. Routes switched off by `PROPOSERS=0` or `VOTER_ROUNDS=0` return 404.
7. Deployment: the DB file isn't compatible and must be rebuilt. `VOTE_ROUNDS_THRESHOLD` is now `VOTER_ROUNDS`, and the window is the last N rounds rather than N+1. `SYNC_THRESHOLD` and `EMIT_SPEED_EVERY` are gone, `DB_CHUNKS` means blocks per batch (default 1000), and `ALGOD_HOST`/`ALGOD_PORT` default to `http://localhost` and `8080`.

Unchanged: paths, JSON keys, `pp` omitted when 0, the stats shape, eviction ordering and `exists`. Added: `start`/`end` on every ranged route, and explicit oldest-first order where block-proposers relied on insertion order.

### Logic sigs

These routes exist only with `LSIGS` on; otherwise they 404.

Every txn authorized by a logic sig is indexed (inner txns have none): by an escrow, where the authorizer is the program's own address, or by a delegation, where an account signed the program (by sig, msig, lmsig or pqsig). Rows are per program, delegator and rekeyed sender, without per-round history, so lists of uses are newest first by `lastRound`, and every list is capped by `limit` (default 1000, at most 100000). A use:

```
{"lsig":"…","template":"3f2c9a1b7d0e4c55","signer":null,"sender":null,"txns":12,"firstRound":46000000,"lastRound":46500000}
```

- `lsig`: the program's address, `Hash("Program" || program)`
- `template`: its template id (below)
- `signer`: the delegator, i.e. the authorizer that signed the program; `null` for an escrow
- `sender`: the sender when rekeyed (to the escrow, or to `signer`), else `null`
- `txns`: the txns it authorized; `firstRound`, `lastRound`: the first and last round it did, so a txn can be looked up there

**Templates.** Many lsigs are one program filled in with other values (TMPL_ variables, per-user or pool escrows). A template id hashes the program with its constants and branch offsets left out, so such programs share it: constant blocks are dropped, and every int load (`intc*`, `pushint`) counts as one op and every bytes load (`bytec*`, `pushbytes`) as another, because goal's assembler merges equal values and orders blocks by use, so one source assembles differently per value. That also covers autosalting: from TEAL v13, goal adds a salt (an extra `intcblock` entry, or a trailing `intcblock`) to a program whose address would be an Ed25519 key, so salted and unsalted instances share a template. That is the same opcode sequence, while constants **and branch targets** may differ: anyone can fill a template with their own values, so never use it to judge a lsig genuine. A program that doesn't parse, or has a TEAL version newer than this build knows (13), is its own template. Template ids from a build that knows another version can differ, so a DB built with one should be rebuilt rather than ingested into by another.

`/v0/lsig/<address>`: `{"template":"…","programs":1234,"uses":[…]}`, where `programs` counts the distinct programs of its template, or 404 if the lsig was never used.

`/v0/delegations/<address>`: uses where the address is the `signer` or the `sender`: the lsigs it delegated, and those that moved it while rekeyed. `[]` if none.

`/v0/templates`: templates with 2 or more programs, or delegated by 2 or more accounts, most programs first: `[{"template":"…","programs":1234,"delegators":0,"txns":56789,"firstRound":…,"lastRound":…}, ...]`. `delegators` counts the distinct signers.

`/v0/template/<id>`: `{"template":"…","logic":"<base64>","programs":1234,"uses":[…]}`, or 404. `logic` is the first program seen of the template (disassemble it with `goal clerk compile -D`). `id` is 16 lowercase hex digits.

## Environment Variables

| name        | description                                                    | default_value    |
| ----------- | -------------------------------------------------------------- | ---------------- |
| PORT        | HTTP server port                                               | 8118             |
| ALGOD_HOST  | Algod host; `;`-separate several for failover                  | http://localhost |
| ALGOD_PORT  | Algod port; one value, or one per host                         | 8080             |
| ALGOD_TOKEN | Algod token; one value, or one per host                        |                  |
| CONCURRENCY | Concurrent block fetches (algod requests, or LEDGER_DB reads)  | 10               |
| DB_CHUNKS   | Blocks per batch; each batch is fetched, then inserted at once | 1000             |
| LEDGER_DB   | Path to an archival `ledger.block.sqlite`: bulk load and exit  |                  |
| PROPOSERS   | Store each block's proposer, payout and evicted accounts       | true             |
| VOTER_ROUNDS | Store cert voters of the last N rounds; 0 for none            | 100000           |
| LSIGS       | Store logic sig uses, delegators and templates                 | false            |
| START_ROUND | Trials only: an empty DB starts at this round ([bulk load](#bulk-load-from-an-archival-nodes-ledger)) | 0 |

`PROPOSERS`, `VOTER_ROUNDS` and `LSIGS` are fixed once the DB holds a block: a mismatch is refused at startup. To change them, rebuild into a new file (see [bulk load](#bulk-load-from-an-archival-nodes-ledger)). A fees-only DB from before these existed runs with `PROPOSERS=0 VOTER_ROUNDS=0`, and is altered on its first open (two NULL columns added), so back it up first: an older binary can still serve it but no longer ingest into it. A binary from before `LSIGS` ingests into an `LSIGS` DB without noticing, leaving a gap in its logic sigs: rebuild it if that happens.

Disk and bandwidth, mainnet at 65.7M rounds: blocks take ~265 MiB fees-only, ~412 MiB with proposers. Voters take ~27 MiB per 100k rounds, and each round in the window is a full block fetch with cert (~70 KB), so filling a 100k window over REST downloads ~7 GB. Logic sigs take an estimated ~80 B per (program, signer, sender) plus one program per template, growing with distinct programs rather than rounds. With `LSIGS` on every round is a full block fetch: free at the tip while `VOTER_ROUNDS` is above 0, else ~70 KB per block (~2 GB/day) over REST, and a ledger import reads whole blocks after the incentives upgrade too.
