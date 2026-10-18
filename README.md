# surf-indexer

Stores the timestamp, txn counter and fees of every block of one network in DuckDB, ingested from an archival algod, and serves fee sums and block lookups. Optionally it also stores each block's proposer, payout and evicted (absent) accounts, and the cert voters of recent rounds, serving the [block-proposers](#proposers-and-voters) routes. It can also index every logic sig used, with its delegators and template ([logic sigs](#logic-sigs)).

API should be considered **unstable**.

## Getting Started

Requires Go 1.26+ and CGO (DuckDB).

```
go build -o surf-indexer .
ALGOD_HOST=http://localhost ALGOD_PORT=8080 ALGOD_TOKEN=... ./surf-indexer mainnet-v1.0
```

The first argument is the expected genesis ID (startup fails if algod reports another). An optional second argument names the DB: `data/<dbID>.duckdb`, default `data/<genesisID>.duckdb`.

Ingestion starts at round 0 (or `START_ROUND`) and catches up in parallel batches, then follows the chain block by block. Once blocks carry `fc` (the incentives upgrade, mainnet round 46512890), only block headers are fetched, except for rounds in the voter window (`VOTER_ROUNDS`), whose certs need full blocks, and for every round with `LSIGS` or `MSIGS` on, which need each block's txns.

Consensus upgrades are found by sampling every 10000th round's header (an upgrade vote lasts exactly 10000 rounds, so none is missed). An existing DB backfills them on first start, a fresh REST ingest once it reaches the tip. This needs an archival algod and a network on standard consensus params; otherwise `upgrades` errors repeat in the log every 10 minutes while fee ingest carries on.

### Install from apt

On Debian 12+ and Ubuntu 22.04+ (amd64, arm64), from the apt repository at `https://apt.d13.co`:

```
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl -fsSL https://apt.d13.co/key.asc -o /etc/apt/keyrings/d13co.asc
echo "deb [signed-by=/etc/apt/keyrings/d13co.asc] https://apt.d13.co stable main" | sudo tee /etc/apt/sources.list.d/d13co.list
sudo apt update && sudo apt install surf-indexer
```

The package starts nothing. Run one `surf-indexer@<genesisID>` instance per network, configured by an env file with the [environment variables](#environment-variables) (give each instance its own `PORT`). Start from the commented example the package ships:

```
sudo install -m 600 /usr/share/surf-indexer/example.env /etc/surf-indexer/mainnet-v1.0.env   # it holds tokens; systemd reads it as root
sudoedit /etc/surf-indexer/mainnet-v1.0.env
sudo surf-indexer check -env-file /etc/surf-indexer/mainnet-v1.0.env mainnet-v1.0
sudo systemctl enable --now surf-indexer@mainnet-v1.0
```

`check` reads the file as the unit does, fails on unknown names (a typo is otherwise silently ignored) and bad values, then checks each algod answers for that genesis ID (`-offline` skips the algods). Run it after editing the file, before `systemctl restart`.

Its DB is `/var/lib/surf-indexer/data/mainnet-v1.0.duckdb`, owned by the `surf-indexer` user. To switch to another DB of the same network, e.g. one rebuilt with other `PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` or `MSIGS`, set `DB_ID=<dbID>` in the env file and restart; remove it to switch back. `apt upgrade` restarts running instances.

### Running in production

- The unit is `Type=notify`: `systemctl start` returns once the DB is open and the server listens, so a failed start or upgrade restart shows in `systemctl status`. Logs are JSON lines in the journal: `journalctl -u surf-indexer@mainnet-v1.0`.
- Config errors exit with code 78, and the unit does not restart on it: a bad env file, a genesis ID algod doesn't report, or a DB of another network or synced with other `PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` or `MSIGS`. Fix the cause, then `systemctl restart`. Other failures (algod down, disk) restart every 5 s.
- Shutdown waits for the current ingest batch and DuckDB's checkpoint, up to 5 minutes.
- Set `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` on a host shared with algod. For a hard cap on the whole process as well, add a drop-in: `sudo systemctl edit surf-indexer@mainnet-v1.0`, then `[Service]` and `MemoryMax=3G`, kept above `DUCKDB_MEMORY_LIMIT`.
- `apt remove` stops running instances; the DBs in `/var/lib/surf-indexer` and the env files stay.

### Releasing

Push a `v1.2.3` tag. The release workflow tests, builds the linux amd64 and arm64 binaries and `.deb`s into a GitHub release, then, once approved in the `apt` environment, adds the `.deb`s to the apt repository and signs its index ([.github/apt-publish.sh](.github/apt-publish.sh)).

### Bulk load from an archival node's ledger

Catching up from round 0 over REST is slow. On the archival node's machine, read its block DB directly instead, then copy the DuckDB file to the server:

```
# on the archival node, as the algod user: any -shm/-wal files SQLite creates must stay algod's
find <algod datadir> -name ledger.block.sqlite
LEDGER_DB=<path> CONCURRENCY=<cores> DB_CHUNKS=10000 nice -n 19 ionice -c3 ./surf-indexer mainnet-v1.0
```

What gets imported is set by `PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` and `MSIGS`, as for the server (see [Environment Variables](#environment-variables)). The command above uses the defaults: proposers on, and voters for the last 100000 rounds. For a fees-only DB, prefix it with `PROPOSERS=0 VOTER_ROUNDS=0`.

This ingests blocks and upgrades up to the ledger's tip as of startup, closes the DB and exits; no server is started. If interrupted, rerun it to resume. Then, with the server stopped:

```
rsync -P data/mainnet-v1.0.duckdb server:surf-indexer/data/mainnet-v1.0.duckdb.new
# on the server
rm -f data/mainnet-v1.0.duckdb.wal   # a stale WAL would be replayed onto the new file
mv data/mainnet-v1.0.duckdb.new data/mainnet-v1.0.duckdb
```

Start the server as usual; it resumes from the last imported round over REST, so its algod must be archival. The import and the server must run with the same `PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` and `MSIGS`.

With proposers on, rounds before the incentives upgrade read their whole cert from the ledger (~60 KB per round, ~2.8 TB over mainnet's first 46.5M rounds), so the import is I/O-bound.

To try a config on recent rounds before a full import, set `START_ROUND` to start an empty DB there, e.g. for the last 3.5M rounds:

```
L=<path to ledger.block.sqlite>
S=$(sqlite3 -readonly "$L" 'SELECT max(rnd) - 3500000 FROM blocks'); echo $S   # note it: resuming and serving need the same value
START_ROUND=$S LSIGS=1 LEDGER_DB=$L CONCURRENCY=<cores> DB_CHUNKS=10000 nice -n 19 ionice -c3 ./surf-indexer mainnet-v1.0 lsig-trial
```

Such a DB is partial: fee sums, series, logic sig `txns` and `firstRound`s, and multisig pointers and `signed` cover only its rounds, and it opens only with the same `START_ROUND` (and a DB from round 0 only without it).

It is safe to run beside a live algod: the DB is opened read-only, and WAL readers don't block algod's writes. It does compete with algod for disk, page cache and CPU, so watch algod's lag (`goal node status`) and throttle with `CONCURRENCY`, or stop algod for the duration.

## Routes

All timestamps are unix seconds. Wherever a time is accepted, `YYYY-MM-DD` (UTC midnight) and RFC3339 are also accepted.

### Rate limits

Each client (IP address, an IPv6 one as its /64) gets `RATE_LIMIT` requests per second across all routes, in bursts of up to 5× that (default 10/s, burst 50). Heavy requests also take one of 30 per minute, in bursts of up to 10. Heavy are `/v0/series`, `/v0/fees` (not `/v0/fees/<period>`), `/v0/templates`, and any [proposer or voter](#proposers-and-voters) route taking a round range (`/v0/proposers`, `/v0/proposer/<address>` and its `/stats`, `/v0/payouts/highest`, `/v0/evictions` and `/v0/evictions/<address>`, `/v0/voters`, `/v0/voter/<address>`) when that range, up to the tip, covers 5M rounds (~6 months) or more: by default it is all of history. Over either limit, a request gets **429** with `Retry-After` (seconds). `RATE_LIMIT=0` turns these off.

At most `HEAVY_CONCURRENCY` heavy requests query at once across all clients (default 4); each frees its turn once its query is done, before its reply is sent. One that waits 10 s for a turn gets **503** with `Retry-After`. Every request has 30 s: a query still running then is interrupted and gets **503**. A response must be sent within 2 minutes of the request.

Every response, 429s and 503s included, carries `Access-Control-Allow-Origin: *` and exposes `Retry-After` and `X-Truncated` to browsers.

Behind a proxy every client shares the proxy's address, so set `REAL_IP_HEADER` to the header holding the visitor's IP, e.g. `CF-Connecting-IP` behind a Cloudflare Tunnel. It is read only from a loopback or private peer (cloudflared on the host, in docker or on the LAN), so a client reaching the port directly can't choose its own. A request from such a peer without a valid value is limited by the peer address, with a warning logged once. With Cloudflare, keep the zone's *Remove visitor IP headers* Managed Transform and *Pseudo IPv4* off: the first strips the header, the second puts a made-up IPv4 in it for IPv6 visitors.

### Status

`/v0/status`

```
{"ok":1,"maxRound":46513899,"records":46513900,"proposers":true,"voterRounds":100000,"lsigs":true,"msigs":true}
```

Sanity check: `records` should be `maxRound + 1` (round 0 is stored), or `maxRound + 1 - START_ROUND` for a trial.

`proposers`, `voterRounds`, `lsigs` and `msigs` are the `PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` and `MSIGS` this instance runs with. A feature that is off (`false`, or `voterRounds` 0) has no routes: they return 404. Clients should check these before calling the feature routes, since several of those also use 404 to mean "not seen".

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

At most 10000 buckets are returned, the oldest first. When more were left out the response has the header `X-Truncated: 10000`: continue from the last bucket's end, or use a coarser interval. On a chain with long gaps between blocks (a private net) it can come with fewer buckets.

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

`/v0/exists/<address>`: `{"exists":true}` if the address proposed a stored block, voted in the window or was ever evicted.

#### Differences from block-proposers

Clients switching from block-proposers should expect:

1. `/v0/status`: `records` is `maxRound + 1`, as round 0 is stored; block-proposers started at round 1.
2. `/v0/round/<rnd>` for a round not stored is a 404, not a 200 with an empty body. `/v0/round/0` returns genesis with `"proposer":null`.
3. Stricter params return 400: `minRound`/`maxRound` must be integers in [0, 2³²) (floats and negatives were coerced), and addresses must pass the checksum (any 58 characters used to return empty results).
4. No CORS preflight: only `Access-Control-Allow-Origin: *` is set, so a cross-origin request that needs a preflight gets 405. Plain `fetch` GETs are unaffected.
5. Error bodies are plain text (`not found`, or the param error), not Fastify's JSON (`{"statusCode":400,"error":"Bad Request","message":…}`), and an unknown route gets a plain-text 404. A client that parses error bodies as JSON will throw.
6. Routes switched off by `PROPOSERS=0` or `VOTER_ROUNDS=0` return 404.
7. Deployment: the DB file isn't compatible and must be rebuilt. `VOTE_ROUNDS_THRESHOLD` is now `VOTER_ROUNDS`, and the window is the last N rounds rather than N+1. `SYNC_THRESHOLD` and `EMIT_SPEED_EVERY` are gone, `DB_CHUNKS` means blocks per batch (default 1000), and `ALGOD_HOST`/`ALGOD_PORT` default to `http://localhost` and `8080`.

Unchanged: paths, JSON keys, `pp` omitted when 0, the stats shape, eviction ordering. Changed: `exists` is also true for an account only ever evicted. Added: `start`/`end` on every ranged route, and explicit oldest-first order where block-proposers relied on insertion order.

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

`/v0/lsig/<address>`: `{"template":"…","programs":1234,"uses":[…]}`, where `programs` counts the distinct programs of its template, or 404 if the lsig was never used. `programs` here and in `/v0/template/<id>` comes from the cached `/v0/templates` list, so it can lag ingest by up to a minute and briefly count fewer programs than `uses` shows.

`/v0/delegations/<address>`: uses where the address is the `signer` or the `sender`: the lsigs it delegated, and those that moved it while rekeyed. `[]` if none.

`/v0/templates`: templates with 2 or more programs, or delegated by 2 or more accounts, most programs first: `[{"template":"…","programs":1234,"delegators":0,"txns":56789,"firstRound":…,"lastRound":…}, ...]`. `delegators` counts the distinct signers. The list is cached for up to a minute (a smaller `limit` is its first rows); an [admin](#admin) rename clears the cache.

`/v0/template/<id>`: `{"template":"…","name":null,"logic":"<base64>","programs":1234,"uses":[…]}`, or 404. `logic` is the first program seen of the template (disassemble it with `goal clerk compile -D`). `id` is 16 lowercase hex digits.

`name`, also in `/v0/templates` and `/v0/lsig/<address>`, is the template's name as set by an [admin](#admin), or `null`.

### Admin

These routes exist only with `ADMIN_TOKEN` set, which needs `LSIGS` on (startup fails otherwise); without it they 404. Each needs the header `Authorization: Bearer <ADMIN_TOKEN>`, else 401. They are meant for curl, not browsers: there is no CORS preflight.

`PUT /v0/admin/template/<id>` with body `{"name":"Tinyman v1 pool"}`: names a template, replacing any name. The name is trimmed, and must be 1 to 100 visible characters or spaces: no control, zero-width or text-direction characters (400 otherwise; 413 for a body over 4 KiB). Returns `{"name":"…","template":"…"}`, or 404 if no lsig of that template was seen.

```
curl -X PUT -H "Authorization: Bearer $ADMIN_TOKEN" -d '{"name":"Tinyman v1 pool"}' localhost:8118/v0/admin/template/3f2c9a1b7d0e4c55
```

`DELETE /v0/admin/template/<id>`: removes its name. 204 whether or not it had one.

Names are stored in the DB (`template_names`), so a rebuilt DB starts without them. Copy them over with the `duckdb` CLI, server stopped: `duckdb new.duckdb "ATTACH 'old.duckdb' AS o (READ_ONLY); INSERT INTO template_names SELECT * FROM o.template_names"`.

### Multisig subsigners

These routes exist only with `MSIGS` on; otherwise they 404.

Every multisig that authorized a top-level txn is indexed (inner txns have none): the txn's own msig, or one that signed its delegated logic sig (msig or lmsig). The multisig address comes from its preimage, so it is the txn's authorizer, which differs from the sender when rekeyed. There is one row per subsigner and multisig, without history, pointing at one txn: the first the subsigner signed, or while it never has, the first that used the multisig. Lists are newest pointer first and capped by `limit` (default 1000, at most 100000). A row:

```
{"msig":"…","signed":true,"round":46500000,"index":3}
```

- `signed`: whether this key ever signed for this multisig. For an old-style delegated logic sig (msig, valid until consensus v41), the key signed only the program, not bound to the multisig, so anyone could copy that sig into another multisig holding the key: there `signed` means the key signed the program. A txn msig, and an lmsig, are bound to the multisig.
- `round`, `index`: the txn is `index` in that block's payset (top-level txns only: not the indexer's `intra`, which also counts inner txns)

`/v0/subsigner/<address>`: the multisigs the key belongs to, as above. `[]` if none.

`/v0/msig/<address>`: the subsigners of a multisig, with `signer` in place of `msig`. `[]` if it was never used.

A multisig preimage this build can't read (a new msig version, or a key that isn't 32 bytes) is skipped with an `msig skipped` warning in the log, leaving a gap: upgrade the binary and rebuild if that appears.

### Account summary

`/v0/account/<address>`: what each tracked feature knows of an address, in one request (~40–50 ms on mainnet), for a client to choose what to fetch next:

```
{"lsig":{"template":"…","name":null},"delegations":2,"multisigs":1,"signers":0,"proposed":true,"evicted":false,"voted":false}
```

- `lsig`: its template and name if it is a logic sig, else `null` (`LSIGS`)
- `delegations`: the number of uses `/v0/delegations/<address>` lists, before its `limit` (`LSIGS`)
- `multisigs`: the number of multisigs it is a subsigner of, as `/v0/subsigner/<address>` lists before its `limit` (`MSIGS`)
- `signers`: the number of subsigners if it is a multisig, as `/v0/msig/<address>` lists before its `limit` (`MSIGS`)
- `proposed`, `evicted`: whether it ever proposed a stored block or was evicted (`PROPOSERS`)
- `voted`: whether it voted in the window (`VOTER_ROUNDS` above 0)

Keys of features that are off are left out, and an address never seen gets zeros and `false`. With all four off the route 404s. Not ranged and never heavy.

## Environment Variables

| name        | description                                                    | default_value    |
| ----------- | -------------------------------------------------------------- | ---------------- |
| PORT        | HTTP server port                                               | 8118             |
| LISTEN_ADDR | Address to bind, e.g. `127.0.0.1` behind a proxy; empty for every interface |  |
| ALGOD_HOST  | Algod host; `;`-separate several for failover                  | http://localhost |
| ALGOD_PORT  | Algod port; one value, or one per host                         | 8080             |
| ALGOD_TOKEN | Algod token; one value, or one per host                        |                  |
| CONCURRENCY | Concurrent block fetches (algod requests, or LEDGER_DB reads)  | 10               |
| DB_CHUNKS   | Blocks per batch; each batch is fetched, then inserted at once | 1000             |
| LEDGER_DB   | Path to an archival `ledger.block.sqlite`: bulk load and exit  |                  |
| PROPOSERS   | Store each block's proposer, payout and evicted accounts       | true             |
| VOTER_ROUNDS | Store cert voters of the last N rounds; 0 for none            | 100000           |
| LSIGS       | Store logic sig uses, delegators and templates                 | false            |
| MSIGS       | Store multisig subsigners, whether each signed, and a txn      | false            |
| ADMIN_TOKEN | Enables the [admin](#admin) routes (needs `LSIGS`); 16+ printable ASCII characters, no spaces, e.g. `openssl rand -hex 32` |                  |
| RATE_LIMIT  | Per-client requests/s across all routes, burst 5×; 0 for no per-client limits ([rate limits](#rate-limits)) | 10 |
| HEAVY_CONCURRENCY | Heavy requests running at once, across all clients; 0 for no cap | 4 |
| REAL_IP_HEADER | Header holding the visitor IP behind a proxy, read only from a loopback or private peer, e.g. `CF-Connecting-IP` |  |
| DUCKDB_MEMORY_LIMIT | DuckDB memory cap, e.g. `2GB`; DuckDB's default is 80% of RAM |  |
| DUCKDB_THREADS | DuckDB worker threads; 0 for DuckDB's default (every core) | 0 |
| DB_ID       | systemd unit only: serve `data/<DB_ID>.duckdb` (the dbID argument) | the genesis ID |
| START_ROUND | Trials only: an empty DB starts at this round ([bulk load](#bulk-load-from-an-archival-nodes-ledger)) | 0 |

`PROPOSERS`, `VOTER_ROUNDS`, `LSIGS` and `MSIGS` are fixed once the DB holds a block: a mismatch is refused at startup. To change them, rebuild into a new file (see [bulk load](#bulk-load-from-an-archival-nodes-ledger)). A fees-only DB from before these existed runs with `PROPOSERS=0 VOTER_ROUNDS=0`, and is altered on its first open (two NULL columns added), so back it up first: an older binary can still serve it but no longer ingest into it. A binary from before `LSIGS` ingests into an `LSIGS` DB without noticing, leaving a gap in its logic sigs: rebuild it if that happens. The same goes for a binary from before `MSIGS` on an `MSIGS` DB.

Disk and bandwidth, mainnet at 65.7M rounds: blocks take ~265 MiB fees-only, ~412 MiB with proposers. Voters take ~27 MiB per 100k rounds, and each round in the window is a full block fetch with cert (~70 KB), so filling a 100k window over REST downloads ~7 GB. Logic sigs take an estimated ~80 B per (program, signer, sender) plus one program per template, growing with distinct programs rather than rounds. Multisig subsigners take an estimated ~80 B per (subsigner, multisig), growing with distinct multisigs rather than rounds. With `LSIGS` or `MSIGS` on every round is a full block fetch (shared when both are on): free at the tip while `VOTER_ROUNDS` is above 0, else ~70 KB per block (~2 GB/day) over REST, and a ledger import reads whole blocks after the incentives upgrade too.

Estimated mainnet DB size: ~0.45 GB with the defaults (proposers on, 100k-round voter window), and ~0.5–0.9 GB with `LSIGS`. Blocks grow by ~11 B per round at today's format, ~100 MiB a year; the voter window stays flat. The logic sig share is a guess (0.5–5M rows at ~80 B): a trial over rounds 58.0M–65.8M stored only 4,645 rows (~1 MiB), but most lsig use is older (Tinyman v1, NFT-listing escrows), so only a full `LSIGS` import gives the real figure. The file is about the size of its data: the trial's 121.5 MiB file is nearly all used blocks.
