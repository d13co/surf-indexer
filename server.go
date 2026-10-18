package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/types"
)

// parseTime accepts unix seconds, YYYY-MM-DD (UTC midnight) or RFC3339.
func parseTime(s string) (uint64, error) {
	if n, err := strconv.ParseUint(s, 10, 32); err == nil {
		return n, nil
	}
	for _, layout := range []string{time.DateOnly, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil && t.Unix() >= 0 && t.Unix() <= math.MaxUint32 {
			return uint64(t.Unix()), nil
		}
	}
	return 0, fmt.Errorf("bad time %q: want unix seconds, YYYY-MM-DD or RFC3339", s)
}

func parseRound(s string) (uint64, error) { return strconv.ParseUint(s, 10, 32) }

const maxLimit = 100000

func parseLimit(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err == nil && (n < 1 || n > maxLimit) {
		err = errors.New("want 1 to 100000")
	}
	return n, err
}

// firstN returns the JSON array s cut to its first n elements, each as it was.
func firstN(s string, n uint64) (string, error) {
	var list []json.RawMessage
	if err := json.Unmarshal([]byte(s), &list); err != nil || uint64(len(list)) <= n {
		return s, err
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = string(list[i])
	}
	return "[" + strings.Join(parts, ",") + "]", nil
}

// opt parses q[name], or returns def if it is absent.
func opt(q url.Values, name string, def uint64, parse func(string) (uint64, error)) (uint64, error) {
	if v := q.Get(name); v != "" {
		n, err := parse(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", name, err)
		}
		return n, nil
	}
	return def, nil
}

// fail replies to a request whose query failed: 503 if it ran out of time (DuckDB was interrupted,
// so err needn't say why), nothing if the client left, else 500.
func fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(r.Context().Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		slog.Warn("query timed out", "path", r.URL.Path, "query", r.URL.RawQuery)
		retryLater(w, http.StatusServiceUnavailable, 5*time.Second)
	case r.Context().Err() != nil:
	default:
		slog.Error("reply", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func reply(w http.ResponseWriter, r *http.Request, v any, err error) {
	queried(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// querySQL returns q's single JSON value, built by DuckDB as to_json(...)::VARCHAR (uncast, the
// driver would unmarshal it). q takes $n params: a repeated $1 binds one arg.
func querySQL(ctx context.Context, db *sql.DB, q string, args ...any) (string, error) {
	var s string
	err := db.QueryRowContext(ctx, q, args...).Scan(&s)
	return s, err
}

// replyJSON writes a querySQL result. No row is a 404.
func replyJSON(w http.ResponseWriter, r *http.Request, s string, err error) {
	queried(r)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, s)
}

func replySQL(w http.ResponseWriter, r *http.Request, db *sql.DB, q string, args ...any) {
	s, err := querySQL(r.Context(), db, q, args...)
	replyJSON(w, r, s, err)
}

// roundRange parses minRound/maxRound and narrows them by the time window [start, end). ts never
// decreases with rnd, so the window is a contiguous round interval, found by the same zonemap-pruned
// lookups as /v0/block. An empty range comes back as lo > hi. On a bad param or error it replies.
func roundRange(w http.ResponseWriter, r *http.Request, db *sql.DB) (lo, hi uint64, ok bool) {
	q := r.URL.Query()
	lo, e1 := opt(q, "minRound", 0, parseRound)
	hi, e2 := opt(q, "maxRound", math.MaxUint32, parseRound)
	start, e3 := opt(q, "start", 0, parseTime)
	end, e4 := opt(q, "end", math.MaxUint32, parseTime)
	if err := errors.Join(e1, e2, e3, e4); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return 0, 0, false
	}
	if end == 0 {
		return 1, 0, true
	}
	var err error
	first, last := &block{Rnd: 0}, &block{Rnd: math.MaxUint32} // unbounded
	if start > 0 {
		first, err = blockAt(r.Context(), db, start, "after")
	}
	if err == nil && end < math.MaxUint32 {
		last, err = blockAt(r.Context(), db, end-1, "before")
	}
	if err != nil {
		reply(w, r, nil, err)
		return 0, 0, false
	}
	if first == nil || last == nil {
		return 1, 0, true
	}
	return max(lo, first.Rnd), min(hi, last.Rnd), true
}

// addr returns the {addr} path value, replying 400 if it isn't a valid address.
func addr(w http.ResponseWriter, r *http.Request) (string, bool) {
	a := r.PathValue("addr")
	if _, err := types.DecodeAddress(a); err != nil {
		http.Error(w, "addr: "+err.Error(), http.StatusBadRequest)
		return "", false
	}
	return a, true
}

// tmplID returns the {id} path value, replying 400 if it isn't a template id.
func tmplID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if len(id) != 16 || strings.Trim(id, "0123456789abcdef") != "" {
		http.Error(w, "id: want 16 lowercase hex digits", http.StatusBadRequest)
		return "", false
	}
	return id, true
}

// heavyRounds is the round range from which a ranged route is heavy: ~6 months. Over it a
// proposer's stats take ~13 ms, over all of history ~150 ms.
const heavyRounds = 5_000_000

var periods = map[string]time.Duration{"24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour}

func routes(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	lim := newLimiter(limits)
	// light registers a cheap route; heavy one that scans or returns a lot, which also takes a heavy
	// token and one of the global heavy slots.
	light := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, h) }
	heavy := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, lim.heavy(h)) }

	light("GET /v0/status", func(w http.ResponseWriter, r *http.Request) {
		n, _, hi, err := rounds(r.Context(), db)
		// The config says which feature routes exist: those off are 404s.
		reply(w, r, struct {
			OK       int   `json:"ok"`
			MaxRound int64 `json:"maxRound"`
			Records  int64 `json:"records"`
			config
		}{1, hi, n, track}, err)
	})

	light("GET /v0/block", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		var b *block
		var err error
		switch ts, tc, dir := q.Get("ts"), q.Get("tc"), q.Get("dir"); {
		case ts != "" && tc == "":
			t, perr := parseTime(ts)
			if perr == nil && dir != "" && dir != "closest" && dir != "before" && dir != "after" {
				perr = fmt.Errorf("dir: want before, after or closest")
			}
			if perr != nil {
				http.Error(w, perr.Error(), http.StatusBadRequest)
				return
			}
			b, err = blockAt(r.Context(), db, t, dir)
		case tc != "" && ts == "":
			n, perr := strconv.ParseUint(tc, 10, 63) // database/sql rejects uint64 params >= 2^63
			if perr != nil {
				http.Error(w, "tc: "+perr.Error(), http.StatusBadRequest)
				return
			}
			b, err = blockByTc(r.Context(), db, n)
		default:
			http.Error(w, "want exactly one of ts, tc", http.StatusBadRequest)
			return
		}
		if err == nil && b == nil {
			http.Error(w, "no such block", http.StatusNotFound)
			return
		}
		reply(w, r, b, err)
	})

	heavy("GET /v0/fees", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		minRnd, e1 := opt(q, "minRound", 0, parseRound)
		maxRnd, e2 := opt(q, "maxRound", math.MaxUint32, parseRound)
		start, e3 := opt(q, "start", 0, parseTime)
		end, e4 := opt(q, "end", math.MaxUint32, parseTime)
		if err := errors.Join(e1, e2, e3, e4); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s, err := sumFees(r.Context(), db, minRnd, maxRnd, start, end)
		reply(w, r, s, err)
	})

	light("GET /v0/fees/{period}", func(w http.ResponseWriter, r *http.Request) {
		d, ok := periods[r.PathValue("period")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		s, err := sumFees(r.Context(), db, 0, math.MaxUint32, uint64(time.Now().Add(-d).Unix()), math.MaxUint32)
		reply(w, r, s, err)
	})

	heavy("GET /v0/series", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		iv := cmp.Or(q.Get("interval"), "day")
		var e0 error
		if _, ok := intervals[iv]; !ok {
			e0 = errors.New("interval: want block, minute, hour, day, week or month")
		}
		start, e1 := opt(q, "start", 0, parseTime)
		end, e2 := opt(q, "end", math.MaxUint32, parseTime)
		if err := errors.Join(e0, e1, e2); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s, truncated, err := series(r.Context(), db, iv, start, end)
		if truncated {
			w.Header().Set("X-Truncated", strconv.Itoa(maxBuckets))
		}
		reply(w, r, s, err)
	})

	light("GET /v0/upgrades", func(w http.ResponseWriter, r *http.Request) {
		u, err := queryUpgrades(r.Context(), db, "ORDER BY rnd")
		reply(w, r, u, err)
	})

	// ranged serves q with the round range as $1, $2 and, for an {addr} pattern, the address as $3.
	// Its cost is the rounds it scans, so a range of heavyRounds or more (by default all of history)
	// is heavy; a recent window, like the explorer's timeframes, is not.
	ranged := func(pattern, q string) {
		withAddr := strings.Contains(pattern, "{addr}")
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			lo, hi, ok := roundRange(w, r, db)
			if !ok {
				return
			}
			args := []any{lo, hi}
			if withAddr {
				a, ok := addr(w, r)
				if !ok {
					return
				}
				args = append(args, a)
			}
			if top := min(hi, storedTip.Load()); top >= lo && top-lo >= heavyRounds {
				var release func()
				if r, release, ok = lim.admit(w, r); !ok {
					return
				}
				defer release()
			}
			replySQL(w, r, db, q, args...)
		})
	}
	// Each list is built in DuckDB; list() of no rows is NULL, hence the coalesce to [].
	const in = " rnd BETWEEN $1::UINTEGER AND $2::UINTEGER"
	if track.Proposers {
		ranged("GET /v0/proposers", `SELECT coalesce(to_json(list({'proposer': prop, 'blocks': n, 'payouts': p} ORDER BY n DESC, prop))::VARCHAR, '[]')
			FROM (SELECT prop, count(*) AS n, sum(pp) AS p FROM blocks WHERE prop IS NOT NULL AND`+in+` GROUP BY prop)`)
		// pp is omitted when 0, as block-proposers did.
		ranged("GET /v0/proposer/{addr}", `SELECT coalesce(to_json(list(CASE WHEN pp > 0 THEN to_json({'rnd': rnd, 'ts': ts, 'pp': pp})
			ELSE to_json({'rnd': rnd, 'ts': ts}) END ORDER BY rnd))::VARCHAR, '[]') FROM blocks WHERE prop = $3 AND`+in)
		// One scan: the range is every block in it (genesis excluded), the rest the proposer's own.
		ranged("GET /v0/proposer/{addr}/stats", `SELECT to_json({
			'blocks': count(*) FILTER (WHERE prop = $3),
			'payouts': coalesce(sum(pp) FILTER (WHERE prop = $3), 0),
			'suspensions': (SELECT count(*) FROM evictions WHERE acct = $3 AND`+in+`),
			'firstRound': coalesce(min(rnd) FILTER (WHERE prop = $3), 0),
			'lastRound': coalesce(max(rnd) FILTER (WHERE prop = $3), 0),
			'avgBlocksPerDay': CASE WHEN max(ts) > min(ts) THEN count(*) FILTER (WHERE prop = $3) * 86400 / (max(ts) - min(ts)) ELSE 0 END,
			'range': {'firstRound': coalesce(min(rnd), 0), 'lastRound': coalesce(max(rnd), 0), 'firstTs': coalesce(min(ts), 0), 'lastTs': coalesce(max(ts), 0)}
		})::VARCHAR FROM blocks WHERE rnd > 0 AND`+in)
		ranged("GET /v0/payouts/highest", `SELECT coalesce(to_json(list({'proposer': prop, 'rnd': rnd, 'ts': ts, 'pp': pp} ORDER BY pp DESC, rnd))::VARCHAR, '[]')
			FROM (SELECT prop, rnd, ts, pp FROM blocks WHERE prop IS NOT NULL AND`+in+` ORDER BY pp DESC, rnd LIMIT 10)`)
		// Most recently evicted first.
		ranged("GET /v0/evictions", `SELECT coalesce(to_json(list({'account': acct, 'evictions': n, 'rounds': rs} ORDER BY last DESC, acct))::VARCHAR, '[]')
			FROM (SELECT acct, count(*) AS n, list(rnd ORDER BY rnd DESC) AS rs, max(rnd) AS last FROM evictions WHERE`+in+` GROUP BY acct)`)
		ranged("GET /v0/evictions/{addr}", `SELECT coalesce(to_json(list(rnd ORDER BY rnd))::VARCHAR, '[]') FROM evictions WHERE acct = $3 AND`+in)
		light("GET /v0/round/{rnd}", func(w http.ResponseWriter, r *http.Request) {
			rnd, err := parseRound(r.PathValue("rnd"))
			if err != nil {
				http.Error(w, "rnd: "+err.Error(), http.StatusBadRequest)
				return
			}
			replySQL(w, r, db, `SELECT (CASE WHEN pp > 0 THEN to_json({'proposer': prop, 'rnd': rnd, 'ts': ts, 'pp': pp})
				ELSE to_json({'proposer': prop, 'rnd': rnd, 'ts': ts}) END)::VARCHAR FROM blocks WHERE rnd = $1::UINTEGER`, rnd)
		})
	}
	if track.VoterRounds > 0 {
		ranged("GET /v0/voters", `SELECT coalesce(to_json(list({'voter': voter, 'blocks': n} ORDER BY n DESC, voter))::VARCHAR, '[]')
			FROM (SELECT voter, count(*) AS n FROM voters WHERE`+in+` GROUP BY voter)`)
		ranged("GET /v0/voter/{addr}", `SELECT coalesce(to_json(list(rnd ORDER BY rnd))::VARCHAR, '[]') FROM voters WHERE voter = $3 AND`+in)
	}
	// limitedBy serves query's SQL with ?limit as $1, for an {addr} or {id} pattern that value as $2
	// (also passed to query), then query's args.
	limitedBy := func(reg func(string, http.HandlerFunc), pattern string, query func(ctx context.Context, key string) (string, []any, error)) {
		withAddr, withID := strings.Contains(pattern, "{addr}"), strings.Contains(pattern, "{id}")
		reg(pattern, func(w http.ResponseWriter, r *http.Request) {
			n, err := opt(r.URL.Query(), "limit", 1000, parseLimit)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			args := []any{n}
			switch {
			case withAddr:
				a, ok := addr(w, r)
				if !ok {
					return
				}
				args = append(args, a)
			case withID:
				id, ok := tmplID(w, r)
				if !ok {
					return
				}
				args = append(args, id)
			}
			key, _ := args[len(args)-1].(string)
			q, more, err := query(r.Context(), key)
			if err != nil {
				reply(w, r, nil, err)
				return
			}
			replySQL(w, r, db, q, append(args, more...)...)
		})
	}
	limited := func(reg func(string, http.HandlerFunc), pattern, q string) {
		limitedBy(reg, pattern, func(context.Context, string) (string, []any, error) { return q, nil, nil })
	}
	// name is the admin-set name of a template, or null: name + "<tmpl expr>)".
	const name = `(SELECT name FROM template_names n WHERE n.tmpl = `
	if track.Lsigs {
		// uses lists the newest $1 rows matching where; NULL if none. '' stands for NULL in signer/sender.
		uses := func(where string) string {
			const newest = " ORDER BY last_rnd DESC, prog, signer, sender"
			return `(SELECT list({'lsig': prog, 'template': tmpl, 'signer': nullif(signer, ''), 'sender': nullif(sender, ''),
				'txns': txns, 'firstRound': first_rnd, 'lastRound': last_rnd}` + newest + `)
				FROM (SELECT * FROM lsigs WHERE ` + where + newest + ` LIMIT $1))`
		}
		// Families of programs, and single programs delegated by several accounts.
		// Groups every row (~0.7s at 3M rows), and every visitor of algo.surf's /lsigs asks the same, so
		// the whole list is cached and a smaller limit is its first rows: one entry, whatever the limits.
		templates := newReplyCache(time.Minute)
		qTemplates := `SELECT coalesce(to_json(list({'template': tmpl, 'name': ` + name + `t.tmpl), 'programs': p, 'delegators': d, 'txns': n,
			'firstRound': f, 'lastRound': l} ORDER BY p DESC, tmpl))::VARCHAR, '[]')
			FROM (SELECT tmpl, count(DISTINCT prog) AS p, count(DISTINCT signer) FILTER (WHERE signer <> '') AS d, sum(txns) AS n,
				min(first_rnd) AS f, max(last_rnd) AS l FROM lsigs GROUP BY tmpl HAVING p >= 2 OR d >= 2 ORDER BY p DESC, tmpl LIMIT $1) t`
		allTemplates := func(ctx context.Context) (string, error) { return querySQL(ctx, db, qTemplates, maxLimit) }
		// counts are the cached templates list's program counts, parsed once per list. The list has
		// every template of 2 or more programs, so one missing has 1, unless it was cut at maxLimit
		// with its last still at 2 or more (most first): then complete is false. Reading programs from
		// it saves counting a big template's programs per request (~30 ms at 235k), but it lags by up to
		// the cache's minute.
		var (
			cmu             sync.Mutex
			cSrc            string
			cIDs            []string
			cNs             []uint64
			cByID           map[string]uint64
			cComplete, cSet bool
		)
		counts := func(ctx context.Context) (ids []string, ns []uint64, byID map[string]uint64, complete bool, err error) {
			s, err := templates.get(ctx, allTemplates)
			if err != nil {
				return nil, nil, nil, false, err
			}
			cmu.Lock()
			defer cmu.Unlock()
			if !cSet || s != cSrc {
				var list []struct {
					Template string
					Programs uint64
				}
				if err := json.Unmarshal([]byte(s), &list); err != nil {
					return nil, nil, nil, false, err
				}
				cIDs, cNs, cByID = make([]string, len(list)), make([]uint64, len(list)), make(map[string]uint64, len(list))
				for i, t := range list {
					cIDs[i], cNs[i], cByID[t.Template] = t.Template, t.Programs, t.Programs
				}
				cSrc, cSet = s, true
				cComplete = len(list) < maxLimit || list[len(list)-1].Programs < 2
			}
			return cIDs, cNs, cByID, cComplete, nil
		}
		// countSQL counts tmpl's programs, for when the list can't say: it failed (logged, as the
		// routes still answer) or is incomplete.
		countSQL := func(tmpl string, err error) string {
			if err != nil {
				slog.Warn("templates list: counting programs instead", "err", err)
			}
			return "(SELECT count(DISTINCT prog) FROM lsigs WHERE tmpl = " + tmpl + ")"
		}
		// Driven by the lsig's newest row, so one never seen has none: 404.
		// The lsig's template is found in SQL, so its count is looked up there: list $4 at its index in $3.
		limitedBy(light, "GET /v0/lsig/{addr}", func(ctx context.Context, _ string) (string, []any, error) {
			p, args := "", []any(nil)
			ids, ns, _, complete, err := counts(ctx)
			if err != nil || !complete {
				p = countSQL("l.tmpl", err)
			} else {
				p, args = "coalesce(list_extract($4::UBIGINT[], list_position($3::VARCHAR[], l.tmpl)), 1)", []any{ids, ns}
			}
			return `SELECT to_json({'template': tmpl, 'name': ` + name + `l.tmpl), 'programs': ` + p + `, 'uses': ` + uses("prog = $2") + `})::VARCHAR
				FROM (SELECT tmpl FROM lsigs WHERE prog = $2 ORDER BY last_rnd DESC LIMIT 1) l`, args, nil
		})
		limited(light, "GET /v0/delegations/{addr}", `SELECT coalesce(to_json(`+uses("signer = $2 OR sender = $2")+`)::VARCHAR, '[]')`)
		heavy("GET /v0/templates", func(w http.ResponseWriter, r *http.Request) {
			n, err := opt(r.URL.Query(), "limit", 1000, parseLimit)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s, err := templates.get(r.Context(), allTemplates)
			if err == nil {
				s, err = firstN(s, n)
			}
			replyJSON(w, r, s, err)
		})
		limitedBy(light, "GET /v0/template/{id}", func(ctx context.Context, id string) (string, []any, error) {
			p, args := "", []any(nil)
			_, _, byID, complete, err := counts(ctx)
			if err != nil || !complete {
				p = countSQL("$2", err)
			} else {
				p, args = "$3::UBIGINT", []any{max(1, byID[id])}
			}
			return `SELECT to_json({'template': tmpl, 'name': ` + name + `$2), 'logic': logic, 'programs': ` + p + `,
				'uses': ` + uses("tmpl = $2") + `})::VARCHAR FROM lsig_templates WHERE tmpl = $2`, args, nil
		})
		if adminToken != "" {
			adminRoutes(mux, db, templates.clear) // a rename shows at once
		}
	}
	if track.Msigs {
		// A row's round and index point at one txn: payset[index] of that block, the first the signer
		// signed, or while it never has, the first seen. Newest first.
		// ponytail: no index, so each request scans msig_signers (ms at ~1M rows); add one if it gets big.
		limited(light, "GET /v0/subsigner/{addr}", `SELECT coalesce(to_json(list({'msig': msig, 'signed': signed, 'round': rnd, 'index': pos}
			ORDER BY rnd DESC, pos DESC, msig))::VARCHAR, '[]')
			FROM (SELECT * FROM msig_signers WHERE signer = $2 ORDER BY rnd DESC, pos DESC, msig LIMIT $1)`)
		limited(light, "GET /v0/msig/{addr}", `SELECT coalesce(to_json(list({'signer': signer, 'signed': signed, 'round': rnd, 'index': pos}
			ORDER BY rnd DESC, pos DESC, signer))::VARCHAR, '[]')
			FROM (SELECT * FROM msig_signers WHERE msig = $2 ORDER BY rnd DESC, pos DESC, signer LIMIT $1)`)
	}
	if track.Proposers || track.VoterRounds > 0 {
		light("GET /v0/exists/{addr}", func(w http.ResponseWriter, r *http.Request) {
			if a, ok := addr(w, r); ok {
				replySQL(w, r, db, `SELECT to_json({'exists': EXISTS (SELECT 1 FROM blocks WHERE prop = $1)
					OR EXISTS (SELECT 1 FROM voters WHERE voter = $1) OR EXISTS (SELECT 1 FROM evictions WHERE acct = $1)})::VARCHAR`, a)
			}
		})
	}
	// What algo.surf's account page needs to pick its tabs before fetching any list: a scan of each
	// tracked table (~40–50 ms on mainnet), against one request per list. Keys of untracked features are
	// left out; an address never seen gets zeros.
	if track.Lsigs || track.Msigs || track.Proposers || track.VoterRounds > 0 {
		var keys []string
		from := ""
		if track.Lsigs {
			keys = append(keys, `'lsig': (SELECT {'template': tmpl, 'name': `+name+`l.tmpl)}
				FROM (SELECT tmpl FROM lsigs WHERE prog = $1 ORDER BY last_rnd DESC LIMIT 1) l)`,
				`'delegations': (SELECT count(*) FROM lsigs WHERE signer = $1 OR sender = $1)`)
		}
		if track.Msigs { // both counts in one scan
			keys = append(keys, `'multisigs': m.multisigs`, `'signers': m.signers`)
			from = ` FROM (SELECT count(*) FILTER (WHERE signer = $1) AS multisigs, count(*) FILTER (WHERE msig = $1) AS signers
				FROM msig_signers WHERE signer = $1 OR msig = $1) m`
		}
		if track.Proposers {
			keys = append(keys, `'proposed': EXISTS (SELECT 1 FROM blocks WHERE prop = $1)`,
				`'evicted': EXISTS (SELECT 1 FROM evictions WHERE acct = $1)`)
		}
		if track.VoterRounds > 0 {
			keys = append(keys, `'voted': EXISTS (SELECT 1 FROM voters WHERE voter = $1)`)
		}
		q := `SELECT to_json({` + strings.Join(keys, ", ") + `})::VARCHAR` + from
		light("GET /v0/account/{addr}", func(w http.ResponseWriter, r *http.Request) {
			if a, ok := addr(w, r); ok {
				replySQL(w, r, db, q, a)
			}
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, sw, key := time.Now(), &statusWriter{w, http.StatusOK}, lim.clientKey(r)
		// Set before any reply, 429s and 503s included, or browsers hide them from algo.surf.
		sw.Header().Set("Access-Control-Allow-Origin", "*")
		sw.Header().Set("Access-Control-Expose-Headers", "Retry-After, X-Truncated")
		if d, ok := lim.allow(key, false); !ok {
			retryLater(sw, http.StatusTooManyRequests, d)
		} else {
			ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
			mux.ServeHTTP(sw, r.WithContext(ctx))
			cancel()
		}
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery, "status", sw.status,
			"ms", float64(time.Since(t).Microseconds())/1000, "remote", r.RemoteAddr, "client", key)
	})
}

// statusWriter records the status code for the request log.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
