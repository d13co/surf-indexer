package main

import (
	"cmp"
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

func parseLimit(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err == nil && (n < 1 || n > 100000) {
		err = errors.New("want 1 to 100000")
	}
	return n, err
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

func reply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		slog.Error("reply", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// replySQL writes q's single JSON value, built by DuckDB as to_json(...)::VARCHAR (uncast, the
// driver would unmarshal it). No row is a 404. q takes $n params: a repeated $1 binds one arg.
func replySQL(w http.ResponseWriter, r *http.Request, db *sql.DB, q string, args ...any) {
	var s string
	err := db.QueryRowContext(r.Context(), q, args...).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		slog.Error("reply", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, s)
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
		reply(w, nil, err)
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

var periods = map[string]time.Duration{"24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour}

func routes(db *sql.DB) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v0/status", func(w http.ResponseWriter, r *http.Request) {
		n, _, hi, err := rounds(r.Context(), db)
		reply(w, map[string]int64{"ok": 1, "maxRound": hi, "records": n}, err)
	})

	mux.HandleFunc("GET /v0/block", func(w http.ResponseWriter, r *http.Request) {
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
		reply(w, b, err)
	})

	mux.HandleFunc("GET /v0/fees", func(w http.ResponseWriter, r *http.Request) {
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
		reply(w, s, err)
	})

	mux.HandleFunc("GET /v0/fees/{period}", func(w http.ResponseWriter, r *http.Request) {
		d, ok := periods[r.PathValue("period")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		s, err := sumFees(r.Context(), db, 0, math.MaxUint32, uint64(time.Now().Add(-d).Unix()), math.MaxUint32)
		reply(w, s, err)
	})

	mux.HandleFunc("GET /v0/series", func(w http.ResponseWriter, r *http.Request) {
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
		s, err := series(r.Context(), db, iv, start, end)
		reply(w, s, err)
	})

	mux.HandleFunc("GET /v0/upgrades", func(w http.ResponseWriter, r *http.Request) {
		u, err := queryUpgrades(r.Context(), db, "ORDER BY rnd")
		reply(w, u, err)
	})

	// ranged serves q with the round range as $1, $2 and, for an {addr} pattern, the address as $3.
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
		mux.HandleFunc("GET /v0/round/{rnd}", func(w http.ResponseWriter, r *http.Request) {
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
	if track.Lsigs {
		// limited serves q with ?limit as $1 and, for an {addr} or {id} pattern, that value as $2.
		limited := func(pattern, q string) {
			withAddr, withID := strings.Contains(pattern, "{addr}"), strings.Contains(pattern, "{id}")
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
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
					id := r.PathValue("id")
					if len(id) != 16 || strings.Trim(id, "0123456789abcdef") != "" {
						http.Error(w, "id: want 16 lowercase hex digits", http.StatusBadRequest)
						return
					}
					args = append(args, id)
				}
				replySQL(w, r, db, q, args...)
			})
		}
		// uses lists the newest $1 rows matching where; NULL if none. '' stands for NULL in signer/sender.
		uses := func(where string) string {
			const newest = " ORDER BY last_rnd DESC, prog, signer, sender"
			return `(SELECT list({'lsig': prog, 'template': tmpl, 'signer': nullif(signer, ''), 'sender': nullif(sender, ''),
				'txns': txns, 'firstRound': first_rnd, 'lastRound': last_rnd}` + newest + `)
				FROM (SELECT * FROM lsigs WHERE ` + where + newest + ` LIMIT $1))`
		}
		// Driven by the lsig's newest row, so one never seen has none: 404.
		limited("GET /v0/lsig/{addr}", `SELECT to_json({'template': tmpl,
			'programs': (SELECT count(DISTINCT prog) FROM lsigs WHERE tmpl = l.tmpl), 'uses': `+uses("prog = $2")+`})::VARCHAR
			FROM (SELECT tmpl FROM lsigs WHERE prog = $2 ORDER BY last_rnd DESC LIMIT 1) l`)
		limited("GET /v0/delegations/{addr}", `SELECT coalesce(to_json(`+uses("signer = $2 OR sender = $2")+`)::VARCHAR, '[]')`)
		// Families of programs, and single programs delegated by several accounts.
		// ponytail: groups every row per request (~0.7s at 3M rows); cache it if it gets hot.
		limited("GET /v0/templates", `SELECT coalesce(to_json(list({'template': tmpl, 'programs': p, 'delegators': d, 'txns': n,
			'firstRound': f, 'lastRound': l} ORDER BY p DESC, tmpl))::VARCHAR, '[]')
			FROM (SELECT tmpl, count(DISTINCT prog) AS p, count(DISTINCT signer) FILTER (WHERE signer <> '') AS d, sum(txns) AS n,
				min(first_rnd) AS f, max(last_rnd) AS l FROM lsigs GROUP BY tmpl HAVING p >= 2 OR d >= 2 ORDER BY p DESC, tmpl LIMIT $1)`)
		limited("GET /v0/template/{id}", `SELECT to_json({'template': tmpl, 'logic': logic,
			'programs': (SELECT count(DISTINCT prog) FROM lsigs WHERE tmpl = $2), 'uses': `+uses("tmpl = $2")+`})::VARCHAR
			FROM lsig_templates WHERE tmpl = $2`)
	}
	if track.Proposers || track.VoterRounds > 0 {
		mux.HandleFunc("GET /v0/exists/{addr}", func(w http.ResponseWriter, r *http.Request) {
			if a, ok := addr(w, r); ok {
				replySQL(w, r, db, `SELECT to_json({'exists': EXISTS (SELECT 1 FROM blocks WHERE prop = $1)
					OR EXISTS (SELECT 1 FROM voters WHERE voter = $1)})::VARCHAR`, a)
			}
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, sw := time.Now(), &statusWriter{w, http.StatusOK}
		sw.Header().Set("Access-Control-Allow-Origin", "*")
		mux.ServeHTTP(sw, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery, "status", sw.status,
			"ms", float64(time.Since(t).Microseconds())/1000, "remote", r.RemoteAddr)
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
