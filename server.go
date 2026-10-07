package main

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
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
		log.Print(err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
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

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		mux.ServeHTTP(w, r)
	})
}
