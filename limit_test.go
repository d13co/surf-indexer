package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimit(t *testing.T) {
	defer func(c limitConfig, tr config, tip uint64) { limits, track = c, tr; storedTip.Store(tip) }(limits, track, storedTip.Load())
	track = config{Proposers: true}
	db, err := openDB("", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	get := func(h http.Handler, url, remote string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", url, nil)
		r.RemoteAddr = remote
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	// All routes: a burst of 5x the rate, then 429s that browsers can read. At 1/s, the 6 requests
	// have a second before a token comes back, so a loaded machine can't make this flaky.
	limits = limitConfig{Rate: 1}
	h := routes(db)
	for i := range 5 {
		if w := get(h, "/v0/status", "192.0.2.1:1"); w.Code != 200 {
			t.Fatalf("request %d = %d, want 200", i+1, w.Code)
		}
	}
	w := get(h, "/v0/status", "192.0.2.1:1")
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" || w.Header().Get("Access-Control-Allow-Origin") != "*" ||
		w.Header().Get("Access-Control-Expose-Headers") != "Retry-After, X-Truncated" {
		t.Errorf("request 6 = %d %v, want 429 with Retry-After 1 and CORS headers", w.Code, w.Header())
	}
	if w := get(h, "/v0/status", "192.0.2.2:1"); w.Code != 200 {
		t.Errorf("another client = %d, want 200", w.Code)
	}

	// Heavy routes: their own burst of 10, while light ones still pass.
	limits = limitConfig{Rate: 1000}
	h = routes(db)
	for i := range 10 {
		if w := get(h, "/v0/fees", "192.0.2.1:1"); w.Code != 200 {
			t.Fatalf("heavy request %d = %d, want 200", i+1, w.Code)
		}
	}
	if w := get(h, "/v0/fees", "192.0.2.1:1"); w.Code != 429 || w.Header().Get("Retry-After") != "2" {
		t.Errorf("heavy request 11 = %d %v, want 429 with Retry-After 2", w.Code, w.Header())
	}
	if w := get(h, "/v0/fees/24h", "192.0.2.1:1"); w.Code != 200 {
		t.Errorf("light request after heavy 429 = %d, want 200", w.Code)
	}

	// Ranged routes are heavy only over heavyRounds: a recent window isn't, all of history is.
	storedTip.Store(2 * heavyRounds)
	h = routes(db)
	recent := fmt.Sprintf("/v0/proposers?minRound=%d", 2*heavyRounds-900000)
	for i := range 20 {
		if w := get(h, recent, "192.0.2.1:1"); w.Code != 200 {
			t.Fatalf("recent ranged request %d = %d, want 200", i+1, w.Code)
		}
	}
	for i := range 10 {
		if w := get(h, "/v0/proposers", "192.0.2.1:1"); w.Code != 200 {
			t.Fatalf("lifetime ranged request %d = %d, want 200", i+1, w.Code)
		}
	}
	if w := get(h, "/v0/proposers", "192.0.2.1:1"); w.Code != 429 {
		t.Errorf("lifetime ranged request 11 = %d, want 429", w.Code)
	}
	if w := get(h, recent, "192.0.2.1:1"); w.Code != 200 {
		t.Errorf("recent ranged request after lifetime 429 = %d, want 200", w.Code)
	}

	// Off: no limits.
	limits = limitConfig{}
	h = routes(db)
	for i := range 100 {
		if w := get(h, "/v0/fees", "192.0.2.1:1"); w.Code != 200 {
			t.Fatalf("unlimited request %d = %d, want 200", i+1, w.Code)
		}
	}
}

func TestClientKey(t *testing.T) {
	l := newLimiter(limitConfig{RealIPHeader: "CF-Connecting-IP"})
	for _, c := range []struct{ remote, header, want string }{
		{"127.0.0.1:5", "203.0.113.5", "203.0.113.5"},  // cloudflared on the host
		{"172.17.0.2:5", "203.0.113.5", "203.0.113.5"}, // cloudflared in docker
		{"[::1]:5", "203.0.113.5", "203.0.113.5"},
		{"[::ffff:127.0.0.1]:5", "203.0.113.5", "203.0.113.5"},
		{"198.51.100.1:5", "203.0.113.5", "198.51.100.1"}, // public peer: header ignored
		{"127.0.0.1:5", "", "127.0.0.1"},                  // missing: the peer
		{"127.0.0.1:5", "nope", "127.0.0.1"},
		{"127.0.0.1:5", "2001:db8:1:2:3::4", "2001:db8:1:2::/64"}, // IPv6 by /64
		{"[2001:db8:1:2::9]:5", "", "2001:db8:1:2::/64"},
		{"[::1]:5", "", "::1"},
		{"127.0.0.1:5", "::ffff:203.0.113.5", "203.0.113.5"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		if c.header != "" {
			r.Header.Set("CF-Connecting-IP", c.header)
		}
		if got := l.clientKey(r); got != c.want {
			t.Errorf("clientKey(%s, %q) = %s, want %s", c.remote, c.header, got, c.want)
		}
	}
	// Without REAL_IP_HEADER the header is never read.
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:5"
	r.Header.Set("CF-Connecting-IP", "203.0.113.5")
	if got := newLimiter(limitConfig{}).clientKey(r); got != "127.0.0.1" {
		t.Errorf("clientKey without REAL_IP_HEADER = %s, want 127.0.0.1", got)
	}
}

func TestHeavySlots(t *testing.T) {
	defer func(d time.Duration) { heavyWait = d }(heavyWait)
	heavyWait = 10 * time.Millisecond
	l := newLimiter(limitConfig{Heavy: 1})
	ran := 0
	h := l.heavy(func(w http.ResponseWriter, r *http.Request) {
		ran++
		if len(l.slots) != 1 {
			t.Errorf("slot not held while querying")
		}
		reply(w, r, 1, nil)
		if len(l.slots) != 0 {
			t.Errorf("slot still held after the query, while the reply is written")
		}
	})
	serve := func() int {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest("GET", "/", nil))
		return w.Code
	}
	if code := serve(); code != 200 || ran != 1 {
		t.Fatalf("free slot = %d, ran %d, want 200, ran 1", code, ran)
	}
	l.slots <- struct{}{} // held by another request
	if code := serve(); code != 503 || ran != 1 {
		t.Errorf("all slots busy = %d, ran %d, want 503, not run", code, ran)
	}
	<-l.slots
	if code := serve(); code != 200 || ran != 2 {
		t.Errorf("slot freed = %d, ran %d, want 200, ran 2", code, ran)
	}
}

func TestFailTimeout(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	w := httptest.NewRecorder()
	fail(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx), errors.New("INTERRUPT Error"))
	if w.Code != 503 || w.Header().Get("Retry-After") == "" {
		t.Errorf("timed out = %d %v, want 503 with Retry-After", w.Code, w.Header())
	}
	w = httptest.NewRecorder()
	fail(w, httptest.NewRequest("GET", "/", nil), errors.New("boom"))
	if w.Code != 500 {
		t.Errorf("failed = %d, want 500", w.Code)
	}
}

func TestReplyCache(t *testing.T) {
	c := newReplyCache(time.Hour)
	fetches := 0
	fetch := func(context.Context) (string, error) { fetches++; return "v", nil }
	ctx := context.Background()
	for range 3 {
		if s, err := c.get(ctx, fetch); s != "v" || err != nil {
			t.Fatalf("get = %q %v", s, err)
		}
	}
	if fetches != 1 {
		t.Errorf("fetches = %d, want 1", fetches)
	}
	c.clear()
	c.get(ctx, fetch)
	if fetches != 2 {
		t.Errorf("fetches after clear = %d, want 2", fetches)
	}
	c.clear()
	boom := errors.New("boom")
	if _, err := c.get(ctx, func(context.Context) (string, error) { return "", boom }); err != boom {
		t.Errorf("get error = %v, want boom", err)
	}
	if s, _ := c.get(ctx, fetch); s != "v" || fetches != 3 {
		t.Errorf("an error was cached: get = %q, fetches %d", s, fetches)
	}
	c = newReplyCache(0) // expired at once
	c.get(ctx, fetch)
	c.get(ctx, fetch)
	if fetches != 5 {
		t.Errorf("fetches with ttl 0 = %d, want 5", fetches)
	}
	// A fetch running across a clear doesn't store its pre-clear reply.
	c = newReplyCache(time.Hour)
	c.get(ctx, func(context.Context) (string, error) { c.clear(); return "old", nil })
	if s, _ := c.get(ctx, fetch); s != "v" {
		t.Errorf("after a clear during the fetch: get = %q, want a new fetch", s)
	}
}

func TestFirstN(t *testing.T) {
	for _, c := range []struct {
		s    string
		n    uint64
		want string
	}{
		{`[{"a":"<1>"},{"a":2},{"a":3}]`, 2, `[{"a":"<1>"},{"a":2}]`},
		{`[{"a":1},{"a":2}]`, 2, `[{"a":1},{"a":2}]`},
		{`[]`, 5, `[]`},
	} {
		if got, err := firstN(c.s, c.n); err != nil || got != c.want {
			t.Errorf("firstN(%s, %d) = %s %v, want %s", c.s, c.n, got, err, c.want)
		}
	}
}
