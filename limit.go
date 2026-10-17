package main

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

// limitConfig is set from env at startup. The zero value limits nothing, as tests expect.
type limitConfig struct {
	Rate         int    // per-client requests/s across all routes, burst 5x; 0: no per-client limits
	Heavy        int    // heavy requests running at once, across all clients; 0: no cap
	RealIPHeader string // header holding the visitor IP, trusted only from a loopback or private peer
}

var limits limitConfig

const (
	heavyEvery   = 2 * time.Second // per-client heavy rate: 30/min
	heavyBurst   = 10
	queryTimeout = 30 * time.Second // every request's context; a query past it is interrupted
	clientIdle   = 10 * time.Minute // a client unseen this long is dropped; its buckets are full again by then
)

var heavyWait = 10 * time.Second // longest wait for a heavy slot before a 503; a var for tests

type client struct {
	all, heavy *rate.Limiter
	seen       time.Time
}

// limiter holds per-client token buckets and the heavy-route slots. One per routes() call.
type limiter struct {
	cfg     limitConfig
	mu      sync.Mutex
	clients map[string]*client
	swept   time.Time
	slots   chan struct{} // nil: no cap
	warn    sync.Once
}

func newLimiter(cfg limitConfig) *limiter {
	l := &limiter{cfg: cfg, clients: map[string]*client{}}
	if cfg.Heavy > 0 {
		l.slots = make(chan struct{}, cfg.Heavy)
	}
	return l
}

// clientKey is the visitor's IP, an IPv6 one as its /64. REAL_IP_HEADER is used only when the peer is
// loopback or private (cloudflared), so a client reaching the port directly can't pick its own key.
func (l *limiter) clientKey(r *http.Request) string {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	ip := ap.Addr().Unmap()
	if h := l.cfg.RealIPHeader; h != "" && (ip.IsLoopback() || ip.IsPrivate()) {
		if v, err := netip.ParseAddr(strings.TrimSpace(r.Header.Get(h))); err == nil {
			ip = v.Unmap()
		} else {
			l.warn.Do(func() {
				slog.Warn("no visitor IP in REAL_IP_HEADER: limiting by peer address, which visitors behind the proxy share",
					"header", h, "value", r.Header.Get(h), "remote", r.RemoteAddr)
			})
		}
	}
	if ip.Is6() && !ip.IsLoopback() {
		p, _ := ip.Prefix(64)
		return p.String()
	}
	return ip.String()
}

// allow takes a token from the client's all-routes bucket, or for heavy its heavy bucket. When it is
// empty it takes none and returns how long until one is free.
func (l *limiter) allow(key string, heavy bool) (time.Duration, bool) {
	if l.cfg.Rate <= 0 {
		return 0, true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > time.Minute {
		for k, c := range l.clients {
			if now.Sub(c.seen) > clientIdle {
				delete(l.clients, k)
			}
		}
		l.swept = now
	}
	c := l.clients[key]
	if c == nil {
		c = &client{all: rate.NewLimiter(rate.Limit(l.cfg.Rate), 5*l.cfg.Rate), heavy: rate.NewLimiter(rate.Every(heavyEvery), heavyBurst)}
		l.clients[key] = c
	}
	c.seen = now
	b := c.all
	if heavy {
		b = c.heavy
	}
	res := b.ReserveN(now, 1)
	if d := res.DelayFrom(now); d > 0 {
		res.CancelAt(now)
		return d, false
	}
	return 0, true
}

type slotKey struct{}

// admit lets a heavy request in: a token from the client's heavy bucket, then one of the global
// slots, held until its DB work is done (queried) or release. It returns the request carrying the
// slot. Not ok: it replied 429 or 503, or the client left while waiting.
func (l *limiter) admit(w http.ResponseWriter, r *http.Request) (_ *http.Request, release func(), ok bool) {
	if d, ok := l.allow(l.clientKey(r), true); !ok {
		retryLater(w, http.StatusTooManyRequests, d)
		return r, nil, false
	}
	if l.slots == nil {
		return r, func() {}, true
	}
	t := time.NewTimer(heavyWait)
	defer t.Stop()
	select {
	case l.slots <- struct{}{}:
	case <-t.C:
		retryLater(w, http.StatusServiceUnavailable, 2*time.Second)
		return r, nil, false
	case <-r.Context().Done():
		return r, nil, false // client gone or out of time; nothing to reply to
	}
	release = sync.OnceFunc(func() { <-l.slots })
	return r.WithContext(context.WithValue(r.Context(), slotKey{}, release)), release, true
}

// heavy guards an expensive route with admit.
func (l *limiter) heavy(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r, release, ok := l.admit(w, r)
		if !ok {
			return
		}
		defer release()
		h(w, r)
	}
}

// queried frees r's heavy slot, if it holds one, once its DB work is done and before its reply is
// written: a client that reads a large reply slowly, or never, mustn't keep other heavy requests out.
func queried(r *http.Request) {
	if release, ok := r.Context().Value(slotKey{}).(func()); ok {
		release()
	}
}

// retryLater replies code with Retry-After in whole seconds, at least 1.
func retryLater(w http.ResponseWriter, code int, d time.Duration) {
	w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(d.Seconds())))))
	http.Error(w, http.StatusText(code), code)
}

// replyCache keeps one reply for ttl, with one fetch in flight.
type replyCache struct {
	ttl  time.Duration
	mu   sync.Mutex
	body string
	at   time.Time // zero: none
	gen  uint64    // bumped by clear, so a fetch started before it doesn't store its reply
	sf   singleflight.Group
}

func newReplyCache(ttl time.Duration) *replyCache { return &replyCache{ttl: ttl} }

// get returns the reply, fetching it if missing or expired. The fetch runs on its own context,
// so one caller leaving doesn't fail the others waiting on it. Errors aren't cached.
func (c *replyCache) get(ctx context.Context, fetch func(context.Context) (string, error)) (string, error) {
	c.mu.Lock()
	body, at := c.body, c.at
	c.mu.Unlock()
	if !at.IsZero() && time.Since(at) < c.ttl {
		return body, nil
	}
	v, err, _ := c.sf.Do("", func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
		defer cancel()
		c.mu.Lock()
		gen, at := c.gen, time.Now()
		c.mu.Unlock()
		s, err := fetch(fctx)
		c.mu.Lock()
		if err == nil && gen == c.gen {
			c.body, c.at = s, at
		}
		c.mu.Unlock()
		return s, err
	})
	s, _ := v.(string)
	return s, err
}

// clear drops the reply, e.g. after an admin edit changes it.
func (c *replyCache) clear() {
	c.mu.Lock()
	c.body, c.at = "", time.Time{}
	c.gen++
	c.mu.Unlock()
}
