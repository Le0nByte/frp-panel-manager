package auth

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Default login-guard tuning (spec §10.7: 登录接口限速、失败锁定).
const (
	DefaultMaxFailures        = 5
	DefaultLockoutDuration    = 15 * time.Minute
	DefaultFailureWindow      = 15 * time.Minute
	DefaultRateLimitAttempts  = 20
	DefaultRateLimitWindow    = 10 * time.Minute
	rateLimitedMessage        = "too many login attempts, please try again later"
	invalidCredentialsMessage = "invalid username or password"
)

// Clock provides the current time; injectable so tests can advance time
// deterministically without sleeping.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Option configures a LoginGuard.
type Option func(*LoginGuard)

// WithClock injects the time source (tests).
func WithClock(c Clock) Option { return func(g *LoginGuard) { g.clock = c } }

// WithMaxFailures overrides the consecutive-failure lockout threshold.
func WithMaxFailures(n int) Option { return func(g *LoginGuard) { g.maxFailures = n } }

// WithLockoutDuration overrides how long a locked key stays locked.
func WithLockoutDuration(d time.Duration) Option {
	return func(g *LoginGuard) { g.lockoutDuration = d }
}

// WithFailureWindow overrides how long failures stay "consecutive".
func WithFailureWindow(d time.Duration) Option { return func(g *LoginGuard) { g.failureWindow = d } }

// WithRateLimit overrides the per-window attempt ceiling.
func WithRateLimit(maxAttempts int, window time.Duration) Option {
	return func(g *LoginGuard) {
		g.maxAttempts = maxAttempts
		g.rateWindow = window
	}
}

// LoginGuard is a thread-safe, in-process keyed guard for login attempts:
// per (normalized username, client IP) key it rate-limits attempts and
// locks the key after consecutive failures. NOTE: process-local only —
// it does not provide cross-instance protection in HA deployments.
type LoginGuard struct {
	mu sync.Mutex

	clock           Clock
	maxFailures     int
	lockoutDuration time.Duration
	failureWindow   time.Duration
	maxAttempts     int
	rateWindow      time.Duration

	entries map[string]*loginEntry
}

type loginEntry struct {
	failures     int
	firstFailure time.Time
	lockedUntil  time.Time
	attempts     int
	windowStart  time.Time
}

// NewLoginGuard builds a guard with the default tuning.
func NewLoginGuard(opts ...Option) *LoginGuard {
	g := &LoginGuard{
		clock:           systemClock{},
		maxFailures:     DefaultMaxFailures,
		lockoutDuration: DefaultLockoutDuration,
		failureWindow:   DefaultFailureWindow,
		maxAttempts:     DefaultRateLimitAttempts,
		rateWindow:      DefaultRateLimitWindow,
		entries:         make(map[string]*loginEntry),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

func (g *LoginGuard) entry(key string) *loginEntry {
	e, ok := g.entries[key]
	if !ok {
		e = &loginEntry{}
		g.entries[key] = e
	}
	return e
}

// Allow reports whether an attempt for key may proceed: false while the key
// is locked or the rate-limit window is exhausted. It counts the attempt.
func (g *LoginGuard) Allow(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	e := g.entry(key)
	if now.Before(e.lockedUntil) {
		return false
	}
	if e.windowStart.IsZero() || now.Sub(e.windowStart) >= g.rateWindow {
		e.windowStart = now
		e.attempts = 0
	}
	if e.attempts >= g.maxAttempts {
		return false
	}
	e.attempts++
	return true
}

// RecordFailure records one failed attempt for key, locking the key once
// maxFailures consecutive failures (within the failure window) accumulate.
func (g *LoginGuard) RecordFailure(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	e := g.entry(key)
	if now.Before(e.lockedUntil) {
		return // already locked; do not extend
	}
	if e.firstFailure.IsZero() || now.Sub(e.firstFailure) >= g.failureWindow {
		e.failures = 1
		e.firstFailure = now
	} else {
		e.failures++
	}
	if e.failures >= g.maxFailures {
		e.lockedUntil = now.Add(g.lockoutDuration)
		e.failures = 0
		e.firstFailure = time.Time{}
	}
}

// RecordSuccess clears the failure/lockout state for key.
func (g *LoginGuard) RecordSuccess(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.entries, key)
}

// LoginKey builds the guard key from a normalized username and the request
// client IP. Normalization (trim + lowercase) prevents trivial case/space
// aliasing of the same account.
func LoginKey(username, clientIP string) string {
	u := strings.ToLower(strings.TrimSpace(username))
	return u + "/" + clientIP
}

// ClientHostKey returns the TCP peer host from r.RemoteAddr. Forwarding
// headers (X-Forwarded-For / X-Real-IP) are deliberately ignored: gin's
// ClientIP() honors them when trusted proxies are not pinned, letting a
// client rotate its perceived IP and sidestep the login guard (review I1).
// The RemoteAddr host is set by the transport, not the client. When
// RemoteAddr has no host:port shape (SplitHostPort fails), the raw value is
// used so the key never degrades to empty.
func ClientHostKey(r *http.Request) string {
	if r == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
