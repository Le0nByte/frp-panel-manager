package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// fakeClock is an injectable Clock that the tests advance without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newGuard(t *testing.T, clock Clock, opts ...Option) *LoginGuard {
	t.Helper()
	all := append([]Option{WithClock(clock)}, opts...)
	return NewLoginGuard(all...)
}

// ---- guard unit tests (real guard, injected clock, no sleeps) ----

func TestLoginGuardAllowsWithinLimits(t *testing.T) {
	g := NewLoginGuard()
	for i := 0; i < 5; i++ {
		require.True(t, g.Allow("u/1.2.3.4"), "attempt %d must be allowed", i+1)
	}
}

func TestLoginGuardLocksAfterFiveFailures(t *testing.T) {
	g := NewLoginGuard()
	key := "admin/1.2.3.4"
	for i := 0; i < 4; i++ {
		g.RecordFailure(key)
		require.True(t, g.Allow(key), "must still allow before the 5th failure")
	}
	g.RecordFailure(key)
	require.False(t, g.Allow(key), "5th consecutive failure must lock the key")
}

func TestLoginGuardLockoutExpires(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	g := newGuard(t, clock)
	key := "admin/1.2.3.4"
	for i := 0; i < 5; i++ {
		g.RecordFailure(key)
	}
	require.False(t, g.Allow(key))

	clock.advance(15*time.Minute - time.Second)
	require.False(t, g.Allow(key), "still locked before lockout elapses")
	clock.advance(2 * time.Second)
	require.True(t, g.Allow(key), "lockout must expire after 15 minutes")
}

func TestLoginGuardFailureWindowResetsStaleFailures(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	g := newGuard(t, clock)
	key := "admin/1.2.3.4"
	for i := 0; i < 4; i++ {
		g.RecordFailure(key)
	}
	// failures older than the window no longer count as consecutive
	clock.advance(16 * time.Minute)
	g.RecordFailure(key)
	require.True(t, g.Allow(key), "stale failures must not contribute to the lockout")
}

func TestLoginGuardSuccessResetsFailureCount(t *testing.T) {
	g := NewLoginGuard()
	key := "admin/1.2.3.4"
	for i := 0; i < 4; i++ {
		g.RecordFailure(key)
	}
	g.RecordSuccess(key)
	for i := 0; i < 4; i++ {
		g.RecordFailure(key)
		require.True(t, g.Allow(key), "a success must reset the consecutive failure count")
	}
	g.RecordFailure(key)
	require.False(t, g.Allow(key), "5 fresh failures after a success must lock again")
}

func TestLoginGuardRateLimitWindow(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	g := newGuard(t, clock, WithRateLimit(3, 10*time.Minute))
	key := "admin/1.2.3.4"
	for i := 0; i < 3; i++ {
		require.True(t, g.Allow(key))
	}
	require.False(t, g.Allow(key), "4th attempt inside the rate window must be rejected")
	clock.advance(10*time.Minute + time.Second)
	require.True(t, g.Allow(key), "rate window must roll over")
}

func TestLoginGuardKeyIsolation(t *testing.T) {
	g := NewLoginGuard()
	keyA := "admin/1.2.3.4"
	keyB := "admin/5.6.7.8"
	for i := 0; i < 5; i++ {
		g.RecordFailure(keyA)
	}
	require.False(t, g.Allow(keyA))
	require.True(t, g.Allow(keyB), "a different IP must not be locked")
}

func TestLoginGuardConcurrentFailuresLockDeterministically(t *testing.T) {
	g := NewLoginGuard()
	key := "admin/1.2.3.4"
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.RecordFailure(key)
		}()
	}
	wg.Wait()
	require.False(t, g.Allow(key), "20 concurrent failures must lock the key")
}

func TestLoginGuardConcurrentSuccessWins(t *testing.T) {
	// exactly maxFailures-1 concurrent failures plus one success: whatever
	// the interleaving, the success resets the counter, so the key must
	// never lock.
	g := NewLoginGuard()
	key := "admin/1.2.3.4"
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.RecordFailure(key)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.RecordSuccess(key)
	}()
	wg.Wait()
	require.True(t, g.Allow(key), "success must reset failures under concurrency")
}

func TestLoginKeyNormalization(t *testing.T) {
	require.Equal(t, LoginKey("  Admin ", "1.2.3.4"), LoginKey("admin", "1.2.3.4"),
		"username must be normalized (trim+lowercase)")
	require.NotEqual(t, LoginKey("admin", "1.2.3.4"), LoginKey("admin", "5.6.7.8"),
		"client IP must be part of the key")
	require.NotEqual(t, LoginKey("admin", "1.2.3.4"), LoginKey("root", "1.2.3.4"),
		"username must be part of the key")
	require.NotEmpty(t, LoginKey("admin", ""), "empty IP must still yield a usable key")
}

// ---- S4a review regression (I1: XFF spoofing must not reach the key) ----

// TestClientHostKeyUsesRemoteAddrNotForwardedHeaders: the guard key must
// derive from the TCP peer (RemoteAddr), never from client-spoofable
// forwarding headers (review I1 — gin ClientIP() honors X-Forwarded-For when
// proxies are not pinned, letting an attacker rotate the perceived IP).
func TestClientHostKeyUsesRemoteAddrNotForwardedHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	req.RemoteAddr = "203.0.113.7:51234"
	req.Header.Set("X-Forwarded-For", "6.6.6.6, 7.7.7.7")
	req.Header.Set("X-Real-IP", "8.8.8.8")

	require.Equal(t, "203.0.113.7", ClientHostKey(req), "host must come from RemoteAddr, not forwarded headers")
	require.Equal(t, LoginKey("admin", "203.0.113.7"), LoginKey("admin", ClientHostKey(req)),
		"forged forwarding headers must not alter the guard key")
	require.NotEqual(t, LoginKey("admin", "6.6.6.6"), LoginKey("admin", ClientHostKey(req)),
		"an XFF-supplied address must not be accepted as the key")
}

// TestClientHostKeyFallback: when RemoteAddr lacks a host:port shape
// (SplitHostPort fails), the raw value is used so the key never degrades to
// empty.
func TestClientHostKeyFallback(t *testing.T) {
	for _, raw := range []string{"203.0.113.7", "unix-socket-peer", ""} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = raw
		require.Equal(t, raw, ClientHostKey(req), "raw RemoteAddr must be the fallback for %q", raw)
	}
}

// ---- login handler integration (real router, real DB, real handler) ----

var loginEnvCounter uint64

func seedUserDB(t *testing.T) app.Application {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:login-guard-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)",
		loginEnvCounter, name)
	loginEnvCounter++
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.User{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	hash, err := bcrypt.GenerateFromPassword([]byte("correct-password"), bcrypt.MinCost)
	require.NoError(t, err)
	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   1,
		UserName: "admin",
		Email:    "admin@example.com",
		Password: string(hash),
		Role:     defs.UserRole_Admin,
		TenantID: 1,
	}}).Error)

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	cfg := appInst.GetConfig()
	cfg.App.CookieName = "frp-panel-cookie"
	appInst.SetConfig(cfg)
	return appInst
}

// swapLoginGuard replaces the package-level guard with a fresh one driven
// by the fake clock, restoring the original afterwards.
func swapLoginGuard(t *testing.T) *fakeClock {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	old := loginGuard
	loginGuard = NewLoginGuard(WithClock(clock))
	t.Cleanup(func() { loginGuard = old })
	return clock
}

func loginRouter(appInst app.Application) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/api/v1/auth/login", app.Wrapper(appInst, LoginHandler))
	return router
}

func postLogin(t *testing.T, router *gin.Engine, username, password string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"username":%q,"password":%q}`, username, password)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

type loginResp struct {
	Code int `json:"code"`
	Body struct {
		Status struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"status"`
		Token string `json:"token"`
	} `json:"body"`
}

func parseLogin(t *testing.T, w *httptest.ResponseRecorder) loginResp {
	t.Helper()
	var out loginResp
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	return out
}

// TestLoginHandlerXFFSpoofingCannotBypassLockout (review I1): five wrong
// attempts, each carrying a DIFFERENT forged X-Forwarded-For, must still lock
// the same (username, peer-host) key — the 6th attempt with the correct
// password is rate-limited and no token is issued.
func TestLoginHandlerXFFSpoofingCannotBypassLockout(t *testing.T) {
	appInst := seedUserDB(t)
	router := loginRouter(appInst)
	swapLoginGuard(t)

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
			strings.NewReader(`{"username":"admin","password":"wrong-password"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.99.0.%d", i+1))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, "invalid username or password", parseLogin(t, w).Body.Status.Message, "attempt %d", i+1)
	}

	w := postLogin(t, router, "admin", "correct-password")
	out := parseLogin(t, w)
	require.Equal(t, "too many login attempts, please try again later", out.Body.Status.Message,
		"rotating X-Forwarded-For must not mint a fresh guard key")
	require.Empty(t, out.Body.Token)
}

// TestLoginHandlerLocksAfterFiveFailures: 5 wrong-password attempts must
// lock the (username, IP) key; the 6th attempt is rejected even with the
// correct password, and no token is issued.
func TestLoginHandlerLocksAfterFiveFailures(t *testing.T) {
	appInst := seedUserDB(t)
	router := loginRouter(appInst)
	swapLoginGuard(t)

	for i := 0; i < 5; i++ {
		w := postLogin(t, router, "admin", "wrong-password")
		out := parseLogin(t, w)
		require.Equal(t, "invalid username or password", out.Body.Status.Message, "attempt %d", i+1)
	}

	w := postLogin(t, router, "admin", "correct-password")
	out := parseLogin(t, w)
	require.Equal(t, "too many login attempts, please try again later", out.Body.Status.Message)
	require.Empty(t, out.Body.Token)
}
func TestLoginHandlerSuccessResetsFailures(t *testing.T) {
	appInst := seedUserDB(t)
	router := loginRouter(appInst)
	swapLoginGuard(t)

	for i := 0; i < 4; i++ {
		postLogin(t, router, "admin", "wrong-password")
	}
	w := postLogin(t, router, "admin", "correct-password")
	out := parseLogin(t, w)
	require.Equal(t, int(pb.RespCode_RESP_CODE_SUCCESS), out.Body.Status.Code)
	require.NotEmpty(t, out.Body.Token)
	require.Equal(t, "ok", out.Body.Status.Message)

	for i := 0; i < 4; i++ {
		w = postLogin(t, router, "admin", "wrong-password")
		out = parseLogin(t, w)
		require.Equal(t, "invalid username or password", out.Body.Status.Message,
			"failures after a success must restart from zero (attempt %d)", i+1)
	}
}

// TestLoginHandlerUnknownUserUnifiedError: an unknown username returns the
// same unified message as a wrong password (no user enumeration) and still
// counts as a failure for the lockout.
func TestLoginHandlerUnknownUserUnifiedError(t *testing.T) {
	appInst := seedUserDB(t)
	router := loginRouter(appInst)
	swapLoginGuard(t)

	for i := 0; i < 5; i++ {
		w := postLogin(t, router, "no-such-user", "whatever")
		out := parseLogin(t, w)
		require.Equal(t, "invalid username or password", out.Body.Status.Message)
		require.NotContains(t, w.Body.String(), "record not found", "DB errors must not leak")
	}
	w := postLogin(t, router, "no-such-user", "whatever")
	out := parseLogin(t, w)
	require.Equal(t, "too many login attempts, please try again later", out.Body.Status.Message,
		"unknown-user failures must count toward the lockout")
}

// TestLoginHandlerNoCredentialLeak: the password never appears in any
// response body, successful or not.
func TestLoginHandlerNoCredentialLeak(t *testing.T) {
	appInst := seedUserDB(t)
	router := loginRouter(appInst)
	swapLoginGuard(t)

	for _, tc := range []struct {
		user, pass string
	}{
		{"admin", "correct-password"},
		{"admin", "wrong-password"},
		{"no-such-user", "secret-password"},
	} {
		w := postLogin(t, router, tc.user, tc.pass)
		require.NotContains(t, w.Body.String(), tc.pass, "password must never appear in a response")
	}
}
