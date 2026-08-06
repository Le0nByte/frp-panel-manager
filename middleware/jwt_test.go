package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// jwtTestApp is a minimal Application for JWTAuth tests: the middleware
// only needs GetConfig (cookie/JWT settings) and the logger.
type jwtTestApp struct {
	app.Application
	cfg conf.Config
}

func (f *jwtTestApp) GetConfig() conf.Config { return f.cfg }

func (f *jwtTestApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

// jwtEnv builds a deterministic config (fixed JWT secret) and a signed
// token that JWTAuth accepts.
func jwtEnv(t *testing.T) (*jwtTestApp, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := conf.Config{}
	cfg.App.CookieName = "frp-panel-cookie"
	cfg.App.CookieAge = 3600
	cfg.App.CookiePath = "/"
	cfg.App.GlobalSecret = "test-secret"
	appInst := &jwtTestApp{cfg: cfg}
	secret := conf.JWTSecret(cfg)
	token, err := utils.GetJwtTokenFromMap(secret, time.Now().Unix(), 3600, map[string]interface{}{
		defs.UserIDKey: 1,
	})
	require.NoError(t, err)
	return appInst, token
}

// buildJWTRouter wires the real JWTAuth middleware in front of a marker
// handler; called reports whether the handler was reached.
func buildJWTRouter(appInst app.Application) (*gin.Engine, *bool) {
	called := false
	router := gin.New()
	router.Use(JWTAuth(appInst))
	router.Any("/api/v1/ping", func(c *gin.Context) {
		called = true
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return router, &called
}

// unauthorizedMsg extracts the business error message from the unified
// ErrUnAuthorized JSON body.
func unauthorizedMsg(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var out struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	return out.Msg
}

func TestErrUnauthorizedJSONUsesHTTP401(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/unauthorized", func(c *gin.Context) {
		common.ErrUnAuthorized(c, "invalid authorization")
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/unauthorized", nil))

	require.Equal(t, http.StatusUnauthorized, w.Code)
	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, http.StatusUnauthorized, body.Code)
	require.Equal(t, "invalid authorization", body.Msg)
}

// TestJWTQueryTokenRejected: a request carrying ?token= must be rejected
// with 401 (business) and must NOT reach the handler — spec §10.6 forbids
// tokens in URLs, even when the token itself is valid. The token must never
// surface in the response OR in any log line.
func TestJWTQueryTokenRejected(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)
	lines := captureLogs(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping?token="+token, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.False(t, *called, "handler must not run for query-token auth")
	require.Equal(t, "invalid authorization", unauthorizedMsg(t, w))
	require.NotContains(t, w.Body.String(), token, "query token must not be echoed in the response")
	for _, l := range *lines {
		require.NotContains(t, l, token, "query token must not appear in any log line")
	}
}

// TestJWTBearerHeaderAccepted: the frontend sends Authorization: Bearer on
// every request (www/api/http.ts); that path must keep working untouched.
func TestJWTBearerHeaderAccepted(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.Header.Set(defs.AuthorizationKey, "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.True(t, *called, "Bearer header auth must reach the handler")
	require.Equal(t, http.StatusOK, w.Code)
}

// TestJWTCookieGETAccepted: cookie auth on a read method needs no CSRF.
func TestJWTCookieGETAccepted(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.True(t, *called)
}

// TestJWTCookiePOSTNoCSRFRejected: cookie-authenticated write without the
// double-submit CSRF header must be rejected before the handler runs.
func TestJWTCookiePOSTNoCSRFRejected(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.False(t, *called, "handler must not run on CSRF failure")
	require.Equal(t, "invalid csrf token", unauthorizedMsg(t, w))
}

// TestJWTCookiePOSTMatchingCSRFAccepted: header matching the CSRF cookie
// passes the double-submit check.
func TestJWTCookiePOSTMatchingCSRFAccepted(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName(appInst.cfg.App.CookieName), Value: "csrf-abc"})
	req.Header.Set(CSRFHeaderName, "csrf-abc")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.True(t, *called, "matching CSRF header must reach the handler")
}

// TestJWTCookiePOSTCSRFMismatchRejected: a wrong CSRF header value must be
// rejected (constant-time comparison path).
func TestJWTCookiePOSTCSRFMismatchRejected(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName(appInst.cfg.App.CookieName), Value: "csrf-abc"})
	req.Header.Set(CSRFHeaderName, "csrf-xyz")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.False(t, *called, "mismatched CSRF must be rejected")
	require.Equal(t, "invalid csrf token", unauthorizedMsg(t, w))
}

// TestJWTBearerPOSTNoCSRFRequired: Bearer-header auth on writes must not
// require the CSRF header (existing frontend flow stays intact).
func TestJWTBearerPOSTNoCSRFRequired(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.Header.Set(defs.AuthorizationKey, "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.True(t, *called)
}

// TestPushTokenStrSetsCSRFCookieAndHeader: login/renewal (PushTokenStr)
// must emit a high-entropy CSRF cookie (HttpOnly=false, SameSite=Strict,
// Secure follows config) plus the X-CSRF-Token response header, and the
// auth cookie itself must be SameSite=Strict.
func TestPushTokenStrSetsCSRFCookieAndHeader(t *testing.T) {
	appInst, _ := jwtEnv(t)
	router := gin.New()
	router.Any("/x", func(c *gin.Context) {
		PushTokenStr(c, appInst, "jwt-token-abc")
		c.JSON(http.StatusOK, gin.H{})
	})

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	csrfHeader := w.Header().Get(CSRFHeaderName)
	require.NotEmpty(t, csrfHeader, "X-CSRF-Token response header must be set")

	var authCk, csrfCk *http.Cookie
	for _, ck := range w.Result().Cookies() {
		switch ck.Name {
		case appInst.cfg.App.CookieName:
			authCk = ck
		case CSRFCookieName(appInst.cfg.App.CookieName):
			csrfCk = ck
		}
	}
	require.NotNil(t, authCk, "auth cookie must be set")
	require.NotNil(t, csrfCk, "csrf cookie must be set")
	require.Equal(t, http.SameSiteStrictMode, authCk.SameSite, "auth cookie must be SameSite=Strict")
	require.Equal(t, http.SameSiteStrictMode, csrfCk.SameSite, "csrf cookie must be SameSite=Strict")
	require.False(t, csrfCk.HttpOnly, "csrf cookie must be readable by JS (double submit)")
	require.Equal(t, appInst.cfg.App.CookieSecure, csrfCk.Secure, "csrf cookie Secure must follow config")
	require.Equal(t, csrfCk.Value, csrfHeader, "X-CSRF-Token header must carry the cookie value")
	require.NotEqual(t, "jwt-token-abc", csrfCk.Value, "csrf token must not reuse the auth token")
}

// TestPushTokenStrRegeneratesCSRFToken: every renewal must mint a fresh,
// high-entropy CSRF token.
func TestPushTokenStrRegeneratesCSRFToken(t *testing.T) {
	appInst, _ := jwtEnv(t)
	router := gin.New()
	router.Any("/x", func(c *gin.Context) {
		PushTokenStr(c, appInst, "jwt-token-abc")
		c.JSON(http.StatusOK, gin.H{})
	})

	collect := func() string {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w.Header().Get(CSRFHeaderName)
	}
	first, second := collect(), collect()
	require.NotEqual(t, first, second, "each renewal must mint a fresh csrf token")
	require.GreaterOrEqual(t, len(first), 32, "csrf token must be high-entropy")
}

// ---- S4a review regressions (C1 Bearer priority, I2 token redaction) ----

// captureLogs snapshots the package-global logrus hooks, installs a
// capturing hook, and restores the original hooks at test end.
func captureLogs(t *testing.T) *[]string {
	t.Helper()
	var lines []string
	oldHooks := logger.LoggerInstance.Hooks
	logger.LoggerInstance.ReplaceHooks(logrus.LevelHooks{})
	logger.LoggerInstance.AddHook(&captureHook{lines: &lines})
	t.Cleanup(func() { logger.LoggerInstance.ReplaceHooks(oldHooks) })
	return &lines
}

type captureHook struct{ lines *[]string }

func (h *captureHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *captureHook) Fire(e *logrus.Entry) error {
	*h.lines = append(*h.lines, e.Message)
	return nil
}

// TestJWTBearerWinsOverCookiePOSTNoCSRF (review C1 — the browser-equivalent
// shape): auth cookie AND valid Authorization Bearer AND no X-CSRF-Token on
// a write. The Bearer header must take priority: the handler runs and the
// cookie branch (with its CSRF gate) is never consulted. This is exactly the
// request the web UI sends after login (www/api/http.ts always attaches the
// Bearer header; the browser always attaches the cookie).
func TestJWTBearerWinsOverCookiePOSTNoCSRF(t *testing.T) {
	appInst, token := jwtEnv(t)
	router, called := buildJWTRouter(appInst)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ping", nil)
	req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
	req.Header.Set(defs.AuthorizationKey, "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.True(t, *called, "valid Bearer must take priority over the auth cookie; CSRF must not be required")
	require.Equal(t, http.StatusOK, w.Code)
}

// TestJWTInvalidBearerNoCookieFallback: a present Authorization header is
// authoritative — when it fails to parse, the request is rejected (401) and
// never re-authenticated via a valid auth cookie (no ambiguity, no CSRF
// downgrade), for writes and reads alike.
func TestJWTInvalidBearerNoCookieFallback(t *testing.T) {
	appInst, token := jwtEnv(t)
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"garbage", "Bearer not-a-jwt"},
		{"null", "Bearer null"},
		{"empty-after-prefix", "Bearer "},
		{"bare-word", "Bearer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				router, called := buildJWTRouter(appInst)
				req := httptest.NewRequest(method, "/api/v1/ping", nil)
				req.AddCookie(&http.Cookie{Name: appInst.cfg.App.CookieName, Value: token})
				req.Header.Set(defs.AuthorizationKey, tc.header)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)

				require.False(t, *called, "%s: handler must not run", method)
				require.Equal(t, "invalid authorization", unauthorizedMsg(t, w),
					"%s: must be an auth rejection, not a CSRF rejection", method)
				require.NotContains(t, w.Body.String(), token, "%s: cookie token must not be echoed", method)
			}
		})
	}
}

// TestJWTInvalidBearerNotLogged (review I2): the parse-failure log line must
// not contain the bearer token plaintext (§10.9 token redaction).
func TestJWTInvalidBearerNotLogged(t *testing.T) {
	appInst, _ := jwtEnv(t)
	router, called := buildJWTRouter(appInst)
	lines := captureLogs(t)

	leak := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjEsImlhdCI6MX0.sig"
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.Header.Set(defs.AuthorizationKey, "Bearer "+leak)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.False(t, *called, "invalid bearer must not reach the handler")
	require.Equal(t, "invalid authorization", unauthorizedMsg(t, w))
	for _, l := range *lines {
		require.NotContains(t, l, leak, "bearer token must never be logged on parse failure")
	}
}

// TestCSRFHelpers: the pure helpers used by the middleware.
func TestCSRFHelpers(t *testing.T) {
	require.Equal(t, "frp-panel-cookie-csrf", CSRFCookieName("frp-panel-cookie"))

	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		require.True(t, IsStateChangingMethod(m), m)
	}
	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		require.False(t, IsStateChangingMethod(m), m)
	}

	require.True(t, ValidateCSRF("abc", "abc"))
	require.False(t, ValidateCSRF("abc", "abd"))
	require.False(t, ValidateCSRF("", "abc"))
	require.False(t, ValidateCSRF("abc", ""))

	tok1, err := NewCSRFToken()
	require.NoError(t, err)
	tok2, err := NewCSRFToken()
	require.NoError(t, err)
	require.NotEqual(t, tok1, tok2)
	require.False(t, strings.ContainsAny(tok1, "=+/"), "csrf token must be URL-safe base64")
}
