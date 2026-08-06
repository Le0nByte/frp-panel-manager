package middleware

// I1-B read-only HTTP gate tests (pure middleware, no router):
// RequireWritable aborts with HTTP 403 and the EXACT fixed JSON body when
// the master is blocked in read-only mode, before any business handler
// runs; when unblocked it passes straight through. The body must never
// carry the configured reason or any fingerprint material.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// writableGateBody is the exact fixed payload every blocked write surface
// must answer with (I1-B contract). No reason, no fingerprint, no request
// material.
const writableGateBody = `{"error":"master is read-only: snapshot key fingerprint missing or mismatched"}`

func TestRequireWritableBlockedAbortsExactBodyNoHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	readonly.Set(true, "secret-fingerprint-0000abc-must-not-leak")
	t.Cleanup(readonly.ResetForTest)

	var handlerRan bool
	router := gin.New()
	router.GET("/write", RequireWritable(), func(c *gin.Context) {
		handlerRan = true
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/write", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Equal(t, writableGateBody, w.Body.String(),
		"the gate body must be the exact fixed JSON and must not embed the reason")
	require.False(t, handlerRan, "the business handler must never run while blocked")
}

func TestRequireWritableBlockedBodyDoesNotLeakReasonOrFingerprint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	readonly.Set(true, "snapshot key fingerprint deadbeef42 does not match")
	t.Cleanup(readonly.ResetForTest)

	router := gin.New()
	router.GET("/write", RequireWritable(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/write", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.NotContains(t, w.Body.String(), "deadbeef42", "the gate body must never leak the reason/fingerprint")
}

func TestRequireWritableUnblockedPassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	readonly.ResetForTest() // default: unblocked
	t.Cleanup(readonly.ResetForTest)

	var handlerRan bool
	router := gin.New()
	router.GET("/write", RequireWritable(), func(c *gin.Context) {
		handlerRan = true
		c.String(http.StatusOK, "business")
	})

	req := httptest.NewRequest(http.MethodGet, "/write", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "business", w.Body.String())
	require.True(t, handlerRan, "the handler must run when the master is writable")
	require.NotEqual(t, writableGateBody, w.Body.String())
}
