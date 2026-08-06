package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/user"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

// signTokenFlowRouter mounts the REAL production chain for the sign-token
// endpoint — JWTAuth -> AuthCtx -> user.SignTokenHandler — plus a ping route
// to exercise a minted sign token afterwards.
func signTokenFlowRouter(appInst app.Application) *gin.Engine {
	router := gin.New()
	router.Use(JWTAuth(appInst), AuthCtx(appInst))
	router.POST("/api/v1/user/sign-token", app.Wrapper(appInst, user.SignTokenHandler))
	router.Any("/api/v1/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return router
}

type signTokenEnvelope struct {
	Code int `json:"code"`
	Body struct {
		Token string `json:"token"`
	} `json:"body"`
}

// TestSignTokenAfterResetRoundTrip pins the exact I1 regression: after a
// reset bumped the stored session version, a freshly minted sign token MUST
// carry the CURRENT version and pass the real AuthCtx session check. A
// claim-less sign token would be rejected forever (AuthCtx demands the claim
// once effective version > 1). The request never carries a version — it is
// taken from the authenticated user entity AuthCtx placed in the context.
func TestSignTokenAfterResetRoundTrip(t *testing.T) {
	appInst := authCtxEnv(t, 2) // user was reset: stored version = 2
	router := signTokenFlowRouter(appInst)

	// login token as LoginHandler mints it: claim == current version (2)
	v := int64(2)
	loginTok := sessionToken(t, appInst, 1, &v)

	// call the real sign-token endpoint with the login token
	req := httptest.NewRequest(http.MethodPost, "/api/v1/user/sign-token",
		strings.NewReader(`{"expires_in":60,"permissions":[{"path":"/api/v1/port/list","method":"POST"}]}`))
	req.Header.Set(defs.AuthorizationKey, "Bearer "+loginTok)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "sign-token must succeed: %s", w.Body.String())

	var env signTokenEnvelope
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &env))
	require.NotEmpty(t, env.Body.Token, "sign-token response must carry a token")

	// the minted token carries the CURRENT session version and the
	// permissions claim (RBAC unchanged)
	claims, err := utils.ParseToken(conf.JWTSecret(appInst.GetConfig()), env.Body.Token)
	require.NoError(t, err)
	require.Equal(t, int64(2), cast.ToInt64(claims[defs.SessionVersionKey]),
		"sign token must carry the user's current session version")
	require.NotNil(t, claims[defs.TokenPayloadKey_Permissions], "permissions claim must still be minted")

	// and it passes the real AuthCtx session check
	w2 := getPing(t, router, env.Body.Token)
	require.Equal(t, http.StatusOK, w2.Code, "sign token must pass AuthCtx after the reset")
}
