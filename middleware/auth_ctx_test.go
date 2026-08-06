package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// authCtxTestApp is a minimal Application for real AuthCtx tests: AuthCtx
// loads the user from the DB (GetDBManager) and reads JWT settings from
// GetConfig.
type authCtxTestApp struct {
	app.Application
	cfg conf.Config
	dbm app.DBManager
}

func (f *authCtxTestApp) GetConfig() conf.Config      { return f.cfg }
func (f *authCtxTestApp) GetDBManager() app.DBManager { return f.dbm }
func (f *authCtxTestApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

var authCtxDSNCounter uint64

// authCtxEnv builds a real in-memory SQLite DB with one user at the given
// session version (0 = legacy row) and a real DB manager wired to it.
func authCtxEnv(t *testing.T, dbVersion int64) *authCtxTestApp {
	t.Helper()
	gin.SetMode(gin.TestMode)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:auth-ctx-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)",
		atomic.AddUint64(&authCtxDSNCounter, 1), name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, db.AutoMigrate(&models.User{}))
	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   1,
		UserName: "admin",
		Email:    "admin@example.com",
		Role:     defs.UserRole_Admin,
		TenantID: 1,
	}}).Error)
	if dbVersion != 1 {
		require.NoError(t, db.Model(&models.User{}).Where("user_id = ?", 1).Update("session_version", dbVersion).Error)
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)

	cfg := conf.Config{}
	cfg.App.CookieName = "frp-panel-cookie"
	cfg.App.CookieAge = 3600
	cfg.App.CookiePath = "/"
	cfg.App.GlobalSecret = "test-secret"
	return &authCtxTestApp{cfg: cfg, dbm: dbm}
}

// authCtxRouter wires the REAL JWTAuth + AuthCtx chain in front of a marker
// handler, exactly like the production v1 group.
func authCtxRouter(appInst app.Application) (*gin.Engine, *bool) {
	called := false
	router := gin.New()
	router.Use(JWTAuth(appInst), AuthCtx(appInst))
	router.Any("/api/v1/ping", func(c *gin.Context) {
		called = true
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return router, &called
}

// sessionToken mints a real signed token for userID. version nil = legacy
// token WITHOUT the session-version claim; otherwise the claim is set.
func sessionToken(t *testing.T, appInst app.Application, userID int, version *int64) string {
	t.Helper()
	payload := map[string]interface{}{
		defs.UserIDKey:                   userID,
		defs.TokenPayloadKey_Permissions: conf.AllPermission(),
	}
	if version != nil {
		payload[defs.SessionVersionKey] = *version
	}
	token, err := utils.GetJwtTokenFromMap(conf.JWTSecret(appInst.GetConfig()), time.Now().Unix(), 3600, payload)
	require.NoError(t, err)
	return token
}

func getPing(t *testing.T, router *gin.Engine, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.Header.Set(defs.AuthorizationKey, "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestAuthCtxSessionVersionMatch: a token whose claim equals the user's
// current session version passes.
func TestAuthCtxSessionVersionMatch(t *testing.T) {
	appInst := authCtxEnv(t, 2)
	router, called := authCtxRouter(appInst)

	v := int64(2)
	w := getPing(t, router, sessionToken(t, appInst, 1, &v))

	require.True(t, *called)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestAuthCtxSessionVersionMismatchRejected: a token carrying a STALE
// session version (issued before a reset) is rejected with the generic
// auth error — no claim values leak.
func TestAuthCtxSessionVersionMismatchRejected(t *testing.T) {
	appInst := authCtxEnv(t, 2)
	router, called := authCtxRouter(appInst)

	v := int64(1)
	w := getPing(t, router, sessionToken(t, appInst, 1, &v))

	require.False(t, *called)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "token invalid", unauthorizedMsg(t, w))
	require.NotContains(t, w.Body.String(), "session")
}

// TestAuthCtxLegacyTokenAllowedAtVersionOne: pre-feature tokens carry no
// session claim and stay valid while the user's effective version is 1.
func TestAuthCtxLegacyTokenAllowedAtVersionOne(t *testing.T) {
	appInst := authCtxEnv(t, 1)
	router, called := authCtxRouter(appInst)

	w := getPing(t, router, sessionToken(t, appInst, 1, nil))

	require.True(t, *called)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestAuthCtxLegacyTokenRejectedAfterReset: once a reset bumped the version
// above 1, a claim-less legacy token must be rejected.
func TestAuthCtxLegacyTokenRejectedAfterReset(t *testing.T) {
	appInst := authCtxEnv(t, 2)
	router, called := authCtxRouter(appInst)

	w := getPing(t, router, sessionToken(t, appInst, 1, nil))

	require.False(t, *called)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "token invalid", unauthorizedMsg(t, w))
}

// TestAuthCtxLegacyZeroVersionUserAllowed: a DB row still at version 0 (0
// treated as 1) accepts a legacy claim-less token.
func TestAuthCtxLegacyZeroVersionUserAllowed(t *testing.T) {
	appInst := authCtxEnv(t, 0)
	router, called := authCtxRouter(appInst)

	w := getPing(t, router, sessionToken(t, appInst, 1, nil))

	require.True(t, *called)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestAuthCtxNewClaimAfterSecondReset: the login-issued token for the NEW
// version passes even when the user has been reset more than once.
func TestAuthCtxNewClaimAfterSecondReset(t *testing.T) {
	appInst := authCtxEnv(t, 3)
	router, called := authCtxRouter(appInst)

	v := int64(3)
	w := getPing(t, router, sessionToken(t, appInst, 1, &v))

	require.True(t, *called)
	require.Equal(t, http.StatusOK, w.Code)
}
