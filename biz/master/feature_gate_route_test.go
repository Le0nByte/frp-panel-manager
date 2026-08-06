package master

// S5a feature-gate route tests. These tests live in a NEW file on purpose:
// biz/master/routes_test.go is owned by the S4b reset-password batch and
// must not be edited. They reuse the package fixtures defined there
// (routeTestApp, newRouteEnv, postJSON, emptyClientsManager) unmodified.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newFeatureEnv is newRouteEnv plus RBAC permissions covering the gated
// dangerous routes, so real-HTTP hits reach the feature gate (and, while
// on, the business handler) instead of being rejected by RBAC first.
func newFeatureEnv(t *testing.T) (*routeTestApp, string) {
	t.Helper()
	dsn := "file:master-features?mode=memory&cache=shared&_pragma=busy_timeout(3000)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.User{}, &models.Client{}, &models.Server{},
		&models.AuditEvent{}, &models.ChangeTransaction{}, &models.ConfigSnapshot{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	user := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1, Token: "t"}
	require.NoError(t, db.Create(&models.User{UserEntity: user}).Error)

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := &routeTestApp{
		cfg:  conf.Config{},
		dbm:  dbm,
		cm:   &emptyClientsManager{},
		recv: &sync.Map{},
	}

	secret := conf.JWTSecret(conf.Config{})
	token, err := utils.GetJwtTokenFromMap(secret, time.Now().Unix(), 3600, map[string]interface{}{
		defs.UserIDKey: 1,
		"permissions": []defs.APIPermission{
			{Method: "POST", Path: "/api/v1/proxy/preflight"},
			{Method: "POST", Path: "/api/v1/port/list"},
			{Method: "POST", Path: "/api/v1/audit/list"},
			{Method: "POST", Path: "/api/v1/worker/.*"},
			{Method: "POST", Path: "/api/v1/wg/.*"},
			{Method: "GET", Path: "/api/v1/pty/.*"},
			{Method: "POST", Path: "/api/v1/client/install_workerd"},
		},
	})
	require.NoError(t, err)
	return appInst, token
}

// featureOffApp returns an Application whose master features are all off
// (zero-value config), the fail-closed startup shape.
func featureOffApp() app.Application {
	return app.NewApp()
}

// featureOnApp returns an Application with all three master features
// explicitly on (config shape, not env), the opt-in startup shape.
func featureOnApp() app.Application {
	cfg := conf.Config{}
	cfg.Master.Features.EnablePTY = true
	cfg.Master.Features.EnableWorker = true
	cfg.Master.Features.EnableWireGuard = true
	return &routeTestApp{cfg: cfg}
}

// registeredRoutes returns "METHOD /path" for every route on the router.
func registeredRoutes(router *gin.Engine) map[string]bool {
	out := map[string]bool{}
	for _, r := range router.Routes() {
		out[r.Method+" "+r.Path] = true
	}
	return out
}

// TestDangerousRoutesNotRegisteredWhenFeaturesOff (S5a L1): with the
// default (off) config the router must NOT expose any worker/wg/pty route
// nor /client/install_workerd — a request hits 404 before any middleware.
// The safe surface (port, preflight, audit, client, server) is untouched.
func TestDangerousRoutesNotRegisteredWhenFeaturesOff(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	ConfigureRouter(featureOffApp(), router)

	registered := registeredRoutes(router)

	for _, absent := range []string{
		"POST /api/v1/client/install_workerd",
		"GET /api/v1/pty/:clientID",
	} {
		require.False(t, registered[absent], "dangerous route %s must not be registered when features are off", absent)
	}
	for path := range registered {
		require.NotContains(t, path, "/api/v1/worker/", "worker route %s must not be registered when features are off", path)
		require.NotContains(t, path, "/api/v1/wg/", "wg route %s must not be registered when features are off", path)
	}

	for _, kept := range []string{
		"POST /api/v1/port/list",
		"POST /api/v1/proxy/preflight",
		"POST /api/v1/audit/list",
		"POST /api/v1/audit/get",
		"POST /api/v1/audit/rollback",
		"POST /api/v1/client/list",
		"POST /api/v1/client/get",
		"POST /api/v1/server/list",
		"GET /api/v1/log",
	} {
		require.True(t, registered[kept], "safe route %s must stay registered", kept)
	}
}

// TestDangerousRoutesRegisteredWhenFeaturesOn (S5a L2): with every master
// feature explicitly on, the full legacy dangerous surface is registered
// again, next to the safe routes.
func TestDangerousRoutesRegisteredWhenFeaturesOn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	ConfigureRouter(featureOnApp(), router)

	registered := registeredRoutes(router)

	for _, present := range []string{
		"POST /api/v1/client/install_workerd",
		"GET /api/v1/pty/:clientID",
		"POST /api/v1/worker/get",
		"POST /api/v1/worker/status",
		"POST /api/v1/worker/create",
		"POST /api/v1/worker/list",
		"POST /api/v1/worker/remove",
		"POST /api/v1/worker/update",
		"POST /api/v1/worker/redeploy",
		"POST /api/v1/worker/create_ingress",
		"POST /api/v1/worker/get_ingress",
		"POST /api/v1/wg/network/create",
		"POST /api/v1/wg/network/delete",
		"POST /api/v1/wg/endpoint/create",
		"POST /api/v1/wg/endpoint/list",
		"POST /api/v1/wg/link/create",
		"POST /api/v1/wg/link/list",
		"POST /api/v1/wg/create",
		"POST /api/v1/wg/delete",
		"POST /api/v1/wg/update",
		"POST /api/v1/wg/restart",
		"POST /api/v1/wg/get",
		"POST /api/v1/wg/list",
		"POST /api/v1/wg/runtime/get",
	} {
		require.True(t, registered[present], "dangerous route %s must be registered when features are on", present)
	}

	for _, kept := range []string{
		"POST /api/v1/port/list",
		"POST /api/v1/proxy/preflight",
		"POST /api/v1/audit/list",
		"POST /api/v1/client/list",
		"POST /api/v1/server/list",
	} {
		require.True(t, registered[kept], "safe route %s must stay registered when features are on", kept)
	}
}

// TestDangerousFeatureOffRealHTTP404 (S5a L1): with features off, a real
// HTTP request to each gated surface gets 404 — the route is simply not
// there, so no middleware and no handler ever runs.
func TestDangerousFeatureOffRealHTTP404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/v1/worker/get", `{}`},
		{http.MethodPost, "/api/v1/wg/get", `{}`},
		{http.MethodPost, "/api/v1/client/install_workerd", `{}`},
		{http.MethodGet, "/api/v1/pty/some-client", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte(tc.body)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusNotFound, w.Code, "%s %s must 404 when features are off", tc.method, tc.path)
		})
	}
}

// TestDangerousFeatureFlipOffRealHTTP403 (S5a L2): the gate is a request-
// time check against the LIVE app config. Routes registered while the
// feature is on must serve normally, and after the config is flipped off
// at runtime the very same requests must get HTTP 403 before the business
// handler (or the PTY websocket upgrade) runs. "Handler did not run" is
// proven by the response body: an executed handler answers with the
// business envelope (JSON code/body, or a 400 upgrade failure for PTY),
// never with the gate's exact 403 payload.
func TestDangerousFeatureFlipOffRealHTTP403(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newFeatureEnv(t)
	appInst.cfg.Master.Features.EnablePTY = true
	appInst.cfg.Master.Features.EnableWorker = true
	appInst.cfg.Master.Features.EnableWireGuard = true
	router := gin.New()
	ConfigureRouter(appInst, router)

	gateBody := `{"error":"feature disabled"}`

	type hit struct {
		method      string
		path        string
		body        string
		onOK        int // expected status while the feature is on (handler reached)
		notBusiness bool
	}

	hits := []hit{
		{method: http.MethodPost, path: "/api/v1/worker/get", body: `{}`, onOK: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/wg/get", body: `{}`, onOK: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/client/install_workerd", body: `{"client_id":"nope"}`, onOK: http.StatusOK},
		// PTY: while on, the plain GET reaches the handler, whose websocket
		// upgrade fails with HTTP 400 (proof the handler ran); when flipped
		// off the gate must abort before the upgrade.
		{method: http.MethodGet, path: "/api/v1/pty/some-client", body: "", onOK: http.StatusBadRequest},
	}

	do := func(tc hit) *httptest.ResponseRecorder {
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte(tc.body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// Phase 1: features on — handlers run, business responses come back.
	for _, tc := range hits {
		w := do(tc)
		require.Equal(t, tc.onOK, w.Code, "%s %s while ON must reach the handler", tc.method, tc.path)
		require.NotEqual(t, gateBody, w.Body.String(), "%s %s while ON must not hit the gate", tc.method, tc.path)
	}

	// Phase 2: runtime flip — same router, same app instance, config off.
	appInst.cfg.Master.Features.EnablePTY = false
	appInst.cfg.Master.Features.EnableWorker = false
	appInst.cfg.Master.Features.EnableWireGuard = false

	for _, tc := range hits {
		w := do(tc)
		require.Equal(t, http.StatusForbidden, w.Code, "%s %s after flip-off must 403", tc.method, tc.path)
		require.Equal(t, gateBody, w.Body.String(), "%s %s after flip-off must be stopped by the gate, not by the handler", tc.method, tc.path)
	}

	// Phase 3: the safe surface is unaffected by the flip.
	safe := postJSON(t, router, token, "/api/v1/port/list", `{}`)
	require.Equal(t, http.StatusOK, safe.Code)
	var out struct {
		Code int `json:"code"`
	}
	require.NoError(t, json.Unmarshal(safe.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
}
