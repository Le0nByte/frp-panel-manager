package master

// I1-B read-only HTTP gate route tests (real router, real middleware
// chain): while the master is blocked, EVERY mutation route must answer
// HTTP 403 with the exact fixed gate body BEFORE its business handler
// runs; every read route must still reach its handler (never the gate).
// Feature-gated dangerous surfaces (worker/wg/pty) are registered by
// switching the S5a feature flags ON, so the readonly gate — not a 404 —
// is what a blocked write hits.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// readonlyGateBody is the exact fixed payload of the readonly write gate
// (must match middleware.RequireWritable verbatim).
const readonlyGateBody = `{"error":"master is read-only: snapshot key fingerprint missing or mismatched"}`

// newReadonlyRouteEnv is newFeatureEnv with ALL S5a features ON and a
// wildcard RBAC permission covering every /api/v1 route, so blocked-write
// hits are stopped by the readonly gate rather than by RBAC or a 404.
func newReadonlyRouteEnv(t *testing.T) (*routeTestApp, string) {
	t.Helper()
	dsn := "file:master-readonly-route?mode=memory&cache=shared&_pragma=busy_timeout(3000)"
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
	// S5a features ON: worker/wg/pty routes are registered so the readonly
	// gate is what a blocked write hits (not a 404).
	appInst.cfg.Master.Features.EnablePTY = true
	appInst.cfg.Master.Features.EnableWorker = true
	appInst.cfg.Master.Features.EnableWireGuard = true

	secret := conf.JWTSecret(conf.Config{})
	token, err := utils.GetJwtTokenFromMap(secret, time.Now().Unix(), 3600, map[string]interface{}{
		defs.UserIDKey: 1,
		"permissions": []defs.APIPermission{
			{Method: "POST", Path: "/api/v1/.*"},
			{Method: "GET", Path: "/api/v1/.*"},
		},
	})
	require.NoError(t, err)
	return appInst, token
}

// authedReq performs one authenticated HTTP request through the real
// router (token travels in the Authorization header, never the URL).
func authedReq(t *testing.T, router *gin.Engine, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestReadOnlyWriteRoutesBlockedRealHTTP403: every mutation route answers
// the exact fixed 403 gate body while blocked — the body proves the
// readonly gate (not the business handler, not RBAC, not a 404) answered.
func TestReadOnlyWriteRoutesBlockedRealHTTP403(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newReadonlyRouteEnv(t)
	readonly.Set(true, "snapshot key fingerprint deadbeef42 does not match")
	t.Cleanup(readonly.ResetForTest)
	router := gin.New()
	ConfigureRouter(appInst, router)

	writeRoutes := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/v1/auth/register", `{}`},
		{http.MethodPost, "/api/v1/user/update", `{}`},
		{http.MethodPost, "/api/v1/client/init", `{}`},
		{http.MethodPost, "/api/v1/client/delete", `{}`},
		{http.MethodPost, "/api/v1/client/upgrade", `{}`},
		{http.MethodPost, "/api/v1/client/install_workerd", `{}`},
		{http.MethodPost, "/api/v1/server/init", `{}`},
		{http.MethodPost, "/api/v1/server/delete", `{}`},
		{http.MethodPost, "/api/v1/frpc/update", `{}`},
		{http.MethodPost, "/api/v1/frpc/delete", `{}`},
		{http.MethodPost, "/api/v1/frpc/stop", `{}`},
		{http.MethodPost, "/api/v1/frpc/start", `{}`},
		{http.MethodPost, "/api/v1/frps/update", `{}`},
		{http.MethodPost, "/api/v1/frps/delete", `{}`},
		{http.MethodPost, "/api/v1/proxy/create_config", `{}`},
		{http.MethodPost, "/api/v1/proxy/update_config", `{}`},
		{http.MethodPost, "/api/v1/proxy/delete_config", `{}`},
		{http.MethodPost, "/api/v1/proxy/start_proxy", `{}`},
		{http.MethodPost, "/api/v1/proxy/stop_proxy", `{}`},
		{http.MethodPost, "/api/v1/audit/rollback", `{}`},
		{http.MethodPost, "/api/v1/worker/create", `{}`},
		{http.MethodPost, "/api/v1/worker/remove", `{}`},
		{http.MethodPost, "/api/v1/worker/update", `{}`},
		{http.MethodPost, "/api/v1/worker/redeploy", `{}`},
		{http.MethodPost, "/api/v1/worker/create_ingress", `{}`},
		{http.MethodPost, "/api/v1/wg/network/create", `{}`},
		{http.MethodPost, "/api/v1/wg/network/delete", `{}`},
		{http.MethodPost, "/api/v1/wg/network/update", `{}`},
		{http.MethodPost, "/api/v1/wg/endpoint/create", `{}`},
		{http.MethodPost, "/api/v1/wg/endpoint/delete", `{}`},
		{http.MethodPost, "/api/v1/wg/endpoint/update", `{}`},
		{http.MethodPost, "/api/v1/wg/link/create", `{}`},
		{http.MethodPost, "/api/v1/wg/link/delete", `{}`},
		{http.MethodPost, "/api/v1/wg/link/update", `{}`},
		{http.MethodPost, "/api/v1/wg/create", `{}`},
		{http.MethodPost, "/api/v1/wg/delete", `{}`},
		{http.MethodPost, "/api/v1/wg/update", `{}`},
		{http.MethodPost, "/api/v1/wg/restart", `{}`},
		// PTY is a remote-command surface: the gate must abort before the
		// websocket upgrade, with the same fixed 403.
		{http.MethodGet, "/api/v1/pty/some-client", ""},
	}

	for _, tc := range writeRoutes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := authedReq(t, router, token, tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusForbidden, w.Code, "%s %s must 403 while blocked", tc.method, tc.path)
			require.Equal(t, readonlyGateBody, w.Body.String(),
				"%s %s must answer the exact gate body, never the handler or the reason", tc.method, tc.path)
			require.NotContains(t, w.Body.String(), "deadbeef42", "the gate body must not leak the reason")
		})
	}
}

// TestReadOnlyReadRoutesStillServeWhileBlocked: the read surface is
// untouched by the gate — every read route reaches its business handler
// (never HTTP 403, never the gate body). port/list is additionally pinned
// to a genuine 200 business envelope.
func TestReadOnlyReadRoutesStillServeWhileBlocked(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newReadonlyRouteEnv(t)
	readonly.Set(true, "snapshot key fingerprint deadbeef42 does not match")
	t.Cleanup(readonly.ResetForTest)
	router := gin.New()
	ConfigureRouter(appInst, router)

	readRoutes := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/v1/user/get", `{}`},
		{http.MethodPost, "/api/v1/user/sign-token", `{}`},
		{http.MethodPost, "/api/v1/platform/clientsstatus", `{}`},
		{http.MethodGet, "/api/v1/platform/baseinfo", ""},
		{http.MethodGet, "/api/v1/platform/version", ""},
		{http.MethodPost, "/api/v1/client/get", `{}`},
		{http.MethodPost, "/api/v1/client/list", `{}`},
		{http.MethodPost, "/api/v1/server/get", `{}`},
		{http.MethodPost, "/api/v1/server/list", `{}`},
		{http.MethodPost, "/api/v1/proxy/preflight", `{}`},
		{http.MethodPost, "/api/v1/proxy/get_config", `{}`},
		{http.MethodPost, "/api/v1/proxy/get_by_cid", `{}`},
		{http.MethodPost, "/api/v1/proxy/get_by_sid", `{}`},
		{http.MethodPost, "/api/v1/proxy/list_configs", `{}`},
		{http.MethodPost, "/api/v1/port/list", `{}`},
		{http.MethodPost, "/api/v1/audit/list", `{}`},
		{http.MethodPost, "/api/v1/audit/get", `{}`},
		{http.MethodPost, "/api/v1/worker/get", `{}`},
		{http.MethodPost, "/api/v1/worker/status", `{}`},
		{http.MethodPost, "/api/v1/worker/list", `{}`},
		{http.MethodPost, "/api/v1/worker/get_ingress", `{}`},
		{http.MethodPost, "/api/v1/wg/network/get", `{}`},
		{http.MethodPost, "/api/v1/wg/network/list", `{}`},
		{http.MethodPost, "/api/v1/wg/network/topology", `{}`},
		{http.MethodPost, "/api/v1/wg/endpoint/get", `{}`},
		{http.MethodPost, "/api/v1/wg/endpoint/list", `{}`},
		{http.MethodPost, "/api/v1/wg/link/get", `{}`},
		{http.MethodPost, "/api/v1/wg/link/list", `{}`},
		{http.MethodPost, "/api/v1/wg/get", `{}`},
		{http.MethodPost, "/api/v1/wg/list", `{}`},
		{http.MethodPost, "/api/v1/wg/runtime/get", `{}`},
		{http.MethodGet, "/api/v1/log", ""},
	}

	for _, tc := range readRoutes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := authedReq(t, router, token, tc.method, tc.path, tc.body)
			require.NotEqual(t, http.StatusForbidden, w.Code, "%s %s must not be stopped by the readonly gate", tc.method, tc.path)
			require.NotEqual(t, readonlyGateBody, w.Body.String(), "%s %s must reach its handler, not the gate", tc.method, tc.path)
		})
	}

	// Strict pin: port/list serves a genuine 200 business envelope.
	w := authedReq(t, router, token, http.MethodPost, "/api/v1/port/list", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Code int `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code, "port/list must return the business envelope while blocked")
}

// TestReadOnlyUnblockedWriteRoutesReachHandler: with the default
// (unblocked) state the very same write routes must NOT hit the gate —
// the business handler answers. create_config is pinned to the real
// coordinator's CONFIG_VALIDATION_FAILED envelope, proving the gate is
// bypassed when the master is writable.
func TestReadOnlyUnblockedWriteRoutesReachHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newReadonlyRouteEnv(t)
	readonly.ResetForTest() // default unblocked
	t.Cleanup(readonly.ResetForTest)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := authedReq(t, router, token, http.MethodPost, "/api/v1/proxy/create_config", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotEqual(t, readonlyGateBody, w.Body.String(), "unblocked create_config must not hit the gate")
	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			ErrorCode string `json:"error_code"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_INVALID), out.Body.Status.Code, "the coordinator's INVALID status must surface")
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, out.Body.ErrorCode,
		"unblocked the REAL coordinator answers — READ_ONLY can only come from the gate")

	// audit/rollback: the boundary handler answers, never the gate.
	w2 := authedReq(t, router, token, http.MethodPost, "/api/v1/audit/rollback", `{}`)
	require.Equal(t, http.StatusOK, w2.Code)
	require.NotEqual(t, readonlyGateBody, w2.Body.String())
}
