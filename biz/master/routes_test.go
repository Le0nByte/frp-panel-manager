package master

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// routeTestApp is a minimal Application for router tests: registration
// needs nothing, and a real HTTP hit needs config + DB + managers.
type routeTestApp struct {
	app.Application
	cfg  conf.Config
	dbm  app.DBManager
	cm   app.ClientsManager
	recv *sync.Map
}

func (f *routeTestApp) GetConfig() conf.Config      { return f.cfg }
func (f *routeTestApp) GetDBManager() app.DBManager { return f.dbm }
func (f *routeTestApp) GetClientsManager() app.ClientsManager {
	return f.cm
}
func (f *routeTestApp) GetClientRecvMap() *sync.Map { return f.recv }
func (f *routeTestApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

// TestPortAndPreflightRoutesRegistered: the live router must expose
// POST /api/v1/port/list and POST /api/v1/proxy/preflight without touching
// the existing route set (spec §7.1). Genuine handler binding is proven by
// the real-HTTP tests below (gin renders wrapper closures without the
// target function name in RouteInfo).
func TestPortAndPreflightRoutesRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	ConfigureRouter(app.NewApp(), router)

	registered := map[string]bool{} // "METHOD /path"
	for _, r := range router.Routes() {
		registered[r.Method+" "+r.Path] = true
	}

	require.True(t, registered["POST /api/v1/port/list"], "port list route must be registered")
	require.True(t, registered["POST /api/v1/proxy/preflight"], "proxy preflight route must be registered")

	// existing paths are unchanged
	require.True(t, registered["POST /api/v1/proxy/start_proxy"])
	require.True(t, registered["POST /api/v1/proxy/create_config"])
	require.True(t, registered["POST /api/v1/client/list"])
	require.True(t, registered["POST /api/v1/server/list"])
}

// newRouteEnv builds the full middleware-capable env for real HTTP hits:
// in-memory DB with a user row, deterministic JWT secret and a signed
// token that passes JWTAuth/AuthCtx/RBAC.
func newRouteEnv(t *testing.T) (*routeTestApp, string) {
	t.Helper()
	dsn := "file:master-routes?mode=memory&cache=shared&_pragma=busy_timeout(3000)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.User{}, &models.Client{}, &models.Server{},
		// audit detail tables: the /audit/list and /audit/get routes read
		// them, and the real-HTTP hits below exercise the full handler.
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
			{Method: "POST", Path: "/api/v1/audit/get"},
			{Method: "POST", Path: "/api/v1/audit/rollback"},
		},
	})
	require.NoError(t, err)
	return appInst, token
}

// emptyClientsManager satisfies app.ClientsManager with no connected agents.
type emptyClientsManager struct{}

func (m *emptyClientsManager) Get(cliID string) *defs.Connector { return nil }
func (m *emptyClientsManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {
}
func (m *emptyClientsManager) Remove(cliID string) {}
func (m *emptyClientsManager) ClientAddr(cliID string) string {
	return ""
}
func (m *emptyClientsManager) ConnectTime(cliID string) (time.Time, bool) {
	return time.Time{}, false
}
func (m *emptyClientsManager) UpdateLastSeenAt(cliID string) {}
func (m *emptyClientsManager) GetLastSeenAt(cliID string) (time.Time, bool) {
	return time.Time{}, false
}

// postJSON performs one authenticated JSON POST through the real router.
// Auth goes via the Authorization: Bearer header (spec §10.6 forbids
// tokens in URLs; query-token auth was removed from JWTAuth).
func postJSON(t *testing.T, router *gin.Engine, token, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestPreflightRouteRealHit: a real HTTP POST to /api/v1/proxy/preflight
// through the full middleware + registered route must reach the Preflight
// handler and return its structured business response (an invalid request
// shape yields CONFIG_VALIDATION_FAILED, zero RPC) — proving the handler is
// genuinely wired, not just named.
func TestPreflightRouteRealHit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := postJSON(t, router, token, "/api/v1/proxy/preflight", `{}`)
	require.Equal(t, http.StatusOK, w.Code, "a structured business failure is an HTTP 200 body, not an error page")

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_INVALID), out.Body.Status.Code)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, out.Body.ErrorCode)
	require.NotEmpty(t, out.Body.ErrorMessage)
}

// TestPortListRouteRealHit: a real HTTP POST to /api/v1/port/list with no
// visible nodes must reach the ListPorts handler and return SUCCESS with an
// empty observation list (zero RPC), proving the handler is genuinely
// wired.
func TestPortListRouteRealHit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := postJSON(t, router, token, "/api/v1/port/list", `{}`)
	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			Observations []interface{} `json:"observations"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_SUCCESS), out.Body.Status.Code)
	require.Empty(t, out.Body.Observations, "an empty result carries no observations")
}

// TestNoPasswordResetHTTPRoute (spec §10.7.8): the admin password reset is
// a LOCAL-ONLY CLI operation. No HTTP route — authenticated or not — may
// exist for it; if one ever appears this test fails.
func TestNoPasswordResetHTTPRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	ConfigureRouter(app.NewApp(), router)

	for _, r := range router.Routes() {
		lower := strings.ToLower(r.Path)
		require.NotContains(t, lower, "reset", "HTTP route %s %s must not exist", r.Method, r.Path)
		require.NotContains(t, lower, "password", "HTTP route %s %s must not exist", r.Method, r.Path)
	}
}

