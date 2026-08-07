package firewall

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/glebarez/sqlite"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var firewallDSNCounter uint64

func uniqueFirewallDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&firewallDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:firewall-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

func setupFirewallEnv(t *testing.T) (app.Application, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueFirewallDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Server{}, &models.AuditEvent{}))
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	a := app.NewApp()
	a.SetDBManager(dbm)
	return a, db
}

func firewallCtx(t *testing.T, a app.Application) *app.Context {
	t.Helper()
	user := &models.UserEntity{UserID: 1, UserName: "admin", Email: "admin@example.com", TenantID: 1}
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), a)
}

func seedDefaultServer(t *testing.T, ctx *app.Context) {
	t.Helper()
	dao.NewMutation(ctx).InitDefaultServer("47.109.18.109")
}

func doOpen(a app.Application, body map[string]interface{}) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	// simulate middleware.AuthCtx (the production chain runs
	// JWTAuth -> AuthCtx -> RBAC before the handler)
	router.Use(func(c *gin.Context) {
		c.Set(defs.UserInfoKey, &models.UserEntity{UserID: 1, UserName: "admin", Email: "admin@example.com", TenantID: 1})
		c.Next()
	})
	router.POST("/open", OpenFirewallPortHandler(a))
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/open", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func decodeResult(t *testing.T, w *httptest.ResponseRecorder) common.Result {
	t.Helper()
	var res common.Result
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &res))
	return res
}

func TestOpenFirewallPortSuccess(t *testing.T) {
	a, db := setupFirewallEnv(t)
	ctx := firewallCtx(t, a)
	seedDefaultServer(t, ctx)

	// inject a fake firewall-cmd executor that records calls
	var calls [][]string
	runFirewallCmd = func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, args)
		return "success", nil
	}

	w := doOpen(a, map[string]interface{}{"server_id": defs.DefaultServerID, "port": 5700, "protocol": "tcp"})
	require.Equal(t, http.StatusOK, w.Code)
	res := decodeResult(t, w)
	require.Equal(t, 200, res.Code)
	require.Equal(t, "success", res.Msg)

	require.Len(t, calls, 2, "permanent add-port + reload")
	require.Equal(t, []string{"--permanent", "--add-port=5700/tcp"}, calls[0])
	require.Equal(t, []string{"--reload"}, calls[1])

	// audit row written
	var ev models.AuditEvent
	require.NoError(t, db.Where("event_type = ?", models.AuditEventTypeFirewall).First(&ev).Error)
	require.True(t, ev.Success)
	require.Contains(t, ev.Summary, "server_id="+defs.DefaultServerID)
	require.Contains(t, ev.Summary, "port=5700/tcp")
}

func TestOpenFirewallPortInvalidPort(t *testing.T) {
	a, _ := setupFirewallEnv(t)
	w := doOpen(a, map[string]interface{}{"server_id": "default", "port": 0, "protocol": "tcp"})
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "1..65535")
}

func TestOpenFirewallPortInvalidProtocol(t *testing.T) {
	a, _ := setupFirewallEnv(t)
	w := doOpen(a, map[string]interface{}{"server_id": "default", "port": 5700, "protocol": "icmp"})
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "tcp or udp")
}

func TestOpenFirewallPortServerNotFound(t *testing.T) {
	a, _ := setupFirewallEnv(t)
	w := doOpen(a, map[string]interface{}{"server_id": "missing", "port": 5700, "protocol": "tcp"})
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "server not found")
}

func TestOpenFirewallPortFirewallCmdFails(t *testing.T) {
	a, db := setupFirewallEnv(t)
	ctx := firewallCtx(t, a)
	seedDefaultServer(t, ctx)

	runFirewallCmd = func(_ context.Context, args ...string) (string, error) {
		return "FirewallD is not running", fmt.Errorf("exit status 252")
	}

	w := doOpen(a, map[string]interface{}{"server_id": defs.DefaultServerID, "port": 5700, "protocol": "tcp"})
	require.Equal(t, http.StatusOK, w.Code)
	res := decodeResult(t, w)
	require.NotEqual(t, 200, res.Code)
	require.Contains(t, res.Msg, "FirewallD is not running")

	// failed audit row
	var ev models.AuditEvent
	require.NoError(t, db.Where("event_type = ?", models.AuditEventTypeFirewall).First(&ev).Error)
	require.False(t, ev.Success)
	require.Contains(t, ev.Summary, "failed")
}

func TestOpenFirewallPortReloadFails(t *testing.T) {
	a, db := setupFirewallEnv(t)
	ctx := firewallCtx(t, a)
	seedDefaultServer(t, ctx)

	callCount := 0
	runFirewallCmd = func(_ context.Context, args ...string) (string, error) {
		callCount++
		if callCount == 1 {
			return "success", nil
		}
		return "reload failed", fmt.Errorf("exit status 251")
	}

	w := doOpen(a, map[string]interface{}{"server_id": defs.DefaultServerID, "port": 5700, "protocol": "udp"})
	require.Equal(t, http.StatusOK, w.Code)
	res := decodeResult(t, w)
	require.NotEqual(t, 200, res.Code)
	require.Contains(t, res.Msg, "reload failed")

	var ev models.AuditEvent
	require.NoError(t, db.Where("event_type = ?", models.AuditEventTypeFirewall).First(&ev).Error)
	require.False(t, ev.Success)
}
