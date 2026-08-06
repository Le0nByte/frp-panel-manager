package dao_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var serverCountDSNCounter uint64

func uniqueServerDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&serverCountDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:server-count-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

func setupServerTestApp(t *testing.T) *app.Context {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueServerDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Server{}))
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst)
}

// TestCountServersIncludesDefaultForAdmin pins the overview counts: the
// system default server (tenant/user 0) must be visible to any user the
// same way ListServers already exposes it (upstream defect: CountServers
// filters strictly by user while ListServers ORs the default server in,
// so the dashboard showed 0 total servers even though the default frps
// was running and visible in the list).
func TestCountServersIncludesDefaultForAdmin(t *testing.T) {
	ctx := setupServerTestApp(t)
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)
	admin := testUserInfo() // user_id=1, tenant_id=1

	// default server (system-level, user/tenant 0)
	m.InitDefaultServer("47.109.18.109")

	// one server owned by the admin
	require.NoError(t, m.CreateServer(admin, &models.ServerEntity{
		ServerID:      "admin.srv.1",
		ServerIP:      "10.0.0.1",
		ConnectSecret: "secret-1",
	}))

	total, err := q.CountServers(admin)
	require.NoError(t, err)
	require.Equal(t, int64(2), total, "default server must count toward the total for any user")

	// list view must agree with the count (regression guard)
	listed, err := q.ListServers(admin, 1, 10)
	require.NoError(t, err)
	require.Len(t, listed, 2)

	// a stranger (different tenant) still sees the shared default server
	other := &models.UserEntity{UserID: 2, UserName: "other", Email: "other@example.com", TenantID: 2}
	total2, err := q.CountServers(other)
	require.NoError(t, err)
	require.Equal(t, int64(1), total2, "stranger sees only the shared default server")
}

// TestCountConfiguredServersIncludesDefaultWhenConfigured: once the default
// server has a config, it must count as "configured" for the overview.
func TestCountConfiguredServersIncludesDefaultWhenConfigured(t *testing.T) {
	ctx := setupServerTestApp(t)
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)
	admin := testUserInfo()

	m.InitDefaultServer("47.109.18.109")
	require.NoError(t, m.CreateServer(admin, &models.ServerEntity{
		ServerID:      "admin.srv.2",
		ServerIP:      "10.0.0.2",
		ConnectSecret: "secret-2",
	}))

	// nothing configured yet
	cfgCount, err := q.CountConfiguredServers(admin)
	require.NoError(t, err)
	require.Equal(t, int64(0), cfgCount)

	// configure the default server only (persist the config)
	def, err := q.GetDefaultServer()
	require.NoError(t, err)
	require.NoError(t, def.SetConfigContent(&v1.ServerConfig{}))
	require.NoError(t, m.UpdateDefaultServer(&models.Server{ServerEntity: def}))

	cfgCount, err = q.CountConfiguredServers(admin)
	require.NoError(t, err)
	require.Equal(t, int64(1), cfgCount, "configured default server must count for any user")
}
