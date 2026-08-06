package server

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var safeUpdateDSNCounter uint64

func uniqueSafeDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&safeUpdateDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:safe-frps-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

func setupSafeUpdateEnv(t *testing.T) (*app.Context, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueSafeDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Server{}, &models.AuditEvent{}))
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	a := app.NewApp()
	a.SetDBManager(dbm)
	return app.NewContext(context.Background(), a), db
}

func adminCtx(c *app.Context) *app.Context {
	user := &models.UserEntity{UserID: 1, UserName: "admin", Email: "admin@example.com", TenantID: 1}
	return app.NewContext(context.WithValue(c, defs.UserInfoKey, user), c.GetApp())
}

func TestUpdateFrpsHandlerSafePersistsAndAudits(t *testing.T) {
	base, db := setupSafeUpdateEnv(t)
	ctx := adminCtx(base)
	m := dao.NewMutation(ctx)
	m.InitDefaultServer("47.109.18.109")

	resp, err := UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{
		ServerId: proto.String(defs.DefaultServerID),
		Config:   []byte(`{"bindPort":7002,"allowPorts":[{"start":7000,"end":7020}]}`),
		Comment:  proto.String("aliyun frps"),
	})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// server row persisted with config + comment
	var srv models.Server
	require.NoError(t, db.Where("server_id = ?", defs.DefaultServerID).First(&srv).Error)
	require.Equal(t, "aliyun frps", srv.Comment)
	require.NotEmpty(t, srv.ConfigContent)
	cfg, err := srv.GetConfigContent()
	require.NoError(t, err)
	require.Equal(t, 7002, cfg.BindPort)

	// audit row written, summary has no config bytes (no secret leak)
	var audit models.AuditEvent
	require.NoError(t, db.Where("event_type = ?", models.AuditEventTypeUpdate).First(&audit).Error)
	require.True(t, audit.Success)
	require.Equal(t, 1, audit.ActorUserID) // tenant-scoped, forced from user
	require.NotContains(t, audit.Summary, "7002")
	require.Contains(t, audit.Summary, defs.DefaultServerID)
}

func TestUpdateFrpsHandlerSafeRejectsInvalidConfig(t *testing.T) {
	base, db := setupSafeUpdateEnv(t)
	ctx := adminCtx(base)
	m := dao.NewMutation(ctx)
	m.InitDefaultServer("47.109.18.109")

	// invalid bindPort out of range
	_, err := UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{
		ServerId: proto.String(defs.DefaultServerID),
		Config:   []byte(`{"bindPort":99999}`),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bindPort")

	// invalid JSON
	_, err = UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{
		ServerId: proto.String(defs.DefaultServerID),
		Config:   []byte(`{not-json`),
	})
	require.Error(t, err)

	// nothing persisted on failure
	var srv models.Server
	require.NoError(t, db.Where("server_id = ?", defs.DefaultServerID).First(&srv).Error)
	require.Empty(t, srv.ConfigContent)
}

func TestUpdateFrpsHandlerSafeRequiresRequestFields(t *testing.T) {
	base, _ := setupSafeUpdateEnv(t)
	ctx := adminCtx(base)

	_, err := UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{Config: []byte(`{"bindPort":7000}`)})
	require.Error(t, err)

	_, err = UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{ServerId: proto.String("x")})
	require.Error(t, err)
}

func TestUpdateFrpsHandlerSafeOwnedServerByOwner(t *testing.T) {
	base, db := setupSafeUpdateEnv(t)
	ctx := adminCtx(base)
	m := dao.NewMutation(ctx)
	require.NoError(t, m.CreateServer(&models.UserEntity{UserID: 1, TenantID: 1}, &models.ServerEntity{
		ServerID:      "my.srv",
		ServerIP:      "10.0.0.5",
		ConnectSecret: "sec",
	}))

	resp, err := UpdateFrpsHandlerSafe(ctx, &pb.UpdateFRPSRequest{
		ServerId: proto.String("my.srv"),
		Config:   []byte(`{"bindPort":7100}`),
	})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	var srv models.Server
	require.NoError(t, db.Where("server_id = ?", "my.srv").First(&srv).Error)
	cfg, err := srv.GetConfigContent()
	require.NoError(t, err)
	require.Equal(t, 7100, cfg.BindPort)
}