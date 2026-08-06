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
	"github.com/glebarez/sqlite"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var clientQueryDSNCounter uint64

func clientQueryTestApp(t *testing.T) *app.Context {
	t.Helper()
	n := atomic.AddUint64(&clientQueryDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:dao-client-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Client{}, &models.ProxyConfig{}))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst)
}

func seedClientRows(t *testing.T, c *app.Context) {
	t.Helper()
	m := dao.NewMutation(c)
	u := testUserInfo()

	origin := &models.ClientEntity{
		ClientID:      "cli-origin",
		IsShadow:      true, // a shadowed origin keeps its own id, no OriginClientID
		Stopped:       false,
		ConnectSecret: "origin-secret",
	}
	require.NoError(t, m.CreateClient(u, origin))

	child := &models.ClientEntity{
		ClientID:       "cli-child",
		OriginClientID: "cli-origin",
		IsShadow:       false,
		Stopped:        false,
		ConnectSecret:  "child-secret",
	}
	require.NoError(t, m.CreateClient(u, child))
}

// TestGetClientByFilterIsShadowExplicitSQL pins the GORM zero-value trap:
// Where(&ClientEntity{IsShadow: false}) silently DROPS the condition
// (false is the zero value), so a shadow=false filter used to match the
// shadowed origin row as well. The condition must be an explicit SQL
// clause.
func TestGetClientByFilterIsShadowExplicitSQL(t *testing.T) {
	c := clientQueryTestApp(t)
	seedClientRows(t, c)
	u := testUserInfo()
	q := dao.NewQuery(c)

	// the shadowed origin must NOT match a shadow=false filter
	_, err := q.GetClientByFilter(u, &models.ClientEntity{ClientID: "cli-origin"}, lo.ToPtr(false))
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "shadowed origin must be excluded by is_shadow=false")

	// the shadowed origin MUST match a shadow=true filter
	origin, err := q.GetClientByFilter(u, &models.ClientEntity{ClientID: "cli-origin"}, lo.ToPtr(true))
	require.NoError(t, err)
	require.Equal(t, "cli-origin", origin.ClientID)

	// the child MUST match a shadow=false filter
	child, err := q.GetClientByFilter(u, &models.ClientEntity{ClientID: "cli-child"}, lo.ToPtr(false))
	require.NoError(t, err)
	require.Equal(t, "cli-child", child.ClientID)
	require.Equal(t, "cli-origin", child.OriginClientID)

	// the child must NOT match a shadow=true filter
	_, err = q.GetClientByFilter(u, &models.ClientEntity{ClientID: "cli-child"}, lo.ToPtr(true))
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "child must be excluded by is_shadow=true")

	// nil shadow keeps the old behavior: any row matches
	anyRow, err := q.GetClientByFilter(u, &models.ClientEntity{ClientID: "cli-origin"}, nil)
	require.NoError(t, err)
	require.Equal(t, "cli-origin", anyRow.ClientID)
}

// TestGetOriginClientID pins the lock-key resolver: an origin-addressed
// id resolves to itself, a child-addressed id resolves to its origin,
// and both MUST be tenant-scoped (unknown ids fail).
func TestGetOriginClientID(t *testing.T) {
	c := clientQueryTestApp(t)
	seedClientRows(t, c)
	u := testUserInfo()
	q := dao.NewQuery(c)

	originID, err := q.GetOriginClientID(u, "cli-child")
	require.NoError(t, err)
	require.Equal(t, "cli-origin", originID, "child id must resolve to the origin")

	originID, err = q.GetOriginClientID(u, "cli-origin")
	require.NoError(t, err)
	require.Equal(t, "cli-origin", originID, "origin id must resolve to itself")

	_, err = q.GetOriginClientID(u, "cli-ghost")
	require.Error(t, err, "unknown client id must fail (tenant-scoped)")

	_, err = q.GetOriginClientID(u, "")
	require.Error(t, err, "empty client id must fail")
}
