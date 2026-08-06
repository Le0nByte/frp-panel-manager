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
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var secretDSNCounter uint64

// secretTestApp migrates the Client and Server tables on a per-test
// in-memory SQLite database (cache=shared + unique name isolates tests).
func secretTestApp(t *testing.T) *app.Context {
	t.Helper()
	n := atomic.AddUint64(&secretDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:dao-secret-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Client{}, &models.Server{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst)
}

func seedClientWithSecret(t *testing.T, c *app.Context, clientID, secret string) {
	t.Helper()
	require.NoError(t, dao.NewMutation(c).CreateClient(testUserInfo(), &models.ClientEntity{
		ClientID:      clientID,
		ConnectSecret: secret,
	}))
}

func seedServerWithSecret(t *testing.T, c *app.Context, serverID, secret string) {
	t.Helper()
	require.NoError(t, dao.NewMutation(c).CreateServer(testUserInfo(), &models.ServerEntity{
		ServerID:      serverID,
		ConnectSecret: secret,
	}))
}

// TestValidateClientSecretTypedErrors pins the I3-A contract: empty and
// mismatch inputs return the exported ErrInvalidClientSecret sentinel
// (errors.Is MUST hold), while a missing row keeps gorm.ErrRecordNotFound
// and other DB errors untouched — classification never relies on strings.
func TestValidateClientSecretTypedErrors(t *testing.T) {
	c := secretTestApp(t)
	seedClientWithSecret(t, c, "cli-a", "good-secret")
	q := dao.NewQuery(c)

	// happy path: exact secret match returns the entity
	got, err := q.ValidateClientSecret("cli-a", "good-secret")
	require.NoError(t, err)
	require.Equal(t, "cli-a", got.ClientID)

	// empty clientID / empty secret are secret validation failures
	_, err = q.ValidateClientSecret("", "good-secret")
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "empty client id must be the typed sentinel")
	_, err = q.ValidateClientSecret("cli-a", "")
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "empty secret must be the typed sentinel")
	_, err = q.ValidateClientSecret("", "")
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)

	// mismatch is the same sentinel, not a raw fmt error
	_, err = q.ValidateClientSecret("cli-a", "wrong-secret")
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "mismatch must be the typed sentinel")
	require.NotErrorIs(t, err, gorm.ErrRecordNotFound)

	// unknown client id is the ORIGINAL gorm error, never the sentinel
	_, err = q.ValidateClientSecret("cli-ghost", "good-secret")
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "missing row must keep gorm.ErrRecordNotFound")
	require.NotErrorIs(t, err, dao.ErrInvalidClientSecret)
}

// TestValidateServerSecretTypedErrors is the server-side twin of the
// client contract above.
func TestValidateServerSecretTypedErrors(t *testing.T) {
	c := secretTestApp(t)
	seedServerWithSecret(t, c, "srv-a", "good-secret")
	q := dao.NewQuery(c)

	got, err := q.ValidateServerSecret("srv-a", "good-secret")
	require.NoError(t, err)
	require.Equal(t, "srv-a", got.ServerID)

	_, err = q.ValidateServerSecret("", "good-secret")
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret, "empty server id must be the typed sentinel")
	_, err = q.ValidateServerSecret("srv-a", "")
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret, "empty secret must be the typed sentinel")

	_, err = q.ValidateServerSecret("srv-a", "wrong-secret")
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret, "mismatch must be the typed sentinel")
	require.NotErrorIs(t, err, gorm.ErrRecordNotFound)

	_, err = q.ValidateServerSecret("srv-ghost", "good-secret")
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "missing row must keep gorm.ErrRecordNotFound")
	require.NotErrorIs(t, err, dao.ErrInvalidServerSecret)
}

// TestValidateSecretSentinelsDistinct pins that the client and server
// sentinels are separate errors so callers can branch per side.
func TestValidateSecretSentinelsDistinct(t *testing.T) {
	require.NotEqual(t, dao.ErrInvalidClientSecret, dao.ErrInvalidServerSecret)
	require.ErrorIs(t, dao.ErrInvalidClientSecret, dao.ErrInvalidClientSecret)
	require.ErrorIs(t, dao.ErrInvalidServerSecret, dao.ErrInvalidServerSecret)
}
