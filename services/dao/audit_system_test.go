package dao_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// TestLogSystemForcesSystemIdentity pins the I3-A system-audit contract:
// LogSystem appends a row with TenantID=0 and ActorUserID=0 REGARDLESS of
// what the caller set, and only accepts rpc.* event types so ordinary
// authenticated flows (which must use Log with their own tenant/actor)
// can never forge a system-level row.
func TestLogSystemForcesSystemIdentity(t *testing.T) {
	c := setupTestApp(t)
	m := dao.NewMutation(c)

	require.NoError(t, m.LogSystem(&models.AuditEvent{
		EventType:   models.AuditEventTypeRPCSecretFailure,
		Summary:     "rpc secret failure: ip=1.2.3.4 node=n1 layer=client count=7",
		NodeID:      "n1",
		TenantID:    999, // must be overwritten
		ActorUserID: 888, // must be overwritten
		ErrorCode:   "RPC_SECRET_FAILURE",
	}))

	db := c.GetApp().GetDBManager().GetDefaultDB()
	var row models.AuditEvent
	require.NoError(t, db.First(&row, "event_type = ?", models.AuditEventTypeRPCSecretFailure).Error)
	assert.Equal(t, 0, row.TenantID, "system row tenant must be forced to 0")
	assert.Equal(t, 0, row.ActorUserID, "system row actor must be forced to 0")
	assert.Equal(t, "n1", row.NodeID)
	assert.Equal(t, "RPC_SECRET_FAILURE", row.ErrorCode)
	assert.False(t, row.Success, "system audit rows must never claim success")
}

// TestLogSystemRejectsNil pins the nil guard.
func TestLogSystemRejectsNil(t *testing.T) {
	c := setupTestApp(t)
	m := dao.NewMutation(c)
	err := m.LogSystem(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, models.ErrNilAuditEvent)
}

// TestLogSystemRejectsNonRPCEvent pins that only the rpc.* prefix is
// accepted: an authenticated-flow event type must fail so LogSystem can
// never be used to write tenant rows behind the caller's back.
func TestLogSystemRejectsNonRPCEvent(t *testing.T) {
	c := setupTestApp(t)
	m := dao.NewMutation(c)

	err := m.LogSystem(&models.AuditEvent{
		EventType: "proxy.create", // ordinary authenticated event type
		Summary:   "must be rejected",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, dao.ErrSystemAuditEventTypeForbidden)

	// rpc.* but empty summary body still accepted (append-only contract:
	// only nil/type-prefix are policed here)
	require.NoError(t, m.LogSystem(&models.AuditEvent{
		EventType: models.AuditEventTypeRPCHandshakeFailure,
		Summary:   "rpc handshake failure: ip=9.9.9.9 node= layer= count=1",
	}))
}

// TestLogSystemNilDB pins the defensive nil-DB path: without a registered
// default DB the mutation returns an error instead of panicking.
func TestLogSystemNilDB(t *testing.T) {
	c := setupTestApp(t)
	// Remove the default DB so GetDefaultDB() returns nil.
	dbm := c.GetApp().GetDBManager()
	dbm.RemoveDB("sqlite3", "default")
	m := dao.NewMutation(c)

	err := m.LogSystem(&models.AuditEvent{
		EventType: models.AuditEventTypeRPCSecretFailure,
		Summary:   "rpc secret failure: ip=1.1.1.1 node= layer= count=1",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, dao.ErrSystemAuditDBUnavailable)
}

// TestLogSystemDBErrorPropagates pins that a real DB error is returned
// unchanged (not swallowed, not reclassified).
func TestLogSystemDBErrorPropagates(t *testing.T) {
	// The audit_event table is NOT migrated in this app: the CREATE fails
	// with a real gorm error which must come back as-is.
	appNoAudit := clientQueryTestApp(t)
	m := dao.NewMutation(appNoAudit)
	err := m.LogSystem(&models.AuditEvent{
		EventType: models.AuditEventTypeRPCSecretFailure,
		Summary:   "rpc secret failure: ip=1.1.1.1 node= layer= count=1",
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, dao.ErrSystemAuditDBUnavailable)
	require.NotErrorIs(t, err, dao.ErrSystemAuditEventTypeForbidden)
	require.False(t, errors.Is(err, models.ErrNilAuditEvent))
}

// TestLogSystemCanceledContextReturnsErrorNoRow (I3-C): LogSystem must
// create rows through db.WithContext so a cancelled/deadlined mutation
// context aborts the write — the shutdown-time audit drain can never hang
// forever on a stuck database. A cancelled context returns an error and
// leaves no row behind.
func TestLogSystemCanceledContextReturnsErrorNoRow(t *testing.T) {
	// Build the same in-memory app as setupTestApp but with a context that
	// is already cancelled: the mutation inherits it via WithContext.
	db, err := gorm.Open(sqlite.Open(uniqueMemoryDSN(t)), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.AuditEvent{}))

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)

	appInst := app.NewApp()
	appInst.SetDBManager(dbm)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := app.NewContext(ctx, appInst)
	m := dao.NewMutation(c)

	err = m.LogSystem(&models.AuditEvent{
		EventType: models.AuditEventTypeRPCSecretFailure,
		Summary:   "rpc secret failure: ip=1.2.3.4 node=n1 layer=client count=1",
	})
	require.Error(t, err, "a cancelled context must abort the system audit write")

	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	assert.Zero(t, count, "a cancelled write must leave no audit row")

	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

// TestLogSystemUnchangedByReadOnlyMode (I3-C): the guard's system audit
// sink stays writable while the master runs read-only (blocked snapshot
// key) — the blocked state only gates snapshot/rule writes, never the
// append-only system audit trail.
func TestLogSystemUnchangedByReadOnlyMode(t *testing.T) {
	readonly.Set(true, "snapshot key mismatch (test fixture)")
	t.Cleanup(readonly.ResetForTest)

	c := setupTestApp(t)
	m := dao.NewMutation(c)
	require.NoError(t, m.LogSystem(&models.AuditEvent{
		EventType: models.AuditEventTypeRPCHandshakeFailure,
		Summary:   "rpc handshake failure: ip=1.2.3.4 node= layer=pre_tls count=1",
	}))

	db := c.GetApp().GetDBManager().GetDefaultDB()
	var row models.AuditEvent
	require.NoError(t, db.First(&row, "event_type = ?", models.AuditEventTypeRPCHandshakeFailure).Error)
	assert.Zero(t, row.TenantID)
	assert.Zero(t, row.ActorUserID)
}

// TestLogUnchangedByLogSystem pins that the existing tenant-scoped Log
// keeps its exact behavior (force tenant/actor from userInfo, no rpc.*
// restriction) — adding LogSystem must not change Log.
func TestLogUnchangedByLogSystem(t *testing.T) {
	c := setupTestApp(t)
	user := testUserInfo()
	m := dao.NewMutation(c)

	require.NoError(t, m.Log(user, &models.AuditEvent{
		EventType: "proxy.create", // non-rpc is fine for tenant-scoped Log
		Summary:   "create tcp ok",
		TenantID:  77,
	}))

	db := c.GetApp().GetDBManager().GetDefaultDB()
	var row models.AuditEvent
	require.NoError(t, db.First(&row, "event_type = ?", "proxy.create").Error)
	assert.Equal(t, user.GetTenantID(), row.TenantID, "Log must keep forcing tenant from userInfo")
	assert.Equal(t, user.GetUserID(), row.ActorUserID, "Log must keep forcing actor from userInfo")
}
