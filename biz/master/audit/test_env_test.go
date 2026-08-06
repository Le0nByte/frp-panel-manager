package audit

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

var auditDSNCounter uint64

// uniqueAuditDSN returns a per-call in-memory SQLite DSN (same isolation
// recipe as the DAO tests: monotonic counter + sanitized test name; the
// %p-of-T recipe is NOT used because Go reuses T addresses across tests).
func uniqueAuditDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&auditDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:audit-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

// newAuditTestEnv builds an app.Context backed by an in-memory SQLite
// database with the three port-manager tables the audit handlers read
// (audit_event, change_transaction, config_snapshot), and an authenticated
// user in the context (tenant 1). The same DB is reachable through the
// real DAO for seeding.
func newAuditTestEnv(t *testing.T) (*app.Context, models.UserInfo) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueAuditDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.AuditEvent{},
		&models.ChangeTransaction{},
		&models.ConfigSnapshot{},
	))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)

	user := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1}
	ctx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), appInst)
	return ctx, user
}

// newAuditTestEnvTenant builds a context authenticated as a DIFFERENT
// tenant over the SAME database (tenant isolation tests).
func newAuditTestEnvTenant(t *testing.T, ctx *app.Context, tenantID int) (*app.Context, models.UserInfo) {
	t.Helper()
	user := &models.UserEntity{UserID: 900 + tenantID, UserName: "other", Email: "other@example.com", TenantID: tenantID}
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), ctx.GetApp()), user
}

// newAuditTestEnvAnonymous builds a context with NO authenticated user
// (fail-closed user check).
func newAuditTestEnvAnonymous(ctx *app.Context) *app.Context {
	return app.NewContext(context.Background(), ctx.GetApp())
}

// seedAuditEvent writes one audit event through the REAL DAO Log (which
// forces TenantID/ActorUserID from the caller).
func seedAuditEvent(t *testing.T, ctx *app.Context, user models.UserInfo, ev *models.AuditEvent) {
	t.Helper()
	require.NoError(t, dao.NewMutation(ctx).Log(user, ev))
}

// seedCommittedTx writes a committed ChangeTransaction row through the REAL
// DAO Create (which forces TenantID/ActorUserID from the caller).
func seedCommittedTx(t *testing.T, ctx *app.Context, user models.UserInfo, txID string, proxyID *uint, evidence string) {
	t.Helper()
	require.NoError(t, dao.NewMutation(ctx).Create(user, &models.ChangeTransaction{
		TransactionID:  txID,
		IdempotencyKey: "idem-" + txID,
		RequestHash:    "hash-" + txID,
		Operation:      models.TxOperationCreate,
		ProxyID:        proxyID,
		Status:         models.TxStatusCommitted,
		EvidenceJSON:   evidence,
	}))
}

// seedSnapshot writes one encrypted snapshot through the REAL DAO
// SaveEncrypted (metadata-only reads are what the GetAudit handler is
// allowed to surface; ciphertext/nonce never leave the server).
func seedSnapshot(t *testing.T, ctx *app.Context, user models.UserInfo, txID, side, nodeType, nodeID string) {
	t.Helper()
	key := make([]byte, 32)
	_, err := dao.NewMutation(ctx).SaveEncrypted(user, txID, side, nodeType, nodeID, []byte(`{"name":"ssh"}`), key)
	require.NoError(t, err)
}
