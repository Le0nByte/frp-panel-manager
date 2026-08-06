package dao

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var internalRecoveryDSNCounter uint64

// internalRecoveryDB builds an in-memory SQLite DB for white-box tests of
// the phase-B internals (recoverStaleNoReservationTx /
// recoverStaleTransactionsPhase). Same isolation discipline as
// uniqueMemoryDSN: per-test unique shared-cache name.
func internalRecoveryDB(t *testing.T) *gorm.DB {
	t.Helper()
	n := atomic.AddUint64(&internalRecoveryDSNCounter, 1)
	name := fmt.Sprintf("file:dao-internal-%d-%s", n,
		strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())) +
		"?mode=memory&cache=shared&_pragma=busy_timeout(3000)"
	db, err := gorm.Open(sqlite.Open(name), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.PortReservation{},
		&models.ChangeTransaction{},
		&models.ConfigSnapshot{},
		&models.AuditEvent{},
	))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	return db
}

func internalSeedTx(t *testing.T, db *gorm.DB, txID, status string, startedAt time.Time) {
	t.Helper()
	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID:  txID,
		IdempotencyKey: "k-" + txID,
		RequestHash:    "h",
		ActorUserID:    1,
		TenantID:       1,
		Operation:      models.TxOperationCreate,
		Status:         status,
		StartedAt:      startedAt,
	}).Error)
}

func internalSeedReservation(t *testing.T, db *gorm.DB, txID string, port uint32) {
	t.Helper()
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID:      1,
		ServerID:      "srv-1",
		Protocol:      "tcp",
		RemotePort:    port,
		TransactionID: txID,
		Status:        models.ReservationStatusPending,
		ExpiresAt:     time.Now().Add(time.Hour),
	}).Error)
}

// TestStaleNoReservationTxCASMissesWhenReservationAppears proves the
// guarded CAS re-checks NOT EXISTS reservation: a lease that appeared after
// the candidate query (the concurrent-coordinator race) makes the CAS miss
// with ErrTxStatusConflict — the transaction is never failed and no audit
// is written. Deterministic: the reservation exists before the CAS runs.
func TestStaleNoReservationTxCASMissesWhenReservationAppears(t *testing.T) {
	db := internalRecoveryDB(t)
	now := time.Now()
	cutoff := now.Add(-DefaultStaleTransactionAfter)

	internalSeedTx(t, db, "tx-race", models.TxStatusPending, now.Add(-10*time.Minute))
	// the lease wins the race: it exists before the CAS statement executes
	internalSeedReservation(t, db, "tx-race", 9901)

	var txRow models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", "tx-race").First(&txRow).Error)

	var result SystemRecoveryResult
	err := db.Transaction(func(tx *gorm.DB) error {
		return recoverStaleNoReservationTx(tx, &txRow, cutoff, now, &result)
	})
	require.ErrorIs(t, err, defs.ErrTxStatusConflict, "CAS must miss when a reservation appeared, got %v", err)

	var after models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", "tx-race").First(&after).Error)
	require.Equal(t, models.TxStatusPending, after.Status, "the tx must not be failed on a CAS miss")
	require.Empty(t, after.ErrorCode)

	var audits int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&audits).Error)
	require.Zero(t, audits, "a CAS miss must not audit")
	require.Zero(t, result.FailedTransactions)
	require.Zero(t, result.AuditedEvents)
}

// TestStaleRecoveryPhaseRollsBackPriorChangesOnConflict proves the whole
// recovery pass is atomic: when a later candidate's CAS conflicts, the
// EARLIER candidate's already-applied failure + audit roll back with the
// pass. Deterministic: both per-row recoveries run inside one transaction,
// the second one provably misses.
func TestStaleRecoveryPhaseRollsBackPriorChangesOnConflict(t *testing.T) {
	db := internalRecoveryDB(t)
	now := time.Now()
	cutoff := now.Add(-DefaultStaleTransactionAfter)

	// tx-a: stale pending, NO reservation — recovery succeeds first
	internalSeedTx(t, db, "tx-a", models.TxStatusPending, now.Add(-10*time.Minute))
	// tx-b: stale pending, WITH reservation — recovery CAS-conflicts second
	internalSeedTx(t, db, "tx-b", models.TxStatusPending, now.Add(-10*time.Minute))
	internalSeedReservation(t, db, "tx-b", 9902)

	var result SystemRecoveryResult
	err := db.Transaction(func(tx *gorm.DB) error {
		var a models.ChangeTransaction
		require.NoError(t, tx.Where("transaction_id = ?", "tx-a").First(&a).Error)
		if err := recoverStaleNoReservationTx(tx, &a, cutoff, now, &result); err != nil {
			return err
		}
		var b models.ChangeTransaction
		require.NoError(t, tx.Where("transaction_id = ?", "tx-b").First(&b).Error)
		return recoverStaleNoReservationTx(tx, &b, cutoff, now, &result)
	})
	require.ErrorIs(t, err, defs.ErrTxStatusConflict)

	// the whole pass rolled back: tx-a is still pending, no audit survived
	var a models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", "tx-a").First(&a).Error)
	require.Equal(t, models.TxStatusPending, a.Status, "tx-a's failure must roll back with the pass")
	require.Empty(t, a.ErrorCode)

	var audits int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&audits).Error)
	require.Zero(t, audits, "no audit row may survive a rolled-back pass")
}

// TestStaleRecoveryPhaseFork exercises the phase-B fork directly through the
// internal phase function: pending/preflight → failed (RESERVATION_EXPIRED),
// applying/verifying/rolling_back → rollback_failed (ROLLBACK_FAILED), each
// audited once, with the counters populated.
func TestStaleRecoveryPhaseFork(t *testing.T) {
	db := internalRecoveryDB(t)
	now := time.Now()

	internalSeedTx(t, db, "tx-p", models.TxStatusPending, now.Add(-10*time.Minute))
	internalSeedTx(t, db, "tx-pf", models.TxStatusPreflight, now.Add(-10*time.Minute))
	internalSeedTx(t, db, "tx-ap", models.TxStatusApplying, now.Add(-10*time.Minute))
	internalSeedTx(t, db, "tx-v", models.TxStatusVerifying, now.Add(-10*time.Minute))
	internalSeedTx(t, db, "tx-rb", models.TxStatusRollingBack, now.Add(-10*time.Minute))
	// a fresh tx is not a candidate
	internalSeedTx(t, db, "tx-fresh", models.TxStatusPending, now)

	var result SystemRecoveryResult
	err := db.Transaction(func(tx *gorm.DB) error {
		return recoverStaleTransactionsPhase(tx, now, &result)
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.FailedTransactions)
	require.Equal(t, int64(3), result.BlockedTransactions)
	require.Equal(t, int64(5), result.AuditedEvents)

	expect := map[string]struct {
		status    string
		errorCode string
	}{
		"tx-p":     {models.TxStatusFailed, defs.ErrorCodeReservationExpired},
		"tx-pf":    {models.TxStatusFailed, defs.ErrorCodeReservationExpired},
		"tx-ap":    {models.TxStatusRollbackFailed, defs.ErrorCodeRollbackFailed},
		"tx-v":     {models.TxStatusRollbackFailed, defs.ErrorCodeRollbackFailed},
		"tx-rb":    {models.TxStatusRollbackFailed, defs.ErrorCodeRollbackFailed},
		"tx-fresh": {models.TxStatusPending, ""},
	}
	for txID, want := range expect {
		var row models.ChangeTransaction
		require.NoError(t, db.Where("transaction_id = ?", txID).First(&row).Error)
		require.Equal(t, want.status, row.Status, "%s fork", txID)
		require.Equal(t, want.errorCode, row.ErrorCode, "%s error code", txID)
	}

	var audits int64
	require.NoError(t, db.Model(&models.AuditEvent{}).
		Where("event_type = ?", models.AuditEventTypeTransactionStale).Count(&audits).Error)
	require.Equal(t, int64(5), audits)
}
