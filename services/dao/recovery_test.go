package dao_test

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestSystemRecoverExpiredPending is the RED contract for the startup cleanup
// (spec §6.1.3): expired pending leases are released, their non-terminal
// transactions are marked failed with a stable code and audited, committed
// reservations and terminal transactions are never touched.
func TestSystemRecoverExpiredPending(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	// 1. expired pending lease owned by a tx stuck in preflight
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9001, "tx-stuck", "k1", -time.Minute, now, models.TxStatusPreflight)

	// 2. expired pending lease owned by a tx already terminal (failed)
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9002, "tx-dead", "k2", -time.Minute, now, models.TxStatusFailed)

	// 3. live pending lease (not expired) — must survive
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9003, "tx-live", "k3", time.Hour, now, models.TxStatusPending)

	// 4. committed reservation — must survive
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9004, "tx-committed", "k4", time.Hour, now, models.TxStatusCommitted)
	require.NoError(t, m.CommitReservation(user, "tx-committed", 42))

	res, err := dao.SystemRecoverExpiredPending(ctx, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.ReleasedReservations, "only the two expired pending leases are released")
	assert.Equal(t, int64(1), res.FailedTransactions, "only the non-terminal owner is failed")

	// tx-stuck: failed with stable code + audit trail
	stuck, err := q.FindByID(user, "tx-stuck")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusFailed, stuck.Status)
	assert.Equal(t, "RESERVATION_EXPIRED", stuck.ErrorCode)
	require.NotNil(t, stuck.FinishedAt, "terminal status must stamp finished_at")

	// tx-dead: terminal already, must not be reopened or touched
	dead, err := q.FindByID(user, "tx-dead")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusFailed, dead.Status)
	assert.NotEqual(t, "RESERVATION_EXPIRED", dead.ErrorCode)

	// live pending lease still there
	live, err := q.GetByServerFilter(user, "srv-1", "tcp", 9003)
	require.NoError(t, err)
	require.Len(t, live, 1)
	assert.Equal(t, models.ReservationStatusPending, live[0].Status)

	// committed reservation still there
	committed, err := q.GetByServerFilter(user, "srv-1", "tcp", 9004)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	assert.Equal(t, models.ReservationStatusCommitted, committed[0].Status)

	// cleanup events are audited (lock disappears but reason is known)
	events, total, err := q.List(user, 1, 100, "")
	require.NoError(t, err)
	require.Equal(t, int64(3), total, "one audit per released reservation + one for the failed tx")
	found := 0
	for _, ev := range events {
		if ev.EventType == "reservation_expired" {
			found++
		}
	}
	assert.Equal(t, 3, found, "every released reservation and the failed tx must be audited")
}

func TestSystemRecoverExpiredPendingNoOp(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)

	require.NoError(t, m.Create(user, newTestTx(user, "tx-live", "k", "h", models.TxStatusPending)))
	_, err := m.TryAcquire(user, "srv-1", "tcp", 9101, "tx-live", time.Hour, now)
	require.NoError(t, err)
	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.ReleasedReservations)
	assert.Equal(t, int64(0), res.FailedTransactions)
	assert.Equal(t, int64(0), res.RetainedReservations)
	assert.Equal(t, int64(0), res.BlockedTransactions)

	// no audit events were written
	_, total, err := dao.NewQuery(ctx).List(user, 1, 10, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
}

// TestSystemRecoverExpiredPendingBlocksMidApply is the P0-2 fail-closed
// contract: a transaction that crashed while the candidate config may
// already have been applied (applying/verifying) or while compensation was
// in flight (rolling_back) must NEVER have its lease released. Recovery
// marks it rollback_failed (needs manual intervention), retains the
// reservation in the non-evictable releasing state and audits the block.
func TestSystemRecoverExpiredPendingBlocksMidApply(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	for _, tc := range []struct {
		txID, status string
	}{
		{"tx-applying", models.TxStatusApplying},
		{"tx-verifying", models.TxStatusVerifying},
		{"tx-rolling-back", models.TxStatusRollingBack},
	} {
		acquireWithLifecycle(t, m, user, "srv-1", "tcp", uint32(9200+len(tc.txID)), tc.txID, "k-"+tc.txID, -time.Minute, now, tc.status)
	}

	res, err := dao.SystemRecoverExpiredPending(ctx, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.ReleasedReservations, "mid-apply leases must never be released")
	assert.Equal(t, int64(3), res.BlockedTransactions, "every mid-apply owner is blocked for intervention")
	assert.Equal(t, int64(3), res.RetainedReservations, "every mid-apply lease is retained")
	assert.Equal(t, int64(0), res.FailedTransactions)

	for _, tc := range []struct {
		txID, status string
	}{
		{"tx-applying", models.TxStatusApplying},
		{"tx-verifying", models.TxStatusVerifying},
		{"tx-rolling-back", models.TxStatusRollingBack},
	} {
		tx, err := q.FindByID(user, tc.txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusRollbackFailed, tx.Status, "%s must be blocked for manual intervention", tc.txID)
		assert.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
		require.NotNil(t, tx.FinishedAt, "terminal status must stamp finished_at")

		// the lease must survive, in the non-evictable releasing state
		reservations, err := q.GetByTransactionID(user, tc.txID)
		require.NoError(t, err)
		require.Len(t, reservations, 1, "%s lease must be retained", tc.txID)
		assert.Equal(t, models.ReservationStatusReleasing, reservations[0].Status,
			"retained lease must leave the expired-pending sweep (status releasing)")
	}

	// every block is audited
	events, total, err := q.List(user, 1, 100, "")
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	blocked := 0
	for _, ev := range events {
		if ev.EventType == models.AuditEventTypeReservationExpired && !ev.Success {
			blocked++
		}
	}
	assert.Equal(t, 3, blocked, "each blocked transaction must be audited")
}

// TestSystemRecoverExpiredPendingOrphanRetainsLease pins the orphan rule:
// an expired pending lease whose owning transaction row no longer exists
// cannot be proven safe to release (the candidate config may have been
// applied before the tx row vanished), so the lease is retained and the
// anomaly audited with the TenantID taken from the reservation itself —
// never a fabricated zero.
func TestSystemRecoverExpiredPendingOrphanRetainsLease(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)

	// expired pending lease with NO owning transaction row (orphan: the
	// guarded TryAcquire refuses to create such a lease, so it is seeded
	// directly — a historical anomaly)
	seedOrphanReservation(t, ctx, user.GetTenantID(), "srv-1", "tcp", 9301, "ghost-tx", models.ReservationStatusPending, -time.Minute, now)

	res, err := dao.SystemRecoverExpiredPending(ctx, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.ReleasedReservations, "an orphan lease is never released")
	assert.Equal(t, int64(1), res.RetainedReservations)
	assert.Equal(t, int64(0), res.BlockedTransactions, "no transaction row exists to block")

	// lease retained, still pending (nothing to re-mark)
	reservations, err := q.GetByServerFilter(user, "srv-1", "tcp", 9301)
	require.NoError(t, err)
	require.Len(t, reservations, 1)
	assert.Equal(t, models.ReservationStatusPending, reservations[0].Status)

	// the anomaly audit carries the reservation's own tenant id, never 0
	var ev models.AuditEvent
	require.NoError(t, dbAll(ctx).Where("transaction_id = ?", "ghost-tx").First(&ev).Error)
	assert.Equal(t, user.GetTenantID(), ev.TenantID, "orphan audit tenant must come from the reservation")
	assert.False(t, ev.Success)
}

// TestSystemRecoverExpiredPendingRetainsTerminalRetainOwners pins the
// terminal-retain rule: leases owned by rollback_failed (already blocked)
// or committed (should never be pending, but defend anyway) transactions
// are kept, the transaction row is untouched, and the anomaly is audited.
func TestSystemRecoverExpiredPendingRetainsTerminalRetainOwners(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	for _, tc := range []struct {
		txID, status string
	}{
		{"tx-rf", models.TxStatusRollbackFailed},
		{"tx-committed", models.TxStatusCommitted},
	} {
		acquireWithLifecycle(t, m, user, "srv-1", "tcp", uint32(9400+len(tc.txID)), tc.txID, "k-"+tc.txID, -time.Minute, now, tc.status)
	}

	res, err := dao.SystemRecoverExpiredPending(ctx, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.ReleasedReservations)
	assert.Equal(t, int64(2), res.RetainedReservations)
	assert.Equal(t, int64(0), res.FailedTransactions)
	assert.Equal(t, int64(0), res.BlockedTransactions)

	for _, tc := range []struct {
		txID, status string
	}{
		{"tx-rf", models.TxStatusRollbackFailed},
		{"tx-committed", models.TxStatusCommitted},
	} {
		tx, err := q.FindByID(user, tc.txID)
		require.NoError(t, err)
		assert.Equal(t, tc.status, tx.Status, "terminal owner must not be reopened")
		reservations, err := q.GetByTransactionID(user, tc.txID)
		require.NoError(t, err)
		require.Len(t, reservations, 1, "%s lease must be retained", tc.txID)
	}
}

// TestSystemRecoverExpiredPendingReleasesRolledBackOwners: a rolled_back
// owner is terminal AND proven compensated — its expired lease is safe to
// hard-release (same as failed).
func TestSystemRecoverExpiredPendingReleasesRolledBackOwners(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9501, "tx-rb", "k-rb", -time.Minute, now, models.TxStatusRolledBack)

	res, err := dao.SystemRecoverExpiredPending(ctx, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.ReleasedReservations)
	assert.Equal(t, int64(0), res.RetainedReservations)

	after, err := q.GetByServerFilter(user, "srv-1", "tcp", 9501)
	require.NoError(t, err)
	assert.Empty(t, after, "rolled_back owner's expired lease is released")
}

// dbAll reaches the raw default DB from the app context (audit checks that
// cross the tenant-scoped query surface).
func dbAll(ctx *app.Context) *gorm.DB {
	return ctx.GetApp().GetDBManager().GetDefaultDB()
}

// ---------------------------------------------------------------------------
// Recovery A2 phase B: stale transactions with NO port reservation.
// A transaction that crashed between row creation and TryAcquire (or, for
// updates, reused the old committed reservation without ever creating its
// own lease) stays in-flight forever — the expired-pending sweep never sees
// it because it owns no lease. The same recovery pass therefore also
// recovers transactions older than DefaultStaleTransactionAfter with no
// live reservation: pending/preflight → failed (RESERVATION_EXPIRED),
// applying/verifying/rolling_back → rollback_failed (manual intervention).
// ---------------------------------------------------------------------------

// TestSystemRecoverStaleNoReservationTransactions: stale pending/preflight
// transactions without any reservation are marked failed with the stable
// RESERVATION_EXPIRED code, stamped finished_at and audited as
// transaction_stale.
func TestSystemRecoverStaleNoReservationTransactions(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	// the tx row without a lease IS the historical anomaly being recovered,
	// so creating it directly with an old StartedAt is the sanctioned setup
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-stale-pending", "k-sp", "h", models.TxStatusPending, now.Add(-10*time.Minute))))
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-stale-preflight", "k-sf", "h", models.TxStatusPreflight, now.Add(-10*time.Minute))))

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.FailedTransactions)
	assert.Equal(t, int64(0), res.BlockedTransactions)
	assert.Equal(t, int64(2), res.AuditedEvents)
	assert.Equal(t, int64(0), res.ReleasedReservations, "no reservation exists to release")

	for _, txID := range []string{"tx-stale-pending", "tx-stale-preflight"} {
		tx, err := q.FindByID(user, txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusFailed, tx.Status, "%s must be failed", txID)
		assert.Equal(t, defs.ErrorCodeReservationExpired, tx.ErrorCode)
		require.NotNil(t, tx.FinishedAt, "terminal status must stamp finished_at")
	}

	// one transaction_stale audit per recovered tx
	events, total, err := q.List(user, 1, 100, "")
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	stale := 0
	for _, ev := range events {
		if ev.EventType == models.AuditEventTypeTransactionStale {
			stale++
			assert.False(t, ev.Success)
			assert.Equal(t, defs.ErrorCodeReservationExpired, ev.ErrorCode)
			assert.NotContains(t, ev.Summary, "config", "audit summary must not leak config content")
		}
	}
	assert.Equal(t, 2, stale)
}

// TestSystemRecoverStaleNoReservationMidApplyBlocked: a stale transaction
// that crashed mid-apply (applying/verifying/rolling_back) with no
// reservation may still have applied the candidate config, so it is blocked
// for manual intervention (rollback_failed + ROLLBACK_FAILED), never failed.
func TestSystemRecoverStaleNoReservationMidApplyBlocked(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	for _, status := range []string{
		models.TxStatusApplying,
		models.TxStatusVerifying,
		models.TxStatusRollingBack,
	} {
		txID := "tx-stale-" + status
		require.NoError(t, m.Create(user, newTestTxStartedAt(user, txID, "k-"+txID, "h", status, now.Add(-10*time.Minute))))
	}

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(3), res.BlockedTransactions)
	assert.Equal(t, int64(0), res.FailedTransactions)
	assert.Equal(t, int64(3), res.AuditedEvents)

	for _, status := range []string{
		models.TxStatusApplying,
		models.TxStatusVerifying,
		models.TxStatusRollingBack,
	} {
		tx, err := q.FindByID(user, "tx-stale-"+status)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusRollbackFailed, tx.Status, "%s must be blocked", status)
		assert.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
		require.NotNil(t, tx.FinishedAt)
	}
}

// TestSystemRecoverRecentTransactionNotKilled: the stale cutoff is
// StartedAt-based (never CreatedAt) and a recent slow transaction — even
// one that has been alive for almost the whole threshold — must not be
// touched. Only StartedAt older than now - DefaultStaleTransactionAfter is
// a recovery candidate.
func TestSystemRecoverRecentTransactionNotKilled(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	// just started (a live coordinator between tx creation and acquire)
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-fresh", "k-fr", "h", models.TxStatusPending, now)))
	// slow but under the threshold
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-slow", "k-sl", "h", models.TxStatusPending, now.Add(-dao.DefaultStaleTransactionAfter+time.Minute))))
	// exactly at the boundary: still not stale (strictly older than cutoff)
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-boundary", "k-bo", "h", models.TxStatusPending, now.Add(-dao.DefaultStaleTransactionAfter))))

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.FailedTransactions)
	assert.Equal(t, int64(0), res.BlockedTransactions)
	assert.Equal(t, int64(0), res.AuditedEvents)

	for _, txID := range []string{"tx-fresh", "tx-slow", "tx-boundary"} {
		tx, err := q.FindByID(user, txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusPending, tx.Status, "%s must survive", txID)
	}

	// past the boundary: recovered
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-past", "k-pa", "h", models.TxStatusPending, now.Add(-dao.DefaultStaleTransactionAfter-time.Second))))
	res, err = dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.FailedTransactions)
}

// TestSystemRecoverStaleSkipsTransactionsWithLiveReservations: a
// transaction that owns ANY live reservation — pending (unexpired),
// releasing (blocked), committed — is never a phase-B candidate: its lease
// lifecycle belongs to the reservation sweep (phase A). An EXPIRED pending
// lease is handled by phase A with its original semantics and must not be
// double-handled by phase B.
func TestSystemRecoverStaleSkipsTransactionsWithLiveReservations(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	// live pending lease (not expired): phase A skips, phase B must too
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9601, "tx-live-pending", "k-lp", time.Hour, now, models.TxStatusPending)
	require.NoError(t, dbAll(ctx).Model(&models.ChangeTransaction{}).
		Where("transaction_id = ?", "tx-live-pending").Update("started_at", now.Add(-10*time.Minute)).Error)

	// committed reservation: phase A and B both skip
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9602, "tx-committed-res", "k-cr", time.Hour, now, models.TxStatusPending)
	require.NoError(t, m.CommitReservation(user, "tx-committed-res", 7))
	require.NoError(t, dbAll(ctx).Model(&models.ChangeTransaction{}).
		Where("transaction_id = ?", "tx-committed-res").Update("started_at", now.Add(-10*time.Minute)).Error)

	// releasing reservation (anomaly: recovery-produced status on a pending
	// owner — seeded directly, the only sanctioned way to build it)
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-releasing-res", "k-rr", "h", models.TxStatusPending, now.Add(-10*time.Minute))))
	seedOrphanReservation(t, ctx, user.GetTenantID(), "srv-1", "tcp", 9603, "tx-releasing-res", models.ReservationStatusReleasing, time.Hour, now)

	// expired pending lease: phase A fails the owner and releases the lease
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9604, "tx-expired-res", "k-er", -time.Minute, now, models.TxStatusPending)

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	// exactly ONE failure: the expired-pending owner (phase A). None of the
	// live-reservation owners may be failed by phase B.
	assert.Equal(t, int64(1), res.FailedTransactions)
	assert.Equal(t, int64(0), res.BlockedTransactions)
	assert.Equal(t, int64(1), res.ReleasedReservations, "only the expired lease is released")

	for _, txID := range []string{"tx-live-pending", "tx-committed-res", "tx-releasing-res"} {
		tx, err := q.FindByID(user, txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusPending, tx.Status, "%s must survive phase B", txID)
	}

	// the expired-pending owner was failed exactly once (phase A semantics),
	// and its lease is gone
	expired, err := q.FindByID(user, "tx-expired-res")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusFailed, expired.Status)
	assert.Equal(t, defs.ErrorCodeReservationExpired, expired.ErrorCode)
	rows, err := q.GetByServerFilter(user, "srv-1", "tcp", 9604)
	require.NoError(t, err)
	assert.Empty(t, rows)

	// live leases survive
	for _, port := range []uint32{9601, 9602, 9603} {
		rows, err := q.GetByServerFilter(user, "srv-1", "tcp", port)
		require.NoError(t, err)
		require.Len(t, rows, 1, "lease on %d must survive", port)
	}
}

// TestSystemRecoverStaleRecoversUpdateTxLeavingOldCommittedReservation:
// the root-cause scenario — a same-port UPDATE transaction that reused the
// old committed reservation and crashed before creating its own lease. The
// old committed reservation belongs to the OLD transaction and must not
// shield the CURRENT transaction from phase B; the old row stays
// byte-identical.
func TestSystemRecoverStaleRecoversUpdateTxLeavingOldCommittedReservation(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	// old committed reservation on port 9701 owned by the OLD transaction
	acquireWithLifecycle(t, m, user, "srv-1", "tcp", 9701, "tx-old", "k-old", time.Hour, now, models.TxStatusCommitted)
	require.NoError(t, m.CommitReservation(user, "tx-old", 7))

	// current same-port update tx: pending, stale, NO reservation of its own
	// (crashed between tx creation and its own acquire)
	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-current", "k-cur", "h", models.TxStatusPending, now.Add(-10*time.Minute))))

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.FailedTransactions, "the current update tx must be recovered")
	assert.Equal(t, int64(0), res.ReleasedReservations, "the old committed reservation is untouched")

	current, err := q.FindByID(user, "tx-current")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusFailed, current.Status)
	assert.Equal(t, defs.ErrorCodeReservationExpired, current.ErrorCode)

	// old committed reservation byte-identical
	rows, err := q.GetByServerFilter(user, "srv-1", "tcp", 9701)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, models.ReservationStatusCommitted, rows[0].Status)
	assert.Equal(t, "tx-old", rows[0].TransactionID)
	require.NotNil(t, rows[0].OwnerProxyID)
	assert.Equal(t, uint(7), *rows[0].OwnerProxyID)

	oldTx, err := q.FindByID(user, "tx-old")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusCommitted, oldTx.Status, "the old committed transaction is untouched")
}

// TestSystemRecoverStaleLeavesTerminalTransactionsUntouched: terminal
// transactions (committed/failed/rolled_back/rollback_failed) with no
// reservation are never phase-B candidates and never audited.
func TestSystemRecoverStaleLeavesTerminalTransactionsUntouched(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	for _, status := range []string{
		models.TxStatusCommitted,
		models.TxStatusFailed,
		models.TxStatusRolledBack,
		models.TxStatusRollbackFailed,
	} {
		txID := "tx-term-" + status
		require.NoError(t, m.Create(user, newTestTxStartedAt(user, txID, "k-"+txID, "h", status, now.Add(-10*time.Minute))))
	}

	res, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.FailedTransactions)
	assert.Equal(t, int64(0), res.BlockedTransactions)
	assert.Equal(t, int64(0), res.AuditedEvents)

	for _, status := range []string{
		models.TxStatusCommitted,
		models.TxStatusFailed,
		models.TxStatusRolledBack,
		models.TxStatusRollbackFailed,
	} {
		tx, err := q.FindByID(user, "tx-term-"+status)
		require.NoError(t, err)
		assert.Equal(t, status, tx.Status, "%s must be untouched", status)
	}
}

// TestSystemRecoverExpiredPendingRollsBackWholePassOnAuditFailure proves
// the pass is atomic: when the phase-B audit insert fails mid-pass, the
// ENTIRE pass (including the already-applied status change) rolls back. The
// failure is injected deterministically with a database trigger — no sleep,
// no production test hooks.
func TestSystemRecoverExpiredPendingRollsBackWholePassOnAuditFailure(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	q := dao.NewQuery(ctx)
	m := dao.NewMutation(ctx)

	require.NoError(t, m.Create(user, newTestTxStartedAt(user, "tx-stale", "k-s", "h", models.TxStatusPending, now.Add(-10*time.Minute))))

	// inject a deterministic audit failure: any audit_event INSERT aborts
	require.NoError(t, dbAll(ctx).Exec(
		"CREATE TRIGGER fail_stale_audit BEFORE INSERT ON audit_event BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END",
	).Error)

	_, err := dao.SystemRecoverExpiredPending(ctx, now)
	require.Error(t, err, "the injected audit failure must abort the whole pass")

	// the whole pass rolled back: the tx is still pending and no audit row
	// was persisted
	tx, err := q.FindByID(user, "tx-stale")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, tx.Status, "status change must roll back with the failed pass")
	assert.Empty(t, tx.ErrorCode)

	_, total, err := q.List(user, 1, 10, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), total, "no audit row may survive a rolled-back pass")
}
