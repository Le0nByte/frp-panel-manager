package dao_test

import (
	"errors"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSnapshotKey returns a fixed 32-byte AES key for DAO round-trip tests.
func testSnapshotKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}

func TestTryAcquireSuccess(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	require.NoError(t, m.Create(user, newTestTx(user, "tx-1", "k-1", "h", models.TxStatusPending)))
	res, err := m.TryAcquire(user, "aliyun-1", "tcp", 8000, "tx-1", 120*time.Second, now)
	require.NoError(t, err)
	assert.Equal(t, models.ReservationStatusPending, res.Status)
	assert.False(t, res.IsExpired(now.Add(119*time.Second)))
	assert.True(t, res.IsExpired(now.Add(121*time.Second)))
}

func TestTryAcquireConflict(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	require.NoError(t, m.Create(user, newTestTx(user, "tx-1", "k-1", "h", models.TxStatusPending)))
	_, err := m.TryAcquire(user, "aliyun-1", "tcp", 8000, "tx-1", 120*time.Second, now)
	require.NoError(t, err)

	require.NoError(t, m.Create(user, newTestTx(user, "tx-2", "k-2", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(user, "aliyun-1", "tcp", 8000, "tx-2", 120*time.Second, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrReservationConflict), "expected reservation conflict, got %v", err)
}

func TestTryAcquireDifferentProtocolAllowed(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	require.NoError(t, m.Create(user, newTestTx(user, "tx-1", "k-1", "h", models.TxStatusPending)))
	_, err := m.TryAcquire(user, "aliyun-1", "tcp", 8000, "tx-1", 120*time.Second, now)
	require.NoError(t, err)
	require.NoError(t, m.Create(user, newTestTx(user, "tx-2", "k-2", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(user, "aliyun-1", "udp", 8000, "tx-2", 120*time.Second, now)
	require.NoError(t, err)
}

func TestTryAcquireReplacesExpiredPending(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	m := dao.NewMutation(ctx)
	user := testUserInfo()

	// expired pending lease owned by a terminal FAILED transaction: the
	// owner is proven safe to forget, so the lease may be evicted
	acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", 8000, "tx-stale", "k-stale", -time.Minute, now, models.TxStatusFailed)

	// new acquire must evict the stale lease, audit it and succeed
	require.NoError(t, m.Create(user, newTestTx(user, "tx-new", "k-new", "h", models.TxStatusPending)))
	res, err := m.TryAcquire(user, "aliyun-1", "tcp", 8000, "tx-new", 120*time.Second, now)
	require.NoError(t, err)
	assert.Equal(t, "tx-new", res.TransactionID)

	// the eviction must be audited (spec §6.1.5: no lock disappears
	// without a reason)
	_, total, err := dao.NewQuery(ctx).List(user, 1, 10, "")
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "eviction audit must be written")

	// committed reservation must NOT be evicted by a new acquire
	acquireWithLifecycle(t, m, user, "aliyun-1", "udp", 9999, "tx-c", "k-c", 120*time.Second, now, models.TxStatusCommitted)
	require.NoError(t, m.CommitReservation(user, "tx-c", 42))
	require.NoError(t, m.Create(user, newTestTx(user, "tx-d", "k-d", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(user, "aliyun-1", "udp", 9999, "tx-d", 120*time.Second, now.Add(time.Hour))
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrReservationConflict))
}

// TestTryAcquireEvictsExpiredPendingOfPendingOwner: an expired lease whose
// owner is still pending/preflight is stale (the 120s lease is the crash
// signal). TryAcquire marks the owner failed with RESERVATION_EXPIRED
// (same semantics as the system recovery), audits BOTH the failure and the
// eviction, then inserts the new lease — all in one transaction.
func TestTryAcquireEvictsExpiredPendingOfPendingOwner(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)
	user := testUserInfo()

	for _, tc := range []struct {
		txID, status string
	}{
		{"tx-pending", models.TxStatusPending},
		{"tx-preflight", models.TxStatusPreflight},
	} {
		acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", uint32(8100+len(tc.txID)), tc.txID, "k-"+tc.txID, -time.Minute, now, tc.status)

		require.NoError(t, m.Create(user, newTestTx(user, "tx-new-"+tc.txID, "k-new-"+tc.txID, "h", models.TxStatusPending)))
		res, err := m.TryAcquire(user, "aliyun-1", "tcp", uint32(8100+len(tc.txID)), "tx-new-"+tc.txID, time.Hour, now)
		require.NoError(t, err)
		assert.Equal(t, "tx-new-"+tc.txID, res.TransactionID)

		// the stale owner is failed with the stable code and stamped
		owner, err := q.FindByID(user, tc.txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusFailed, owner.Status)
		assert.Equal(t, defs.ErrorCodeReservationExpired, owner.ErrorCode)
		require.NotNil(t, owner.FinishedAt)
	}

	// two audits per eviction: tx failure + lease release
	_, total, err := q.List(user, 1, 100, "")
	require.NoError(t, err)
	assert.Equal(t, int64(4), total)
}

// TestTryAcquireRefusesMidApplyAndOrphanOwners pins the P0-2 steal
// protection: an expired pending lease whose owner may have applied the
// config (applying/verifying/rolling_back), is already blocked
// (rollback_failed), is committed, is orphaned, or belongs to ANOTHER
// tenant must NEVER be silently deleted — the new acquire gets
// RESERVATION_CONFLICT and the lease survives untouched.
func TestTryAcquireRefusesMidApplyAndOrphanOwners(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)
	user := testUserInfo()

	cases := []struct {
		name   string
		port   uint32
		txID   string
		status string // "" => orphan (no tx row)
	}{
		{"applying", 8201, "tx-applying", models.TxStatusApplying},
		{"verifying", 8202, "tx-verifying", models.TxStatusVerifying},
		{"rolling_back", 8203, "tx-rolling-back", models.TxStatusRollingBack},
		{"rollback_failed", 8204, "tx-rf", models.TxStatusRollbackFailed},
		{"committed", 8205, "tx-committed", models.TxStatusCommitted},
		{"orphan", 8206, "ghost-tx", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.status == "" {
				// orphan: no owner row exists — TryAcquire refuses to create
				// such a lease, so it is seeded directly (test-only anomaly).
				seedOrphanReservation(t, ctx, user.GetTenantID(), "aliyun-1", "tcp", tc.port, tc.txID, models.ReservationStatusPending, -time.Minute, now)
			} else {
				acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", tc.port, tc.txID, "k-"+tc.txID, -time.Minute, now, tc.status)
			}

			intruderTx := "tx-intruder-" + tc.txID
			require.NoError(t, m.Create(user, newTestTx(user, intruderTx, "k-intruder-"+tc.txID, "h", models.TxStatusPending)))
			_, err := m.TryAcquire(user, "aliyun-1", "tcp", tc.port, intruderTx, time.Hour, now)
			require.Error(t, err)
			assert.True(t, errors.Is(err, defs.ErrReservationConflict), "expected conflict, got %v", err)

			// the lease survives, still owned by the original tx
			rows, err := q.GetByServerFilter(user, "aliyun-1", "tcp", tc.port)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tc.txID, rows[0].TransactionID)
			if tc.status != "" {
				owner, err := q.FindByID(user, tc.txID)
				require.NoError(t, err)
				assert.Equal(t, tc.status, owner.Status, "owner must not be touched on conflict")
			}
		})
	}

	// no audits: nothing was released
	_, total, err := q.List(user, 1, 100, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), total, "refused evictions must not write audits")
}

// TestTryAcquireRefusesCrossTenantEviction: a lease owned by another
// tenant is untouchable even when expired — the global unique index makes
// the physical port one resource, but only its owner's recovery path may
// release it.
func TestTryAcquireRefusesCrossTenantEviction(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	m := dao.NewMutation(ctx)
	tenant1 := testUserInfo()
	tenant2 := testUserFor(2, 2)

	require.NoError(t, m.Create(tenant2, newTestTx(tenant2, "tx-other", "k-other", "h", models.TxStatusPending)))
	_, err := m.TryAcquire(tenant2, "aliyun-1", "tcp", 8301, "tx-other", -time.Minute, now)
	require.NoError(t, err)

	require.NoError(t, m.Create(tenant1, newTestTx(tenant1, "tx-mine", "k-mine", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(tenant1, "aliyun-1", "tcp", 8301, "tx-mine", time.Hour, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrReservationConflict))

	// the other tenant's lease and transaction are untouched
	rows, err := dao.NewQuery(ctx).GetByServerFilter(tenant2, "aliyun-1", "tcp", 8301)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "tx-other", rows[0].TransactionID)
	owner, err := dao.NewQuery(ctx).FindByID(tenant2, "tx-other")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, owner.Status)
}

// ---- owner transaction guard (Recovery A2): a lease may only ever be
// created for a transaction that exists, belongs to the calling
// tenant+actor, and is still pending (spec §8.1 step 3). Every rejection
// must leave zero reservation rows behind. ----

// TestTryAcquireRejectsMissingOwnerTransaction: no owner row at all →
// ErrTransactionNotFound and no reservation. A lease for a transaction that
// never existed would otherwise be an orphan from birth.
func TestTryAcquireRejectsMissingOwnerTransaction(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	_, err := m.TryAcquire(user, "srv-1", "tcp", 8001, "ghost-tx", time.Hour, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound), "expected ErrTransactionNotFound, got %v", err)

	rows, err := q.GetByServerFilter(user, "srv-1", "tcp", 8001)
	require.NoError(t, err)
	assert.Empty(t, rows, "no reservation may be created for a missing owner")
}

// TestTryAcquireRejectsForeignOwnerTransaction: the owner row exists but is
// NOT owned by this tenant+actor — the acquire must miss exactly like a
// missing row (ErrTransactionNotFound, no leak of the row's existence).
func TestTryAcquireRejectsForeignOwnerTransaction(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)
	tenant1 := testUserInfo()
	tenant2 := testUserFor(2, 2)
	tenant1OtherActor := testUserFor(3, 1) // same tenant, different actor

	require.NoError(t, m.Create(tenant2, newTestTx(tenant2, "tx-other-tenant", "k-ot", "h", models.TxStatusPending)))
	require.NoError(t, m.Create(tenant1OtherActor, newTestTx(tenant1OtherActor, "tx-other-actor", "k-oa", "h", models.TxStatusPending)))

	// other tenant
	_, err := m.TryAcquire(tenant1, "srv-1", "tcp", 8002, "tx-other-tenant", time.Hour, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound), "cross-tenant owner must look like NotFound, got %v", err)

	// same tenant, different actor
	_, err = m.TryAcquire(tenant1, "srv-1", "tcp", 8003, "tx-other-actor", time.Hour, now)
	require.Error(t, err)
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound), "cross-actor owner must look like NotFound, got %v", err)

	rows, err := q.GetByServerFilter(tenant1, "srv-1", "tcp", 8002)
	require.NoError(t, err)
	assert.Empty(t, rows)
	rows, err = q.GetByServerFilter(tenant1, "srv-1", "tcp", 8003)
	require.NoError(t, err)
	assert.Empty(t, rows)

	// the foreign owner rows are untouched
	owner, err := q.FindByID(tenant2, "tx-other-tenant")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, owner.Status)
	owner, err = q.FindByID(tenant1OtherActor, "tx-other-actor")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, owner.Status)
}

// TestTryAcquireRejectsNonPendingOwner: an owner that has left pending —
// preflight, applying, verifying, rolling_back, or any terminal state — is
// a crashed/stale or committed transaction; acquiring a lease for it would
// create a reservation owned by a transaction that will never run the
// normal commit path. Rejected with ErrInvalidTxTransition (stable
// sentinel), zero reservations, owner untouched.
func TestTryAcquireRejectsNonPendingOwner(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	cases := []struct {
		txID, status string
	}{
		{"tx-preflight", models.TxStatusPreflight},
		{"tx-applying", models.TxStatusApplying},
		{"tx-verifying", models.TxStatusVerifying},
		{"tx-rolling-back", models.TxStatusRollingBack},
		{"tx-committed", models.TxStatusCommitted},
		{"tx-failed", models.TxStatusFailed},
		{"tx-rolled-back", models.TxStatusRolledBack},
		{"tx-rf", models.TxStatusRollbackFailed},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			require.NoError(t, m.Create(user, newTestTx(user, tc.txID, "k-"+tc.txID, "h", tc.status)))
			_, err := m.TryAcquire(user, "srv-1", "tcp", uint32(8100+len(tc.txID)), tc.txID, time.Hour, now)
			require.Error(t, err)
			assert.True(t, errors.Is(err, defs.ErrInvalidTxTransition),
				"owner %s must be rejected with ErrInvalidTxTransition, got %v", tc.status, err)

			rows, err := q.GetByServerFilter(user, "srv-1", "tcp", uint32(8100+len(tc.txID)))
			require.NoError(t, err)
			assert.Empty(t, rows, "no reservation may be created for a non-pending owner")

			owner, err := q.FindByID(user, tc.txID)
			require.NoError(t, err)
			assert.Equal(t, tc.status, owner.Status, "rejected acquire must leave the owner untouched")
		})
	}
}

// TestTryAcquireRejectsEmptyTransactionID: an empty transaction id is a
// programming error — the lease must never be created for it.
func TestTryAcquireRejectsEmptyTransactionID(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	_, err := m.TryAcquire(user, "srv-1", "tcp", 8200, "", time.Hour, now)
	require.Error(t, err)

	rows, err := q.GetByServerFilter(user, "srv-1", "tcp", 8200)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestCommitAndReleaseLifecycle(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)

	require.NoError(t, m.Create(user, newTestTx(user, "tx-1", "k-1", "h", models.TxStatusPending)))
	res, err := m.TryAcquire(user, "aliyun-1", "tcp", 8080, "tx-1", 120*time.Second, now)
	require.NoError(t, err)

	require.NoError(t, m.CommitReservation(user, "tx-1", 7))

	list, err := dao.NewQuery(ctx).GetByServerFilter(testUserInfo(), "aliyun-1", "tcp", 8080)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, models.ReservationStatusCommitted, list[0].Status)
	assert.NotNil(t, list[0].OwnerProxyID)
	assert.Equal(t, uint(7), *list[0].OwnerProxyID)

	require.NoError(t, dao.NewMutation(ctx).Release(testUserInfo(), "tx-1"))
	after, err := dao.NewQuery(ctx).GetByServerFilter(testUserInfo(), "aliyun-1", "tcp", 8080)
	require.NoError(t, err)
	assert.Empty(t, after)
	_ = res
}

func TestListAndDeleteExpiredPending(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()
	m := dao.NewMutation(ctx)

	acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", 1001, "tx-old", "k-old", -time.Second, now, models.TxStatusPending)
	acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", 1002, "tx-live", "k-live", time.Hour, now, models.TxStatusPending)
	acquireWithLifecycle(t, m, user, "aliyun-1", "tcp", 1003, "tx-c", "k-c", time.Hour, now, models.TxStatusCommitted)
	require.NoError(t, m.CommitReservation(user, "tx-c", 1))

	expired, err := dao.NewQuery(ctx).ListExpiredPending(user, now.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, uint32(1001), expired[0].RemotePort)

	n, err := m.DeleteExpiredPending(user, now.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	// committed must survive cleanup
	remains, err := dao.NewQuery(ctx).GetByServerFilter(user, "aliyun-1", "tcp", 1003)
	require.NoError(t, err)
	assert.Len(t, remains, 1)
}
