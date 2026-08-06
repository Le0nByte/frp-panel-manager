package dao_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testUserFor returns a user scoped to the given tenant.
func testUserFor(userID, tenantID int) models.UserInfo {
	return &models.UserEntity{UserID: userID, UserName: "u", Email: "u@example.com", TenantID: tenantID}
}

func newTestTx(user models.UserInfo, txID, idemKey, hash string, status string) *models.ChangeTransaction {
	return &models.ChangeTransaction{
		TransactionID:  txID,
		IdempotencyKey: idemKey,
		RequestHash:    hash,
		ActorUserID:    user.GetUserID(),
		TenantID:       user.GetTenantID(),
		Operation:      models.TxOperationCreate,
		Status:         status,
		StartedAt:      time.Now(),
	}
}

// newTestTxStartedAt builds a transaction with an explicit StartedAt — the
// stale-sweep tests seed old timestamps this way (the recovery cutoff is
// StartedAt-based, never CreatedAt). The tx row itself is the historical
// anomaly being recovered, so creating it directly is the sanctioned setup.
func newTestTxStartedAt(user models.UserInfo, txID, idemKey, hash, status string, startedAt time.Time) *models.ChangeTransaction {
	tx := newTestTx(user, txID, idemKey, hash, status)
	tx.StartedAt = startedAt
	return tx
}

// ---- gate 2: nil userInfo must be rejected by every port-manager DAO entry ----

func TestPortManagerDAOsRejectNilUserInfo(t *testing.T) {
	ctx := setupTestApp(t)
	now := time.Now()
	user := testUserInfo()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	require.NoError(t, m.Create(user, newTestTx(user, "tx-nil", "k", "h", models.TxStatusPending)))

	// transaction query/mutation
	_, err := q.FindByID(nil, "tx-nil")
	assert.Error(t, err, "FindByID must reject nil userInfo")
	_, err = q.FindByIdempotencyKey(nil, "k")
	assert.Error(t, err, "FindByIdempotencyKey must reject nil userInfo")
	_, err = q.ListByStatus(nil, models.TxStatusPending, 10)
	assert.Error(t, err, "ListByStatus must reject nil userInfo")
	_, err = q.ListRecent(nil, 10)
	assert.Error(t, err, "ListRecent must reject nil userInfo")
	assert.Error(t, m.Create(nil, newTestTx(user, "tx-nil2", "k2", "h", models.TxStatusPending)), "Create must reject nil userInfo")
	assert.Error(t, m.UpdateStatus(nil, "tx-nil", models.TxStatusFailed, "", ""), "UpdateStatus must reject nil userInfo")
	assert.Error(t, m.Update(nil, newTestTx(user, "tx-nil", "k", "h", models.TxStatusFailed)), "Update must reject nil userInfo")

	// reservation query/mutation
	require.NoError(t, m.Create(user, newTestTx(user, "tx-res", "k-res", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(user, "srv-1", "tcp", 7001, "tx-res", time.Minute, now)
	require.NoError(t, err)
	_, err = q.GetByServerFilter(nil, "srv-1", "tcp", 7001)
	assert.Error(t, err, "GetByServerFilter must reject nil userInfo")
	_, err = q.GetByTransactionID(nil, "tx-res")
	assert.Error(t, err, "GetByTransactionID must reject nil userInfo")
	_, err = q.ListExpiredPending(nil, now.Add(time.Minute))
	assert.Error(t, err, "ListExpiredPending must reject nil userInfo")
	_, err = m.TryAcquire(nil, "srv-1", "tcp", 7002, "tx-res2", time.Minute, now)
	assert.Error(t, err, "TryAcquire must reject nil userInfo")
	assert.Error(t, m.CommitReservation(nil, "tx-res", 1), "CommitReservation must reject nil userInfo")
	assert.Error(t, m.Release(nil, "tx-res"), "Release must reject nil userInfo")
	_, err = m.DeleteExpiredPending(nil, now.Add(time.Minute))
	assert.Error(t, err, "DeleteExpiredPending must reject nil userInfo")

	// snapshot query/mutation
	_, err = q.FindByTransaction(nil, "tx-nil")
	assert.Error(t, err, "snapshot FindByTransaction must reject nil userInfo")
	_, err = m.SaveEncrypted(nil, "tx-nil", models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "n1", []byte("{}"), testSnapshotKey())
	assert.Error(t, err, "SaveEncrypted must reject nil userInfo")

	// audit query/mutation
	_, _, err = q.List(nil, 1, 10, "")
	assert.Error(t, err, "audit List must reject nil userInfo")
	assert.Error(t, m.Log(nil, &models.AuditEvent{EventType: "x", Summary: "s"}), "audit Log must reject nil userInfo")
}

// ---- gate 1/2: tenant isolation on every port-manager DAO ----

func TestTransactionFindScopedByTenant(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userB := testUserFor(2, 2)

	require.NoError(t, dao.NewMutation(ctx).Create(userA, newTestTx(userA, "tx-a", "idem-a", "hash-a", models.TxStatusPending)))

	// same tenant, same actor: visible
	loaded, err := dao.NewQuery(ctx).FindByID(userA, "tx-a")
	require.NoError(t, err)
	assert.Equal(t, "tx-a", loaded.TransactionID)
	assert.Equal(t, 1, loaded.TenantID)
	assert.Equal(t, "hash-a", loaded.RequestHash)

	// other tenant: invisible
	_, err = dao.NewQuery(ctx).FindByID(userB, "tx-a")
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound), "cross-tenant FindByID must miss, got %v", err)

	_, err = dao.NewQuery(ctx).FindByIdempotencyKey(userB, "idem-a")
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound), "cross-tenant FindByIdempotencyKey must miss, got %v", err)

	byStatus, err := dao.NewQuery(ctx).ListByStatus(userB, models.TxStatusPending, 10)
	require.NoError(t, err)
	assert.Empty(t, byStatus, "cross-tenant ListByStatus must be empty")

	recent, err := dao.NewQuery(ctx).ListRecent(userB, 10)
	require.NoError(t, err)
	assert.Empty(t, recent, "cross-tenant ListRecent must be empty")
}

func TestIdempotencyKeyUniquePerTenantAndActor(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userA2 := testUserFor(3, 1) // same tenant, different actor
	userB := testUserFor(2, 2)
	m := dao.NewMutation(ctx)

	require.NoError(t, m.Create(userA, newTestTx(userA, "tx-a", "idem-shared", "h1", models.TxStatusPending)))

	// same tenant + same actor + same key: unique violation
	err := m.Create(userA, newTestTx(userA, "tx-a2", "idem-shared", "h1", models.TxStatusPending))
	assert.Error(t, err, "same (tenant, actor, key) must violate uniqueness")

	// same tenant + different actor + same key: allowed
	require.NoError(t, m.Create(userA2, newTestTx(userA2, "tx-a3", "idem-shared", "h1", models.TxStatusPending)))

	// different tenant + same key: allowed
	require.NoError(t, m.Create(userB, newTestTx(userB, "tx-b", "idem-shared", "h1", models.TxStatusPending)))
}

func TestTransactionUpdateScopedByTenant(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userB := testUserFor(2, 2)
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	txA := newTestTx(userA, "tx-a", "k-a", "h-a", models.TxStatusPending)
	require.NoError(t, m.Create(userA, txA))

	// cross-tenant UpdateStatus: must not touch the row
	assert.Error(t, m.UpdateStatus(userB, "tx-a", models.TxStatusFailed, "X", "y"))
	loaded, err := q.FindByID(userA, "tx-a")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, loaded.Status, "cross-tenant UpdateStatus must not change the row")

	// cross-tenant Update: must not touch the row
	txA.Status = models.TxStatusFailed
	assert.Error(t, m.Update(userB, txA))
	loaded, err = q.FindByID(userA, "tx-a")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, loaded.Status, "cross-tenant Update must not change the row")
}

func TestReservationScopedByTenant(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userB := testUserFor(2, 2)
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	require.NoError(t, m.Create(userA, newTestTx(userA, "tx-a", "k-a", "h", models.TxStatusPending)))
	_, err := m.TryAcquire(userA, "srv-1", "tcp", 8000, "tx-a", time.Minute, now)
	require.NoError(t, err)

	// B cannot see A's reservation
	list, err := q.GetByServerFilter(userB, "srv-1", "tcp", 8000)
	require.NoError(t, err)
	assert.Empty(t, list, "cross-tenant GetByServerFilter must be empty")
	byTx, err := q.GetByTransactionID(userB, "tx-a")
	require.NoError(t, err)
	assert.Empty(t, byTx, "cross-tenant GetByTransactionID must be empty")
	expired, err := q.ListExpiredPending(userB, now.Add(2*time.Minute))
	require.NoError(t, err)
	assert.Empty(t, expired, "cross-tenant ListExpiredPending must be empty")

	// B cannot commit or release A's reservation
	assert.Error(t, m.CommitReservation(userB, "tx-a", 7), "cross-tenant CommitReservation must fail")
	assert.Error(t, m.Release(userB, "tx-a"), "cross-tenant Release must fail")

	// A can still commit its own reservation (B's failed attempts must not consume it)
	require.NoError(t, m.CommitReservation(userA, "tx-a", 7))

	// physical port is globally unique even across tenants (gate 1): B cannot
	// reserve the same (server, protocol, remote_port) while A holds it.
	require.NoError(t, m.Create(userB, newTestTx(userB, "tx-b", "k-b", "h", models.TxStatusPending)))
	_, err = m.TryAcquire(userB, "srv-1", "tcp", 8000, "tx-b", time.Minute, now)
	assert.True(t, errors.Is(err, defs.ErrReservationConflict), "cross-tenant same physical port must conflict, got %v", err)

	// but B can reserve a different port on the same server
	_, err = m.TryAcquire(userB, "srv-1", "tcp", 8001, "tx-b", time.Minute, now)
	require.NoError(t, err)
}

func TestReservationTenantIDPersisted(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	now := time.Now()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	require.NoError(t, m.Create(userA, newTestTx(userA, "tx-a", "k-a", "h", models.TxStatusPending)))
	res, err := m.TryAcquire(userA, "srv-1", "tcp", 8002, "tx-a", time.Minute, now)
	require.NoError(t, err)
	assert.Equal(t, 1, res.TenantID)

	list, err := q.GetByServerFilter(userA, "srv-1", "tcp", 8002)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, 1, list[0].TenantID)
}

func TestSnapshotAndAuditScopedByTenant(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userB := testUserFor(2, 2)
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	key := testSnapshotKey()
	snap, err := m.SaveEncrypted(userA, "tx-a", models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "cli-1", []byte(`{"a":1}`), key)
	require.NoError(t, err)
	assert.Equal(t, 1, snap.TenantID)

	// A sees its snapshot; B does not
	snaps, err := q.FindByTransaction(userA, "tx-a")
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	snaps, err = q.FindByTransaction(userB, "tx-a")
	require.NoError(t, err)
	assert.Empty(t, snaps, "cross-tenant snapshot query must be empty")

	// audit: A logs, only A lists
	require.NoError(t, m.Log(userA, &models.AuditEvent{EventType: "create", Summary: "ok", TransactionID: "tx-a"}))
	events, total, err := q.List(userA, 1, 10, "")
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	assert.Equal(t, 1, events[0].TenantID)
	events, total, err = q.List(userB, 1, 10, "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, events, "cross-tenant audit list must be empty")
}

// ---- gate 3: UpdateStatus enforced state machine + CAS ----

func TestUpdateStatusRejectsIllegalTransition(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	require.NoError(t, m.Create(user, newTestTx(user, "tx-illegal", "k", "h", models.TxStatusPending)))

	err := m.UpdateStatus(user, "tx-illegal", models.TxStatusCommitted, "", "")
	assert.True(t, errors.Is(err, defs.ErrInvalidTxTransition),
		"pending->committed must be rejected with the stable sentinel, got %v", err)

	loaded, err := q.FindByID(user, "tx-illegal")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, loaded.Status, "rejected transition must leave the row unchanged")

	// committed is terminal: committed->rolling_back is illegal
	require.NoError(t, m.UpdateStatus(user, "tx-illegal", models.TxStatusPreflight, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-illegal", models.TxStatusApplying, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-illegal", models.TxStatusVerifying, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-illegal", models.TxStatusCommitted, "", ""))
	err = m.UpdateStatus(user, "tx-illegal", models.TxStatusRollingBack, "", "")
	assert.True(t, errors.Is(err, defs.ErrInvalidTxTransition),
		"committed->rolling_back must be rejected, got %v", err)
}

// TestUpdateStatusCASMutualExclusion proves concurrent transitions on one row
// are serialized by the CAS: exactly one caller wins, the loser fails with
// either ErrTxStatusConflict (it loaded the old status and its CAS missed) or
// ErrInvalidTxTransition (it loaded the winner's new status), and the row ends
// in the target state exactly once.
func TestUpdateStatusCASMutualExclusion(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	m := dao.NewMutation(ctx)
	q := dao.NewQuery(ctx)

	const pairs = 8
	for i := 0; i < pairs; i++ {
		txID := "tx-race-" + string(rune('a'+i))
		require.NoError(t, m.Create(user, newTestTx(user, txID, "k-"+txID, "h", models.TxStatusVerifying)))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, pairs*2)
	for i := 0; i < pairs; i++ {
		txID := "tx-race-" + string(rune('a'+i))
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				<-start
				results <- m.UpdateStatus(user, id, models.TxStatusCommitted, "", "")
			}(txID)
		}
	}
	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err == nil {
			continue
		}
		assert.True(t,
			errors.Is(err, defs.ErrTxStatusConflict) || errors.Is(err, defs.ErrInvalidTxTransition),
			"loser must fail with the conflict or transition sentinel, got %v", err)
	}

	for i := 0; i < pairs; i++ {
		txID := "tx-race-" + string(rune('a'+i))
		loaded, err := q.FindByID(user, txID)
		require.NoError(t, err)
		assert.Equal(t, models.TxStatusCommitted, loaded.Status)
		require.NotNil(t, loaded.FinishedAt, "terminal status must stamp finished_at")
	}
}

func TestUpdateStatusCrossTenantCannotTransition(t *testing.T) {
	ctx := setupTestApp(t)
	userA := testUserFor(1, 1)
	userB := testUserFor(2, 2)
	m := dao.NewMutation(ctx)

	require.NoError(t, m.Create(userA, newTestTx(userA, "tx-a", "k", "h", models.TxStatusPending)))
	err := m.UpdateStatus(userB, "tx-a", models.TxStatusFailed, "", "")
	assert.True(t, errors.Is(err, defs.ErrTransactionNotFound) || errors.Is(err, defs.ErrTxStatusConflict),
		"cross-tenant UpdateStatus must not succeed, got %v", err)
}
