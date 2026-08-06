package dao_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// commitBackfillFixture seeds the standard existing-child commit scenario:
// shadowed origin, existing child with one proxy row ("ssh"), a mid-flight
// transaction in the given status, and a pending reservation owned by that
// transaction. The commit input carries the FINAL config ["ssh","web"] with
// ProxyName "web".
func commitBackfillFixture(t *testing.T, ctx *app.Context, db *gorm.DB, txStatus string) (*dao.CommitCreateInput, string, string, string) {
	t.Helper()
	now := time.Now()

	const (
		originID = "cli-origin"
		childID  = "cli-child"
		txID     = "tx-backfill"
		srvID    = "srv-1"
	)
	user := testUserInfo()

	origin := &models.ClientEntity{ClientID: originID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	child := &models.ClientEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	child.ConfigContent = clientConfigBytes(t, "ssh")
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, child))
	require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, Name: "ssh", TenantID: user.GetTenantID(), UserID: user.GetUserID(),
	}}).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-backfill", RequestHash: "h",
		Operation: models.TxOperationCreate, Status: txStatus,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: txID, Status: models.ReservationStatusPending, ExpiresAt: now.Add(time.Minute),
	}).Error)

	in := &dao.CommitCreateInput{
		TxID:      txID,
		ProxyName: "web",
		Client: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID,
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			IsShadow: false, ConnectSecret: "child-secret",
			ConfigContent: clientConfigBytes(t, "ssh", "web"),
		},
		Shadow: &dao.ShadowCommit{FirstShadow: false, ChildCreated: false},
		Snapshot: dao.SnapshotInput{
			Side: models.SnapshotSideAfter, NodeType: models.SnapshotNodeTypeClient,
			NodeID: childID, Plaintext: []byte(`{"proxies":["ssh","web"]}`),
			Key: make([]byte, snapshot.KeySize),
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID, EventType: models.AuditEventTypeCommit,
			Summary: "created proxy web", Success: true,
		},
	}
	return in, txID, childID, srvID
}

// TestCommitCreateBackfillsTransactionProxyID is the spec §6.2 happy-path
// contract: proxy_id is nullable only until the rule row exists — after the
// atomic commit it MUST be non-nil and point at the committed proxy row (the
// returned id AND the reservation owner).
func TestCommitCreateBackfillsTransactionProxyID(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, txID, childID, _ := commitBackfillFixture(t, ctx, db, models.TxStatusVerifying)

	proxyID, err := dao.NewMutation(ctx).CommitCreate(testUserInfo(), in)
	require.NoError(t, err)
	require.NotZero(t, proxyID, "commit must return the new proxy row id")

	var tx models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&tx).Error)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID, "transaction proxy_id must be backfilled after commit")
	require.Equal(t, proxyID, *tx.ProxyID, "transaction proxy_id must equal the returned proxy row id")

	var web models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", childID, "web").First(&web).Error)
	require.Equal(t, proxyID, web.ID, "returned id must be the persisted proxy row id")

	var res models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&res).Error)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, proxyID, *res.OwnerProxyID, "reservation owner must be the new proxy row")
}

// TestCommitCreateBackfillFailsClosedOnUncommittableTxStatus pins the CAS
// guard: the proxy_id backfill may only happen while the transaction is in a
// committable state (verifying). A transaction already in a terminal state
// (here failed) must abort the WHOLE commit — snapshot/audit/reservation/
// client/proxy writes included — with the stable ErrInvalidTxTransition
// sentinel surfacing from the backfill guard itself.
func TestCommitCreateBackfillFailsClosedOnUncommittableTxStatus(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, txID, childID, _ := commitBackfillFixture(t, ctx, db, models.TxStatusFailed)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitCreate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrInvalidTxTransition),
		"uncommittable status must surface ErrInvalidTxTransition, got: %v", err)
	require.True(t, strings.Contains(err.Error(), "backfill transaction proxy id"),
		"the backfill guard must fire BEFORE the final transition (nothing later may run), got: %v", err)

	var tx models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&tx).Error)
	require.Equal(t, models.TxStatusFailed, tx.Status, "tx row must be untouched")
	require.Nil(t, tx.ProxyID, "no proxy_id may survive the aborted commit")

	var res models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&res).Error)
	require.Equal(t, models.ReservationStatusPending, res.Status)

	require.Equal(t, beforeProxies, captureCommitProxies(t, db), "proxy rows must be unchanged")
	var childCount int64
	require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", childID).Count(&childCount).Error)
	require.Equal(t, int64(1), childCount)

	var snapCount int64
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).Where("transaction_id = ?", txID).Count(&snapCount).Error)
	require.Zero(t, snapCount, "no after snapshot may survive the aborted commit")
	var auditCount int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Where("transaction_id = ?", txID).Count(&auditCount).Error)
	require.Zero(t, auditCount, "no audit event may survive the aborted commit")
}

// TestCommitCreateBackfillRollsBackOnLaterFailure proves the proxy_id
// backfill is INSIDE the commit transaction: when a later step fails
// (poisoned snapshot key → EncryptSnapshot error at the after-snapshot
// step), the already-written proxy_id must roll back with everything else —
// the transaction stays verifying with a nil proxy_id, the reservation stays
// pending, and no snapshot/audit/proxy delta survives.
func TestCommitCreateBackfillRollsBackOnLaterFailure(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, txID, childID, _ := commitBackfillFixture(t, ctx, db, models.TxStatusVerifying)
	in.Snapshot.Key = []byte{1} // wrong size: EncryptSnapshot fails after the backfill wrote
	beforeChild := captureClientRow(t, db, childID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitCreate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "encrypt after snapshot"),
		"failure must originate at the after-snapshot step, got: %v", err)

	var tx models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&tx).Error)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID, "the backfilled proxy_id must roll back with the failed commit")

	var res models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&res).Error)
	require.Equal(t, models.ReservationStatusPending, res.Status)

	require.Equal(t, beforeChild, captureClientRow(t, db, childID), "child row must be unchanged")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db), "proxy rows must be unchanged")

	var snapCount int64
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).Where("transaction_id = ?", txID).Count(&snapCount).Error)
	require.Zero(t, snapCount)
	var auditCount int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Where("transaction_id = ?", txID).Count(&auditCount).Error)
	require.Zero(t, auditCount)
}
