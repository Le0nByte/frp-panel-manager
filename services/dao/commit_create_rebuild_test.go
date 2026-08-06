package dao_test

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
)

// TestCommitCreateFirstShadowMigratesStoppedOriginSibling pins the
// first-shadow variant of the stopped-sibling invariant. Keeping the row on
// the now-shadow origin is not enough: StartProxy resolves the per-server
// child and queries by child_id, so the stopped row must move to that child
// with its original id. Its committed reservation continues to point at the
// same row and must never become dangling.
func TestCommitCreateFirstShadowMigratesStoppedOriginSibling(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	now := time.Now()
	const (
		originID = "cli-origin-first"
		childID  = "cli-child-first"
		serverID = "srv-first"
		txID     = "tx-first-stopped"
	)

	origin := &models.ClientEntity{
		ClientID: originID, ServerID: serverID, ConnectSecret: "origin-secret",
		ConfigContent: clientConfigBytes(t, "ssh"),
	}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))
	require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: serverID, Name: "ssh", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: false,
	}}).Error)
	stopped := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: serverID, Name: "paused", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: true,
		Content: []byte(`{"name":"paused","type":"tcp","remotePort":9000}`),
	}}
	require.NoError(t, db.Create(stopped).Error)
	stoppedBefore := *stopped
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: serverID, Protocol: "tcp", RemotePort: 9000,
		TransactionID: "tx-paused-owner", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &stopped.ID,
	}).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-first-stopped", RequestHash: "h",
		Operation: models.TxOperationCreate, Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: serverID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: txID, Status: models.ReservationStatusPending, ExpiresAt: now.Add(time.Minute),
	}).Error)

	in := &dao.CommitCreateInput{
		TxID: txID, ProxyName: "web",
		Client: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: serverID,
			ConnectSecret: "child-secret", ConfigContent: clientConfigBytes(t, "ssh", "web"),
		},
		Shadow: &dao.ShadowCommit{
			FirstShadow: true, ChildCreated: true,
			OriginAfter: &models.ClientEntity{ClientID: originID, IsShadow: true},
		},
		Snapshot: dao.SnapshotInput{
			Side: models.SnapshotSideAfter, NodeType: models.SnapshotNodeTypeClient,
			NodeID: childID, Plaintext: []byte(`{"proxies":["ssh","web"]}`), Key: testSnapshotKey(),
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID, EventType: models.AuditEventTypeCommit,
			Summary: "created proxy web", Success: true,
		},
	}

	_, err := dao.NewMutation(ctx).CommitCreate(user, in)
	require.NoError(t, err)

	var migrated models.ProxyConfig
	require.NoError(t, db.Where("id = ?", stopped.ID).First(&migrated).Error,
		"first shadow must migrate, not delete, the stopped sibling")
	require.Equal(t, stoppedBefore.ID, migrated.ID)
	require.Equal(t, childID, migrated.ClientID)
	require.Equal(t, originID, migrated.OriginClientID)
	require.Equal(t, stoppedBefore.ServerID, migrated.ServerID)
	require.Equal(t, stoppedBefore.Name, migrated.Name)
	require.Equal(t, stoppedBefore.Content, migrated.Content)
	require.True(t, migrated.Stopped)

	var owner models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", "tx-paused-owner").First(&owner).Error)
	require.NotNil(t, owner.OwnerProxyID)
	require.Equal(t, migrated.ID, *owner.OwnerProxyID, "reservation owner must remain non-dangling")

	var originStopped int64
	require.NoError(t, db.Model(&models.ProxyConfig{}).
		Where("client_id = ? AND stopped = ?", originID, true).Count(&originStopped).Error)
	require.Zero(t, originStopped, "no stopped rule may remain stranded on the shadow origin")
}

// TestCommitCreateRebuildPreservesStoppedSiblingAndReservation is the
// stopped-sibling contract: a proxy_config row with Stopped=true that is NOT
// part of the applied client config (a stopped rule) must survive the commit
// rebuild with its original id/fields/flag, and a committed reservation owned
// by it must stay non-dangling. The rebuild's hard delete may only remove
// ACTIVE rows (stopped IS NULL OR stopped = false) — the legacy
// DeleteProxyConfigsByClientIDOrOriginClientID semantics.
func TestCommitCreateRebuildPreservesStoppedSiblingAndReservation(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	in, txID, childID, srvID := commitBackfillFixture(t, ctx, db, models.TxStatusVerifying)

	// stopped sibling row NOT present in the applied config
	stoppedRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: "cli-origin", ServerID: srvID, Name: "old-rule",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"old-rule","type":"tcp"}`), Stopped: true,
	}}
	require.NoError(t, db.Create(stoppedRow).Error)

	// committed reservation owned by the stopped row (a stopped rule keeps
	// its lease; it must not dangle when the sibling rebuild runs)
	stoppedRes := &models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 9000,
		TransactionID: "tx-old-rule", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &stoppedRow.ID,
	}
	require.NoError(t, db.Create(stoppedRes).Error)

	beforeStopped := *stoppedRow

	proxyID, err := dao.NewMutation(ctx).CommitCreate(user, in)
	require.NoError(t, err)

	// the stopped sibling survives byte-identical (id, fields, flag)
	var kept models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", childID, "old-rule").First(&kept).Error,
		"the stopped sibling row must survive the rebuild")
	require.Equal(t, beforeStopped.ID, kept.ID, "stopped sibling must keep its original id")
	require.True(t, kept.Stopped, "stopped sibling must stay stopped")
	require.Equal(t, beforeStopped.ProxyConfigEntity, kept.ProxyConfigEntity,
		"stopped sibling fields must be byte-identical")

	// its committed reservation is untouched and still owned by the row
	var res models.PortReservation
	require.NoError(t, db.Where("id = ?", stoppedRes.ID).First(&res).Error)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, kept.ID, *res.OwnerProxyID, "reservation owner must not dangle")

	// the active rebuild still happened: "web" created, "ssh" row present
	var web models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", childID, "web").First(&web).Error)
	require.Equal(t, proxyID, web.ID)
	var ssh models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", childID, "ssh").First(&ssh).Error)
	require.False(t, ssh.Stopped)

	// the new transaction's reservation committed with the new owner
	var txRes models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&txRes).Error)
	require.Equal(t, models.ReservationStatusCommitted, txRes.Status)
	require.NotNil(t, txRes.OwnerProxyID)
	require.Equal(t, proxyID, *txRes.OwnerProxyID)
}

// TestCommitCreateRebuildSameNameStoppedReusesRowNoDuplicate pins the
// same-name corner: when the applied config contains a proxy whose name
// matches an EXISTING stopped row, the rebuild reuses that row's id (the
// legacy GetProxyConfigByOriginClientIDAndName Model-reuse semantics) — it
// must NOT produce a second active row with the same name. Exactly one row
// named "web" exists after the commit and it is the (now active) original
// row.
func TestCommitCreateRebuildSameNameStoppedReusesRowNoDuplicate(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	in, _, childID, srvID := commitBackfillFixture(t, ctx, db, models.TxStatusVerifying)

	// a stopped row whose name equals the NEW rule in the applied config
	stoppedRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: "cli-origin", ServerID: srvID, Name: "web",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"web","type":"tcp"}`), Stopped: true,
	}}
	require.NoError(t, db.Create(stoppedRow).Error)

	proxyID, err := dao.NewMutation(ctx).CommitCreate(user, in)
	require.NoError(t, err)

	var count int64
	require.NoError(t, db.Model(&models.ProxyConfig{}).
		Where("client_id = ? AND name = ?", childID, "web").Count(&count).Error)
	require.Equal(t, int64(1), count, "must not generate a duplicate row for the same name")

	var web models.ProxyConfig
	require.NoError(t, db.Where("id = ?", proxyID).First(&web).Error)
	require.Equal(t, stoppedRow.ID, web.ID, "the active rule must reuse the existing (stopped) row id")
	require.False(t, web.Stopped, "the row now represents the active rule")
}
