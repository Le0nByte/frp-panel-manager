package dao_test

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
)

// TestCommitCreateFirstShadowTransfersSiblingLease pins the "other door" of the
// reservation-leak defect: on a FIRST shadow the origin's active rows are
// hard-deleted and re-created on the child under NEW ids. A committed lease is
// keyed on owner_proxy_id, so without reconciliation every sibling rule's port
// would be owned by a row that no longer exists — still committed, invisible in
// the panel (the list joins on proxy_config) and permanently blocking that port
// with RESERVATION_CONFLICT.
//
// The contract: a lease whose rule survived the rebuild (same name, same
// physical key) must FOLLOW the rule to its new row id.
func TestCommitCreateFirstShadowTransfersSiblingLease(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	now := time.Now()
	const (
		originID = "cli-origin-transfer"
		childID  = "cli-child-transfer"
		serverID = "srv-transfer"
		txID     = "tx-transfer"
	)

	origin := &models.ClientEntity{
		ClientID: originID, ServerID: serverID, ConnectSecret: "origin-secret",
		ConfigContent: clientConfigBytes(t, "ssh"),
	}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	// the ACTIVE sibling rule that the first shadow is about to migrate
	sshRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: serverID, Name: "ssh", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: false,
		Content: []byte(`{"name":"ssh","type":"tcp","localIP":"127.0.0.1","localPort":8080,"remotePort":8000}`),
	}}
	require.NoError(t, db.Create(sshRow).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: serverID, Protocol: "tcp", RemotePort: 8000,
		TransactionID: "tx-ssh-owner", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &sshRow.ID,
	}).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-transfer", RequestHash: "h",
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
		NewKey: dao.ReservationKey{ServerID: serverID, Protocol: "tcp", RemotePort: 8001},
	}

	_, err := dao.NewMutation(ctx).CommitCreate(user, in)
	require.NoError(t, err)

	var rebuilt models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", childID, "ssh").First(&rebuilt).Error,
		"the sibling rule must be rebuilt on the child")
	require.NotEqual(t, sshRow.ID, rebuilt.ID, "a first shadow re-creates rows under new ids")

	var lease models.PortReservation
	require.NoError(t, db.Where("remote_port = ?", 8000).First(&lease).Error,
		"the sibling's lease must survive the migration")
	require.Equal(t, models.ReservationStatusCommitted, lease.Status)
	require.NotNil(t, lease.OwnerProxyID)
	require.Equal(t, rebuilt.ID, *lease.OwnerProxyID,
		"the lease must follow the rule to its rebuilt row id, never dangle on the deleted origin row")

	// the created rule keeps exactly one committed lease, on its own port
	var webLease models.PortReservation
	require.NoError(t, db.Where("remote_port = ?", 8001).First(&webLease).Error)
	require.Equal(t, models.ReservationStatusCommitted, webLease.Status)
	require.NotNil(t, webLease.OwnerProxyID)
}
