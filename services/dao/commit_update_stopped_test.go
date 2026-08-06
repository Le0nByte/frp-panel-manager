package dao_test

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// stoppedFixtureState carries the ids a stopped-branch CommitUpdate test
// asserts against (or mutates to build the failure case).
type stoppedFixtureState struct {
	txID         string
	originID     string
	childID      string
	srvID        string
	oldCreateTx  string // the ORIGINAL create tx that owns the committed reservation
	oldProxyID   uint
	oldKey       dao.ReservationKey
	activeSibID  uint
	stoppedSibID uint
}

func (s *stoppedFixtureState) key(protocol string, port uint32) dao.ReservationKey {
	return dao.ReservationKey{ServerID: s.srvID, Protocol: protocol, RemotePort: port}
}

// stoppedOldJSON is the old stopped row's typed content: tcp/8001, the SAME
// physical key as the fixture's reservation.
const stoppedOldJSON = `{"name":"web","type":"tcp","remotePort":8001}`

// stoppedNewJSON is the happy-path new typed content: the SAME physical key
// (tcp/8001) with a different body, so the commit must run the guarded
// in-place UPDATE (not the value-identical short circuit).
const stoppedNewJSON = `{"name":"web","type":"tcp","remotePort":8001,"localPort":9090}`

// seedStoppedUpdate builds the standard stopped-branch update world: a
// shadowed origin, an existing child carrying a STOPPED rule row "web"
// (the old row) plus an active sibling and a stopped sibling, a verifying
// update transaction, a COMMITTED reservation at the old key owned by the
// old row (transaction_id = the original create tx), and the two before
// snapshots the coordinator saves pre-apply. The returned input is the
// same-name SameKeyOwned stopped commit; a test tweaks it per variant.
func seedStoppedUpdate(t *testing.T, ctx *app.Context, db *gorm.DB) (*dao.CommitUpdateInput, *stoppedFixtureState) {
	t.Helper()
	now := time.Now()
	const (
		originID    = "cli-origin-stopped"
		childID     = "cli-child-stopped"
		txID        = "tx-upd-stopped"
		srvID       = "srv-stopped"
		oldCreateTx = "tx-create-web-stopped"
	)
	user := testUserInfo()

	origin := &models.ClientEntity{ClientID: originID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	child := &models.ClientEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	// The stopped target "web" is deliberately ABSENT from the applied
	// config. Only the active sibling remains, matching StopProxy semantics.
	child.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "api", protocol: "tcp", remotePort: 8003, localPort: 8080})
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, child))

	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: true,
		Content: []byte(stoppedOldJSON),
	}}
	require.NoError(t, db.Create(oldRow).Error)

	// siblings the stopped branch must never touch: one ACTIVE and one
	// STOPPED rule on the same child
	activeSib := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "api", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"api","type":"tcp","remotePort":8003}`),
	}}
	require.NoError(t, db.Create(activeSib).Error)
	stoppedSib := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "paused", Type: "udp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: true,
		Content: []byte(`{"name":"paused","type":"udp","remotePort":9000}`),
	}}
	require.NoError(t, db.Create(stoppedSib).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-upd-stopped", RequestHash: "h",
		Operation: models.TxOperationUpdate, Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)

	oldKey := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8001}
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: oldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &oldRow.ID,
	}).Error)

	seedBeforeSnapshots(t, db, txID, childID, strconv.FormatUint(uint64(oldRow.ID), 10), user.GetTenantID())

	in := &dao.CommitUpdateInput{
		TxID: txID, OldProxyID: oldRow.ID, OldName: "web", OldServerID: srvID, OriginClientID: originID,
		Branch: dao.UpdateBranchStopped,
		StoppedUpdate: &dao.StoppedUpdatePayload{
			ChildClientID: childID,
			NewName:       "web",
			NewType:       "tcp",
			NewServerID:   srvID,
			NewTypedJSON:  []byte(stoppedNewJSON),
		},
		Reservation: dao.ReservationStep{
			Mode:                   dao.ReservationModeSameKeyOwned,
			OldKey:                 oldKey,
			NewKey:                 oldKey,
			OldReservationExpected: true,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			// The stopped branch applies NOTHING to the client, so the after
			// snapshot is the exact unchanged applied child config.
			Plaintext: append([]byte(nil), child.ConfigContent...), Key: testSnapshotKey(),
		},
		ProxyAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"v":1,"name":"web","type":"tcp","remote_port":8001,"stopped":true}`), Key: testSnapshotKey(),
		},
		// deliberately WRONG identity: the DAO must force TxID/tenant/actor/EventType
		AuditEvent: &models.AuditEvent{
			TransactionID: "wrong-tx", EventType: "bogus",
			Summary: "updated stopped proxy web", Success: true,
		},
	}
	return in, &stoppedFixtureState{
		txID: txID, originID: originID, childID: childID, srvID: srvID,
		oldCreateTx: oldCreateTx, oldProxyID: oldRow.ID, oldKey: oldKey,
		activeSibID: activeSib.ID, stoppedSibID: stoppedSib.ID,
	}
}

func loadProxyByID(t *testing.T, db *gorm.DB, id uint) models.ProxyConfig {
	t.Helper()
	var p models.ProxyConfig
	require.NoError(t, db.Where("id = ?", id).First(&p).Error)
	return p
}

// TestCommitUpdateStoppedRejectsTargetStillPresentInChildConfig guards the
// defining stopped-branch invariant: StopProxy removes the stopped rule from
// the child's applied config. If the target name is still present, the rule
// may still be live and a zero-Apply update would silently mutate only DB
// metadata. Fail closed before changing the proxy row or reservation.
func TestCommitUpdateStoppedRejectsTargetStillPresentInChildConfig(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	badConfig := clientConfigWithProxies(t,
		proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080})
	require.NoError(t, db.Model(&models.Client{}).
		Where("client_id = ?", st.childID).Update("config_content", badConfig).Error)

	beforeChild := captureClientRow(t, db, st.childID)
	beforeProxies := captureCommitProxies(t, db)
	var beforeReservations []models.PortReservation
	require.NoError(t, db.Order("id").Find(&beforeReservations).Error)
	beforeTx := loadTxRow(t, db, st.txID)
	beforeCounts := captureUpdateCounts(t, db, st.txID)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.ErrorIs(t, err, defs.ErrProxyConfigChanged)
	require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	var afterReservations []models.PortReservation
	require.NoError(t, db.Order("id").Find(&afterReservations).Error)
	require.Equal(t, beforeReservations, afterReservations)
	require.Equal(t, beforeTx, loadTxRow(t, db, st.txID))
	require.Equal(t, beforeCounts, captureUpdateCounts(t, db, st.txID))
}

// TestCommitUpdateStoppedSameNameSameKeyOwned is the stopped-branch happy
// path: the old row is updated IN PLACE (content + stopped=true, id kept),
// the child client row is byte-identical (ZERO client delta — including
// UpdatedAt), siblings are untouched, the SameKeyOwned reservation keeps
// owner and transaction_id (final==old is a value-identical no-op), the tx
// commits with proxy_id=old, exactly two decryptable after snapshots are
// bound to child/old, and one forced update audit event exists.
func TestCommitUpdateStoppedSameNameSameKeyOwned(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	beforeSnaps := captureSnapshots(t, db, st.txID)
	beforeChild := captureClientRow(t, db, st.childID)
	beforeActiveSib := loadProxyByID(t, db, st.activeSibID)
	beforeStoppedSib := loadProxyByID(t, db, st.stoppedSibID)

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID, "stopped updates keep the old row id (no rebuild)")

	// old row: in-place update, id preserved, stopped stays true
	row := loadProxyByID(t, db, st.oldProxyID)
	require.True(t, row.Stopped)
	require.Equal(t, "web", row.Name)
	require.Equal(t, st.childID, row.ClientID)
	require.Equal(t, st.originID, row.OriginClientID)
	require.Equal(t, st.srvID, row.ServerID)
	require.Equal(t, string(in.StoppedUpdate.NewTypedJSON), string(row.Content), "the row must carry the new typed json")

	// ZERO client delta: the whole child row (ConfigContent + UpdatedAt
	// included) must be byte-identical — the DAO must not issue any client
	// UPDATE on the stopped branch
	require.Equal(t, beforeChild, captureClientRow(t, db, st.childID), "stopped branch must never touch the child row")

	// sibling active/stopped rows byte-identical
	require.Equal(t, beforeActiveSib, loadProxyByID(t, db, st.activeSibID))
	require.Equal(t, beforeStoppedSib, loadProxyByID(t, db, st.stoppedSibID))

	// reservation: committed, owner unchanged (final==old no-op),
	// transaction_id STILL the original create tx
	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, st.oldProxyID, *res.OwnerProxyID)
	require.Equal(t, st.oldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite transaction_id")

	// tx committed with proxy_id = the OLD id
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, st.oldProxyID, *tx.ProxyID)

	// snapshots: 2 before (seeded, untouched) + 2 after, forced node ids
	after := captureSnapshots(t, db, st.txID)
	require.Len(t, after, len(beforeSnaps)+2)
	afterClient := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeClient)
	require.Equal(t, st.childID, afterClient.NodeID, "client after snapshot must bind to the child")
	pt, err := dao.DecryptSnapshotForTest(&afterClient, testSnapshotKey())
	require.NoError(t, err)
	require.Equal(t, in.ClientAfterSnapshot.Plaintext, pt)
	require.Equal(t, sha256Hex(in.ClientAfterSnapshot.Plaintext), afterClient.SHA256)
	require.False(t, bytes.Contains(afterClient.Ciphertext, in.ClientAfterSnapshot.Plaintext),
		"ciphertext must never contain the plaintext")

	afterProxy := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.Equal(t, strconv.FormatUint(uint64(st.oldProxyID), 10), afterProxy.NodeID,
		"proxy after snapshot must bind to the old row id")
	ptProxy, err := dao.DecryptSnapshotForTest(&afterProxy, testSnapshotKey())
	require.NoError(t, err)
	require.Equal(t, in.ProxyAfterSnapshot.Plaintext, ptProxy)
	require.Equal(t, sha256Hex(in.ProxyAfterSnapshot.Plaintext), afterProxy.SHA256)

	// audit: exactly one, forced TxID/tenant/actor/EventType
	var audits []models.AuditEvent
	require.NoError(t, db.Where("transaction_id = ?", st.txID).Find(&audits).Error)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeUpdate, audits[0].EventType, "EventType must be forced to update")
	require.Equal(t, st.txID, audits[0].TransactionID, "audit TransactionID must be forced to the commit tx")
	require.Equal(t, testUserInfo().GetTenantID(), audits[0].TenantID)
	require.Equal(t, testUserInfo().GetUserID(), audits[0].ActorUserID)
	require.False(t, strings.Contains(audits[0].Summary, string(in.ClientAfterSnapshot.Plaintext)))
}

// TestCommitUpdateStoppedValueIdenticalShortCircuit: when the new payload is
// byte-identical to the row's current state, the commit succeeds WITHOUT
// issuing a no-op UPDATE — MySQL reports RowsAffected=0 for a value-identical
// UPDATE, and that must never be misread as a CAS miss.
func TestCommitUpdateStoppedValueIdenticalShortCircuit(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	in.StoppedUpdate.NewName = "web"
	in.StoppedUpdate.NewType = "tcp"
	in.StoppedUpdate.NewTypedJSON = []byte(stoppedOldJSON)
	beforeRow := loadProxyByID(t, db, st.oldProxyID)

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID)
	require.Equal(t, beforeRow, loadProxyByID(t, db, st.oldProxyID), "the row must stay byte-identical")
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

// TestCommitUpdateStoppedAcceptsPreboundProxyID matches the coordinator
// contract: an update transaction is created with ProxyID already pointing
// at the old rule. Committing a stopped/same-name update keeps that id and
// must succeed even on dialects where the value-identical backfill UPDATE
// reports RowsAffected=0.
func TestCommitUpdateStoppedAcceptsPreboundProxyID(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	require.NoError(t, db.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ?", st.txID).Update("proxy_id", st.oldProxyID).Error)

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID)
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, st.oldProxyID, *tx.ProxyID)
}

// TestCommitUpdateStoppedRenameSameKey: the stopped branch renames IN PLACE
// (id preserved, reservation owner unchanged); the S2 TOCTOU guard rejects a
// same-origin collision and must not mis-hit a row of ANOTHER origin.
func TestCommitUpdateStoppedRenameSameKey(t *testing.T) {
	t.Run("rename in place keeps the id", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		in.StoppedUpdate.NewName = "web2"
		in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web2","type":"tcp","remotePort":8001,"localPort":9090}`)

		proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.NoError(t, err)
		require.Equal(t, st.oldProxyID, proxyID, "stopped rename must keep the row id")

		row := loadProxyByID(t, db, st.oldProxyID)
		require.Equal(t, "web2", row.Name)
		require.True(t, row.Stopped)
		require.Equal(t, string(in.StoppedUpdate.NewTypedJSON), string(row.Content))

		res := loadReservation(t, db, st.oldKey)
		require.NotNil(t, res.OwnerProxyID)
		require.Equal(t, st.oldProxyID, *res.OwnerProxyID)
		require.Equal(t, st.oldCreateTx, res.TransactionID)
	})

	t.Run("same-origin conflict fails closed", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		in.StoppedUpdate.NewName = "web2"
		in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web2","type":"tcp","remotePort":8001,"localPort":9090}`)
		require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: st.childID, OriginClientID: st.originID, ServerID: st.srvID, Name: "web2", Type: "tcp",
			TenantID: testUserInfo().GetTenantID(), UserID: testUserInfo().GetUserID(),
			Content: []byte(`{"name":"web2","type":"tcp"}`),
		}}).Error)
		before := captureUpdateCounts(t, db, st.txID)
		beforeChild := captureClientRow(t, db, st.childID)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrProxyConfigNameConflict), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "full rollback on rename conflict")
		require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
		require.Nil(t, tx.ProxyID)
	})

	t.Run("other-origin same name does not conflict", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		in.StoppedUpdate.NewName = "web2"
		in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web2","type":"tcp","remotePort":8001,"localPort":9090}`)
		require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: "cli-other-child", OriginClientID: "cli-other", ServerID: st.srvID, Name: "web2", Type: "tcp",
			TenantID: testUserInfo().GetTenantID(), UserID: testUserInfo().GetUserID(),
			Content: []byte(`{"name":"web2","type":"tcp"}`),
		}}).Error)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.NoError(t, err)
	})
}

// TestCommitUpdateStoppedDifferentKey: the new-key pending lease commits
// with owner = the OLD row id (stopped rows never get a new id), the old
// committed row is hard-deleted, and the row's real NewTypedJSON physical
// key matches the new key.
func TestCommitUpdateStoppedDifferentKey(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	newKey := st.key("tcp", 8002)
	in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web","type":"tcp","remotePort":8002,"localPort":9090}`)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8002,
		TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
	}).Error)
	in.Reservation = dao.ReservationStep{
		Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
		OldReservationExpected: true,
	}

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID, "a stopped key change keeps the row id")

	row := loadProxyByID(t, db, st.oldProxyID)
	require.Equal(t, string(in.StoppedUpdate.NewTypedJSON), string(row.Content))

	newRes := loadReservation(t, db, newKey)
	require.Equal(t, models.ReservationStatusCommitted, newRes.Status)
	require.NotNil(t, newRes.OwnerProxyID)
	require.Equal(t, st.oldProxyID, *newRes.OwnerProxyID)

	var oldCount int64
	require.NoError(t, db.Model(&models.PortReservation{}).
		Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).Count(&oldCount).Error)
	require.Zero(t, oldCount, "the old committed reservation must be hard-deleted")

	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, st.oldProxyID, *tx.ProxyID)
}

// TestCommitUpdateStoppedSameKeyPendingLegacy: legacy path — the update tx
// owns a fresh pending lease at the (old) key; the commit flips it to
// committed owner=old with a cleared expiry.
func TestCommitUpdateStoppedSameKeyPendingLegacy(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedStoppedUpdate(t, ctx, db)
	require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
		st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
	}).Error)
	in.Reservation = dao.ReservationStep{
		Mode: dao.ReservationModeSameKeyPending, OldKey: st.oldKey, NewKey: st.oldKey,
		OldReservationExpected: false,
	}

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID)

	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, st.oldProxyID, *res.OwnerProxyID)
	require.True(t, res.ExpiresAt.IsZero(), "committed leases must have no expiry")
	require.Equal(t, st.txID, res.TransactionID)
}

// TestCommitUpdateStoppedRejectsFirstShadow: the stopped branch may never
// convert an origin (it cannot create a child at all). Both an input-level
// first-shadow plan (S0) and a row that still lives on the unshadowed
// origin / lacks an origin link (S1) fail with ErrStoppedFirstShadow and
// ZERO DB delta.
func TestCommitUpdateStoppedRejectsFirstShadow(t *testing.T) {
	t.Run("input carries a first-shadow plan", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		in.Shadow = &dao.ShadowCommit{FirstShadow: true, ChildCreated: true, OriginAfter: &models.ClientEntity{ClientID: st.originID}}
		before := captureUpdateCounts(t, db, st.txID)
		beforeProxies := captureCommitProxies(t, db)
		beforeChild := captureClientRow(t, db, st.childID)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrStoppedFirstShadow), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "S0 rejection must be zero-delta")
		require.Equal(t, beforeProxies, captureCommitProxies(t, db))
		require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
	})

	t.Run("old row still on the origin", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		// the rule still lives on the never-shadowed origin row itself
		require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).
			Update("client_id", st.originID).Error)
		before := captureUpdateCounts(t, db, st.txID)
		beforeProxies := captureCommitProxies(t, db)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrStoppedFirstShadow), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
		require.Equal(t, beforeProxies, captureCommitProxies(t, db))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})

	t.Run("legacy unshadowed row", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		// historical anomaly: no origin link at all
		require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).
			Update("origin_client_id", "").Error)
		before := captureUpdateCounts(t, db, st.txID)
		beforeProxies := captureCommitProxies(t, db)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrStoppedFirstShadow), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
		require.Equal(t, beforeProxies, captureCommitProxies(t, db))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})
}

// TestCommitUpdateStoppedRejectsInvalidInputsZeroDBTouch is the S0 contract
// for the stopped branch: every statically illegal payload shape surfaces
// ErrInvalidCommitInput (or ErrStoppedFirstShadow for a first-shadow plan)
// with ZERO DB writes.
func TestCommitUpdateStoppedRejectsInvalidInputsZeroDBTouch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(in *dao.CommitUpdateInput)
		want   error
	}{
		{"active client present", func(in *dao.CommitUpdateInput) {
			in.Client = &models.ClientEntity{ClientID: "x", ServerID: in.OldServerID, OriginClientID: in.OriginClientID}
		}, defs.ErrInvalidCommitInput},
		{"nil stopped update", func(in *dao.CommitUpdateInput) { in.StoppedUpdate = nil }, defs.ErrInvalidCommitInput},
		{"empty child client id", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.ChildClientID = "" }, defs.ErrInvalidCommitInput},
		{"empty new name", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewName = "" }, defs.ErrInvalidCommitInput},
		{"empty new type", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewType = "" }, defs.ErrInvalidCommitInput},
		{"unsupported new type", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewType = "http" }, defs.ErrInvalidCommitInput},
		{"empty new server", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewServerID = "" }, defs.ErrInvalidCommitInput},
		{"new server mismatch", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewServerID = "srv-other" }, defs.ErrInvalidCommitInput},
		{"nil typed json", func(in *dao.CommitUpdateInput) { in.StoppedUpdate.NewTypedJSON = nil }, defs.ErrInvalidCommitInput},
		{"unparseable typed json", func(in *dao.CommitUpdateInput) {
			in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web"}`)
		}, defs.ErrInvalidCommitInput},
		{"typed json name mismatch", func(in *dao.CommitUpdateInput) {
			in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web2","type":"tcp","remotePort":8001}`)
		}, defs.ErrInvalidCommitInput},
		{"typed json type mismatch", func(in *dao.CommitUpdateInput) {
			in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web","type":"udp","remotePort":8001}`)
		}, defs.ErrInvalidCommitInput},
		{"typed json key mismatch", func(in *dao.CommitUpdateInput) {
			in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web","type":"tcp","remotePort":8002}`)
		}, defs.ErrInvalidCommitInput},
		{"shadow first shadow", func(in *dao.CommitUpdateInput) {
			in.Shadow = &dao.ShadowCommit{FirstShadow: true, ChildCreated: true, OriginAfter: &models.ClientEntity{ClientID: in.OriginClientID}}
		}, defs.ErrStoppedFirstShadow},
		{"shadow child created", func(in *dao.CommitUpdateInput) {
			in.Shadow = &dao.ShadowCommit{ChildCreated: true}
		}, defs.ErrInvalidCommitInput},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedStoppedUpdate(t, ctx, db)
			before := captureUpdateCounts(t, db, st.txID)
			beforeProxies := captureCommitProxies(t, db)
			tc.mutate(in)

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.want), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "invalid input must not touch the DB")
			require.Equal(t, beforeProxies, captureCommitProxies(t, db))
			tx := loadTxRow(t, db, st.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
		})
	}
}

// TestCommitUpdateStoppedGuardFailuresRollBack covers every S1/S3 divergence:
// missing/diverged old row, old row ACTIVE while the plan says stopped, old
// key mismatch, and a missing/wrong-shape child. Each fails closed with the
// stable sentinel and rolls the whole commit back (zero delta, tx still
// verifying).
func TestCommitUpdateStoppedGuardFailuresRollBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		prep func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState)
		want error
	}{
		{"old row missing", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Unscoped().Where("id = ?", st.oldProxyID).Delete(&models.ProxyConfig{}).Error)
		}, defs.ErrProxyConfigNotFound},
		{"old row active", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("stopped", false).Error)
		}, defs.ErrProxyConfigChanged},
		{"old row renamed", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("name", "web-old").Error)
		}, defs.ErrProxyConfigChanged},
		{"old row server changed", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("server_id", "srv-other").Error)
		}, defs.ErrProxyConfigChanged},
		{"old row on another child", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("client_id", "cli-other-child").Error)
		}, defs.ErrProxyConfigChanged},
		{"old row origin link lost", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("origin_client_id", "cli-other").Error)
		}, defs.ErrProxyConfigChanged},
		{"old key mismatch", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			// plan claims the old key is tcp/8002 while the row really runs
			// tcp/8001 — the NEW side (json + new key) stays aligned at 8002
			// so only the S1 old-key check can catch it
			in.StoppedUpdate.NewTypedJSON = []byte(`{"name":"web","type":"tcp","remotePort":8002,"localPort":9090}`)
			in.Reservation = dao.ReservationStep{
				Mode:                   dao.ReservationModeSameKeyOwned,
				OldKey:                 st.key("tcp", 8002),
				NewKey:                 st.key("tcp", 8002),
				OldReservationExpected: true,
			}
		}, defs.ErrProxyConfigChanged},
		{"child missing", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Unscoped().Where("client_id = ?", st.childID).Delete(&models.Client{}).Error)
		}, defs.ErrClientNotFound},
		{"child server changed", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", st.childID).Update("server_id", "srv-other").Error)
		}, defs.ErrProxyConfigChanged},
		{"child origin changed", func(t *testing.T, db *gorm.DB, in *dao.CommitUpdateInput, st *stoppedFixtureState) {
			require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", st.childID).Update("origin_client_id", "cli-other").Error)
		}, defs.ErrProxyConfigChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedStoppedUpdate(t, ctx, db)
			tc.prep(t, db, in, st)
			before := captureUpdateCounts(t, db, st.txID)
			beforeProxies := captureCommitProxies(t, db)
			// the child may be intentionally ABSENT (child-missing case) —
			// then the assertion is that it stays absent
			var beforeChild models.Client
			childExists := db.Where("client_id = ?", st.childID).First(&beforeChild).Error == nil

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.want), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
			require.Equal(t, beforeProxies, captureCommitProxies(t, db))
			if childExists {
				require.Equal(t, beforeChild, captureClientRow(t, db, st.childID), "child must stay byte-identical")
			} else {
				var childCount int64
				require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", st.childID).Count(&childCount).Error)
				require.Zero(t, childCount, "the deleted child must stay absent")
			}
			tx := loadTxRow(t, db, st.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
			require.Nil(t, tx.ProxyID)
		})
	}
}

// TestCommitUpdateStoppedRollsBackOnFailureAfterRowUpdate proves the
// stopped-branch in-place row update is INSIDE the commit transaction: when
// the reservation owner CAS (S6) or the after-snapshot encryption (S7) fails
// AFTER the row was updated, EVERYTHING — the row, the child, reservations,
// tx, snapshots and audit — rolls back.
func TestCommitUpdateStoppedRollsBackOnFailureAfterRowUpdate(t *testing.T) {
	t.Run("owner CAS miss", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		beforeRow := loadProxyByID(t, db, st.oldProxyID)
		beforeChild := captureClientRow(t, db, st.childID)
		beforeActiveSib := loadProxyByID(t, db, st.activeSibID)
		beforeStoppedSib := loadProxyByID(t, db, st.stoppedSibID)
		beforeCounts := captureUpdateCounts(t, db, st.txID)
		// reservation owner diverged between plan and commit → S6 CAS fails
		require.NoError(t, db.Model(&models.PortReservation{}).
			Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).
			Update("owner_proxy_id", 99999).Error)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
		require.Equal(t, beforeRow, loadProxyByID(t, db, st.oldProxyID), "the in-place row update must roll back")
		require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
		require.Equal(t, beforeActiveSib, loadProxyByID(t, db, st.activeSibID))
		require.Equal(t, beforeStoppedSib, loadProxyByID(t, db, st.stoppedSibID))
		require.Equal(t, beforeCounts, captureUpdateCounts(t, db, st.txID))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
		require.Nil(t, tx.ProxyID)
	})

	t.Run("poisoned snapshot key", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedStoppedUpdate(t, ctx, db)
		beforeRow := loadProxyByID(t, db, st.oldProxyID)
		beforeChild := captureClientRow(t, db, st.childID)
		beforeCounts := captureUpdateCounts(t, db, st.txID)
		in.ProxyAfterSnapshot.Key = []byte{1} // wrong size: EncryptSnapshot fails at S7

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "save proxy after snapshot"), "got: %v", err)
		require.Equal(t, beforeRow, loadProxyByID(t, db, st.oldProxyID), "the in-place row update must roll back")
		require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
		require.Equal(t, beforeCounts, captureUpdateCounts(t, db, st.txID))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})
}
