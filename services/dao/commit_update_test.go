package dao_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
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

// updateFixtureState carries the ids a CommitUpdate test needs to assert
// against after the commit (or to build the failure case before it).
type updateFixtureState struct {
	txID         string
	originID     string
	childID      string
	srvID        string
	oldCreateTx  string // the ORIGINAL create transaction that owns the committed reservation
	oldProxyID   uint
	stoppedRowID uint
	oldKey       dao.ReservationKey
}

func (s *updateFixtureState) key(protocol string, port uint32) dao.ReservationKey {
	return dao.ReservationKey{ServerID: s.srvID, Protocol: protocol, RemotePort: port}
}

// seedExistingChildUpdate builds the standard existing-child update world:
// shadowed origin, existing child with one ACTIVE rule row ("web") that is
// the old rule, a verifying update transaction, a COMMITTED reservation at
// the old key owned by the old row (transaction_id = the ORIGINAL create
// tx), and the two before snapshots (client+proxy) the coordinator saves
// before apply. The returned input is the same-name SameKeyOwned commit; a
// test tweaks it for the variant it exercises. When preSeedSameNameRow is
// true an extra ACTIVE "web" row is created BEFORE the old row so the
// rebuild's same-name id reuse resolves a different id (the id-contract
// guard must fail closed).
func seedExistingChildUpdate(t *testing.T, ctx *app.Context, db *gorm.DB, preSeedSameNameRow bool) (*dao.CommitUpdateInput, *updateFixtureState) {
	t.Helper()
	now := time.Now()
	const (
		originID    = "cli-origin-upd"
		childID     = "cli-child-upd"
		txID        = "tx-upd"
		srvID       = "srv-upd"
		oldCreateTx = "tx-create-web"
	)
	user := testUserInfo()

	origin := &models.ClientEntity{ClientID: originID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	child := &models.ClientEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	child.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080})
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, child))

	if preSeedSameNameRow {
		// legacy duplicate: an ACTIVE "web" row created before the real old
		// row, so the rebuild's id-reuse lookup (smallest id wins) resolves
		// THIS row instead of the old row.
		require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			Content: []byte(`{"name":"web","type":"tcp","remotePort":8000}`),
		}}).Error)
	}

	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"web","type":"tcp","remotePort":8001}`),
	}}
	require.NoError(t, db.Create(oldRow).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-upd", RequestHash: "h",
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
		Branch: dao.UpdateBranchActive,
		Client: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID,
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			IsShadow: false, ConnectSecret: "child-secret",
			ConfigContent: clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080}),
		},
		Shadow:    &dao.ShadowCommit{FirstShadow: false, ChildCreated: false},
		ProxyName: "web",
		Reservation: dao.ReservationStep{
			Mode:                   dao.ReservationModeSameKeyOwned,
			OldKey:                 oldKey,
			NewKey:                 oldKey,
			OldReservationExpected: true,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"proxies":["web"],"server_id":"srv-upd"}`), Key: testSnapshotKey(),
		},
		ProxyAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"v":1,"name":"web","type":"tcp","remote_port":8001}`), Key: testSnapshotKey(),
		},
		// deliberately WRONG identity: the DAO must force TxID/tenant/actor/EventType
		AuditEvent: &models.AuditEvent{
			TransactionID: "wrong-tx", EventType: "bogus",
			Summary: "updated proxy web", Success: true,
		},
	}
	return in, &updateFixtureState{
		txID: txID, originID: originID, childID: childID, srvID: srvID,
		oldCreateTx: oldCreateTx, oldProxyID: oldRow.ID, oldKey: oldKey,
	}
}

// seedFirstShadowUpdate builds the first-shadow update world: a
// NEVER-shadowed origin carrying the active rule "web" (the old row) plus a
// stopped sibling "paused" with its own committed reservation, a verifying
// update transaction and a committed reservation at the old key owned by
// the old row. The child does not exist yet — the commit must convert the
// origin, migrate the stopped sibling to the new child (original id kept,
// reservation non-dangling) and create the child.
func seedFirstShadowUpdate(t *testing.T, ctx *app.Context, db *gorm.DB) (*dao.CommitUpdateInput, *updateFixtureState) {
	t.Helper()
	now := time.Now()
	const (
		originID    = "cli-origin-first-upd"
		childID     = "cli-child-first-upd"
		txID        = "tx-upd-first"
		srvID       = "srv-upd-first"
		oldCreateTx = "tx-create-web-first"
	)
	user := testUserInfo()

	origin := &models.ClientEntity{
		ClientID: originID, ServerID: srvID, ConnectSecret: "origin-secret", IsShadow: false,
	}
	origin.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080})
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"web","type":"tcp","remotePort":8001}`),
	}}
	require.NoError(t, db.Create(oldRow).Error)

	stopped := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: srvID, Name: "paused", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: true,
		Content: []byte(`{"name":"paused","type":"tcp","remotePort":9000}`),
	}}
	require.NoError(t, db.Create(stopped).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 9000,
		TransactionID: "tx-paused-owner", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &stopped.ID,
	}).Error)

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-upd-first", RequestHash: "h",
		Operation: models.TxOperationUpdate, Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)

	oldKey := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8001}
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: oldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &oldRow.ID,
	}).Error)

	// before snapshots: the coordinator snapshots the ORIGIN client and the
	// old proxy row before apply
	seedBeforeSnapshots(t, db, txID, originID, strconv.FormatUint(uint64(oldRow.ID), 10), user.GetTenantID())

	in := &dao.CommitUpdateInput{
		TxID: txID, OldProxyID: oldRow.ID, OldName: "web", OldServerID: srvID, OriginClientID: originID,
		Branch: dao.UpdateBranchActive,
		Client: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID,
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			IsShadow: false, ConnectSecret: "child-secret",
			ConfigContent: clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080}),
		},
		Shadow: &dao.ShadowCommit{
			FirstShadow:  true,
			ChildCreated: true,
			OriginAfter:  &models.ClientEntity{ClientID: originID, IsShadow: true},
		},
		ProxyName: "web",
		Reservation: dao.ReservationStep{
			Mode:                   dao.ReservationModeSameKeyOwned,
			OldKey:                 oldKey,
			NewKey:                 oldKey,
			OldReservationExpected: true,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"proxies":["web"],"server_id":"srv-upd-first"}`), Key: testSnapshotKey(),
		},
		ProxyAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"v":1,"name":"web","type":"tcp","remote_port":8001}`), Key: testSnapshotKey(),
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID, EventType: models.AuditEventTypeUpdate,
			Summary: "updated proxy web", Success: true,
		},
	}
	return in, &updateFixtureState{
		txID: txID, originID: originID, childID: childID, srvID: srvID,
		oldCreateTx: oldCreateTx, oldProxyID: oldRow.ID, stoppedRowID: stopped.ID, oldKey: oldKey,
	}
}

// seedBeforeSnapshots inserts the two before rows (client + proxy) exactly
// as the coordinator would have saved them pre-apply. The commit must never
// touch them.
func seedBeforeSnapshots(t *testing.T, db *gorm.DB, txID, clientNodeID, proxyNodeID string, tenantID int) {
	t.Helper()
	for _, s := range []struct{ nodeType, nodeID, plaintext string }{
		{models.SnapshotNodeTypeClient, clientNodeID, `{"before":"client"}`},
		{models.SnapshotNodeTypeProxy, proxyNodeID, `{"before":"proxy"}`},
	} {
		ciphertext, nonce, sum, err := snapshot.EncryptSnapshot([]byte(s.plaintext), testSnapshotKey())
		require.NoError(t, err)
		require.NoError(t, db.Create(&models.ConfigSnapshot{
			TransactionID: txID, TenantID: tenantID, Side: models.SnapshotSideBefore,
			NodeType: s.nodeType, NodeID: s.nodeID,
			Ciphertext: ciphertext, Nonce: nonce, SHA256: sum,
		}).Error)
	}
}

// updateCounts snapshots the row counts CommitUpdate can touch; equality
// before/after a failed commit proves a full rollback.
type updateCounts struct {
	clients, proxies, reservations, snapshots, audits int64
}

func captureUpdateCounts(t *testing.T, db *gorm.DB, txID string) updateCounts {
	t.Helper()
	var c updateCounts
	require.NoError(t, db.Model(&models.Client{}).Count(&c.clients).Error)
	require.NoError(t, db.Model(&models.ProxyConfig{}).Count(&c.proxies).Error)
	require.NoError(t, db.Model(&models.PortReservation{}).Count(&c.reservations).Error)
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).Where("transaction_id = ?", txID).Count(&c.snapshots).Error)
	require.NoError(t, db.Model(&models.AuditEvent{}).Where("transaction_id = ?", txID).Count(&c.audits).Error)
	return c
}

func captureSnapshots(t *testing.T, db *gorm.DB, txID string) []models.ConfigSnapshot {
	t.Helper()
	var snaps []models.ConfigSnapshot
	require.NoError(t, db.Where("transaction_id = ?", txID).Order("id ASC").Find(&snaps).Error)
	return snaps
}

func findSnapshot(t *testing.T, snaps []models.ConfigSnapshot, side, nodeType string) models.ConfigSnapshot {
	t.Helper()
	for _, s := range snaps {
		if s.Side == side && s.NodeType == nodeType {
			return s
		}
	}
	require.FailNow(t, "snapshot not found", "side=%s node_type=%s", side, nodeType)
	return models.ConfigSnapshot{}
}

func loadTxRow(t *testing.T, db *gorm.DB, txID string) models.ChangeTransaction {
	t.Helper()
	var tx models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&tx).Error)
	return tx
}

func loadReservation(t *testing.T, db *gorm.DB, key dao.ReservationKey) models.PortReservation {
	t.Helper()
	var res models.PortReservation
	require.NoError(t, db.Where("server_id = ? AND protocol = ? AND remote_port = ?",
		key.ServerID, key.Protocol, key.RemotePort).First(&res).Error)
	return res
}

func sha256Hex(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

// TestCommitUpdateExistingSameNameSameKeyOwned is the A3a happy path: same
// name + same key + committed row owned by the old proxy. The row id is
// preserved (final==old), the owner CAS is a value-identical no-op that
// must SUCCEED without relying on RowsAffected, transaction_id is never
// rewritten, the tx is committed with proxy_id=old, exactly two after
// snapshots (client+proxy) are written and decryptable, the before
// snapshots are untouched, and one forced update audit event exists.
func TestCommitUpdateExistingSameNameSameKeyOwned(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	beforeSnaps := captureSnapshots(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, st.oldProxyID, proxyID, "same-name update must keep the old proxy row id")

	// old row preserved with the same id, active, content rebuilt
	var old models.ProxyConfig
	require.NoError(t, db.Where("id = ?", st.oldProxyID).First(&old).Error)
	require.Equal(t, "web", old.Name)
	require.Equal(t, st.childID, old.ClientID)
	require.False(t, old.Stopped)
	require.NotEmpty(t, old.Content, "the rebuilt row must carry the typed rule")

	// reservation: still committed, still owned by the old proxy (final==old
	// no-op), transaction_id STILL the original create tx — SameKeyOwned
	// must never rewrite it
	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, st.oldProxyID, *res.OwnerProxyID)
	require.Equal(t, st.oldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite transaction_id")

	// tx committed with proxy_id backfilled to the OLD id
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, st.oldProxyID, *tx.ProxyID)

	// snapshots: 2 before (seeded, untouched) + 2 after (client + proxy)
	after := captureSnapshots(t, db, st.txID)
	require.Len(t, after, len(beforeSnaps)+2)
	var beforeAfter []models.ConfigSnapshot
	for _, s := range after {
		if s.Side == models.SnapshotSideBefore {
			beforeAfter = append(beforeAfter, s)
		}
	}
	require.Equal(t, beforeSnaps, beforeAfter, "before snapshots must be byte-identical after the commit")

	afterClient := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeClient)
	require.Equal(t, st.childID, afterClient.NodeID, "client after snapshot must be bound to the child")
	pt, err := dao.DecryptSnapshotForTest(&afterClient, testSnapshotKey())
	require.NoError(t, err)
	require.Equal(t, in.ClientAfterSnapshot.Plaintext, pt)
	require.Equal(t, sha256Hex(in.ClientAfterSnapshot.Plaintext), afterClient.SHA256)
	require.False(t, bytes.Contains(afterClient.Ciphertext, in.ClientAfterSnapshot.Plaintext),
		"ciphertext must never contain the plaintext")

	afterProxy := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.Equal(t, strconv.FormatUint(uint64(st.oldProxyID), 10), afterProxy.NodeID,
		"proxy after snapshot must be bound to the final proxy row id")
	ptProxy, err := dao.DecryptSnapshotForTest(&afterProxy, testSnapshotKey())
	require.NoError(t, err)
	require.Equal(t, in.ProxyAfterSnapshot.Plaintext, ptProxy)
	require.Equal(t, sha256Hex(in.ProxyAfterSnapshot.Plaintext), afterProxy.SHA256)
	require.False(t, bytes.Contains(afterProxy.Ciphertext, in.ProxyAfterSnapshot.Plaintext))

	// audit: exactly one, with TxID/tenant/actor/EventType FORCED (the input
	// carried deliberately wrong identity values)
	var audits []models.AuditEvent
	require.NoError(t, db.Where("transaction_id = ?", st.txID).Find(&audits).Error)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeUpdate, audits[0].EventType, "EventType must be forced to update")
	require.Equal(t, st.txID, audits[0].TransactionID, "audit TransactionID must be forced to the commit tx")
	require.Equal(t, testUserInfo().GetTenantID(), audits[0].TenantID)
	require.Equal(t, testUserInfo().GetUserID(), audits[0].ActorUserID)
	require.False(t, strings.Contains(audits[0].Summary, string(in.ClientAfterSnapshot.Plaintext)))

	// no stray proxy rows appeared
	require.Len(t, captureCommitProxies(t, db), len(beforeProxies), "same-name update must not duplicate rows")
}

// TestCommitUpdateActiveRenameSameKeyOwned: renaming deletes the old row,
// creates a new row with a NEW id, CAS-moves the reservation owner old→new
// while keeping transaction_id = the original create tx, backfills
// tx.ProxyID=new and writes the proxy after snapshot bound to the new id.
func TestCommitUpdateActiveRenameSameKeyOwned(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	in.ProxyName = "web2"
	in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web2", protocol: "tcp", remotePort: 8001, localPort: 8080})

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.NotEqual(t, st.oldProxyID, proxyID, "rename must produce a new proxy row id")

	var oldCount int64
	require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Count(&oldCount).Error)
	require.Zero(t, oldCount, "the old rule row must be hard-deleted on rename")

	var renamed models.ProxyConfig
	require.NoError(t, db.Where("id = ?", proxyID).First(&renamed).Error)
	require.Equal(t, "web2", renamed.Name)
	require.Equal(t, st.childID, renamed.ClientID)

	// owner CAS old→new; transaction_id KEPT from the original create tx
	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, proxyID, *res.OwnerProxyID, "owner must move to the new proxy row")
	require.Equal(t, st.oldCreateTx, res.TransactionID, "owner CAS must never rewrite transaction_id")

	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, proxyID, *tx.ProxyID)

	after := captureSnapshots(t, db, st.txID)
	afterProxy := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.Equal(t, strconv.FormatUint(uint64(proxyID), 10), afterProxy.NodeID,
		"proxy after snapshot must be bound to the NEW id")

	var audits []models.AuditEvent
	require.NoError(t, db.Where("transaction_id = ?", st.txID).Find(&audits).Error)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeUpdate, audits[0].EventType)
}

// TestCommitUpdateDifferentKeyReleasesOldCommitted: new-key pending →
// committed owner=final; old exact committed row (owner=old) hard-deleted.
func TestCommitUpdateDifferentKeyReleasesOldCommitted(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	newKey := st.key("tcp", 8002)
	// the applied config's real physical key must match the new reservation
	// key (tcp/8002) — a config still running 8001 would be the P0
	in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
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

	newRes := loadReservation(t, db, newKey)
	require.Equal(t, models.ReservationStatusCommitted, newRes.Status)
	require.NotNil(t, newRes.OwnerProxyID)
	require.Equal(t, proxyID, *newRes.OwnerProxyID)

	var oldCount int64
	require.NoError(t, db.Model(&models.PortReservation{}).
		Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).Count(&oldCount).Error)
	require.Zero(t, oldCount, "the old committed reservation must be hard-deleted")

	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, proxyID, *tx.ProxyID)
}

// TestCommitUpdateProtocolChangeSamePortIsDifferentKey: protocol lives in
// the unique index, so tcp→udp on the same port is a key change: new
// pending (udp/8001) commits, old committed (tcp/8001) is deleted.
func TestCommitUpdateProtocolChangeSamePortIsDifferentKey(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	newKey := st.key("udp", 8001)
	// the applied config really switches the protocol to udp/8001 — the
	// reservation key and the config must agree on the physical key
	in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "udp", remotePort: 8001, localPort: 8080})
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "udp", RemotePort: 8001,
		TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
	}).Error)
	in.Reservation = dao.ReservationStep{
		Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
		OldReservationExpected: true,
	}

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)

	newRes := loadReservation(t, db, newKey)
	require.Equal(t, models.ReservationStatusCommitted, newRes.Status)

	var oldCount int64
	require.NoError(t, db.Model(&models.PortReservation{}).
		Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).Count(&oldCount).Error)
	require.Zero(t, oldCount, "the old tcp lease must be released on the protocol change")
}

// TestCommitUpdateActiveOldRowKeyMismatchFailsClosed pins the P0 physical-key
// binding on the OLD side (S1): the plan's old reservation key must equal the
// real typed key INSIDE the old proxy row's content. When the reservation
// says tcp/8002 but the row actually runs tcp/8001, the commit must fail
// closed with ErrProxyConfigChanged — the pre-fix implementation never looked
// at the row content and died LATE at the reservation step
// (ErrReservationConflict), or worse could re-lock a port the rule never
// ran on. (The applied config matches the NEW key 8002, so only S1 can
// catch the divergence.)
func TestCommitUpdateActiveOldRowKeyMismatchFailsClosed(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	// plan claims the old physical key is tcp/8002, but the old row's typed
	// content still runs tcp/8001 — S1 must reject the mismatch
	in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
	in.Reservation = dao.ReservationStep{
		Mode:                   dao.ReservationModeSameKeyOwned,
		OldKey:                 st.key("tcp", 8002),
		NewKey:                 st.key("tcp", 8002),
		OldReservationExpected: true,
	}
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)
	beforeChild := captureClientRow(t, db, st.childID)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrProxyConfigChanged), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateActiveFinalConfigKeyMismatchFailsClosed pins the P0
// physical-key binding on the NEW side (S0): the applied client config's
// real typed key must equal Reservation.NewKey. The fixture's final config
// runs tcp/8001 while the plan claims the new key is tcp/8002 — the commit
// must be rejected statically with ErrInvalidCommitInput and ZERO DB writes.
// The pre-fix implementation ignored the config and failed LATE at the
// reservation step (or committed a lock for a port the rule never ran on).
func TestCommitUpdateActiveFinalConfigKeyMismatchFailsClosed(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	in.Reservation = dao.ReservationStep{
		Mode:                   dao.ReservationModeSameKeyOwned,
		OldKey:                 st.key("tcp", 8002),
		NewKey:                 st.key("tcp", 8002),
		OldReservationExpected: true,
	}
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrInvalidCommitInput), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "S0 rejection must not touch the DB")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateActiveFinalConfigProtocolMismatchFailsClosed: the applied
// config's real protocol is tcp, but the reservation NewKey claims udp (same
// port — protocol lives inside the unique index). The static S0 check must
// reject the mismatch with ErrInvalidCommitInput before any DB write; the
// pre-fix implementation failed late at the udp pending-lease lookup.
func TestCommitUpdateActiveFinalConfigProtocolMismatchFailsClosed(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	// the config's real protocol stays tcp/8001; only the RESERVATION claims
	// a protocol change the config never made
	in.Reservation = dao.ReservationStep{
		Mode:                   dao.ReservationModeDifferentKey,
		OldKey:                 st.oldKey,
		NewKey:                 st.key("udp", 8001),
		OldReservationExpected: true,
	}
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrInvalidCommitInput), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "S0 rejection must not touch the DB")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateSameKeyPendingLegacy: legacy path — no committed row, the
// coordinator acquired a NEW pending lease at the (old) key; commit flips it
// to committed owner=final with a cleared expiry.
func TestCommitUpdateSameKeyPendingLegacy(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	// legacy: no committed row at the key; the update tx owns a pending lease
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

	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, proxyID, *res.OwnerProxyID)
	require.True(t, res.ExpiresAt.IsZero(), "committed leases must have no expiry")
	require.Equal(t, st.txID, res.TransactionID)

	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

// TestCommitUpdateFirstShadow converts the never-shadowed origin, deletes
// its active rows (the old rule gets a NEW id on the child), migrates the
// stopped sibling to the child with its ORIGINAL id (reservation stays
// non-dangling), CAS-moves the old-key owner old→new and backfills the tx.
func TestCommitUpdateFirstShadow(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedFirstShadowUpdate(t, ctx, db)
	stoppedBefore := models.ProxyConfig{}
	require.NoError(t, db.Where("id = ?", st.stoppedRowID).First(&stoppedBefore).Error)

	proxyID, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.NoError(t, err)
	require.NotEqual(t, st.oldProxyID, proxyID, "first shadow must give the rule a NEW row id")

	// origin converted in place
	var origin models.Client
	require.NoError(t, db.Where("client_id = ?", st.originID).First(&origin).Error)
	require.True(t, origin.IsShadow, "origin must be converted to shadow")
	require.Empty(t, origin.ServerID)
	require.Empty(t, origin.ConfigContent)

	// child created
	var child models.Client
	require.NoError(t, db.Where("client_id = ?", st.childID).First(&child).Error)
	require.Equal(t, st.originID, child.OriginClientID)
	require.Equal(t, st.srvID, child.ServerID)
	require.False(t, child.IsShadow)

	// old active row gone, new rule row on the child
	var oldCount int64
	require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Count(&oldCount).Error)
	require.Zero(t, oldCount, "the old active origin row must be deleted")
	var web models.ProxyConfig
	require.NoError(t, db.Where("client_id = ? AND name = ?", st.childID, "web").First(&web).Error)
	require.Equal(t, proxyID, web.ID)

	// stopped sibling migrated with its ORIGINAL id and origin link
	var migrated models.ProxyConfig
	require.NoError(t, db.Where("id = ?", st.stoppedRowID).First(&migrated).Error)
	require.Equal(t, st.childID, migrated.ClientID, "stopped sibling must move to the child")
	require.Equal(t, st.originID, migrated.OriginClientID)
	require.Equal(t, stoppedBefore.Name, migrated.Name)
	require.Equal(t, stoppedBefore.Content, migrated.Content)
	require.True(t, migrated.Stopped)
	// its committed reservation is still owned by the same (migrated) row
	pausedRes := loadReservation(t, db, st.key("tcp", 9000))
	require.NotNil(t, pausedRes.OwnerProxyID)
	require.Equal(t, st.stoppedRowID, *pausedRes.OwnerProxyID, "stopped sibling reservation must not dangle")

	// no stopped row stranded on the (now shadow) origin
	var originStopped int64
	require.NoError(t, db.Model(&models.ProxyConfig{}).
		Where("client_id = ? AND stopped = ?", st.originID, true).Count(&originStopped).Error)
	require.Zero(t, originStopped)

	// old-key owner CAS old→new, transaction_id kept
	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, proxyID, *res.OwnerProxyID)
	require.Equal(t, st.oldCreateTx, res.TransactionID)

	// tx committed, proxy_id = new id; proxy after snapshot bound to new id
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, proxyID, *tx.ProxyID)

	after := captureSnapshots(t, db, st.txID)
	require.Len(t, after, 4, "2 before (seeded) + 2 after")
	afterProxy := findSnapshot(t, after, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.Equal(t, strconv.FormatUint(uint64(proxyID), 10), afterProxy.NodeID)
}

// TestCommitUpdateFirstShadowRejectsOtherServerStoppedOnOrigin pins the
// cross-server historical-anomaly guard. Once the origin becomes a shadow,
// StartProxy resolves a per-server child and queries by child_id; leaving an
// other-server stopped row on the origin would strand it permanently, while
// migrating it to the current child would attach it to the wrong server.
// The only safe v1 behavior is to reject before any first-shadow write.
func TestCommitUpdateFirstShadowRejectsOtherServerStoppedOnOrigin(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedFirstShadowUpdate(t, ctx, db)
	user := testUserInfo()
	other := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: st.originID, ServerID: "srv-other", Name: "paused-other", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: true,
		Content: []byte(`{"name":"paused-other","type":"tcp","remotePort":9100}`),
	}}
	require.NoError(t, db.Create(other).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: "srv-other", Protocol: "tcp", RemotePort: 9100,
		TransactionID: "tx-other-owner", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &other.ID,
	}).Error)

	beforeOrigin := captureClientRow(t, db, st.originID)
	beforeProxies := captureCommitProxies(t, db)
	var beforeReservations []models.PortReservation
	require.NoError(t, db.Order("id").Find(&beforeReservations).Error)
	beforeTx := loadTxRow(t, db, st.txID)
	beforeCounts := captureUpdateCounts(t, db, st.txID)

	_, err := dao.NewMutation(ctx).CommitUpdate(user, in)
	require.Error(t, err)
	require.ErrorIs(t, err, defs.ErrStoppedFirstShadow)

	require.Equal(t, beforeOrigin, captureClientRow(t, db, st.originID), "origin must not be converted")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db), "proxy rows must be byte-identical")
	var afterReservations []models.PortReservation
	require.NoError(t, db.Order("id").Find(&afterReservations).Error)
	require.Equal(t, beforeReservations, afterReservations, "reservations must be untouched")
	require.Equal(t, beforeTx, loadTxRow(t, db, st.txID), "transaction must remain verifying")
	require.Equal(t, beforeCounts, captureUpdateCounts(t, db, st.txID), "no snapshot/audit row may be added")
}

// TestCommitUpdateSameKeyOwnedMissingRowFailsClosed: when the committed
// reservation the plan expected is gone entirely (not just re-owned), the
// SameKeyOwned shape check fails closed with ErrReservationConflict — the
// lease state no longer matches the plan.
func TestCommitUpdateSameKeyOwnedMissingRowFailsClosed(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
		st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
	before := captureUpdateCounts(t, db, st.txID)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateRenameConflict is the TOCTOU guard: a same-scope row with
// the target name fails the whole commit; a row belonging to ANOTHER origin
// must NOT be mis-hit by the OR-expression (scope stays inside the WHERE).
func TestCommitUpdateRenameConflict(t *testing.T) {
	t.Run("same-origin conflict fails closed", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		in.ProxyName = "web2"
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web2", protocol: "tcp", remotePort: 8001, localPort: 8080})
		require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: st.childID, OriginClientID: st.originID, ServerID: st.srvID, Name: "web2", Type: "tcp",
			TenantID: testUserInfo().GetTenantID(), UserID: testUserInfo().GetUserID(),
			Content: []byte(`{"name":"web2","type":"tcp"}`),
		}}).Error)
		before := captureUpdateCounts(t, db, st.txID)
		beforeProxies := captureCommitProxies(t, db)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrProxyConfigNameConflict), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "full rollback on rename conflict")
		require.Equal(t, beforeProxies, captureCommitProxies(t, db))
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
		require.Nil(t, tx.ProxyID)
	})

	t.Run("other-origin same name does not conflict", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		in.ProxyName = "web2"
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web2", protocol: "tcp", remotePort: 8001, localPort: 8080})
		// a "web2" row belonging to a DIFFERENT origin (same server): the
		// rename guard scope (origin_client_id=origin OR client_id=origin)
		// must not match it
		require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: "cli-other-child", OriginClientID: "cli-other", ServerID: st.srvID, Name: "web2", Type: "tcp",
			TenantID: testUserInfo().GetTenantID(), UserID: testUserInfo().GetUserID(),
			Content: []byte(`{"name":"web2","type":"tcp"}`),
		}}).Error)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.NoError(t, err)
	})
}

// TestCommitUpdateOldRowMissingFailsAndRollsBack mirrors the CommitCreate
// I1 contract for the old row: concurrent deletion between plan and commit
// must surface the stable sentinel and roll back EVERYTHING.
func TestCommitUpdateOldRowMissingFailsAndRollsBack(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	require.NoError(t, db.Unscoped().Where("id = ?", st.oldProxyID).Delete(&models.ProxyConfig{}).Error)
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrProxyConfigNotFound), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateOldRowChangedFails: any divergence of the old row
// (name/server/stopped) from the plan fails closed with
// ErrProxyConfigChanged and rolls the whole commit back.
func TestCommitUpdateOldRowChangedFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, db *gorm.DB, st *updateFixtureState)
	}{
		{"name changed", func(t *testing.T, db *gorm.DB, st *updateFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("name", "web-old").Error)
		}},
		{"server changed", func(t *testing.T, db *gorm.DB, st *updateFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("server_id", "srv-other").Error)
		}},
		{"stopped", func(t *testing.T, db *gorm.DB, st *updateFixtureState) {
			require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", st.oldProxyID).Update("stopped", true).Error)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedExistingChildUpdate(t, ctx, db, false)
			tc.mutate(t, db, st)
			before := captureUpdateCounts(t, db, st.txID)
			beforeProxies := captureCommitProxies(t, db)

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, defs.ErrProxyConfigChanged), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
			require.Equal(t, beforeProxies, captureCommitProxies(t, db))
			tx := loadTxRow(t, db, st.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
			require.Nil(t, tx.ProxyID)
		})
	}
}

// TestCommitUpdateExistingChildMissingFailsAndRollsBack: a child hard-
// deleted between plan and commit must surface ErrClientNotFound (guarded
// UPDATE RowsAffected==1, never a silent GORM Save) and roll everything
// back.
func TestCommitUpdateExistingChildMissingFailsAndRollsBack(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	require.NoError(t, db.Unscoped().Where("client_id = ?", st.childID).Delete(&models.Client{}).Error)
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrClientNotFound), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateSameNameFinalIDMismatchFailsClosed: when the rebuild's
// same-name id reuse resolves a DIFFERENT row (a legacy duplicate with a
// smaller id), the id contract (existing same-name must keep OldProxyID)
// fails the whole commit.
func TestCommitUpdateSameNameFinalIDMismatchFailsClosed(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, true) // duplicate "web" with smaller id
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrProxyConfigChanged), "got: %v", err)
	require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
}

// TestCommitUpdateReservationCASFailuresRollsBack covers SameKeyOwned
// divergences (owner mismatch / releasing status): the S6 CAS fails closed
// AFTER the client/proxy writes and the proxy_id backfill ran, so the whole
// transaction — client row, proxy rows, reservation, tx proxy_id, snapshots,
// audit — must roll back to the pre-commit state.
func TestCommitUpdateReservationCASFailuresRollsBack(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, db *gorm.DB, st *updateFixtureState)
	}{
		{"owner mismatch", func(t *testing.T, db *gorm.DB, st *updateFixtureState) {
			require.NoError(t, db.Model(&models.PortReservation{}).
				Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).
				Update("owner_proxy_id", 99999).Error)
		}},
		{"status releasing", func(t *testing.T, db *gorm.DB, st *updateFixtureState) {
			require.NoError(t, db.Model(&models.PortReservation{}).
				Where("server_id = ? AND protocol = ? AND remote_port = ?", st.srvID, "tcp", 8001).
				Update("status", models.ReservationStatusReleasing).Error)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedExistingChildUpdate(t, ctx, db, false)
			tc.mutate(t, db, st)
			before := captureUpdateCounts(t, db, st.txID)
			beforeProxies := captureCommitProxies(t, db)
			beforeChild := captureClientRow(t, db, st.childID)

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "full rollback on reservation CAS miss")
			require.Equal(t, beforeProxies, captureCommitProxies(t, db), "proxy rows must roll back")
			require.Equal(t, beforeChild, captureClientRow(t, db, st.childID), "client row must roll back")
			tx := loadTxRow(t, db, st.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
			require.Nil(t, tx.ProxyID, "the backfilled proxy_id must roll back with the failed commit")
		})
	}
}

// TestCommitUpdatePendingOwnedByOtherTxFails: the new-key pending lease must
// belong to THIS transaction; a lease held by another tx fails closed.
func TestCommitUpdatePendingOwnedByOtherTxFails(t *testing.T) {
	t.Run("same-key-pending", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
			st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8001,
			TransactionID: "tx-other", Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		in.Reservation = dao.ReservationStep{
			Mode: dao.ReservationModeSameKeyPending, OldKey: st.oldKey, NewKey: st.oldKey,
			OldReservationExpected: false,
		}

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})

	t.Run("different-key new pending", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		newKey := st.key("tcp", 8002)
		// the applied config's real physical key must agree with the new key
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8002,
			TransactionID: "tx-other", Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		in.Reservation = dao.ReservationStep{
			Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
			OldReservationExpected: true,
		}

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})
}

// TestCommitUpdateDifferentKeyOldReservationExpectations pins the
// OldReservationExpected contract: true + missing old row fails
// (ErrReservationNotFound, the new-key commit rolls back with it); false +
// empty old key tolerates the 0 rows; false + an unexpected old row fails
// closed (never leave a silent double lock).
func TestCommitUpdateDifferentKeyOldReservationExpectations(t *testing.T) {
	t.Run("expected old row missing fails", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		newKey := st.key("tcp", 8002)
		// the applied config's real physical key must agree with the new key
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
		require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
			st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8002,
			TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		in.Reservation = dao.ReservationStep{
			Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
			OldReservationExpected: true,
		}
		before := captureUpdateCounts(t, db, st.txID)

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrReservationNotFound), "got: %v", err)
		require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "new-key commit must roll back with the missing old row")
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})

	t.Run("legacy empty old key tolerated", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		newKey := st.key("tcp", 8002)
		// the applied config's real physical key must agree with the new key
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
		require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
			st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8002,
			TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		in.Reservation = dao.ReservationStep{
			Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
			OldReservationExpected: false,
		}

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.NoError(t, err)
		res := loadReservation(t, db, newKey)
		require.Equal(t, models.ReservationStatusCommitted, res.Status)
	})

	t.Run("legacy unexpected old row fails", func(t *testing.T) {
		ctx, db := newCommitTestEnv(t)
		in, st := seedExistingChildUpdate(t, ctx, db, false)
		newKey := st.key("tcp", 8002)
		// the applied config's real physical key must agree with the new key
		in.Client.ConfigContent = clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8002, localPort: 8080})
		// the old committed row is gone, but ANOTHER row (here a pending one)
		// still occupies the old key — the legacy path must fail closed
		require.NoError(t, db.Unscoped().Where("server_id = ? AND protocol = ? AND remote_port = ?",
			st.srvID, "tcp", 8001).Delete(&models.PortReservation{}).Error)
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8001,
			TransactionID: "tx-other", Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: testUserInfo().GetTenantID(), ServerID: st.srvID, Protocol: "tcp", RemotePort: 8002,
			TransactionID: st.txID, Status: models.ReservationStatusPending, ExpiresAt: time.Now().Add(time.Minute),
		}).Error)
		in.Reservation = dao.ReservationStep{
			Mode: dao.ReservationModeDifferentKey, OldKey: st.oldKey, NewKey: newKey,
			OldReservationExpected: false,
		}

		_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
		require.Error(t, err)
		require.True(t, errors.Is(err, defs.ErrReservationConflict), "got: %v", err)
		tx := loadTxRow(t, db, st.txID)
		require.Equal(t, models.TxStatusVerifying, tx.Status)
	})
}

// TestCommitUpdateRollsBackOnPoisonedSnapshotKey proves the after-snapshot
// step is INSIDE the commit transaction: the client after snapshot was
// already written when the proxy snapshot encryption fails, and EVERYTHING
// (client row, proxy rows, reservation, tx proxy_id, client-after row,
// audit) rolls back — the before rows are the only snapshots left.
func TestCommitUpdateRollsBackOnPoisonedSnapshotKey(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, st := seedExistingChildUpdate(t, ctx, db, false)
	in.ProxyAfterSnapshot.Key = []byte{1} // wrong size: EncryptSnapshot fails
	before := captureUpdateCounts(t, db, st.txID)
	beforeProxies := captureCommitProxies(t, db)
	beforeChild := captureClientRow(t, db, st.childID)

	_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "save proxy after snapshot"), "got: %v", err)

	require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "full rollback after the snapshot failure")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db))
	require.Equal(t, beforeChild, captureClientRow(t, db, st.childID))
	tx := loadTxRow(t, db, st.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.ProxyID)
	res := loadReservation(t, db, st.oldKey)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, st.oldProxyID, *res.OwnerProxyID)
}

// TestCommitUpdateRejectsInvalidInputsZeroDBTouch is the S0 contract: every
// statically illegal shape (branch/mode/key geometry, missing payloads,
// snapshot material, nil audit) surfaces ErrInvalidCommitInput with ZERO DB
// delta — the stopped branch is rejected here too (A3b implements it).
func TestCommitUpdateRejectsInvalidInputsZeroDBTouch(t *testing.T) {
	k := dao.ReservationKey{ServerID: "srv-upd", Protocol: "tcp", RemotePort: 8001}
	k2 := dao.ReservationKey{ServerID: "srv-upd", Protocol: "tcp", RemotePort: 8002}
	cases := []struct {
		name   string
		mutate func(in *dao.CommitUpdateInput)
	}{
		{"branch stopped", func(in *dao.CommitUpdateInput) { in.Branch = dao.UpdateBranchStopped }},
		{"branch empty", func(in *dao.CommitUpdateInput) { in.Branch = "" }},
		{"branch unknown", func(in *dao.CommitUpdateInput) { in.Branch = "bogus" }},
		{"nil client", func(in *dao.CommitUpdateInput) { in.Client = nil }},
		{"nil shadow", func(in *dao.CommitUpdateInput) { in.Shadow = nil }},
		{"first-shadow without origin-after", func(in *dao.CommitUpdateInput) {
			in.Shadow = &dao.ShadowCommit{FirstShadow: true, ChildCreated: true}
		}},
		{"first-shadow without child-created", func(in *dao.CommitUpdateInput) {
			in.Shadow = &dao.ShadowCommit{FirstShadow: true, OriginAfter: &models.ClientEntity{ClientID: "x"}}
		}},
		{"existing-child with child-created", func(in *dao.CommitUpdateInput) {
			in.Shadow = &dao.ShadowCommit{FirstShadow: false, ChildCreated: true}
		}},
		{"child origin mismatch", func(in *dao.CommitUpdateInput) { in.Client.OriginClientID = "other" }},
		{"child server mismatch", func(in *dao.CommitUpdateInput) { in.Client.ServerID = "srv-other" }},
		{"empty proxy name", func(in *dao.CommitUpdateInput) { in.ProxyName = "" }},
		{"empty old name", func(in *dao.CommitUpdateInput) { in.OldName = "" }},
		{"zero old proxy id", func(in *dao.CommitUpdateInput) { in.OldProxyID = 0 }},
		{"same-key-owned keys differ", func(in *dao.CommitUpdateInput) {
			in.Reservation = dao.ReservationStep{Mode: dao.ReservationModeSameKeyOwned, OldKey: k, NewKey: k2, OldReservationExpected: true}
		}},
		{"same-key-owned expected false", func(in *dao.CommitUpdateInput) {
			in.Reservation = dao.ReservationStep{Mode: dao.ReservationModeSameKeyOwned, OldKey: k, NewKey: k, OldReservationExpected: false}
		}},
		{"same-key-pending keys differ", func(in *dao.CommitUpdateInput) {
			in.Reservation = dao.ReservationStep{Mode: dao.ReservationModeSameKeyPending, OldKey: k, NewKey: k2}
		}},
		{"same-key-pending expected true", func(in *dao.CommitUpdateInput) {
			in.Reservation = dao.ReservationStep{Mode: dao.ReservationModeSameKeyPending, OldKey: k, NewKey: k, OldReservationExpected: true}
		}},
		{"different-key keys equal", func(in *dao.CommitUpdateInput) {
			in.Reservation = dao.ReservationStep{Mode: dao.ReservationModeDifferentKey, OldKey: k, NewKey: k}
		}},
		{"unknown mode", func(in *dao.CommitUpdateInput) { in.Reservation.Mode = "bogus" }},
		{"reservation port zero", func(in *dao.CommitUpdateInput) { in.Reservation.OldKey.RemotePort = 0 }},
		{"reservation empty server", func(in *dao.CommitUpdateInput) { in.Reservation.OldKey.ServerID = "" }},
		{"reservation key server mismatch", func(in *dao.CommitUpdateInput) {
			in.Reservation.NewKey.ServerID = "srv-other"
		}},
		{"nil audit", func(in *dao.CommitUpdateInput) { in.AuditEvent = nil }},
		{"empty client snapshot key", func(in *dao.CommitUpdateInput) { in.ClientAfterSnapshot.Key = nil }},
		{"empty client snapshot plaintext", func(in *dao.CommitUpdateInput) { in.ClientAfterSnapshot.Plaintext = nil }},
		{"empty proxy snapshot key", func(in *dao.CommitUpdateInput) { in.ProxyAfterSnapshot.Key = nil }},
		{"empty proxy snapshot plaintext", func(in *dao.CommitUpdateInput) { in.ProxyAfterSnapshot.Plaintext = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedExistingChildUpdate(t, ctx, db, false)
			before := captureUpdateCounts(t, db, st.txID)
			beforeProxies := captureCommitProxies(t, db)
			tc.mutate(in)

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, defs.ErrInvalidCommitInput), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID), "invalid input must not touch the DB")
			require.Equal(t, beforeProxies, captureCommitProxies(t, db))
			tx := loadTxRow(t, db, st.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
		})
	}
}

// TestCommitUpdateRejectsAuditSummaryWithPlaintext pins the redaction guard:
// an audit summary embedding the plaintext snapshot material is rejected
// before any DB write — the audit trail must never store config plaintext.
func TestCommitUpdateRejectsAuditSummaryWithPlaintext(t *testing.T) {
	for _, tc := range []struct {
		name string
		leak func(in *dao.CommitUpdateInput) string
	}{
		{"client snapshot", func(in *dao.CommitUpdateInput) string { return string(in.ClientAfterSnapshot.Plaintext) }},
		{"proxy snapshot", func(in *dao.CommitUpdateInput) string { return string(in.ProxyAfterSnapshot.Plaintext) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, st := seedExistingChildUpdate(t, ctx, db, false)
			in.AuditEvent.Summary = tc.leak(in)
			before := captureUpdateCounts(t, db, st.txID)

			_, err := dao.NewMutation(ctx).CommitUpdate(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, defs.ErrInvalidCommitInput), "got: %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db, st.txID))
		})
	}
}
