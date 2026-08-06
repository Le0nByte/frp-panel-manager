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

// lifecycleFixture carries the ids a CommitLifecycle test needs to assert
// against after the commit (or to build the failure case before it).
type lifecycleFixture struct {
	txID        string
	originID    string
	childID     string
	srvID       string
	oldCreateTx string // the ORIGINAL create tx that owns the committed reservation (SameKeyOwned)
	proxyID     uint
	key         dao.ReservationKey
	withRule    []byte // client config that CONTAINS the rule (active state)
	withoutRule []byte // client config WITHOUT the rule (stopped state)
}

// seedLifecycleWorld builds the standard CHILD-SHAPED lifecycle world: a
// shadowed origin, an existing child carrying the rule row "web" (stopped or
// active per the caller), a verifying lifecycle transaction whose operation
// matches op, and a reservation at the rule's key in the requested mode
// (SameKeyOwned: committed owned by the row, transaction_id = the ORIGINAL
// create tx; SameKeyPending: pending owned by THIS tx). The returned input is
// the standard commit for (op, mode, stopped); a test tweaks it per variant.
func seedLifecycleWorld(t *testing.T, ctx *app.Context, db *gorm.DB, op dao.LifecycleOp, resMode dao.ReservationMode, stopped bool) (*dao.CommitLifecycleInput, *lifecycleFixture) {
	t.Helper()
	now := time.Now()
	const (
		originID    = "cli-origin-lc"
		childID     = "cli-child-lc"
		txID        = "tx-lc"
		srvID       = "srv-lc"
		oldCreateTx = "tx-create-web-lc"
	)
	user := testUserInfo()

	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, &models.ClientEntity{
		ClientID: originID, IsShadow: true, ConnectSecret: "origin-secret",
	}))

	withRule := clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080})
	withoutRule := clientConfigWithProxies(t)
	childCfg := withRule
	if stopped {
		childCfg = withoutRule
	}
	child := &models.ClientEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	child.ConfigContent = childCfg
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, child))

	row := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: stopped,
		Content: []byte(`{"name":"web","type":"tcp","remotePort":8001}`),
	}}
	require.NoError(t, db.Create(row).Error)

	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8001}
	if resMode == dao.ReservationModeSameKeyOwned {
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
			TransactionID: oldCreateTx, Status: models.ReservationStatusCommitted,
			OwnerProxyID: &row.ID,
		}).Error)
	} else {
		require.NoError(t, db.Create(&models.PortReservation{
			TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
			TransactionID: txID, Status: models.ReservationStatusPending,
			ExpiresAt: now.Add(120 * time.Second),
		}).Error)
	}

	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-lc", RequestHash: "h",
		Operation: string(op), Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)

	// start re-adds the rule to the applied config; stop/delete remove it
	afterCfg := withRule
	if op != dao.LifecycleOpStart {
		afterCfg = withoutRule
	}
	in := &dao.CommitLifecycleInput{
		TxID: txID, Op: op, ProxyID: row.ID, Name: "web", ServerID: srvID, OriginClientID: originID,
		ExpectedStopped: stopped,
		ClientAfter: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID,
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			IsShadow: false, ConnectSecret: "child-secret", ConfigContent: afterCfg,
		},
		ExpectedClientConfigContent: childCfg,
		Reservation: dao.ReservationStep{
			Mode: resMode, OldKey: key, NewKey: key,
			OldReservationExpected: resMode == dao.ReservationModeSameKeyOwned,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"after":"client","rule":"web"}`), Key: testSnapshotKey(),
		},
		// deliberately WRONG identity: the DAO must force TxID/tenant/actor/EventType
		AuditEvent: &models.AuditEvent{
			TransactionID: "wrong-tx", EventType: "bogus",
			Summary: "lifecycle op " + string(op) + " web", Success: true,
		},
		EvidenceJSON: `{"applied":true,"verify":"ok"}`,
	}
	if op != dao.LifecycleOpDelete {
		in.ProxyAfterSnapshot = &dao.SnapshotInput{
			Plaintext: []byte(`{"after":"proxy","name":"web"}`), Key: testSnapshotKey(),
		}
	}
	return in, &lifecycleFixture{
		txID: txID, originID: originID, childID: childID, srvID: srvID,
		oldCreateTx: oldCreateTx, proxyID: row.ID, key: key,
		withRule: withRule, withoutRule: withoutRule,
	}
}

// seedLifecycleOriginWorld builds the ORIGIN-SHAPED lifecycle world: a
// NEVER-shadowed origin client (ServerID and OriginClientID empty — the
// legacy leniency the S2 guard must tolerate) that directly hosts the rule
// row. SameKeyOwned only.
func seedLifecycleOriginWorld(t *testing.T, ctx *app.Context, db *gorm.DB, op dao.LifecycleOp, stopped bool) (*dao.CommitLifecycleInput, *lifecycleFixture) {
	t.Helper()
	now := time.Now()
	const (
		originID    = "cli-origin-lc-o"
		txID        = "tx-lc-o"
		srvID       = "srv-lc-o"
		oldCreateTx = "tx-create-web-lc-o"
	)
	user := testUserInfo()

	withRule := clientConfigWithProxies(t, proxySpec{name: "web", protocol: "tcp", remotePort: 8001, localPort: 8080})
	withoutRule := clientConfigWithProxies(t)
	cfg := withRule
	if stopped {
		cfg = withoutRule
	}
	origin := &models.ClientEntity{ClientID: originID, ConnectSecret: "origin-secret"}
	origin.ConfigContent = cfg
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	row := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: originID, ServerID: srvID, Name: "web", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(), Stopped: stopped,
		Content: []byte(`{"name":"web","type":"tcp","remotePort":8001}`),
	}}
	require.NoError(t, db.Create(row).Error)

	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8001}
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8001,
		TransactionID: oldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &row.ID,
	}).Error)
	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-lc-o", RequestHash: "h",
		Operation: string(op), Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)

	afterCfg := withRule
	if op != dao.LifecycleOpStart {
		afterCfg = withoutRule
	}
	in := &dao.CommitLifecycleInput{
		TxID: txID, Op: op, ProxyID: row.ID, Name: "web", ServerID: srvID, OriginClientID: originID,
		ExpectedStopped: stopped,
		ClientAfter: &models.ClientEntity{
			ClientID: originID, TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			ConnectSecret: "origin-secret", ConfigContent: afterCfg,
		},
		ExpectedClientConfigContent: cfg,
		Reservation: dao.ReservationStep{
			Mode: dao.ReservationModeSameKeyOwned, OldKey: key, NewKey: key,
			OldReservationExpected: true,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Plaintext: []byte(`{"after":"client","rule":"web"}`), Key: testSnapshotKey(),
		},
		AuditEvent: &models.AuditEvent{
			Summary: "lifecycle op " + string(op) + " web", Success: true,
		},
	}
	if op != dao.LifecycleOpDelete {
		in.ProxyAfterSnapshot = &dao.SnapshotInput{
			Plaintext: []byte(`{"after":"proxy","name":"web"}`), Key: testSnapshotKey(),
		}
	}
	return in, &lifecycleFixture{
		txID: txID, originID: originID, srvID: srvID,
		oldCreateTx: oldCreateTx, proxyID: row.ID, key: key,
		withRule: withRule, withoutRule: withoutRule,
	}
}

// loadProxyRow loads a proxy_config row (hard-delete aware callers use
// proxyRowExists first).
func loadProxyRow(t *testing.T, db *gorm.DB, id uint) models.ProxyConfig {
	t.Helper()
	var p models.ProxyConfig
	require.NoError(t, db.Unscoped().Where("id = ?", id).First(&p).Error)
	return p
}

func proxyRowExists(t *testing.T, db *gorm.DB, id uint) bool {
	t.Helper()
	var c int64
	require.NoError(t, db.Unscoped().Model(&models.ProxyConfig{}).Where("id = ?", id).Count(&c).Error)
	return c > 0
}

func loadClientRow(t *testing.T, db *gorm.DB, id string) models.Client {
	t.Helper()
	var c models.Client
	require.NoError(t, db.Where("client_id = ?", id).First(&c).Error)
	return c
}

func reservationCountAt(t *testing.T, db *gorm.DB, key dao.ReservationKey) int64 {
	t.Helper()
	var c int64
	require.NoError(t, db.Unscoped().Model(&models.PortReservation{}).
		Where("server_id = ? AND protocol = ? AND remote_port = ?", key.ServerID, key.Protocol, key.RemotePort).
		Count(&c).Error)
	return c
}

func loadLifecycleAudits(t *testing.T, db *gorm.DB, txID string) []models.AuditEvent {
	t.Helper()
	var audits []models.AuditEvent
	require.NoError(t, db.Where("transaction_id = ?", txID).Find(&audits).Error)
	return audits
}

// coreCounts are the globally-scoped row counts a lifecycle commit may
// change by design: delete removes exactly the target proxy row and its
// reservation; start/stop change neither. Snapshot/audit rows grow by
// design and are asserted separately.
type coreCounts struct {
	clients, proxies, reservations int64
}

func captureCoreCounts(t *testing.T, db *gorm.DB) coreCounts {
	t.Helper()
	var c coreCounts
	require.NoError(t, db.Model(&models.Client{}).Count(&c.clients).Error)
	require.NoError(t, db.Model(&models.ProxyConfig{}).Count(&c.proxies).Error)
	require.NoError(t, db.Model(&models.PortReservation{}).Count(&c.reservations).Error)
	return c
}

// assertLifecycleCommittedHappy asserts the common happy-path post-state:
// tx committed with ProxyID=fixture proxy id and evidence, exactly the given
// number of after snapshots with the right node bindings, one forced audit
// event of the given type, and no before/after snapshot plaintext in the
// audit summary.
func assertLifecycleCommittedHappy(t *testing.T, db *gorm.DB, f *lifecycleFixture, in *dao.CommitLifecycleInput, auditType string, afterSnapshots int) {
	t.Helper()
	tx := loadTxRow(t, db, f.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, f.proxyID, *tx.ProxyID, "lifecycle commits must keep the old proxy row id")
	require.Equal(t, in.EvidenceJSON, tx.EvidenceJSON, "evidence must land on the tx inside the commit transaction")

	snaps := captureSnapshots(t, db, f.txID)
	require.Len(t, snaps, afterSnapshots, "exactly the after snapshots must exist")
	afterClient := findSnapshot(t, snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient)
	require.Equal(t, in.ClientAfter.ClientID, afterClient.NodeID, "client after snapshot must be bound to ClientAfter.ClientID")
	pt, err := dao.DecryptSnapshotForTest(&afterClient, testSnapshotKey())
	require.NoError(t, err)
	require.Equal(t, in.ClientAfterSnapshot.Plaintext, pt)
	require.Equal(t, sha256Hex(in.ClientAfterSnapshot.Plaintext), afterClient.SHA256)
	require.False(t, bytes.Contains(afterClient.Ciphertext, in.ClientAfterSnapshot.Plaintext))
	if afterSnapshots == 2 {
		afterProxy := findSnapshot(t, snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
		require.Equal(t, strconv.FormatUint(uint64(f.proxyID), 10), afterProxy.NodeID,
			"proxy after snapshot must be bound to the old proxy row id")
		ptProxy, err := dao.DecryptSnapshotForTest(&afterProxy, testSnapshotKey())
		require.NoError(t, err)
		require.Equal(t, in.ProxyAfterSnapshot.Plaintext, ptProxy)
		require.Equal(t, sha256Hex(in.ProxyAfterSnapshot.Plaintext), afterProxy.SHA256)
		require.False(t, bytes.Contains(afterProxy.Ciphertext, in.ProxyAfterSnapshot.Plaintext))
	} else {
		for _, s := range snaps {
			require.NotEqual(t, models.SnapshotNodeTypeProxy, s.NodeType, "delete must never write a proxy after snapshot")
		}
	}

	audits := loadLifecycleAudits(t, db, f.txID)
	require.Len(t, audits, 1)
	require.Equal(t, auditType, audits[0].EventType, "EventType must be forced per op")
	require.Equal(t, f.txID, audits[0].TransactionID, "audit TransactionID must be forced to the commit tx")
	require.Equal(t, testUserInfo().GetTenantID(), audits[0].TenantID)
	require.Equal(t, testUserInfo().GetUserID(), audits[0].ActorUserID)
	require.False(t, strings.Contains(audits[0].Summary, string(in.ClientAfterSnapshot.Plaintext)))
	if in.ProxyAfterSnapshot != nil {
		require.False(t, strings.Contains(audits[0].Summary, string(in.ProxyAfterSnapshot.Plaintext)))
	}
}

// TestCommitLifecycleStartSameKeyOwned: stopped rule -> active. The row is
// flipped to stopped=false, the rule is re-added to the client config, the
// committed reservation keeps its owner AND its historical transaction_id,
// the tx commits with proxy_id=old, exactly two after snapshots exist, the
// audit type is forced to start and the evidence is written atomically.
func TestCommitLifecycleStartSameKeyOwned(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
	before := captureCoreCounts(t, db)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	row := loadProxyRow(t, db, f.proxyID)
	require.False(t, row.Stopped, "start must flip the row to active")
	require.Equal(t, f.childID, row.ClientID)
	require.Equal(t, f.originID, row.OriginClientID)
	require.Equal(t, "web", row.Name)
	require.NotEmpty(t, row.Content, "start must never touch the typed rule content")

	child := loadClientRow(t, db, f.childID)
	require.Equal(t, f.withRule, child.ConfigContent, "start must re-add the rule to the client config")
	require.Equal(t, "child-secret", child.ConnectSecret, "start must never touch the secret")
	require.False(t, child.Stopped)

	res := loadReservation(t, db, f.key)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, f.proxyID, *res.OwnerProxyID)
	require.Equal(t, f.oldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite transaction_id")

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
	require.Equal(t, before, captureCoreCounts(t, db), "start must not add or remove rows")
}

// TestCommitLifecycleStopSameKeyOwned: active rule -> stopped. The row is
// flipped to stopped=true, the rule is removed from the client config, the
// committed reservation is untouched, tx commits with proxy_id=old.
func TestCommitLifecycleStopSameKeyOwned(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStop, dao.ReservationModeSameKeyOwned, false)
	before := captureCoreCounts(t, db)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	row := loadProxyRow(t, db, f.proxyID)
	require.True(t, row.Stopped, "stop must flip the row to stopped")
	require.Equal(t, f.childID, row.ClientID)

	child := loadClientRow(t, db, f.childID)
	require.Equal(t, f.withoutRule, child.ConfigContent, "stop must remove the rule from the client config")
	require.Equal(t, "child-secret", child.ConnectSecret)

	res := loadReservation(t, db, f.key)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, f.proxyID, *res.OwnerProxyID)
	require.Equal(t, f.oldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite transaction_id")

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStop, 2)
	require.Equal(t, before, captureCoreCounts(t, db), "stop must not add or remove rows")
}

// TestCommitLifecycleDeleteSameKeyOwned covers both the active and the
// stopped delete: the row is hard-deleted, the rule is removed from the
// client config (the stopped variant is value-identical and must skip the
// write), the committed reservation is hard-deleted BY OWNER, only the
// client after snapshot is written and the tx still commits with the old
// proxy id.
func TestCommitLifecycleDeleteSameKeyOwned(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		name := "active"
		if stopped {
			name = "stopped"
		}
		t.Run(name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpDelete, dao.ReservationModeSameKeyOwned, stopped)
			before := captureCoreCounts(t, db)

			proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.NoError(t, err)
			require.Equal(t, f.proxyID, proxyID)

			require.False(t, proxyRowExists(t, db, f.proxyID), "delete must hard-delete the proxy row")

			child := loadClientRow(t, db, f.childID)
			require.Equal(t, f.withoutRule, child.ConfigContent, "delete must remove the rule from the client config")
			require.Equal(t, "child-secret", child.ConnectSecret)

			require.Equal(t, int64(0), reservationCountAt(t, db, f.key), "delete must release the committed reservation by owner")

			tx := loadTxRow(t, db, f.txID)
			require.Equal(t, models.TxStatusCommitted, tx.Status)
			require.NotNil(t, tx.ProxyID)
			require.Equal(t, f.proxyID, *tx.ProxyID, "delete keeps the old proxy id on the tx")

			assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeDelete, 1)
			after := captureCoreCounts(t, db)
			require.Equal(t, before.clients, after.clients, "delete must never touch client rows")
			require.Equal(t, before.proxies-1, after.proxies, "delete must remove exactly the target proxy row")
			require.Equal(t, before.reservations-1, after.reservations, "delete must release exactly the target reservation")
		})
	}
}

// TestCommitLifecycleSameKeyPendingLegacy covers all three ops on the legacy
// path: the reservation is THIS tx's own pending lease. start/stop flip it
// to committed with owner=old proxy id; delete hard-deletes it exactly by
// tx+key+pending — never by the delete tx id against an old committed row.
func TestCommitLifecycleSameKeyPendingLegacy(t *testing.T) {
	for _, tc := range []struct {
		op        dao.LifecycleOp
		stopped   bool
		auditType string
		resStatus string // expected reservation status after commit
		resGone   bool   // delete: no reservation row may remain
	}{
		{op: dao.LifecycleOpStart, stopped: true, auditType: models.AuditEventTypeStart, resStatus: models.ReservationStatusCommitted},
		{op: dao.LifecycleOpStop, stopped: false, auditType: models.AuditEventTypeStop, resStatus: models.ReservationStatusCommitted},
		{op: dao.LifecycleOpDelete, stopped: true, auditType: models.AuditEventTypeDelete, resGone: true},
	} {
		t.Run(string(tc.op), func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, tc.op, dao.ReservationModeSameKeyPending, tc.stopped)
			before := captureCoreCounts(t, db)

			proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.NoError(t, err)
			require.Equal(t, f.proxyID, proxyID)

			if tc.resGone {
				require.Equal(t, int64(0), reservationCountAt(t, db, f.key), "delete must hard-delete this tx's pending lease")
			} else {
				res := loadReservation(t, db, f.key)
				require.Equal(t, tc.resStatus, res.Status)
				require.NotNil(t, res.OwnerProxyID)
				require.Equal(t, f.proxyID, *res.OwnerProxyID)
				require.Equal(t, f.txID, res.TransactionID, "pending commit must keep the committing tx id")
			}

			if tc.op == dao.LifecycleOpDelete {
				require.False(t, proxyRowExists(t, db, f.proxyID))
			} else {
				row := loadProxyRow(t, db, f.proxyID)
				require.Equal(t, tc.op == dao.LifecycleOpStop, row.Stopped)
			}

			assertLifecycleCommittedHappy(t, db, f, in, tc.auditType, map[dao.LifecycleOp]int{
				dao.LifecycleOpStart:  2,
				dao.LifecycleOpStop:   2,
				dao.LifecycleOpDelete: 1,
			}[tc.op])
			after := captureCoreCounts(t, db)
			require.Equal(t, before.clients, after.clients)
			if tc.resGone {
				require.Equal(t, before.proxies-1, after.proxies)
				require.Equal(t, before.reservations-1, after.reservations)
			} else {
				require.Equal(t, before.proxies, after.proxies)
				require.Equal(t, before.reservations, after.reservations)
			}
		})
	}
}

// TestCommitLifecycleOriginShapedStart: a never-shadowed origin client with
// EMPTY ServerID/OriginClientID directly hosts the rule row. The S2 guard
// must tolerate the legacy leniency (origin clients may carry empty
// server/origin) and the commit must succeed end to end.
func TestCommitLifecycleOriginShapedStart(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleOriginWorld(t, ctx, db, dao.LifecycleOpStart, true)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	row := loadProxyRow(t, db, f.proxyID)
	require.False(t, row.Stopped)

	origin := loadClientRow(t, db, f.originID)
	require.Equal(t, f.withRule, origin.ConfigContent)
	require.Equal(t, "origin-secret", origin.ConnectSecret)

	res := loadReservation(t, db, f.key)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, f.oldCreateTx, res.TransactionID)

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
}

// TestCommitLifecycleStartFastPathAlreadyActive: start of an ALREADY-active
// rule (ExpectedStopped=false, config identical). The proxy and client rows
// must be byte-identical INCLUDING UpdatedAt (explicit no-op skip — MySQL
// reports RowsAffected=0 for value-identical UPDATEs and that must never be
// misread as a CAS miss), while the tx/snapshots/audit still commit.
func TestCommitLifecycleStartFastPathAlreadyActive(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, false)
	proxyBefore := loadProxyRow(t, db, f.proxyID)
	childBefore := loadClientRow(t, db, f.childID)
	resBefore := loadReservation(t, db, f.key)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	require.Equal(t, proxyBefore, loadProxyRow(t, db, f.proxyID), "already-active start must skip the proxy write (UpdatedAt included)")
	require.Equal(t, childBefore, loadClientRow(t, db, f.childID), "already-active start must skip the client write (UpdatedAt included)")
	require.Equal(t, resBefore, loadReservation(t, db, f.key), "same-key owned reservation must be untouched")

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
}

// TestCommitLifecycleStopFastPathAlreadyStopped mirrors the start fast path
// for a stop of an already-stopped rule.
func TestCommitLifecycleStopFastPathAlreadyStopped(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStop, dao.ReservationModeSameKeyOwned, true)
	proxyBefore := loadProxyRow(t, db, f.proxyID)
	childBefore := loadClientRow(t, db, f.childID)
	resBefore := loadReservation(t, db, f.key)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	require.Equal(t, proxyBefore, loadProxyRow(t, db, f.proxyID), "already-stopped stop must skip the proxy write (UpdatedAt included)")
	require.Equal(t, childBefore, loadClientRow(t, db, f.childID), "already-stopped stop must skip the client write (UpdatedAt included)")
	require.Equal(t, resBefore, loadReservation(t, db, f.key))

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStop, 2)
}

// TestCommitLifecycleFastPathLegacyPendingStillCommits: the no-op fast path
// on the SAME-KEY-PENDING mode must still run the reservation migration —
// the pending lease flips to committed owned by the old proxy id.
func TestCommitLifecycleFastPathLegacyPendingStillCommits(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyPending, false)
	proxyBefore := loadProxyRow(t, db, f.proxyID)
	childBefore := loadClientRow(t, db, f.childID)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	require.Equal(t, proxyBefore, loadProxyRow(t, db, f.proxyID))
	require.Equal(t, childBefore, loadClientRow(t, db, f.childID))

	res := loadReservation(t, db, f.key)
	require.Equal(t, models.ReservationStatusCommitted, res.Status, "legacy pending must commit even on the fast path")
	require.Equal(t, f.proxyID, *res.OwnerProxyID)

	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
}

// TestCommitLifecyclePreboundProxyID: the tx row already carries
// proxy_id=target. The S1 guard accepts it (nil OR equal) and the
// setTransactionProxyID backfill is the value-identical idempotent success
// (MySQL RowsAffected=0) — the commit must still succeed.
func TestCommitLifecyclePreboundProxyID(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
	require.NoError(t, db.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ?", f.txID).Update("proxy_id", f.proxyID).Error)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	tx := loadTxRow(t, db, f.txID)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, f.proxyID, *tx.ProxyID)
	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
}

// TestCommitLifecycleThinWrappers: the CommitStart/CommitStop/CommitDelete
// wrappers bind Op themselves — an input without Op set must commit as the
// wrapped operation.
func TestCommitLifecycleThinWrappers(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStop, dao.ReservationModeSameKeyOwned, false)
	in.Op = "" // wrapper must bind it

	proxyID, err := dao.NewMutation(ctx).CommitStop(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)
	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStop, 2)
}

// TestCommitLifecycleDeleteOnlyTargetAndSiblingsUntouched: an active sibling
// rule on the same child plus a sibling client and its reservation must be
// byte-identical after a delete of the target — including UpdatedAt.
func TestCommitLifecycleDeleteOnlyTargetAndSiblingsUntouched(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpDelete, dao.ReservationModeSameKeyOwned, false)
	user := testUserInfo()

	sibRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: f.childID, OriginClientID: f.originID, ServerID: f.srvID, Name: "api", Type: "tcp",
		TenantID: user.GetTenantID(), UserID: user.GetUserID(),
		Content: []byte(`{"name":"api","type":"tcp","remotePort":8003}`),
	}}
	require.NoError(t, db.Create(sibRow).Error)
	sibKey := dao.ReservationKey{ServerID: f.srvID, Protocol: "tcp", RemotePort: 8003}
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: f.srvID, Protocol: "tcp", RemotePort: 8003,
		TransactionID: "tx-create-api", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &sibRow.ID,
	}).Error)

	sibClient := &models.ClientEntity{
		ClientID: "cli-sibling-lc", OriginClientID: f.originID, ServerID: f.srvID,
		IsShadow: false, ConnectSecret: "sibling-secret",
		ConfigContent: clientConfigWithProxies(t, proxySpec{name: "api", protocol: "tcp", remotePort: 8003, localPort: 8081}),
	}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, sibClient))

	sibRowBefore := loadProxyRow(t, db, sibRow.ID)
	sibClientBefore := loadClientRow(t, db, sibClient.ClientID)
	sibResBefore := loadReservation(t, db, sibKey)

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	require.False(t, proxyRowExists(t, db, f.proxyID), "only the target row may be deleted")
	require.Equal(t, sibRowBefore, loadProxyRow(t, db, sibRow.ID), "sibling proxy row must be byte-identical")
	require.Equal(t, sibClientBefore, loadClientRow(t, db, sibClient.ClientID), "sibling client row must be byte-identical")
	require.Equal(t, sibResBefore, loadReservation(t, db, sibKey), "sibling reservation must be byte-identical")
	require.Equal(t, int64(0), reservationCountAt(t, db, f.key))
}

// TestCommitLifecycleNilExpectedConfigBytes: nil/empty ExpectedClientConfigContent
// is a legal value — a client row whose persisted config is NULL must pass
// the S2 byte compare and the S4 CAS must match NULL (never `= NULL`).
func TestCommitLifecycleNilExpectedConfigBytes(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
	require.NoError(t, db.Model(&models.Client{}).
		Where("client_id = ?", f.childID).Update("config_content", nil).Error)
	in.ExpectedClientConfigContent = nil

	proxyID, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)
	require.Equal(t, f.proxyID, proxyID)

	child := loadClientRow(t, db, f.childID)
	require.Equal(t, f.withRule, child.ConfigContent, "start must write the rule into the previously-empty config")
	assertLifecycleCommittedHappy(t, db, f, in, models.AuditEventTypeStart, 2)
}

func TestCommitLifecycleDistinguishesNullAndEmptyConfigBytes(t *testing.T) {
	tests := []struct {
		name     string
		setDB    func(t *testing.T, db *gorm.DB, clientID string)
		expected []byte
	}{
		{
			name: "expected_null_but_db_empty_blob",
			setDB: func(t *testing.T, db *gorm.DB, clientID string) {
				t.Helper()
				require.NoError(t, db.Exec("UPDATE clients SET config_content = X'' WHERE client_id = ?", clientID).Error)
			},
			expected: nil,
		},
		{
			name: "expected_empty_blob_but_db_null",
			setDB: func(t *testing.T, db *gorm.DB, clientID string) {
				t.Helper()
				require.NoError(t, db.Exec("UPDATE clients SET config_content = NULL WHERE client_id = ?", clientID).Error)
			},
			expected: make([]byte, 0),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
			tc.setDB(t, db, f.childID)
			in.ExpectedClientConfigContent = tc.expected

			_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.ErrorIs(t, err, defs.ErrProxyConfigChanged)
			tx := loadTxRow(t, db, f.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
		})
	}
}

// TestCommitLifecycleGuardsFailAndRollBack exercises the S1/S2/S6 guards:
// each mutated world must fail with the stable sentinel AND leave the whole
// database untouched (tx still verifying, no snapshots/audits/evidence).
func TestCommitLifecycleGuardsFailAndRollBack(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture)
		wantErr error
	}{
		{
			name: "wrong_operation",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ChangeTransaction{}).
					Where("transaction_id = ?", f.txID).Update("operation", models.TxOperationUpdate).Error)
			},
			wantErr: defs.ErrInvalidTxTransition,
		},
		{
			name: "wrong_status",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ChangeTransaction{}).
					Where("transaction_id = ?", f.txID).Update("status", models.TxStatusCommitted).Error)
			},
			wantErr: defs.ErrInvalidTxTransition,
		},
		{
			name: "wrong_actor",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ChangeTransaction{}).
					Where("transaction_id = ?", f.txID).Update("actor_user_id", 2).Error)
			},
			wantErr: defs.ErrTransactionNotFound,
		},
		{
			name: "wrong_tenant",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ChangeTransaction{}).
					Where("transaction_id = ?", f.txID).Update("tenant_id", 2).Error)
			},
			wantErr: defs.ErrTransactionNotFound,
		},
		{
			name: "proxy_row_missing",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Unscoped().Where("id = ?", f.proxyID).Delete(&models.ProxyConfig{}).Error)
			},
			wantErr: defs.ErrProxyConfigNotFound,
		},
		{
			name: "proxy_name_drift",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", f.proxyID).Update("name", "renamed").Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "proxy_server_drift",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", f.proxyID).Update("server_id", "other-srv").Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "proxy_stopped_drift",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", f.proxyID).Update("stopped", !in.ExpectedStopped).Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "proxy_physical_key_mismatch",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", f.proxyID).
					Update("content", []byte(`{"name":"web","type":"tcp","remotePort":8002}`)).Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "proxy_content_unparseable",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.ProxyConfig{}).Where("id = ?", f.proxyID).
					Update("content", []byte(`not a typed proxy`)).Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "client_missing",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Unscoped().Where("client_id = ?", f.childID).Delete(&models.Client{}).Error)
			},
			wantErr: defs.ErrClientNotFound,
		},
		{
			name: "client_shape_drift",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", f.childID).
					Update("origin_client_id", "some-other-origin").Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "client_config_bytes_drift",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", f.childID).
					Update("config_content", []byte(`{"drifted":true}`)).Error)
			},
			wantErr: defs.ErrProxyConfigChanged,
		},
		{
			name: "reservation_wrong_owner",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				other := uint(9999)
				require.NoError(t, db.Model(&models.PortReservation{}).
					Where("server_id = ? AND protocol = ? AND remote_port = ?", f.key.ServerID, f.key.Protocol, f.key.RemotePort).
					Update("owner_proxy_id", other).Error)
			},
			wantErr: defs.ErrReservationConflict,
		},
		{
			name: "reservation_wrong_status_for_owned",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				require.NoError(t, db.Model(&models.PortReservation{}).
					Where("server_id = ? AND protocol = ? AND remote_port = ?", f.key.ServerID, f.key.Protocol, f.key.RemotePort).
					Updates(map[string]interface{}{"status": models.ReservationStatusPending, "transaction_id": "other-tx"}).Error)
			},
			wantErr: defs.ErrReservationConflict,
		},
		{
			name: "reservation_mode_mismatch_committed_at_pending_key",
			mutate: func(t *testing.T, db *gorm.DB, in *dao.CommitLifecycleInput, f *lifecycleFixture) {
				// caller claims SameKeyPending but the DB holds a COMMITTED row
				// owned by the old proxy (the owned world) — S6 must fail closed.
				in.Reservation.Mode = dao.ReservationModeSameKeyPending
				in.Reservation.OldReservationExpected = false
			},
			wantErr: defs.ErrReservationConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
			tc.mutate(t, db, in, f)
			before := captureUpdateCounts(t, db, f.txID)

			_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.wantErr), "want %v got %v", tc.wantErr, err)

			require.Equal(t, before, captureUpdateCounts(t, db, f.txID), "a failed commit must not touch any row")
			// the tx must stay EXACTLY where the mutation left it (verifying
			// for every case except wrong_status, which mutated it away —
			// either way the failed commit must not advance it)
			var want models.ChangeTransaction
			require.NoError(t, db.Where("transaction_id = ?", f.txID).First(&want).Error)
			require.Equal(t, want, loadTxRow(t, db, f.txID), "tx row must be unchanged by the failed commit")
			require.Nil(t, want.ProxyID, "no proxy_id backfill may survive")
			require.Empty(t, want.EvidenceJSON)
			require.Empty(t, loadLifecycleAudits(t, db, f.txID))
			require.Empty(t, captureSnapshots(t, db, f.txID))
		})
	}
}

// TestCommitLifecycleRollsBackOnBadSnapshotKey proves the atomicity
// contract: a wrong-size AES key passes S0 (non-empty) but makes
// EncryptSnapshot fail at S7 INSIDE the transaction — every earlier write
// (proxy flip / hard delete, client config CAS, proxy_id backfill,
// reservation migration) rolls back and the tx stays verifying. For
// start/stop the proxy snapshot is the SECOND S7 write (after the client
// snapshot row), so the rollback also proves the client snapshot insert is
// undone.
func TestCommitLifecycleRollsBackOnBadSnapshotKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		op      dao.LifecycleOp
		stopped bool
		poison  func(t *testing.T, in *dao.CommitLifecycleInput)
	}{
		{
			name: "start_bad_proxy_key", op: dao.LifecycleOpStart, stopped: true,
			poison: func(t *testing.T, in *dao.CommitLifecycleInput) {
				in.ProxyAfterSnapshot.Key = make([]byte, 16)
			},
		},
		{
			name: "stop_bad_proxy_key", op: dao.LifecycleOpStop, stopped: false,
			poison: func(t *testing.T, in *dao.CommitLifecycleInput) {
				in.ProxyAfterSnapshot.Key = make([]byte, 16)
			},
		},
		{
			name: "delete_bad_client_key", op: dao.LifecycleOpDelete, stopped: false,
			poison: func(t *testing.T, in *dao.CommitLifecycleInput) {
				in.ClientAfterSnapshot.Key = make([]byte, 16)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, tc.op, dao.ReservationModeSameKeyOwned, tc.stopped)
			tc.poison(t, in)
			before := captureUpdateCounts(t, db, f.txID)

			_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.Error(t, err)
			require.Contains(t, err.Error(), "snapshot key must be 32 bytes")

			require.Equal(t, before, captureUpdateCounts(t, db, f.txID), "every S3-S6 write must roll back")
			require.True(t, proxyRowExists(t, db, f.proxyID), "proxy row must survive the rollback")
			row := loadProxyRow(t, db, f.proxyID)
			require.Equal(t, tc.stopped, row.Stopped, "the stopped flip must be rolled back")
			child := loadClientRow(t, db, f.childID)
			want := f.withRule
			if tc.stopped {
				want = f.withoutRule
			}
			require.Equal(t, want, child.ConfigContent, "the client config write must be rolled back")
			require.Equal(t, int64(1), reservationCountAt(t, db, f.key), "the reservation migration must be rolled back")
			res := loadReservation(t, db, f.key)
			require.Equal(t, models.ReservationStatusCommitted, res.Status)
			require.Equal(t, f.oldCreateTx, res.TransactionID)

			tx := loadTxRow(t, db, f.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status, "tx must stay verifying")
			require.Nil(t, tx.ProxyID)
			require.Empty(t, tx.EvidenceJSON)
			require.Empty(t, captureSnapshots(t, db, f.txID))
			require.Empty(t, loadLifecycleAudits(t, db, f.txID))
		})
	}
}

// TestCommitLifecycleRejectsInvalidInputsZeroDBTouch: S0 violations are
// rejected with the stable ErrInvalidCommitInput BEFORE any DB write.
func TestCommitLifecycleRejectsInvalidInputsZeroDBTouch(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
	base := *in
	key := f.key

	mutations := []struct {
		name   string
		mutate func(in *dao.CommitLifecycleInput)
	}{
		{"unknown_op", func(in *dao.CommitLifecycleInput) { in.Op = "bogus" }},
		{"empty_tx", func(in *dao.CommitLifecycleInput) { in.TxID = "" }},
		{"zero_proxy_id", func(in *dao.CommitLifecycleInput) { in.ProxyID = 0 }},
		{"empty_name", func(in *dao.CommitLifecycleInput) { in.Name = "" }},
		{"empty_server", func(in *dao.CommitLifecycleInput) { in.ServerID = "" }},
		{"empty_origin", func(in *dao.CommitLifecycleInput) { in.OriginClientID = "" }},
		{"nil_client_after", func(in *dao.CommitLifecycleInput) { in.ClientAfter = nil }},
		{"empty_client_after_id", func(in *dao.CommitLifecycleInput) { in.ClientAfter.ClientID = "" }},
		{"key_missing_server", func(in *dao.CommitLifecycleInput) { in.Reservation.OldKey.ServerID = "" }},
		{"key_missing_protocol", func(in *dao.CommitLifecycleInput) { in.Reservation.OldKey.Protocol = "" }},
		{"key_zero_port", func(in *dao.CommitLifecycleInput) { in.Reservation.OldKey.RemotePort = 0 }},
		{"key_server_mismatch", func(in *dao.CommitLifecycleInput) { in.Reservation.OldKey.ServerID = "other-srv" }},
		{"different_old_new_key", func(in *dao.CommitLifecycleInput) { in.Reservation.NewKey.RemotePort = 8002 }},
		{"different_key_mode", func(in *dao.CommitLifecycleInput) {
			in.Reservation.Mode = dao.ReservationModeDifferentKey
			in.Reservation.NewKey.RemotePort = 8002
		}},
		{"same_key_owned_without_expectation", func(in *dao.CommitLifecycleInput) { in.Reservation.OldReservationExpected = false }},
		{"same_key_pending_with_expectation", func(in *dao.CommitLifecycleInput) {
			in.Reservation.Mode = dao.ReservationModeSameKeyPending
			in.Reservation.OldReservationExpected = true
		}},
		{"delete_with_proxy_snapshot", func(in *dao.CommitLifecycleInput) {
			in.Op = dao.LifecycleOpDelete
			in.ProxyAfterSnapshot = &dao.SnapshotInput{Plaintext: []byte("x"), Key: testSnapshotKey()}
		}},
		{"start_without_proxy_snapshot", func(in *dao.CommitLifecycleInput) { in.ProxyAfterSnapshot = nil }},
		{"empty_client_snapshot_key", func(in *dao.CommitLifecycleInput) { in.ClientAfterSnapshot.Key = nil }},
		{"empty_client_snapshot_plaintext", func(in *dao.CommitLifecycleInput) { in.ClientAfterSnapshot.Plaintext = nil }},
		{"empty_proxy_snapshot_key", func(in *dao.CommitLifecycleInput) { in.ProxyAfterSnapshot.Key = nil }},
		{"nil_audit", func(in *dao.CommitLifecycleInput) { in.AuditEvent = nil }},
		{"evidence_not_json", func(in *dao.CommitLifecycleInput) { in.EvidenceJSON = `{"x":` }},
		{"evidence_too_large", func(in *dao.CommitLifecycleInput) {
			in.EvidenceJSON = `{"pad":"` + strings.Repeat("a", 70*1024) + `"}`
		}},
		{"evidence_contains_secret", func(in *dao.CommitLifecycleInput) {
			in.EvidenceJSON = `{"leak":"child-secret"}`
		}},
		{"evidence_contains_expected_before_config", func(in *dao.CommitLifecycleInput) {
			in.EvidenceJSON = string(in.ExpectedClientConfigContent)
		}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			ctx2, db2 := newCommitTestEnv(t)
			in2, f2 := seedLifecycleWorld(t, ctx2, db2, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
			_ = key
			_ = base
			tc.mutate(in2)
			before := captureUpdateCounts(t, db2, f2.txID)

			_, err := dao.NewMutation(ctx2).CommitLifecycle(testUserInfo(), in2)
			require.Error(t, err)
			require.True(t, errors.Is(err, defs.ErrInvalidCommitInput), "want ErrInvalidCommitInput got %v", err)
			require.Equal(t, before, captureUpdateCounts(t, db2, f2.txID), "S0 must be zero DB writes")
			tx := loadTxRow(t, db2, f2.txID)
			require.Equal(t, models.TxStatusVerifying, tx.Status)
		})
	}
}

// TestCommitLifecycleRejectsAuditSummaryPlaintextZeroDBTouch: the audit
// summary must never embed client/proxy after-snapshot plaintext.
func TestCommitLifecycleRejectsAuditSummaryPlaintextZeroDBTouch(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
	in.AuditEvent.Summary = string(in.ClientAfterSnapshot.Plaintext)
	before := captureUpdateCounts(t, db, f.txID)

	_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.Error(t, err)
	require.True(t, errors.Is(err, defs.ErrInvalidCommitInput))
	require.Equal(t, before, captureUpdateCounts(t, db, f.txID))
	tx := loadTxRow(t, db, f.txID)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
}

func TestCommitLifecycleRejectsAuditSummaryBeforeConfigOrSecret(t *testing.T) {
	for _, tc := range []struct {
		name    string
		summary func(*dao.CommitLifecycleInput) string
	}{
		{"before_config", func(in *dao.CommitLifecycleInput) string { return string(in.ExpectedClientConfigContent) }},
		{"connect_secret", func(in *dao.CommitLifecycleInput) string { return in.ClientAfter.ConnectSecret }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db := newCommitTestEnv(t)
			in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStart, dao.ReservationModeSameKeyOwned, true)
			in.AuditEvent.Summary = tc.summary(in)
			before := captureUpdateCounts(t, db, f.txID)

			_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
			require.ErrorIs(t, err, defs.ErrInvalidCommitInput)
			require.Equal(t, before, captureUpdateCounts(t, db, f.txID))
			require.Equal(t, models.TxStatusVerifying, loadTxRow(t, db, f.txID).Status)
		})
	}
}

// TestCommitLifecycleForcesAuditIdentity: a poisoned AuditEvent input
// (wrong TxID/tenant/actor/EventType and a config-like NodeID) must be
// fully overridden by the DAO.
func TestCommitLifecycleForcesAuditIdentity(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	in, f := seedLifecycleWorld(t, ctx, db, dao.LifecycleOpStop, dao.ReservationModeSameKeyOwned, false)
	in.AuditEvent = &models.AuditEvent{
		TransactionID: "wrong-tx", TenantID: 99, ActorUserID: 99,
		EventType: "bogus", Summary: "stopped web", Success: true, NodeID: f.childID,
	}

	_, err := dao.NewMutation(ctx).CommitLifecycle(testUserInfo(), in)
	require.NoError(t, err)

	audits := loadLifecycleAudits(t, db, f.txID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeStop, audits[0].EventType)
	require.Equal(t, f.txID, audits[0].TransactionID)
	require.Equal(t, testUserInfo().GetTenantID(), audits[0].TenantID)
	require.Equal(t, testUserInfo().GetUserID(), audits[0].ActorUserID)
}
