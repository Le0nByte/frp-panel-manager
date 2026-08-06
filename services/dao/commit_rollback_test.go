package dao_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// commitRollbackEnv is the D2c-C CommitRollback test environment: ALL seven
// tables the commit touches (clients/proxy_config included) over a unique
// in-memory SQLite DB.
type commitRollbackEnv struct {
	t    *testing.T
	ctx  *app.Context
	db   *gorm.DB
	user models.UserInfo
}

// setupFullTestApp builds an app.Context with every table the rollback
// commit touches migrated (the shared setupTestApp only migrates four).
func setupFullTestApp(t *testing.T) *app.Context {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueMemoryDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.Client{},
		&models.Server{},
		&models.ProxyConfig{},
		&models.PortReservation{},
		&models.ChangeTransaction{},
		&models.ConfigSnapshot{},
		&models.AuditEvent{},
	))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst)
}

func newCommitRollbackEnv(t *testing.T) *commitRollbackEnv {
	t.Helper()
	ctx := setupFullTestApp(t)
	return &commitRollbackEnv{t: t, ctx: ctx, db: dbAll(ctx), user: testUserInfo()}
}

// rbTxSeed describes one rollback-commit fixture: the rollback transaction
// (verifying), the committed source transaction and the world state the
// commit is expected to CAS against.
type rbTxSeed struct {
	rollbackTxID string
	sourceTxID   string
	sourceOp     string
	proxyID      uint // the rollback tx's proxy_id (the current rule row id)
}

// seedRollbackTxWorld writes the rollback tx + source tx rows.
func (e *commitRollbackEnv) seedRollbackTxWorld(s rbTxSeed) {
	t := e.t
	t.Helper()
	now := time.Now()
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: s.rollbackTxID, IdempotencyKey: "idem-rb-" + s.rollbackTxID, RequestHash: "h",
		Operation: models.TxOperationRollback, Status: models.TxStatusVerifying, ProxyID: &s.proxyID,
		ActorUserID: e.user.GetUserID(), TenantID: e.user.GetTenantID(), StartedAt: now,
	}).Error)
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: s.sourceTxID, IdempotencyKey: "idem-src-" + s.sourceTxID, RequestHash: "h",
		Operation: s.sourceOp, Status: models.TxStatusCommitted, ProxyID: &s.proxyID,
		ActorUserID: e.user.GetUserID(), TenantID: e.user.GetTenantID(), StartedAt: now,
	}).Error)
}

// seedClient writes a client row with the given config bytes.
func (e *commitRollbackEnv) seedClient(id, serverID, originID string, config []byte) {
	t := e.t
	t.Helper()
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.user, &models.ClientEntity{
		ClientID: id, OriginClientID: originID, ServerID: serverID,
		ConnectSecret: "secret", ConfigContent: config,
	}))
}

// seedProxyRow inserts a proxy_config row directly (exact id + fields).
func (e *commitRollbackEnv) seedProxyRow(id uint, clientID, originID, serverID, name, typ string, stopped bool, content []byte) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ProxyConfig{
		Model: &gorm.Model{ID: id},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: serverID, ClientID: clientID, Name: name, Type: typ,
			OriginClientID: originID, Content: content, Stopped: stopped,
			TenantID: e.user.GetTenantID(), UserID: e.user.GetUserID(),
		},
	}).Error)
}

// seedCommittedReservation inserts a committed lease at the key owned by the
// given proxy row id, stamped with the given (historical) transaction id.
func (e *commitRollbackEnv) seedCommittedReservation(key dao.ReservationKey, ownerID uint, txID string) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.PortReservation{
		TenantID: e.user.GetTenantID(), ServerID: key.ServerID, Protocol: key.Protocol, RemotePort: key.RemotePort,
		TransactionID: txID, Status: models.ReservationStatusCommitted, OwnerProxyID: &ownerID,
	}).Error)
}

func (e *commitRollbackEnv) reservationAt(key dao.ReservationKey) *models.PortReservation {
	t := e.t
	t.Helper()
	var row models.PortReservation
	err := e.db.Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ?",
		e.user.GetTenantID(), key.ServerID, key.Protocol, key.RemotePort).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	return &row
}

func (e *commitRollbackEnv) proxyRowByID(id uint) *models.ProxyConfig {
	t := e.t
	t.Helper()
	var row models.ProxyConfig
	err := e.db.Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	return &row
}

func (e *commitRollbackEnv) clientByID(id string) *models.ClientEntity {
	t := e.t
	t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByClientID(e.user, id)
	require.NoError(t, err)
	return cli.ClientEntity
}

func (e *commitRollbackEnv) txByID(id string) *models.ChangeTransaction {
	t := e.t
	t.Helper()
	tx, err := dao.NewQuery(e.ctx).FindByID(e.user, id)
	require.NoError(t, err)
	return tx
}

func (e *commitRollbackEnv) snapshotsOf(txID string) []*models.ConfigSnapshot {
	t := e.t
	t.Helper()
	list, err := dao.NewQuery(e.ctx).FindByTransaction(e.user, txID)
	require.NoError(t, err)
	return list
}

func (e *commitRollbackEnv) auditEventsOf(txID string) []*models.AuditEvent {
	t := e.t
	t.Helper()
	var list []*models.AuditEvent
	require.NoError(t, e.db.Where("transaction_id = ?", txID).Find(&list).Error)
	return list
}

// rollbackInputBase returns the common input fields every commit-rollback
// test tweaks.
func (e *commitRollbackEnv) rollbackInputBase(s rbTxSeed) *dao.CommitRollbackInput {
	return &dao.CommitRollbackInput{
		TxID: s.rollbackTxID, SourceTxID: s.sourceTxID, SourceOp: s.sourceOp, TxProxyID: s.proxyID,
		ClientAfterSnapshot: dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"proxies":[]}`)},
		AuditEvent: &models.AuditEvent{
			TransactionID: "wrong", EventType: "bogus",
			Summary: "rolled back proxy web", Success: true,
		},
		EvidenceJSON: `{"stage":"rollback-done"}`,
	}
}

// ---------------------------------------------------------------------------
// create rollback: delete the current row + release the committed lease by
// exact owner.
// ---------------------------------------------------------------------------

func TestCommitRollbackCreateDeletesRowAndReleasesLease(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID   = "cli-child"
		srcTx     = "src-create"
		rbTx      = "rb-create"
		rowID     = uint(11)
		srvID     = "srv-1"
		histTxID  = "orig-create-tx" // the ORIGINAL create tx that owns the lease
	)
	beforePlain := []byte(`{"proxies":[]}`)
	afterPlain := []byte(`{"proxies":[{"name":"web"}]}`)
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8000}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	env.seedClient(childID, srvID, "", afterPlain)
	env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8000}`))
	env.seedCommittedReservation(key, rowID, histTxID)

	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = afterPlain
	in.DesiredClientConfig = beforePlain
	in.CurrentProxy = env.proxyRowByID(rowID)
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationRelease, Key: key, CurrentProxyID: rowID,
	}
	in.ClientAfterSnapshot = dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: beforePlain}

	got, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	// the row is gone, the client config is restored, the lease is released
	// BY OWNER (never by the rollback tx id — the historical tx id is gone
	// with the row).
	require.Nil(t, env.proxyRowByID(rowID), "create rollback must delete the current proxy row")
	require.Equal(t, beforePlain, env.clientByID(childID).ConfigContent)
	require.Nil(t, env.reservationAt(key), "create rollback must release the committed lease")
	require.Equal(t, rowID, got)

	// source tx untouched; rollback tx committed
	src := env.txByID(srcTx)
	require.Equal(t, models.TxStatusCommitted, src.Status)
	rb := env.txByID(rbTx)
	require.Equal(t, models.TxStatusCommitted, rb.Status)

	// after snapshot on the rollback tx (client only — no desired row)
	snaps := env.snapshotsOf(rbTx)
	require.Len(t, snaps, 1)
	require.Equal(t, models.SnapshotSideAfter, snaps[0].Side)
	require.Equal(t, models.SnapshotNodeTypeClient, snaps[0].NodeType)
	require.Equal(t, childID, snaps[0].NodeID)

	// forced rollback audit
	events := env.auditEventsOf(rbTx)
	require.Len(t, events, 1)
	require.Equal(t, models.AuditEventTypeRollback, events[0].EventType)
	require.Equal(t, env.user.GetTenantID(), events[0].TenantID)
	require.Equal(t, env.user.GetUserID(), events[0].ActorUserID)
	// evidence landed on the rollback tx inside the commit
	require.Equal(t, `{"stage":"rollback-done"}`, rb.EvidenceJSON)
}

// ---------------------------------------------------------------------------
// delete rollback: recreate the EXACT before row (id/content/stopped) and a
// committed lease owned by the restored row (stamped with the source tx).
// ---------------------------------------------------------------------------

func TestCommitRollbackDeleteRestoresExactRowAndLease(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID  = "cli-child"
		srcTx    = "src-delete"
		rbTx     = "rb-delete"
		rowID    = uint(22)
		srvID    = "srv-1"
	)
	beforePlain := []byte(`{"proxies":[{"name":"web"}]}`)
	afterPlain := []byte(`{"proxies":[]}`)
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8001}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationDelete, proxyID: rowID})
	env.seedClient(childID, srvID, "", afterPlain)
	// current proxy ABSENT; no lease at the key

	desired := &models.ProxyConfig{
		Model: &gorm.Model{ID: rowID},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: srvID, ClientID: childID, Name: "web", Type: "tcp",
			Content: []byte(`{"name":"web","type":"tcp","remotePort":8001}`), Stopped: true,
		},
	}
	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationDelete, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = afterPlain
	in.DesiredClientConfig = beforePlain
	in.CurrentProxy = nil
	in.DesiredProxy = desired
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationRestore, Key: key, DesiredProxyID: rowID, SourceTxID: srcTx,
	}
	in.ClientAfterSnapshot = dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: beforePlain}
	in.ProxyAfterSnapshot = &dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"version":1,"name":"web"}`)}

	got, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	row := env.proxyRowByID(rowID)
	require.NotNil(t, row, "delete rollback must recreate the exact row")
	require.Equal(t, rowID, row.ID)
	require.Equal(t, "web", row.Name)
	require.Equal(t, true, row.Stopped)
	require.Equal(t, `{"name":"web","type":"tcp","remotePort":8001}`, string(row.Content))

	res := env.reservationAt(key)
	require.NotNil(t, res, "delete rollback must recreate the committed lease")
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, rowID, *res.OwnerProxyID)
	// the lease is stamped with the SOURCE tx id, never the rollback tx —
	// compensation by rollback txID can never delete it.
	require.Equal(t, srcTx, res.TransactionID)
	require.Equal(t, rowID, got)

	// proxy after snapshot written (node = restored row id)
	snaps := env.snapshotsOf(rbTx)
	require.Len(t, snaps, 2)
	var proxySnap *models.ConfigSnapshot
	for _, s := range snaps {
		if s.NodeType == models.SnapshotNodeTypeProxy {
			proxySnap = s
		}
	}
	require.NotNil(t, proxySnap)
	require.Equal(t, "22", proxySnap.NodeID)
}

// ---------------------------------------------------------------------------
// update rollback, same key, DIFFERENT row id: delete the after row, insert
// the before row with the before id, CAS the lease owner old→new without
// rewriting transaction_id.
// ---------------------------------------------------------------------------

func TestCommitRollbackUpdateSameKeySwapsOwnerKeepsTxID(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-update"
		rbTx    = "rb-update"
		srvID   = "srv-1"
	)
	afterRowID, beforeRowID := uint(31), uint(32)
	histTxID := "orig-create-tx"
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8002}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: afterRowID})
	env.seedClient(childID, srvID, "cli-origin", []byte(`{"proxies":[{"name":"web","remotePort":8002}]}`))
	env.seedProxyRow(afterRowID, childID, "cli-origin", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8002}`))
	env.seedCommittedReservation(key, afterRowID, histTxID)

	desired := &models.ProxyConfig{
		Model: &gorm.Model{ID: beforeRowID},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: srvID, ClientID: childID, Name: "web", Type: "tcp",
			OriginClientID: "cli-origin", Content: []byte(`{"name":"web","type":"tcp","remotePort":8002}`), Stopped: false,
		},
	}
	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: afterRowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"proxies":[{"name":"web","remotePort":8002}]}`)
	in.DesiredClientConfig = []byte(`{"proxies":[{"name":"web","remotePort":8002}]}`)
	in.CurrentProxy = env.proxyRowByID(afterRowID)
	in.DesiredProxy = desired
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationOwnerCAS, Key: key, CurrentProxyID: afterRowID, DesiredProxyID: beforeRowID,
	}
	in.ProxyAfterSnapshot = &dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"version":1,"name":"web"}`)}

	_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	require.Nil(t, env.proxyRowByID(afterRowID), "after row must be deleted when the ids differ")
	before := env.proxyRowByID(beforeRowID)
	require.NotNil(t, before)
	require.Equal(t, "web", before.Name)

	res := env.reservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, beforeRowID, *res.OwnerProxyID, "owner must CAS to the restored row id")
	require.Equal(t, histTxID, res.TransactionID, "transaction_id must NEVER be rewritten")
}

// ---------------------------------------------------------------------------
// update rollback, same key, SAME row id: in-place update, lease untouched.
// ---------------------------------------------------------------------------

func TestCommitRollbackUpdateSameIDInPlaceKeepsLease(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-update2"
		rbTx    = "rb-update2"
		rowID   = uint(41)
		srvID   = "srv-1"
	)
	histTxID := "orig-create-tx"
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8003}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: rowID})
	env.seedClient(childID, srvID, "cli-origin", []byte(`{"proxies":[{"name":"web2","remotePort":8003}]}`))
	env.seedProxyRow(rowID, childID, "cli-origin", srvID, "web2", "tcp", false, []byte(`{"name":"web2","type":"tcp","remotePort":8003}`))
	env.seedCommittedReservation(key, rowID, histTxID)

	desired := &models.ProxyConfig{
		Model: &gorm.Model{ID: rowID},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: srvID, ClientID: childID, Name: "web", Type: "tcp",
			OriginClientID: "cli-origin", Content: []byte(`{"name":"web","type":"tcp","remotePort":8003}`), Stopped: false,
		},
	}
	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"proxies":[{"name":"web2","remotePort":8003}]}`)
	in.DesiredClientConfig = []byte(`{"proxies":[{"name":"web","remotePort":8003}]}`)
	in.CurrentProxy = env.proxyRowByID(rowID)
	in.DesiredProxy = desired
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationOwnerCAS, Key: key, CurrentProxyID: rowID, DesiredProxyID: rowID,
	}
	in.ProxyAfterSnapshot = &dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"version":1,"name":"web"}`)}

	_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	row := env.proxyRowByID(rowID)
	require.NotNil(t, row)
	require.Equal(t, "web", row.Name, "in-place rename back to the before name")
	require.Equal(t, `{"name":"web","type":"tcp","remotePort":8003}`, string(row.Content))

	res := env.reservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, rowID, *res.OwnerProxyID)
	require.Equal(t, histTxID, res.TransactionID)
}

// ---------------------------------------------------------------------------
// update rollback, DIFFERENT key: release the new-key lease (owner = after
// row), restore the old-key lease (owner = before row).
// ---------------------------------------------------------------------------

func TestCommitRollbackUpdateKeySwitchMigratesLease(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-update3"
		rbTx    = "rb-update3"
		srvID   = "srv-1"
	)
	afterRowID, beforeRowID := uint(51), uint(52)
	afterKey := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8004}
	beforeKey := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8005}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: afterRowID})
	env.seedClient(childID, srvID, "cli-origin", []byte(`{"proxies":[{"name":"web","remotePort":8004}]}`))
	env.seedProxyRow(afterRowID, childID, "cli-origin", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8004}`))
	env.seedCommittedReservation(afterKey, afterRowID, srcTx)

	desired := &models.ProxyConfig{
		Model: &gorm.Model{ID: beforeRowID},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: srvID, ClientID: childID, Name: "web", Type: "tcp",
			OriginClientID: "cli-origin", Content: []byte(`{"name":"web","type":"tcp","remotePort":8005}`), Stopped: false,
		},
	}
	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationUpdate, proxyID: afterRowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"proxies":[{"name":"web","remotePort":8004}]}`)
	in.DesiredClientConfig = []byte(`{"proxies":[{"name":"web","remotePort":8005}]}`)
	in.CurrentProxy = env.proxyRowByID(afterRowID)
	in.DesiredProxy = desired
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationKeySwitch,
		ReleaseKey: afterKey, CurrentProxyID: afterRowID,
		RestoreKey: beforeKey, DesiredProxyID: beforeRowID, SourceTxID: srcTx,
	}
	in.ProxyAfterSnapshot = &dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"version":1,"name":"web"}`)}

	_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	require.Nil(t, env.reservationAt(afterKey), "new-key lease must be released")
	restored := env.reservationAt(beforeKey)
	require.NotNil(t, restored)
	require.Equal(t, beforeRowID, *restored.OwnerProxyID)
	require.Equal(t, srcTx, restored.TransactionID)
	require.Nil(t, env.proxyRowByID(afterRowID))
	require.NotNil(t, env.proxyRowByID(beforeRowID))
}

// ---------------------------------------------------------------------------
// start/stop rollback: in-place stopped flip, SameKey lease shape-check.
// ---------------------------------------------------------------------------

func TestCommitRollbackStartFlippedStoppedKeepsLease(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-start"
		rbTx    = "rb-start"
		rowID   = uint(61)
		srvID   = "srv-1"
	)
	histTxID := "orig-create-tx"
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8006}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationStart, proxyID: rowID})
	env.seedClient(childID, srvID, "cli-origin", []byte(`{"proxies":[]}`))
	env.seedProxyRow(rowID, childID, "cli-origin", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8006}`))
	env.seedCommittedReservation(key, rowID, histTxID)

	desired := &models.ProxyConfig{
		Model: &gorm.Model{ID: rowID},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: srvID, ClientID: childID, Name: "web", Type: "tcp",
			OriginClientID: "cli-origin", Content: []byte(`{"name":"web","type":"tcp","remotePort":8006}`), Stopped: true,
		},
	}
	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationStart, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"proxies":[]}`)
	in.DesiredClientConfig = []byte(`{"proxies":[]}`)
	in.CurrentProxy = env.proxyRowByID(rowID)
	in.DesiredProxy = desired
	in.Reservation = dao.RollbackReservationStep{
		Mode: dao.RollbackReservationSameKey, Key: key, CurrentProxyID: rowID,
	}
	in.ProxyAfterSnapshot = &dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"version":1,"name":"web","stopped":true}`)}

	_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.NoError(t, err)

	row := env.proxyRowByID(rowID)
	require.NotNil(t, row)
	require.True(t, row.Stopped, "start rollback must flip stopped back to true")
	res := env.reservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, rowID, *res.OwnerProxyID)
	require.Equal(t, histTxID, res.TransactionID, "SameKey lease owner/transaction must never change")
}

// ---------------------------------------------------------------------------
// guards: rollback tx state, source tx state, client CAS drift, proxy drift,
// reservation owner mismatch — all fail closed with the whole tx rolled back.
// ---------------------------------------------------------------------------

func TestCommitRollbackGuardRejections(t *testing.T) {
	newSeed := func() (rbTxSeed, *dao.CommitRollbackInput, *commitRollbackEnv) {
		env := newCommitRollbackEnv(t)
		const (
			childID = "cli-child"
			srcTx   = "src-g"
			rbTx    = "rb-g"
			rowID   = uint(71)
			srvID   = "srv-1"
		)
		key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8007}
		env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
		env.seedClient(childID, srvID, "", []byte(`{"after":true}`))
		env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8007}`))
		env.seedCommittedReservation(key, rowID, srcTx)
		seed := rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID}
		in := env.rollbackInputBase(seed)
		in.ClientNodeID = childID
		in.ExpectedClientConfig = []byte(`{"after":true}`)
		in.DesiredClientConfig = []byte(`{"before":true}`)
		in.CurrentProxy = env.proxyRowByID(rowID)
		in.Reservation = dao.RollbackReservationStep{Mode: dao.RollbackReservationRelease, Key: key, CurrentProxyID: rowID}
		in.ClientAfterSnapshot = dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"before":true}`)}
		return seed, in, env
	}
	zeroDelta := func(env *commitRollbackEnv, rbTx string, rowID uint, key dao.ReservationKey) {
		require.NotNil(t, env.proxyRowByID(rowID), "the current row must survive a failed commit")
		require.NotNil(t, env.reservationAt(key), "the lease must survive a failed commit")
		require.Equal(t, models.TxStatusVerifying, env.txByID(rbTx).Status, "the rollback tx must stay verifying")
		require.Empty(t, env.snapshotsOf(rbTx), "no after snapshots on a failed commit")
		require.Empty(t, env.auditEventsOf(rbTx), "no audit on a failed commit")
	}

	t.Run("source tx missing", func(t *testing.T) {
		seed, in, env := newSeed()
		in.SourceTxID = "no-such-tx"
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.ErrorIs(t, err, defs.ErrTransactionNotFound)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("source op mismatch", func(t *testing.T) {
		seed, in, env := newSeed()
		in.SourceOp = models.TxOperationStop
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("client config drift", func(t *testing.T) {
		seed, in, env := newSeed()
		in.ExpectedClientConfig = []byte(`{"drifted":true}`)
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("proxy shape drift", func(t *testing.T) {
		seed, in, env := newSeed()
		in.CurrentProxy.Content = []byte(`{"name":"other","type":"tcp","remotePort":9999}`)
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("reservation owner mismatch", func(t *testing.T) {
		seed, in, env := newSeed()
		in.Reservation.CurrentProxyID = 999
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("source non-committed", func(t *testing.T) {
		seed, in, env := newSeed()
		// flip the source tx to verifying (illegal source state)
		require.NoError(t, env.db.Model(&models.ChangeTransaction{}).
			Where("transaction_id = ?", seed.sourceTxID).Update("status", models.TxStatusVerifying).Error)
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		zeroDelta(env, seed.rollbackTxID, seed.proxyID, in.Reservation.Key)
	})
	t.Run("rollback tx wrong status", func(t *testing.T) {
		seed, in, env := newSeed()
		require.NoError(t, env.db.Model(&models.ChangeTransaction{}).
			Where("transaction_id = ?", seed.rollbackTxID).Update("status", models.TxStatusApplying).Error)
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.Error(t, err)
		require.Equal(t, models.TxStatusApplying, env.txByID(seed.rollbackTxID).Status)
	})
}

// ---------------------------------------------------------------------------
// Atomicity: the after-snapshot ENCRYPTION failure lands INSIDE the
// transaction (after the client/proxy/lease writes) — the whole commit must
// roll back to the byte-identical pre-commit state.
// ---------------------------------------------------------------------------

func TestCommitRollbackAtomicRollbackOnSnapshotEncryptFailure(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-atomic"
		rbTx    = "rb-atomic"
		rowID   = uint(81)
		srvID   = "srv-1"
	)
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8008}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	env.seedClient(childID, srvID, "", []byte(`{"after":true}`))
	env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8008}`))
	env.seedCommittedReservation(key, rowID, srcTx)

	before := env.clientByID(childID).ConfigContent
	beforeRes := *env.reservationAt(key)

	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"after":true}`)
	in.DesiredClientConfig = []byte(`{"before":true}`)
	in.CurrentProxy = env.proxyRowByID(rowID)
	in.Reservation = dao.RollbackReservationStep{Mode: dao.RollbackReservationRelease, Key: key, CurrentProxyID: rowID}
	// S0 validates SHAPE only (non-empty key); the AES-256 SIZE check fails
	// at the snapshot step INSIDE the transaction — after the client/proxy/
	// lease writes already happened.
	in.ClientAfterSnapshot = dao.SnapshotInput{Key: []byte{1}, Plaintext: []byte(`{"before":true}`)}

	_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
	require.ErrorIs(t, err, snapshot.ErrInvalidKeySize)

	require.Equal(t, before, env.clientByID(childID).ConfigContent, "client write must roll back")
	require.NotNil(t, env.proxyRowByID(rowID), "proxy delete must roll back")
	after := env.reservationAt(key)
	require.NotNil(t, after)
	require.Equal(t, beforeRes, *after, "lease release must roll back")
	require.Equal(t, models.TxStatusVerifying, env.txByID(rbTx).Status)
	require.Empty(t, env.snapshotsOf(rbTx))
	require.Empty(t, env.auditEventsOf(rbTx))
}

// ---------------------------------------------------------------------------
// tenant/actor scoping: a different actor cannot commit the rollback tx.
// ---------------------------------------------------------------------------

func TestCommitRollbackCrossTenantScoped(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-x"
		rbTx    = "rb-x"
		rowID   = uint(91)
		srvID   = "srv-1"
	)
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8009}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	env.seedClient(childID, srvID, "", []byte(`{"after":true}`))
	env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8009}`))
	env.seedCommittedReservation(key, rowID, srcTx)

	in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	in.ClientNodeID = childID
	in.ExpectedClientConfig = []byte(`{"after":true}`)
	in.DesiredClientConfig = []byte(`{"before":true}`)
	in.CurrentProxy = env.proxyRowByID(rowID)
	in.Reservation = dao.RollbackReservationStep{Mode: dao.RollbackReservationRelease, Key: key, CurrentProxyID: rowID}
	in.ClientAfterSnapshot = dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"before":true}`)}

	other := &models.UserEntity{UserID: 900, UserName: "other", Email: "o@example.com", TenantID: 2}
	_, err := dao.NewMutation(env.ctx).CommitRollback(other, in)
	require.ErrorIs(t, err, defs.ErrTransactionNotFound)

	require.Equal(t, models.TxStatusVerifying, env.txByID(rbTx).Status)
	require.NotNil(t, env.proxyRowByID(rowID))
	require.NotNil(t, env.reservationAt(key))
}

// ---------------------------------------------------------------------------
// S0 static validation: audit summary/evidence must never embed plaintext.
// ---------------------------------------------------------------------------

func TestCommitRollbackRejectsPlaintextAuditAndEvidence(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		srcTx   = "src-s0"
		rbTx    = "rb-s0"
		rowID   = uint(101)
		srvID   = "srv-1"
	)
	key := dao.ReservationKey{ServerID: srvID, Protocol: "tcp", RemotePort: 8010}
	env.seedRollbackTxWorld(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
	env.seedClient(childID, srvID, "", []byte(`{"after":true}`))
	env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8010}`))
	env.seedCommittedReservation(key, rowID, srcTx)

	build := func() *dao.CommitRollbackInput {
		in := env.rollbackInputBase(rbTxSeed{rollbackTxID: rbTx, sourceTxID: srcTx, sourceOp: models.TxOperationCreate, proxyID: rowID})
		in.ClientNodeID = childID
		in.ExpectedClientConfig = []byte(`{"after":true}`)
		in.DesiredClientConfig = []byte(`{"before":true}`)
		in.CurrentProxy = env.proxyRowByID(rowID)
		in.Reservation = dao.RollbackReservationStep{Mode: dao.RollbackReservationRelease, Key: key, CurrentProxyID: rowID}
		in.ClientAfterSnapshot = dao.SnapshotInput{Key: testSnapshotKey(), Plaintext: []byte(`{"before":true}`)}
		return in
	}

	t.Run("audit summary embeds the plaintext", func(t *testing.T) {
		in := build()
		in.AuditEvent.Summary = "restored config " + string(in.DesiredClientConfig)
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.ErrorIs(t, err, defs.ErrInvalidCommitInput)
	})
	t.Run("evidence embeds the plaintext", func(t *testing.T) {
		in := build()
		// a valid-JSON evidence value that embeds the RAW plaintext bytes
		// as a nested object (string-escaped quotes would not contain the
		// raw substring)
		in.EvidenceJSON = `{"stash":` + string(in.DesiredClientConfig) + `}`
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.ErrorIs(t, err, defs.ErrInvalidCommitInput)
	})
	t.Run("evidence is not valid json", func(t *testing.T) {
		in := build()
		in.EvidenceJSON = "not-json"
		_, err := dao.NewMutation(env.ctx).CommitRollback(env.user, in)
		require.ErrorIs(t, err, defs.ErrInvalidCommitInput)
	})
}

// ---------------------------------------------------------------------------
// GetProxyConfigByID: tenant-scoped id lookup.
// ---------------------------------------------------------------------------

func TestGetProxyConfigByIDTenantScoped(t *testing.T) {
	env := newCommitRollbackEnv(t)
	const (
		childID = "cli-child"
		rowID   = uint(111)
		srvID   = "srv-1"
	)
	env.seedClient(childID, srvID, "", []byte(`{"proxies":[]}`))
	env.seedProxyRow(rowID, childID, "", srvID, "web", "tcp", false, []byte(`{"name":"web","type":"tcp","remotePort":8011}`))

	row, err := dao.NewQuery(env.ctx).GetProxyConfigByID(env.user, rowID)
	require.NoError(t, err)
	require.Equal(t, rowID, row.ID)
	require.Equal(t, "web", row.Name)

	_, err = dao.NewQuery(env.ctx).GetProxyConfigByID(env.user, 999999)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	other := &models.UserEntity{UserID: 900, UserName: "other", Email: "o@example.com", TenantID: 2}
	_, err = dao.NewQuery(env.ctx).GetProxyConfigByID(other, rowID)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "cross-tenant lookup must not leak the row")
}
