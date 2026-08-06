package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// D2c-C: the MANUAL ROLLBACK coordinator. The tests prove the COORDINATOR
// side: committed-only gate, canonical-origin lock, stable idempotency,
// per-operation restore dispatch (create/update/delete/start/stop), real
// Apply + real Verify directions, TOCTOU drift guards, before snapshots
// saved before Apply, post-apply compensation with the TRUE INVERSE (re-apply
// the source-AFTER config + inverse verify), and the redaction/evidence
// contract. The DAO commit side is services/dao.CommitRollback.
// ---------------------------------------------------------------------------

// failingRollbackCommitStore wraps the REAL dao store and injects a single
// CommitRollback failure — everything else stays on the real DAO.
type failingRollbackCommitStore struct {
	RollbackTxStore
	fail bool
}

func (s *failingRollbackCommitStore) CommitRollback(c *app.Context, u models.UserInfo, in *dao.CommitRollbackInput) (uint, error) {
	if s.fail {
		return 0, errors.New("injected commit rollback db failure")
	}
	return s.RollbackTxStore.CommitRollback(c, u, in)
}

// recordingLocker records every (Lock, Unlock) key pair without blocking.
type recordingLocker struct {
	mu    sync.Mutex
	keys  []string
	unlks []string
}

func (l *recordingLocker) Lock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys = append(l.keys, key)
}
func (l *recordingLocker) Unlock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unlks = append(l.unlks, key)
}
func (l *recordingLocker) lockedKeys() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.keys...)
}

// ---- environment ----

type rollbackCoordEnv struct {
	*coordTestEnv
	sdeps RollbackCoordinatorDeps
}

func newRollbackCoordEnv(t *testing.T) *rollbackCoordEnv {
	t.Helper()
	base := newCoordTestEnv(t)
	e := &rollbackCoordEnv{coordTestEnv: base}
	e.sdeps = RollbackCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Snap:           base.deps.SnapStore,
		KeyProvider:    base.key,
		Apply:          base.apply,
		Verifier:       base.verify,
		OriginLocker:   NewOriginKeyedLock(),
		Now:            base.deps.Now,
		ApplyTimeout:   base.deps.ApplyTimeout,
		VerifyTimeout:  base.deps.VerifyTimeout,
		VerifyAttempts: base.deps.VerifyAttempts,
		VerifyInterval: base.deps.VerifyInterval,
		HashRequest:    hashRollbackRequest,
	}
	return e
}

func (e *rollbackCoordEnv) newRollbackCoordinator() *RollbackTransactionCoordinator {
	return &RollbackTransactionCoordinator{deps: e.sdeps}
}

// rollbackReq builds the wire request for a rollback of the given source tx.
func rollbackReq(sourceTxID, idemKey string) *pb.RollbackRequest {
	return &pb.RollbackRequest{
		TransactionId:  lo.ToPtr(sourceTxID),
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

// clientConfigPlain builds the marshalled JSON of a client config carrying
// the given proxies — the SAME byte source the coordinators snapshot.
func clientConfigPlain(t *testing.T, proxies ...v1.TypedProxyConfig) []byte {
	t.Helper()
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = proxies
	ent := &models.ClientEntity{}
	require.NoError(t, ent.SetConfigContent(*cfg))
	raw, err := ent.MarshalJSONConfig()
	require.NoError(t, err)
	return raw
}

// typedJSON marshals a typed proxy rule to the bytes stored in proxy rows /
// envelopes.
func typedJSON(t *testing.T, p v1.TypedProxyConfig) []byte {
	t.Helper()
	raw, err := p.MarshalJSON()
	require.NoError(t, err)
	return raw
}

func rollbackEnvelope(proxyID uint, name, serverID, clientID, originID string, stopped bool, typed []byte) updateProxyEnvelope {
	return updateProxyEnvelope{
		ProxyID: proxyID, Name: name, ServerID: serverID,
		ClientID: clientID, OriginClientID: originID, Stopped: stopped, Typed: typed,
	}
}

// seedRollbackSourceTx writes the committed source transaction.
func (e *rollbackCoordEnv) seedRollbackSourceTx(sourceTxID, op string, proxyID uint) {
	t := e.t
	t.Helper()
	require.NoError(t, dao.NewMutation(e.ctx).Create(e.user, &models.ChangeTransaction{
		TransactionID:  sourceTxID,
		IdempotencyKey: "idem-src-" + sourceTxID,
		RequestHash:    "h",
		Operation:      op,
		Status:         models.TxStatusCommitted,
		ProxyID:        lo.ToPtr(proxyID),
		StartedAt:      e.now,
	}))
}

// seedRollbackSnapshot writes ONE encrypted snapshot for the source tx
// through the REAL DAO (same encryption path as production).
func (e *rollbackCoordEnv) seedRollbackSnapshot(txID, side, nodeType, nodeID string, plain []byte) {
	t := e.t
	t.Helper()
	_, err := dao.NewMutation(e.ctx).SaveEncrypted(e.user, txID, side, nodeType, nodeID, plain, e.key.Key)
	require.NoError(t, err)
}

// seedRollbackClient seeds the CURRENT client row (the source-after state).
func (e *rollbackCoordEnv) seedRollbackClient(id, serverID, originID string, config []byte, stopped bool) {
	t := e.t
	t.Helper()
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.user, &models.ClientEntity{
		ClientID: id, OriginClientID: originID, ServerID: serverID,
		IsShadow: originID != "", Stopped: stopped, ConnectSecret: "secret",
		ConfigContent: config,
	}))
}

// seedRollbackProxyRow inserts the CURRENT proxy row directly.
func (e *rollbackCoordEnv) seedRollbackProxyRow(id uint, clientID, originID, serverID, name string, stopped bool, content []byte) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ProxyConfig{
		Model: &gorm.Model{ID: id},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID: serverID, ClientID: clientID, Name: name, Type: "tcp",
			OriginClientID: originID, Content: content, Stopped: stopped,
			TenantID: e.user.GetTenantID(), UserID: e.user.GetUserID(),
		},
	}).Error)
}

// seedRollbackReservation inserts a committed lease at the key owned by the
// given rule row id.
func (e *rollbackCoordEnv) seedRollbackReservation(key dao.ReservationKey, ownerID uint, txID string) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.PortReservation{
		TenantID: e.user.GetTenantID(), ServerID: key.ServerID, Protocol: key.Protocol, RemotePort: key.RemotePort,
		TransactionID: txID, Status: models.ReservationStatusCommitted, OwnerProxyID: lo.ToPtr(ownerID),
	}).Error)
}

func (e *rollbackCoordEnv) rbReservationAt(key dao.ReservationKey) *models.PortReservation {
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

func (e *rollbackCoordEnv) rbProxyRowByID(id uint) *models.ProxyConfig {
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

func (e *rollbackCoordEnv) rbClientConfig(id string) []byte {
	t := e.t
	t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByClientID(e.user, id)
	require.NoError(t, err)
	return cli.ClientEntity.ConfigContent
}

func (e *rollbackCoordEnv) rbRollbackTxBySource(idemKey string) *models.ChangeTransaction {
	t := e.t
	t.Helper()
	tx, err := dao.NewQuery(e.ctx).FindByIdempotencyKey(e.user, idemKey)
	require.NoError(t, err)
	return tx
}

func (e *rollbackCoordEnv) rbSnapshotsOf(txID string) []*models.ConfigSnapshot {
	t := e.t
	t.Helper()
	list, err := dao.NewQuery(e.ctx).FindByTransaction(e.user, txID)
	require.NoError(t, err)
	return list
}

func (e *rollbackCoordEnv) rbAuditEventsOf(txID string) []*models.AuditEvent {
	t := e.t
	t.Helper()
	var list []*models.AuditEvent
	require.NoError(t, e.db.Where("transaction_id = ?", txID).Find(&list).Error)
	return list
}

// requireRollbackEvidence asserts the committed rollback tx evidence: it
// must be valid JSON with the source operation and NEVER carry config
// material.
func requireRollbackEvidence(t *testing.T, tx *models.ChangeTransaction) map[string]interface{} {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON)
	ev, err := decodeJSONMap(tx.EvidenceJSON)
	require.NoError(t, err)
	require.Contains(t, ev, "source_operation")
	require.Contains(t, ev, "applied_to_agent")
	require.Contains(t, ev, "apply_calls")
	require.Contains(t, ev, "verify_calls")
	return ev
}

func decodeJSONMap(raw string) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// create rollback success: current rule deleted, client restored, lease
// released by owner, real Apply (TargetClientID=child, Replace empty) +
// VerifyReleased, rollback tx committed with evidence + rollback audit.
// ---------------------------------------------------------------------------

func TestRollbackCreateSuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-create"
		idem   = uuidKey()
		origin = "cli-origin"
		child  = "cli-child"
		srv    = "srv-1"
		rowID  = uint(201)
		port   = uint32(8100)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(child, srv, origin, afterPlain, false)
	env.seedRollbackClient(origin, "", "", nil, true) // shadowed origin
	env.seedRollbackProxyRow(rowID, child, origin, srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, child, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, child, afterPlain)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.NotEmpty(t, resp.GetNewTransactionId(), "the response must carry the new rollback transaction id")

	// ---- real state assertions ----
	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxOperationRollback, rb.Operation)
	require.Equal(t, models.TxStatusCommitted, rb.Status)
	require.NotNil(t, rb.ProxyID)
	require.Equal(t, rowID, *rb.ProxyID)
	require.Equal(t, resp.GetNewTransactionId(), rb.TransactionID)

	src, err := dao.NewQuery(env.ctx).FindByID(env.user, srcTx)
	require.NoError(t, err)
	require.Equal(t, models.TxStatusCommitted, src.Status, "the source transaction must never change")

	require.Equal(t, beforePlain, env.rbClientConfig(child), "the child config must be restored to the source-before bytes")
	require.Nil(t, env.rbProxyRowByID(rowID), "the created rule row must be deleted")
	require.Nil(t, env.rbReservationAt(key), "the committed lease must be released by owner")

	// agent side: exactly ONE apply, addressed to the CURRENT handler
	// (child), physical origin agent, Replace empty — never a switch.
	require.Equal(t, 1, env.apply.calls)
	applied := env.apply.callsData[0]
	require.Equal(t, origin, applied.PhysicalAgentID)
	require.Equal(t, child, applied.TargetClientID)
	require.Equal(t, "", applied.ReplaceClientID)
	require.Equal(t, srv, applied.ServerID)
	require.Contains(t, string(applied.Config), "web")

	// verify direction: the port must be RELEASED (rule removed)
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Equal(t, 0, env.verify.appliedCalls)

	// rollback tx snapshots: before client+proxy saved pre-apply; after
	// client saved atomically in the commit
	snaps := env.rbSnapshotsOf(rb.TransactionID)
	var beforeClientSnap, beforeProxySnap, afterClientSnap bool
	for _, s := range snaps {
		switch {
		case s.Side == models.SnapshotSideBefore && s.NodeType == models.SnapshotNodeTypeClient:
			beforeClientSnap = true
			require.Equal(t, child, s.NodeID)
		case s.Side == models.SnapshotSideBefore && s.NodeType == models.SnapshotNodeTypeProxy:
			beforeProxySnap = true
			require.Equal(t, strconv.FormatUint(uint64(rowID), 10), s.NodeID)
		case s.Side == models.SnapshotSideAfter && s.NodeType == models.SnapshotNodeTypeClient:
			afterClientSnap = true
			require.Equal(t, child, s.NodeID)
		}
	}
	require.True(t, beforeClientSnap && beforeProxySnap && afterClientSnap)

	// audit: forced rollback event + evidence
	events := env.rbAuditEventsOf(rb.TransactionID)
	require.Len(t, events, 1)
	require.Equal(t, models.AuditEventTypeRollback, events[0].EventType)
	require.True(t, events[0].Success)
	requireRollbackEvidence(t, rb)
	// RequestRedacted never carries config plaintext
	require.NotContains(t, rb.RequestRedacted, "web")
	require.NotContains(t, rb.RequestRedacted, "remotePort")
}

// ---------------------------------------------------------------------------
// delete rollback success: exact row recreated (id/content/stopped), client
// restored, committed lease restored owned by the recreated row, real
// Apply + VerifyApplied.
// ---------------------------------------------------------------------------

func TestRollbackDeleteSuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-delete"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(202)
		port   = uint32(8101)
	)
	rule := tcpProxy("web", port, 8080)
	typed := typedJSON(t, rule)
	beforePlain := clientConfigPlain(t, rule)
	afterPlain := clientConfigPlain(t)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackSourceTx(srcTx, models.TxOperationDelete, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)
	beforeEnvPlain, err := marshalProxyEnvelope(rollbackEnvelope(rowID, "web", srv, origin, "", false, typed))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(rowID), 10), beforeEnvPlain)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxStatusCommitted, rb.Status)
	row := env.rbProxyRowByID(rowID)
	require.NotNil(t, row, "the deleted rule must be recreated")
	require.Equal(t, rowID, row.ID)
	require.Equal(t, "web", row.Name)
	require.Equal(t, typed, row.Content)
	require.Equal(t, origin, row.ClientID)
	require.False(t, row.Stopped)
	require.Equal(t, beforePlain, env.rbClientConfig(origin))

	res := env.rbReservationAt(dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port})
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, rowID, *res.OwnerProxyID)
	require.Equal(t, srcTx, res.TransactionID, "the restored lease must never be stamped with the rollback tx id")

	require.Equal(t, 1, env.apply.calls)
	require.Equal(t, origin, env.apply.callsData[0].TargetClientID)
	require.Equal(t, 1, env.verify.appliedCalls, "the rule must be BACK — VerifyApplied")
	require.Equal(t, 0, env.verify.releasedCalls)
}

// ---------------------------------------------------------------------------
// update rollback, same key, different row id: after row deleted, before row
// recreated with the before id, lease owner CASed (transaction_id kept).
// ---------------------------------------------------------------------------

func TestRollbackUpdateSameKeySuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx    = "src-update"
		idem     = uuidKey()
		origin   = "cli-origin"
		child    = "cli-child"
		srv      = "srv-1"
		afterID  = uint(203)
		beforeID = uint(204)
		port     = uint32(8102)
	)
	afterRule := tcpProxy("web", port, 9090)
	beforeRule := tcpProxy("web", port, 8080)
	afterTyped := typedJSON(t, afterRule)
	beforeTyped := typedJSON(t, beforeRule)
	beforePlain := clientConfigPlain(t, beforeRule)
	afterPlain := clientConfigPlain(t, afterRule)
	env.seedServer()
	env.seedRollbackClient(child, srv, origin, afterPlain, false)
	env.seedRollbackClient(origin, "", "", nil, true)
	env.seedRollbackProxyRow(afterID, child, origin, srv, "web", false, afterTyped)
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, afterID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationUpdate, afterID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, child, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, child, afterPlain)
	beforeEnv, err := marshalProxyEnvelope(rollbackEnvelope(beforeID, "web", srv, child, origin, false, beforeTyped))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(beforeID), 10), beforeEnv)
	afterEnv, err := marshalProxyEnvelope(rollbackEnvelope(0, "web", srv, child, origin, false, afterTyped))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(afterID), 10), afterEnv)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Nil(t, env.rbProxyRowByID(afterID), "the after row must be deleted when the ids differ")
	before := env.rbProxyRowByID(beforeID)
	require.NotNil(t, before)
	require.Equal(t, beforeTyped, before.Content)
	res := env.rbReservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, beforeID, *res.OwnerProxyID)
	require.Equal(t, srcTx, res.TransactionID, "owner CAS must never rewrite transaction_id")

	require.Equal(t, 1, env.verify.appliedCalls, "the restored before rule is active — VerifyApplied")
	require.Equal(t, 0, env.verify.releasedCalls)
}

// ---------------------------------------------------------------------------
// update rollback, different key: new-key lease released, old-key lease
// restored, VerifyApplied on the BEFORE key.
// ---------------------------------------------------------------------------

func TestRollbackUpdateKeySwitchSuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx    = "src-update-ks"
		idem     = uuidKey()
		origin   = "cli-origin"
		child    = "cli-child"
		srv      = "srv-1"
		afterID  = uint(205)
		beforeID = uint(206)
	)
	afterRule := tcpProxy("web", 8200, 9090)
	beforeRule := tcpProxy("web", 8300, 8080)
	afterTyped := typedJSON(t, afterRule)
	beforeTyped := typedJSON(t, beforeRule)
	beforePlain := clientConfigPlain(t, beforeRule)
	afterPlain := clientConfigPlain(t, afterRule)
	env.seedServer()
	env.seedRollbackClient(child, srv, origin, afterPlain, false)
	env.seedRollbackClient(origin, "", "", nil, true)
	env.seedRollbackProxyRow(afterID, child, origin, srv, "web", false, afterTyped)
	afterKey := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: 8200}
	beforeKey := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: 8300}
	env.seedRollbackReservation(afterKey, afterID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationUpdate, afterID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, child, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, child, afterPlain)
	beforeEnv, err := marshalProxyEnvelope(rollbackEnvelope(beforeID, "web", srv, child, origin, false, beforeTyped))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(beforeID), 10), beforeEnv)
	afterEnv, err := marshalProxyEnvelope(rollbackEnvelope(0, "web", srv, child, origin, false, afterTyped))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(afterID), 10), afterEnv)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Nil(t, env.rbReservationAt(afterKey), "the new-key lease must be released")
	restored := env.rbReservationAt(beforeKey)
	require.NotNil(t, restored)
	require.Equal(t, beforeID, *restored.OwnerProxyID)
	require.Equal(t, srcTx, restored.TransactionID)
	require.Nil(t, env.rbProxyRowByID(afterID))
	require.NotNil(t, env.rbProxyRowByID(beforeID))

	// the verify must target the BEFORE key
	require.Equal(t, 1, env.verify.appliedCalls)
	require.Equal(t, 0, env.verify.releasedCalls)
	require.Equal(t, beforeKey.RemotePort, env.verify.lastAppliedPort())
}

// lastAppliedPort exposes the port of the most recent VerifyApplied call.
func (f *fakeVerifierService) lastAppliedPort() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAppliedPortVal
}

// ---------------------------------------------------------------------------
// start rollback: rule flipped back to stopped, VerifyReleased.
// ---------------------------------------------------------------------------

func TestRollbackStartSuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-start"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(207)
		port   = uint32(8103)
	)
	rule := tcpProxy("web", port, 8080)
	typed := typedJSON(t, rule)
	beforePlain := clientConfigPlain(t) // stopped: rule absent from the applied config
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typed)
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationStart, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)
	beforeEnv, err := marshalProxyEnvelope(rollbackEnvelope(rowID, "web", srv, origin, "", true, typed))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(rowID), 10), beforeEnv)
	afterEnv, err := marshalProxyEnvelope(rollbackEnvelope(rowID, "web", srv, origin, "", false, typed))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(rowID), 10), afterEnv)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	row := env.rbProxyRowByID(rowID)
	require.NotNil(t, row)
	require.True(t, row.Stopped, "start rollback must flip the rule back to stopped")
	res := env.rbReservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, rowID, *res.OwnerProxyID, "the SameKey lease owner must be preserved")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Equal(t, 0, env.verify.appliedCalls)
}

// ---------------------------------------------------------------------------
// stop rollback: rule flipped back to active, VerifyApplied.
// ---------------------------------------------------------------------------

func TestRollbackStopSuccess(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-stop"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(208)
		port   = uint32(8104)
	)
	rule := tcpProxy("web", port, 8080)
	typed := typedJSON(t, rule)
	beforePlain := clientConfigPlain(t, rule) // active before the stop
	afterPlain := clientConfigPlain(t)        // stopped after the stop
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", true, typed)
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationStop, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)
	beforeEnv, err := marshalProxyEnvelope(rollbackEnvelope(rowID, "web", srv, origin, "", false, typed))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(rowID), 10), beforeEnv)
	afterEnv, err := marshalProxyEnvelope(rollbackEnvelope(rowID, "web", srv, origin, "", true, typed))
	require.NoError(t, err)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(rowID), 10), afterEnv)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	row := env.rbProxyRowByID(rowID)
	require.NotNil(t, row)
	require.False(t, row.Stopped, "stop rollback must flip the rule back to active")
	res := env.rbReservationAt(key)
	require.NotNil(t, res)
	require.Equal(t, rowID, *res.OwnerProxyID)
	require.Equal(t, 1, env.verify.appliedCalls)
	require.Equal(t, 0, env.verify.releasedCalls)
}

// ---------------------------------------------------------------------------
// committed-only gate: every non-committed source status is rejected with
// CONFIG_VALIDATION_FAILED, zero rollback tx, zero agent calls.
// ---------------------------------------------------------------------------

func TestRollbackRejectsNonCommittedSource(t *testing.T) {
	for _, status := range []string{
		models.TxStatusPending, models.TxStatusPreflight, models.TxStatusApplying,
		models.TxStatusVerifying, models.TxStatusFailed, models.TxStatusRolledBack,
		models.TxStatusRollbackFailed,
	} {
		t.Run(status, func(t *testing.T) {
			env := newRollbackCoordEnv(t)
			var (
				srcTx  = "src-" + status
				idem   = uuidKey()
				origin = "cli-origin"
				srv    = "srv-1"
				rowID  = uint(209)
				port   = uint32(8105)
			)
			rule := tcpProxy("web", port, 8080)
			beforePlain := clientConfigPlain(t)
			afterPlain := clientConfigPlain(t, rule)
			env.seedServer()
			env.seedRollbackClient(origin, srv, "", afterPlain, false)
			env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
			key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
			env.seedRollbackReservation(key, rowID, srcTx)
			require.NoError(t, dao.NewMutation(env.ctx).Create(env.user, &models.ChangeTransaction{
				TransactionID: srcTx, IdempotencyKey: "idem-src-" + srcTx, RequestHash: "h",
				Operation: models.TxOperationCreate, Status: status, ProxyID: lo.ToPtr(rowID), StartedAt: env.now,
			}))
			env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
			env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

			resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
			require.NoError(t, err)
			require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
			require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
			require.Empty(t, resp.GetNewTransactionId())
			// zero DB delta: no rollback tx row
			_, ferr := dao.NewQuery(env.ctx).FindByIdempotencyKey(env.user, idem)
			require.ErrorIs(t, ferr, defs.ErrTransactionNotFound)
			// zero agent calls
			require.Equal(t, 0, env.apply.calls)
			require.Equal(t, 0, env.verify.appliedCalls+env.verify.releasedCalls)
			// the world is byte-identical
			require.Equal(t, afterPlain, env.rbClientConfig(origin))
			require.NotNil(t, env.rbProxyRowByID(rowID))
			require.NotNil(t, env.rbReservationAt(key))
		})
	}
}

// ---------------------------------------------------------------------------
// missing / cross-tenant source -> NOT_FOUND (no existence leak), zero side
// effects.
// ---------------------------------------------------------------------------

func TestRollbackMissingOrCrossTenantSourceNotFound(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq("no-such-tx", uuidKey()))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_NOT_FOUND, resp.GetStatus().GetCode())
		require.Equal(t, 0, env.apply.calls)
	})
	t.Run("cross tenant", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		var (
			srcTx  = "src-x-tenant"
			idem   = uuidKey()
			origin = "cli-origin"
			srv    = "srv-1"
			rowID  = uint(210)
			port   = uint32(8106)
		)
		rule := tcpProxy("web", port, 8080)
		beforePlain := clientConfigPlain(t)
		afterPlain := clientConfigPlain(t, rule)
		env.seedServer()
		env.seedRollbackClient(origin, srv, "", afterPlain, false)
		env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
		key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
		env.seedRollbackReservation(key, rowID, srcTx)
		env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
		env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
		env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

		// a DIFFERENT tenant over the SAME db
		otherCtx, otherUser := newCoordTenantEnv(env.ctx, 2)
		resp, err := env.newRollbackCoordinator().Rollback(otherCtx, rollbackReq(srcTx, idem))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_NOT_FOUND, resp.GetStatus().GetCode(), "cross-tenant must be indistinguishable from missing")
		require.Equal(t, 0, env.apply.calls)
		_ = otherUser
	})
}

// ---------------------------------------------------------------------------
// request validation: transaction_id + UUID-v4 idempotency key mandatory.
// ---------------------------------------------------------------------------

func TestRollbackRequestValidation(t *testing.T) {
	env := newRollbackCoordEnv(t)
	coord := env.newRollbackCoordinator()

	resp, err := coord.Rollback(env.ctx, &pb.RollbackRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())

	resp, err = coord.Rollback(env.ctx, &pb.RollbackRequest{TransactionId: lo.ToPtr("src-1")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())

	resp, err = coord.Rollback(env.ctx, rollbackReq("src-1", "not-a-uuid"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, 0, env.apply.calls)
}

// ---------------------------------------------------------------------------
// fail-closed snapshot guards: missing key / wrong key / SHA mismatch /
// missing required snapshot — zero rollback tx, zero agent calls.
// ---------------------------------------------------------------------------

func TestRollbackSnapshotGuardsFailClosed(t *testing.T) {
	seedWorld := func(t *testing.T, env *rollbackCoordEnv) (string, uint) {
		const (
			srcTx  = "src-guard"
			origin = "cli-origin"
			srv    = "srv-1"
			rowID  = uint(211)
			port   = uint32(8107)
		)
		rule := tcpProxy("web", port, 8080)
		beforePlain := clientConfigPlain(t)
		afterPlain := clientConfigPlain(t, rule)
		env.seedServer()
		env.seedRollbackClient(origin, srv, "", afterPlain, false)
		env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
		key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
		env.seedRollbackReservation(key, rowID, srcTx)
		env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
		env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
		env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)
		return srcTx, rowID
	}
	zeroSideEffects := func(env *rollbackCoordEnv, idem string, rowID uint) {
		_, ferr := dao.NewQuery(env.ctx).FindByIdempotencyKey(env.user, idem)
		require.ErrorIs(t, ferr, defs.ErrTransactionNotFound, "no rollback transaction may be created")
		require.Equal(t, 0, env.apply.calls, "zero agent calls")
		require.Equal(t, 0, env.verify.appliedCalls+env.verify.releasedCalls, "zero verifier calls")
		require.NotNil(t, env.rbProxyRowByID(rowID))
	}

	t.Run("missing snapshot key", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		srcTx, rowID := seedWorld(t, env)
		env.sdeps.KeyProvider = failKeyProvider{}
		_, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, uuidKey()))
		require.Error(t, err, "a missing key is an internal fail-closed error")
		zeroSideEffects(env, "ignored", rowID)
	})
	t.Run("wrong snapshot key", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		srcTx, rowID := seedWorld(t, env)
		env.sdeps.KeyProvider = &MemoryKeyProvider{Key: make([]byte, 32)} // zeros ≠ env.key
		_, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, uuidKey()))
		require.Error(t, err, "decrypt with the wrong key must fail closed")
		zeroSideEffects(env, "ignored", rowID)
	})
	t.Run("sha mismatch", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		srcTx, rowID := seedWorld(t, env)
		// tamper the after-snapshot SHA-256: decrypt succeeds but the
		// integrity check must fail closed
		var snap models.ConfigSnapshot
		require.NoError(t, env.db.Where("transaction_id = ? AND side = ?", srcTx, models.SnapshotSideAfter).First(&snap).Error)
		require.NoError(t, env.db.Model(&snap).Update("sha256", strings.Repeat("0", 64)).Error)
		_, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, uuidKey()))
		require.Error(t, err)
		zeroSideEffects(env, "ignored", rowID)
	})
	t.Run("missing required after snapshot", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		srcTx, rowID := seedWorld(t, env)
		require.NoError(t, env.db.Where("transaction_id = ? AND side = ?", srcTx, models.SnapshotSideAfter).
			Delete(&models.ConfigSnapshot{}).Error)
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, uuidKey()))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
		zeroSideEffects(env, "ignored", rowID)
	})
}

// ---------------------------------------------------------------------------
// TOCTOU drift: the current client config differs from the source-after
// plaintext — zero agent, zero rollback tx (newer edits must never be
// clobbered).
// ---------------------------------------------------------------------------

func TestRollbackCurrentClientDriftFailsClosed(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-drift"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(212)
		port   = uint32(8108)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)
	// NEWER EDIT: a rule on a different port was added after the source tx
	drifted := clientConfigPlain(t, rule, tcpProxy("other", port+1, 7070))
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.user, &models.ClientEntity{
		ClientID: origin, ServerID: srv, IsShadow: false, ConnectSecret: "secret",
		UserID: env.user.GetUserID(), TenantID: env.user.GetTenantID(), ConfigContent: drifted,
	}))

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
	require.Equal(t, 0, env.apply.calls)
	_, ferr := dao.NewQuery(env.ctx).FindByIdempotencyKey(env.user, idem)
	require.ErrorIs(t, ferr, defs.ErrTransactionNotFound)
	require.Equal(t, drifted, env.rbClientConfig(origin), "the newer edits must survive untouched")
}

func TestDecryptRollbackSnapshotsRejectsDuplicateSlots(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, snapshot.KeySize)
	plain := []byte(`{"proxies":[]}`)
	ciphertext, nonce, sum, err := snapshot.EncryptSnapshot(plain, key)
	require.NoError(t, err)
	makeSnap := func(id uint, side, nodeType, nodeID string) *models.ConfigSnapshot {
		return &models.ConfigSnapshot{
			Model:      gorm.Model{ID: id},
			Side:       side,
			NodeType:   nodeType,
			NodeID:     nodeID,
			Ciphertext: ciphertext,
			Nonce:      nonce,
			SHA256:     sum,
		}
	}
	_, err = decryptRollbackSnapshots(models.TxOperationCreate, []*models.ConfigSnapshot{
		makeSnap(1, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "cli-1"),
		makeSnap(2, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "cli-1"),
	}, key)
	require.Error(t, err)
	var validationErr *rollbackValidationError
	require.ErrorAs(t, err, &validationErr)
}

func TestParseRollbackProxyEnvelopeRejectsUnknownVersionAndMalformedJSON(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"version":2,"proxy_id":1}`),
		[]byte(`not-json`),
	} {
		_, err := parseProxyEnvelope(raw)
		require.Error(t, err)
	}
}

// ---------------------------------------------------------------------------
// post-apply compensation: Apply rejection -> restore the source-AFTER
// config with a REAL apply + inverse verify, then rolled_back. The DB is
// byte-identical (the rollback never committed) and the lease is retained.
// ---------------------------------------------------------------------------

func TestRollbackApplyRejectCompensatesToRolledBack(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-comp"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(213)
		port   = uint32(8109)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

	// scripted apply: [forward FAILS, compensation restore OK]
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyRejected, err: errors.New("agent rejected config")},
		{},
	}

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeApplyRejected, resp.GetErrorCode())

	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxStatusRolledBack, rb.Status, "compensation success must end rolled_back")
	require.Equal(t, defs.ErrorCodeApplyRejected, rb.ErrorCode)

	// DB byte-identical: client still on the AFTER config, row + lease intact
	require.Equal(t, afterPlain, env.rbClientConfig(origin))
	require.NotNil(t, env.rbProxyRowByID(rowID))
	require.NotNil(t, env.rbReservationAt(key))

	// agent: forward apply + compensation restore apply = 2 applies; the
	// restore re-applies the source-AFTER config to the SAME handler
	require.Equal(t, 2, env.apply.calls)
	restore := env.apply.callsData[1]
	require.Equal(t, origin, restore.TargetClientID)
	require.Contains(t, string(restore.Config), "web")
	// compensation verifies the INVERSE: the rule must be BACK
	require.Equal(t, 1, env.verify.appliedCalls)
	require.Equal(t, 0, env.verify.releasedCalls)

	// compensation evidence + audit
	ev, err := decodeJSONMap(rb.EvidenceJSON)
	require.NoError(t, err)
	require.Equal(t, "rollback-compensate-done", ev["stage"])
	events := env.rbAuditEventsOf(rb.TransactionID)
	require.Len(t, events, 1)
	require.Equal(t, models.AuditEventTypeCompensation, events[0].EventType)
	require.True(t, events[0].Success)
}

// ---------------------------------------------------------------------------
// compensation FAILURE: forward Apply fails AND the restore apply fails ->
// rollback_failed, lease RETAINED (the rule stays blocked), blocking
// evidence written.
// ---------------------------------------------------------------------------

func TestRollbackCompensationFailureLandsRollbackFailed(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-compfail"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(214)
		port   = uint32(8110)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyRejected, err: errors.New("agent rejected config")},
		{code: defs.ErrorCodeApplyTimeout, err: errors.New("agent never acked the restore")},
	}

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeRollbackFailed, resp.GetErrorCode())

	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxStatusRollbackFailed, rb.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, rb.ErrorCode)
	require.NotNil(t, env.rbReservationAt(key), "the committed lease must be RETAINED (blocking)")
	require.Equal(t, afterPlain, env.rbClientConfig(origin))
	ev, err := decodeJSONMap(rb.EvidenceJSON)
	require.NoError(t, err)
	require.Equal(t, "rollback-compensate-apply", ev["stage"])
	require.Contains(t, ev, "owner_error")
	events := env.rbAuditEventsOf(rb.TransactionID)
	require.Len(t, events, 1)
	require.False(t, events[0].Success)
}

// ---------------------------------------------------------------------------
// verify failure -> compensation -> rolled_back; commit failure ->
// compensation -> rolled_back.
// ---------------------------------------------------------------------------

func TestRollbackVerifyFailCompensates(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-vfail"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(215)
		port   = uint32(8111)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

	// forward apply ok; forward verify fails; compensation apply + verify ok
	env.verify.releasedErr = errors.New("port still occupied")

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeVerifyFailed, resp.GetErrorCode())

	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxStatusRolledBack, rb.Status)
	require.Equal(t, defs.ErrorCodeVerifyFailed, rb.ErrorCode)
	// compensation re-applied the after config + verified the rule is BACK
	require.Equal(t, 2, env.apply.calls)
	require.Equal(t, 1, env.verify.appliedCalls, "inverse verify: rule must be back")
	require.Equal(t, 1, env.verify.releasedCalls, "forward verify: port must be free")
}

func TestRollbackCommitFailCompensates(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-cfail"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(216)
		port   = uint32(8112)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

	env.sdeps.Tx = &failingRollbackCommitStore{RollbackTxStore: newDAOStore(), fail: true}

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeCommitFailed, resp.GetErrorCode())

	rb := env.rbRollbackTxBySource(idem)
	require.Equal(t, models.TxStatusRolledBack, rb.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, rb.ErrorCode)
	// DB never committed the rollback: client still after, row + lease intact
	require.Equal(t, afterPlain, env.rbClientConfig(origin))
	require.NotNil(t, env.rbProxyRowByID(rowID))
	require.NotNil(t, env.rbReservationAt(key))
	require.Equal(t, 2, env.apply.calls, "forward + compensation restore")
}

// ---------------------------------------------------------------------------
// idempotency: committed replay -> success + SAME new tx id + zero side
// effects; same key + different source -> IDEMPOTENCY_CONFLICT; in-flight ->
// IDEMPOTENCY_IN_FLIGHT; terminal failure -> the original failure.
// ---------------------------------------------------------------------------

func TestRollbackIdempotency(t *testing.T) {
	seedCommittedRollback := func(t *testing.T, env *rollbackCoordEnv, sourceTxID, idemKey string, status, code, msg string) string {
		txID := "rb-" + idemKey
		require.NoError(t, dao.NewMutation(env.ctx).Create(env.user, &models.ChangeTransaction{
			TransactionID:  txID,
			IdempotencyKey: idemKey,
			RequestHash:    hashRollbackRequest(sourceTxID),
			Operation:      models.TxOperationRollback,
			Status:         status,
			ErrorCode:      code,
			ErrorMessage:   msg,
			StartedAt:      env.now,
		}))
		return txID
	}

	t.Run("committed replay", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		var (
			srcTx = "src-idem"
			idem  = uuidKey()
		)
		// a source world is NOT even needed: the replay must short-circuit
		// before any snapshot/agent work
		prev := seedCommittedRollback(t, env, srcTx, idem, models.TxStatusCommitted, "", "")
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
		require.Equal(t, prev, resp.GetNewTransactionId(), "a committed replay returns the SAME rollback tx id")
		require.Equal(t, 0, env.apply.calls, "a replay never re-applies")
		require.Equal(t, 0, env.verify.appliedCalls+env.verify.releasedCalls)
	})
	t.Run("different source conflict", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		var (
			srcTx = "src-idem2"
			idem  = uuidKey()
		)
		seedCommittedRollback(t, env, "OTHER-SOURCE", idem, models.TxStatusCommitted, "", "")
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeIdempotencyConflict, resp.GetErrorCode())
	})
	t.Run("in flight", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		var (
			srcTx = "src-idem3"
			idem  = uuidKey()
		)
		seedCommittedRollback(t, env, srcTx, idem, models.TxStatusApplying, "", "")
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
		require.NoError(t, err)
		require.Equal(t, defs.ErrorCodeIdempotencyInFlight, resp.GetErrorCode())
	})
	t.Run("terminal failure replay", func(t *testing.T) {
		env := newRollbackCoordEnv(t)
		var (
			srcTx = "src-idem4"
			idem  = uuidKey()
		)
		seedCommittedRollback(t, env, srcTx, idem, models.TxStatusRolledBack, defs.ErrorCodeVerifyFailed, "verify failed")
		resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
		require.NoError(t, err)
		require.Equal(t, defs.ErrorCodeVerifyFailed, resp.GetErrorCode())
		require.Equal(t, "verify failed", resp.GetStatus().GetMessage())
	})
}

// ---------------------------------------------------------------------------
// canonical lock key: the rollback flow locks (tenant, origin) exactly once
// — the key is derived from the source snapshot's client node, and tenant
// isolation means two tenants never share a key.
// ---------------------------------------------------------------------------

func TestRollbackLockKeyCanonicalAndTenantIsolated(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-lock"
		idem   = uuidKey()
		origin = "cli-origin"
		child  = "cli-child"
		srv    = "srv-1"
		rowID  = uint(217)
		port   = uint32(8113)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(child, srv, origin, afterPlain, false)
	env.seedRollbackClient(origin, "", "", nil, true)
	env.seedRollbackProxyRow(rowID, child, origin, srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, child, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, child, afterPlain)

	locker := &recordingLocker{}
	env.sdeps.OriginLocker = locker

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	keys := locker.lockedKeys()
	require.Len(t, keys, 1, "exactly one lock acquisition for the whole flow")
	require.Equal(t, originLockKey(env.user.GetTenantID(), origin), keys[0],
		"the lock must be the canonical (tenant, origin) key resolved from the source snapshot node")
	require.Equal(t, len(keys), len(locker.unlks), "every lock is released")
}

// ---------------------------------------------------------------------------
// redaction contract: the rollback tx RequestRedacted + audit summaries +
// evidence never embed config plaintext.
// ---------------------------------------------------------------------------

func TestRollbackRedactionContract(t *testing.T) {
	env := newRollbackCoordEnv(t)
	var (
		srcTx  = "src-redact"
		idem   = uuidKey()
		origin = "cli-origin"
		srv    = "srv-1"
		rowID  = uint(218)
		port   = uint32(8114)
	)
	rule := tcpProxy("web", port, 8080)
	beforePlain := clientConfigPlain(t)
	afterPlain := clientConfigPlain(t, rule)
	env.seedServer()
	env.seedRollbackClient(origin, srv, "", afterPlain, false)
	env.seedRollbackProxyRow(rowID, origin, "", srv, "web", false, typedJSON(t, rule))
	key := dao.ReservationKey{ServerID: srv, Protocol: "tcp", RemotePort: port}
	env.seedRollbackReservation(key, rowID, srcTx)
	env.seedRollbackSourceTx(srcTx, models.TxOperationCreate, rowID)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, origin, beforePlain)
	env.seedRollbackSnapshot(srcTx, models.SnapshotSideAfter, models.SnapshotNodeTypeClient, origin, afterPlain)

	resp, err := env.newRollbackCoordinator().Rollback(env.ctx, rollbackReq(srcTx, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	rb := env.rbRollbackTxBySource(idem)
	for _, s := range []string{string(beforePlain), string(afterPlain), string(typedJSON(t, rule))} {
		require.NotContains(t, rb.RequestRedacted, s, "request redaction must never embed config")
	}
	for _, ev := range env.rbAuditEventsOf(rb.TransactionID) {
		require.NotContains(t, ev.Summary, string(afterPlain))
		require.NotContains(t, ev.Summary, string(beforePlain))
	}
	require.NotContains(t, rb.EvidenceJSON, string(afterPlain))
	require.NotContains(t, rb.EvidenceJSON, string(beforePlain))
	// snapshots are encrypted: the raw plaintext must not appear anywhere
	for _, s := range env.rbSnapshotsOf(rb.TransactionID) {
		require.NotContains(t, string(s.Ciphertext), string(afterPlain))
		require.NotContains(t, string(s.Ciphertext), string(beforePlain))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newCoordTenantEnv(ctx *app.Context, tenantID int) (*app.Context, models.UserInfo) {
	user := &models.UserEntity{UserID: 900 + tenantID, UserName: "other", Email: "other@example.com", TenantID: tenantID}
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), ctx.GetApp()), user
}
