package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/originlock"
	"github.com/VaalaCat/frp-panel/services/rpc"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Round-8 I3 cross-path tests: the live writers — the legacy delete body
// and the /start and /update ROUTES (thin delegates to the safe
// coordinators) — must contend the SAME (tenant, origin) keyed lock the
// SafeCreate coordinator holds, so a request that arrives mid-SafeCreate is
// blocked BEFORE its first DB mutation and only runs after the safe flow
// released the lock. Deterministic by construction: SafeCreate is held
// mid-apply (its agent ACK is withheld), the concurrent request's
// contention on the lock is a real event (contentionSignaling locker), and
// no test sleeps anywhere.

// legacyLockEnv = coordTestEnv + a real agent stream for the physical
// origin client, so BOTH the SafeCreate coordinator (real rpcApplyService)
// and the legacy handlers (UpdateFrpcHanderLocked) talk to the same fake
// agent.
type legacyLockEnv struct {
	*coordTestEnv
	manager *mapClientsManager
	stream  *fakeStream
}

func newLegacyLockEnv(t *testing.T) *legacyLockEnv {
	t.Helper()
	e := newCoordFileTestEnv(t)
	manager := &mapClientsManager{conns: map[string]*defs.Connector{}}
	e.appInst.SetClientsManager(manager)
	e.appInst.SetClientRecvMap(&sync.Map{})

	stream := &fakeStream{
		ctx:  context.Background(),
		sent: make(chan *pb.ServerMessage, 16),
		recv: make(chan *pb.ClientMessage, 16),
	}
	manager.conns[testClientID] = &defs.Connector{CliID: testClientID, Conn: stream, CliType: "frpc"}
	_ = rpc.Recv(e.appInst, testClientID)
	t.Cleanup(func() { close(stream.recv) })

	env := &legacyLockEnv{coordTestEnv: e, manager: manager, stream: stream}
	// the coordinator uses the REAL apply (agent RPC) + instant verify
	env.deps.Apply = rpcApplyService{}
	env.deps.Verifier = &fakeVerifierService{}
	return env
}

// wireSharedLocker points BOTH the coordinator and the legacy writers at
// the same locker instance and returns it: without this, SafeCreate and a
// legacy handler would each lock their own private mutex and the
// serialization test would be vacuous.
func (e *legacyLockEnv) wireSharedLocker(t *testing.T) *contentionSignalingLocker {
	t.Helper()
	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	e.deps.OriginLocker = sig
	old := liveOriginLocker
	liveOriginLocker = sig
	t.Cleanup(func() { liveOriginLocker = old })
	return sig
}

// seedLegacyPair seeds the shadowed pair whose child carries one legacy
// proxy rule + the server, and materializes the child's proxy_config rows.
func (e *legacyLockEnv) seedLegacyPair() {
	e.t.Helper()
	e.seedShadowedPair(false, tcpProxy("legacy-rule", 8100, 22))
	child, err := dao.NewQuery(e.ctx).GetClientByClientID(e.userInfo, coordChildID)
	require.NoError(e.t, err)
	require.NoError(e.t, dao.NewMutation(e.ctx).RebuildProxyConfigFromClient(e.userInfo, child))
}

func (e *legacyLockEnv) childProxyNames() []string {
	e.t.Helper()
	child := e.clientByID(coordChildID)
	cfg, err := child.GetConfigContent()
	require.NoError(e.t, err)
	return lo.Map(cfg.Proxies, func(p v1.TypedProxyConfig, _ int) string { return p.GetBaseConfig().Name })
}

func (e *legacyLockEnv) latestTx() *models.ChangeTransaction {
	e.t.Helper()
	var txs []models.ChangeTransaction
	require.NoError(e.t, e.db.Order("id desc").Find(&txs).Error)
	require.NotEmpty(e.t, txs, "SafeCreate must have created a transaction row")
	return &txs[0]
}

// ackUpdateFRPC builds the SUCCESS ACK for a pending UPDATE_FRPC event
// (marshal cannot fail for this fixed struct, so it is goroutine-safe).
func ackUpdateFRPC(req *pb.ServerMessage) *pb.ClientMessage {
	data, _ := proto.Marshal(&pb.UpdateFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}})
	return &pb.ClientMessage{Event: pb.Event_EVENT_UPDATE_FRPC, SessionId: req.GetSessionId(), Data: data}
}

// scriptedAgentScript ACKs UPDATE_FRPC events. The FIRST event's ACK is
// withheld until releaseFirst closes — the test holds SafeCreate mid-apply
// while the legacy request contends on the lock — every later event ACKs
// immediately. done closes when all totalEvents were handled; the cleanup
// asserts it, so a missing event (handler failed early) is a test error.
//
// The abort channel is closed by the cleanup BEFORE stream.recv is closed:
// a scripted goroutine still waiting for an event (failing test) aborts
// instead of sending an ACK into the closed channel (which would panic the
// whole test binary and mask the real failure).
type scriptedAgentScript struct {
	stream        *fakeStream
	releaseFirst  <-chan struct{}
	firstReceived chan *pb.ServerMessage
	done          chan struct{}
	abort         chan struct{}
}

func (e *legacyLockEnv) scriptACK(releaseFirst <-chan struct{}, totalEvents int) *scriptedAgentScript {
	e.t.Helper()
	s := &scriptedAgentScript{
		stream:        e.stream,
		releaseFirst:  releaseFirst,
		firstReceived: make(chan *pb.ServerMessage, 1),
		done:          make(chan struct{}),
		abort:         make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		select {
		case req := <-s.stream.sent:
			s.firstReceived <- req
			select {
			case <-s.releaseFirst:
			case <-s.abort:
				return
			}
			if !s.ack(req) {
				return
			}
		case <-time.After(20 * time.Second):
			return
		case <-s.abort:
			return
		}
		for i := 1; i < totalEvents; i++ {
			select {
			case req := <-s.stream.sent:
				if !s.ack(req) {
					return
				}
			case <-time.After(20 * time.Second):
				return
			case <-s.abort:
				return
			}
		}
	}()
	e.t.Cleanup(func() {
		close(s.abort)
		select {
		case <-s.done:
		default:
			e.t.Error("scripted agent did not finish — an expected agent event never arrived")
		}
	})
	return s
}

// ack replies SUCCESS to req unless the test is already tearing down (the
// abort channel is closed by the cleanup before stream.recv is closed).
func (s *scriptedAgentScript) ack(req *pb.ServerMessage) bool {
	select {
	case s.stream.recv <- ackUpdateFRPC(req):
		return true
	case <-s.abort:
		return false
	}
}

func safeCreateReq() *pb.CreateProxyConfigRequest {
	return createReq(uuidKey(), candidateWebTOML)
}

// legacyUpdateTOML is a valid safe-update request body for the seeded
// legacy rule: same physical key (tcp/8100), only the local port changes.
var legacyUpdateTOML = []byte("[[proxies]]\nname = \"legacy-rule\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 2222\nremotePort = 8100\n")

// TestSafeCreateSerializesLegacyDeleteProxyConfig: while SafeCreate holds
// the origin lock (its apply ACK withheld), the legacy delete blocks
// BEFORE its first DB mutation; after the release it runs to completion.
// The delete goes through DeleteProxyConfigLegacy — the explicit legacy
// compatibility entry (no idempotency gate) that worker-ingress teardown
// and legacy-lock tests use; the transactional /delete_config route
// (DeleteProxyConfig) is a thin delegate and is NOT exercised here.
// Final state: the safe rule survives, the legacy rule is gone, no
// compensation ever ran.
func TestSafeCreateSerializesLegacyDeleteProxyConfig(t *testing.T) {
	env := newLegacyLockEnv(t)
	sig := env.wireSharedLocker(t)
	env.seedLegacyPair()

	releaseFirst := make(chan struct{})
	agent := env.scriptACK(releaseFirst, 2)

	beforeClients, beforeProxies := env.captureConfigRows()
	createDone := make(chan error, 1)
	go func() {
		_, err := env.newCoordinator().SafeCreateProxy(env.ctx, safeCreateReq())
		createDone <- err
	}()
	<-agent.firstReceived // SafeCreate provably holds the lock (mid-apply)

	delDone := make(chan struct{})
	var delErr error
	go func() {
		defer close(delDone)
		_, delErr = DeleteProxyConfigLegacy(env.ctx, &pb.DeleteProxyConfigRequest{
			ClientId: lo.ToPtr(coordChildID),
			ServerId: lo.ToPtr(testServerID),
			Name:     lo.ToPtr("legacy-rule"),
		})
	}()

	// the legacy delete is provably blocked at the lock: no DB mutation
	// may have happened on its behalf
	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy delete never contended on the origin lock")
	}
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "legacy delete must not mutate any client row while blocked at the lock")
	require.Equal(t, beforeProxies, afterProxies, "legacy delete must not mutate any proxy row while blocked at the lock")

	close(releaseFirst)
	require.NoError(t, <-createDone)
	<-delDone
	require.NoError(t, delErr, "legacy delete must complete after the safe flow released the lock")

	names := env.childProxyNames()
	require.Contains(t, names, "web", "the SafeCreate rule must survive the legacy delete")
	require.NotContains(t, names, "legacy-rule", "the legacy delete must still apply")
	tx := env.latestTx()
	require.Equal(t, models.TxStatusCommitted, tx.Status, "SafeCreate must commit — no compensation")
	select {
	case <-agent.done:
	default:
		t.Fatal("expected exactly two agent events (safe apply + legacy update), got fewer")
	}
}

// TestSafeCreateSerializesStartRouteProxyConfig: the /start_proxy route is
// now a THIN DELEGATE to the safe start coordinator (B4), so a valid start
// request must be equally blocked at the shared (tenant, origin) lock while
// SafeCreate holds it mid-apply — and after the release it runs the
// transactional start (its own committed start transaction, DB-only fast
// branch: the target rule is already present in the child's applied config,
// so no agent event is needed). No lost update, no compensation.
func TestSafeCreateSerializesStartRouteProxyConfig(t *testing.T) {
	env := newLegacyLockEnv(t)
	sig := env.wireSharedLocker(t)
	env.seedLegacyPair()

	// the route delegates to the package-global safeStartCoordinator:
	// swap it for a coordinator sharing the SAME locker + the real agent
	// apply so both flows talk to the same fake agent.
	sdeps := StartCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Res:            newDAOStore(),
		Snap:           newDAOStore(),
		KeyProvider:    env.key,
		Preflight:      env.pre,
		Apply:          rpcApplyService{},
		Verifier:       &fakeVerifierService{},
		OriginLocker:   sig,
		Now:            env.deps.Now,
		Lease:          env.deps.Lease,
		ApplyTimeout:   env.deps.ApplyTimeout,
		VerifyTimeout:  env.deps.VerifyTimeout,
		VerifyAttempts: env.deps.VerifyAttempts,
		VerifyInterval: env.deps.VerifyInterval,
		HashRequest:    hashStartRequest,
	}
	swapStartCoordinator(t, &StartTransactionCoordinator{deps: sdeps})

	releaseFirst := make(chan struct{})
	agent := env.scriptACK(releaseFirst, 1) // the safe create apply only — the start is a DB-only fast branch

	beforeClients, beforeProxies := env.captureConfigRows()
	createDone := make(chan error, 1)
	go func() {
		_, err := env.newCoordinator().SafeCreateProxy(env.ctx, safeCreateReq())
		createDone <- err
	}()
	<-agent.firstReceived // SafeCreate provably holds the lock (mid-apply)

	startDone := make(chan struct{})
	var startErr error
	go func() {
		defer close(startDone)
		_, startErr = StartProxy(env.ctx, &pb.StartProxyRequest{
			ClientId:       lo.ToPtr(coordChildID), // child-addressed: resolves to the same origin key
			ServerId:       lo.ToPtr(testServerID),
			Name:           lo.ToPtr("legacy-rule"),
			IdempotencyKey: lo.ToPtr(uuidKey()),
		})
	}()

	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("the start route never contended on the shared origin lock")
	}
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "the start route must not mutate any client row while blocked at the lock")
	require.Equal(t, beforeProxies, afterProxies, "the start route must not mutate any proxy row while blocked at the lock")

	close(releaseFirst)
	require.NoError(t, <-createDone)
	<-startDone
	require.NoError(t, startErr, "the start route must complete after the safe flow released the lock")

	names := env.childProxyNames()
	require.Contains(t, names, "web", "the SafeCreate rule must survive")
	require.Contains(t, names, "legacy-rule", "the started rule must still be live — no lost update")

	// TWO transactions: the create AND the transactional start
	require.Equal(t, 2, env.countTransactions())
	startTx := env.latestTx()
	require.Equal(t, models.TxOperationStart, startTx.Operation, "the route must create its own transactional start")
	require.Equal(t, models.TxStatusCommitted, startTx.Status)
	var compEvents []models.AuditEvent
	require.NoError(t, env.db.Where("event_type = ?", models.AuditEventTypeCompensation).Find(&compEvents).Error)
	require.Empty(t, compEvents, "no compensation may have run")
	select {
	case <-agent.done:
	default:
		t.Fatal("expected exactly one agent event (safe create apply), got fewer")
	}
}

// TestSafeCreateSerializesUpdateRouteProxyConfig (A4c): the /update_config
// route is now a THIN DELEGATE to the safe update coordinator (D.12), so a
// valid update request must be equally blocked at the shared (tenant,
// origin) lock while SafeCreate holds it mid-apply — and after the release
// it runs the FULL transactional update (its own tx, committed, its own
// agent apply). No lost update, no compensation.
func TestSafeCreateSerializesUpdateRouteProxyConfig(t *testing.T) {
	env := newLegacyLockEnv(t)
	sig := env.wireSharedLocker(t)
	env.seedLegacyPair()

	// the route delegates to the package-global safeUpdateCoordinator:
	// swap it for a coordinator sharing the SAME locker + the real agent
	// apply so both flows talk to the same fake agent.
	udeps := UpdateCoordinatorDeps{
		Query:            newDAOStore(),
		Tx:               newDAOStore(),
		Res:              newDAOStore(),
		Snap:             newDAOStore(),
		KeyProvider:      env.key,
		Preflight:        env.pre,
		Apply:            rpcApplyService{},
		Verifier:         &fakeVerifierService{},
		OriginLocker:     sig,
		Now:              env.deps.Now,
		Lease:            env.deps.Lease,
		PreflightTimeout: 2 * time.Second,
		ApplyTimeout:     env.deps.ApplyTimeout,
		VerifyTimeout:    env.deps.VerifyTimeout,
		VerifyAttempts:   env.deps.VerifyAttempts,
		VerifyInterval:   env.deps.VerifyInterval,
		HashRequest:      hashUpdateRequest,
	}
	swapSafeCoordinator(t, &UpdateTransactionCoordinator{deps: udeps})

	releaseFirst := make(chan struct{})
	agent := env.scriptACK(releaseFirst, 2) // safe create apply + safe update apply

	beforeClients, beforeProxies := env.captureConfigRows()
	createDone := make(chan error, 1)
	go func() {
		_, err := env.newCoordinator().SafeCreateProxy(env.ctx, safeCreateReq())
		createDone <- err
	}()
	<-agent.firstReceived // SafeCreate provably holds the lock (mid-apply)

	updDone := make(chan struct{})
	var updErr error
	go func() {
		defer close(updDone)
		_, updErr = UpdateProxyConfig(env.ctx, &pb.UpdateProxyConfigRequest{
			ClientId:       lo.ToPtr(coordChildID), // child-addressed: resolves to the same origin key
			ServerId:       lo.ToPtr(testServerID),
			Name:           lo.ToPtr("legacy-rule"),
			Config:         legacyUpdateTOML,
			IdempotencyKey: lo.ToPtr(uuidKey()),
		})
	}()

	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("the update route never contended on the shared origin lock")
	}
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "the update route must not mutate any client row while blocked at the lock")
	require.Equal(t, beforeProxies, afterProxies, "the update route must not mutate any proxy row while blocked at the lock")

	close(releaseFirst)
	require.NoError(t, <-createDone)
	<-updDone
	require.NoError(t, updErr, "the update route must complete after the safe flow released the lock")

	names := env.childProxyNames()
	require.Contains(t, names, "web", "the SafeCreate rule must survive")
	require.Contains(t, names, "legacy-rule", "the updated rule must still be live — no lost update")

	// TWO transactions: the create AND the transactional update
	require.Equal(t, 2, env.countTransactions())
	updTx := env.latestTx()
	require.Equal(t, models.TxOperationUpdate, updTx.Operation, "the route must create its own transactional update")
	require.Equal(t, models.TxStatusCommitted, updTx.Status)
	var compEvents []models.AuditEvent
	require.NoError(t, env.db.Where("event_type = ?", models.AuditEventTypeCompensation).Find(&compEvents).Error)
	require.Empty(t, compEvents, "no compensation may have run")
	select {
	case <-agent.done:
	default:
		t.Fatal("expected exactly two agent events (safe create apply + safe update apply), got fewer")
	}
}

// TestLegacyDeleteContendsSameKeyForChildAndOriginInput: a child-addressed
// and an origin-addressed LEGACY delete (DeleteProxyConfigLegacy — the
// explicit legacy compatibility entry) must lock the SAME (tenant, origin)
// key.
func TestLegacyDeleteContendsSameKeyForChildAndOriginInput(t *testing.T) {
	env := newLegacyLockEnv(t)
	env.seedLegacyPair()

	releaseFirst := make(chan struct{})
	close(releaseFirst) // nothing to withhold
	env.scriptACK(releaseFirst, 1)

	rec := &proxyRecordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	_, err := DeleteProxyConfigLegacy(env.ctx, &pb.DeleteProxyConfigRequest{
		ClientId: lo.ToPtr(coordChildID),
		ServerId: lo.ToPtr(testServerID),
		Name:     lo.ToPtr("legacy-rule"),
	})
	require.NoError(t, err)

	// origin-addressed: the body fails later (server mismatch on the
	// shadowed origin) but the lock key must be recorded identically
	_, err = DeleteProxyConfigLegacy(env.ctx, &pb.DeleteProxyConfigRequest{
		ClientId: lo.ToPtr(testClientID),
		ServerId: lo.ToPtr(testServerID),
		Name:     lo.ToPtr("legacy-rule"),
	})
	require.Error(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.keys, 2)
	require.Equal(t, rec.keys[0], rec.keys[1], "child- and origin-addressed deletes must lock the SAME key")
	require.Equal(t, originlock.Key(1, testClientID), rec.keys[0])
	require.Equal(t, len(rec.keys), len(rec.unlocks), "every Lock must be paired with an Unlock")
}

// TestLegacyDeleteDifferentTenantSameOriginStringDoesNotBlock: the tenant
// is part of the lock key — a second tenant operating on the same origin
// id STRING must proceed while tenant 1 holds its key. This is a property
// of the KEY, not of the DB: the models-level unique index on client_id is
// global (a second tenant can never seed the same id string in the same
// database — the old DB-integration version of this test was invalid and
// deleted WITHOUT relaxing that constraint). The key-level property is
// covered here and in services/originlock (TestKey asserts
// Key(1, "x") != Key(2, "x"); TestKeyedLockDifferentKeysDoNotBlock proves
// different keys proceed in parallel).
func TestLegacyDeleteDifferentTenantSameOriginStringDoesNotBlock(t *testing.T) {
	require.NotEqual(t, originlock.Key(1, testClientID), originlock.Key(2, testClientID),
		"the tenant must be part of the lock key")

	locker := NewOriginKeyedLock()
	k1, k2 := originlock.Key(1, testClientID), originlock.Key(2, testClientID)
	locker.Lock(k1)
	defer locker.Unlock(k1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		locker.Lock(k2)
		locker.Unlock(k2)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tenant-2 key blocked on tenant-1's key — the tenant must be part of the lock key")
	}
}

// proxyRecordingLocker records Lock/Unlock keys without blocking.
type proxyRecordingLocker struct {
	mu      sync.Mutex
	keys    []string
	unlocks []string
}

func (r *proxyRecordingLocker) Lock(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
}

func (r *proxyRecordingLocker) Unlock(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unlocks = append(r.unlocks, key)
}

// TestSafeCreateChildAddressedContendsCanonicalLegacyOriginLock pins the
// lock-key dimension of the D1 addendum P0 fix: a CHILD-addressed
// SafeCreate must hold the (tenant, CANONICAL ORIGIN) key — the SAME key
// the legacy writers resolve via LockLegacyOrigin — so a legacy request
// addressed to the ORIGIN that arrives mid-apply is blocked at the lock
// and only acquires after the safe flow released it. No sleeps: the
// contention is a real event (contentionSignalingLocker), the release is
// a real channel close, everything else is channel + timeout only.
func TestSafeCreateChildAddressedContendsCanonicalLegacyOriginLock(t *testing.T) {
	env := newLegacyLockEnv(t)
	sig := env.wireSharedLocker(t)
	env.seedLegacyPair()

	releaseFirst := make(chan struct{})
	agent := env.scriptACK(releaseFirst, 1)

	childReq := &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(coordChildID),
		ServerId:       lo.ToPtr(testServerID),
		Config:         candidateWebTOML,
		IdempotencyKey: lo.ToPtr(uuidKey()),
	}
	createDone := make(chan error, 1)
	go func() {
		_, err := env.newCoordinator().SafeCreateProxy(env.ctx, childReq)
		createDone <- err
	}()
	// the canonical-origin flow reaches the agent apply (PhysicalAgentID =
	// origin, which IS registered); the pre-fix flow fails the apply before
	// any agent event (PhysicalAgentID = raw child, not registered) — a
	// timeout, not an indefinite block, keeps the RED failure clean
	select {
	case <-agent.firstReceived: // SafeCreate provably holds the lock (mid-apply)
	case <-time.After(20 * time.Second):
		t.Fatal("SafeCreate never reached the agent apply")
	}

	type lockResult struct {
		originID string
		unlock   func()
		err      error
	}
	resCh := make(chan lockResult, 1)
	go func() {
		o, u, err := LockLegacyOrigin(env.ctx, testClientID)
		resCh <- lockResult{o, u, err}
	}()

	// the legacy lock request is provably blocked at the (tenant, ORIGIN)
	// key the child-addressed SafeCreate holds — a raw (tenant, child) key
	// would let it straight through
	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("LockLegacyOrigin never contended on the canonical origin lock held by the child-addressed SafeCreate")
	}

	close(releaseFirst)
	require.NoError(t, <-createDone)
	res := <-resCh
	require.NoError(t, res.err)
	require.Equal(t, testClientID, res.originID, "the legacy lock resolves the same canonical origin the child-addressed flow locked on")
	res.unlock()

	tx := env.latestTx()
	require.Equal(t, models.TxStatusCommitted, tx.Status, "SafeCreate must commit — no compensation")
	select {
	case <-agent.done:
	default:
		t.Fatal("expected exactly one agent event (safe apply), got fewer")
	}
}
