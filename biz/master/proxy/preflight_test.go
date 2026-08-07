package proxy

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	testServerID = "srv-1"
	testClientID = "cli-1"
)

// ---- test doubles ----

type fakeStream struct {
	ctx  context.Context
	sent chan *pb.ServerMessage
	recv chan *pb.ClientMessage
}

func (f *fakeStream) Send(m *pb.ServerMessage) error {
	f.sent <- m
	return nil
}
func (f *fakeStream) Recv() (*pb.ClientMessage, error) {
	m, ok := <-f.recv
	if !ok {
		return nil, io.EOF
	}
	return m, nil
}
func (f *fakeStream) Context() context.Context    { return f.ctx }
func (f *fakeStream) SetHeader(metadata.MD) error { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error {
	return nil
}
func (f *fakeStream) SetTrailer(metadata.MD)      {}
func (f *fakeStream) SendMsg(m interface{}) error { return nil }
func (f *fakeStream) RecvMsg(m interface{}) error { return nil }

type mapClientsManager struct {
	mu    sync.RWMutex
	conns map[string]*defs.Connector
}

func (m *mapClientsManager) Get(cliID string) *defs.Connector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[cliID]
}
func (m *mapClientsManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {
}
func (m *mapClientsManager) Remove(cliID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, cliID)
}
func (m *mapClientsManager) ClientAddr(cliID string) string { return "" }
func (m *mapClientsManager) ConnectTime(cliID string) (time.Time, bool) {
	return time.Time{}, false
}
func (m *mapClientsManager) UpdateLastSeenAt(cliID string) {}
func (m *mapClientsManager) GetLastSeenAt(cliID string) (time.Time, bool) {
	return time.Time{}, false
}

type fakeApp struct {
	app.Application
	cm   app.ClientsManager
	recv *sync.Map
}

func (f *fakeApp) GetClientsManager() app.ClientsManager { return f.cm }
func (f *fakeApp) GetClientRecvMap() *sync.Map           { return f.recv }
func (f *fakeApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

// preflightEnv wires two scripted agents (server + client).
type preflightEnv struct {
	app      *fakeApp
	server   *fakeStream
	client   *fakeStream
	requests []*pb.ValidatePortRequest // captured outbound requests
}

func newPreflightEnv(t *testing.T) *preflightEnv {
	t.Helper()
	env := &preflightEnv{
		server: &fakeStream{ctx: context.Background(), sent: make(chan *pb.ServerMessage, 16), recv: make(chan *pb.ClientMessage, 16)},
		client: &fakeStream{ctx: context.Background(), sent: make(chan *pb.ServerMessage, 16), recv: make(chan *pb.ClientMessage, 16)},
	}
	env.app = &fakeApp{
		cm: &mapClientsManager{conns: map[string]*defs.Connector{
			testServerID: {CliID: testServerID, Conn: env.server, CliType: "frps"},
			testClientID: {CliID: testClientID, Conn: env.client, CliType: "frpc"},
		}},
		recv: &sync.Map{},
	}
	return env
}

// pump starts the production Recv pump and a responder that scripts replies
// per agent. respond returns nil to simulate an agent that never replies.
func (e *preflightEnv) pump(t *testing.T, agentID string, stream *fakeStream, respond func(req *pb.ServerMessage) *pb.ClientMessage) {
	t.Helper()
	_ = rpc.Recv(e.app, agentID)
	go func() {
		for req := range stream.sent {
			var vreq pb.ValidatePortRequest
			_ = proto.Unmarshal(req.GetData(), &vreq)
			e.requests = append(e.requests, &vreq)
			resp := respond(req)
			if resp == nil {
				continue // agent never replies
			}
			resp.SessionId = req.GetSessionId()
			stream.recv <- resp
		}
	}()
}

func validateResp(ok bool, errorCode string, state pb.PortState, protoName, addr string, port uint32) *pb.ClientMessage {
	obs := &pb.PortObservation{
		NodeId:       "node",
		Protocol:     protoName,
		IpFamily:     "v4",
		LocalAddress: addr,
		LocalPort:    port,
		State:        state,
	}
	msg := &pb.ValidatePortResponse{
		Status:      &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Observation: obs,
		Ok:          lo.ToPtr(ok),
	}
	if errorCode != "" {
		msg.ErrorCode = lo.ToPtr(errorCode)
	}
	data, _ := proto.Marshal(msg)
	return &pb.ClientMessage{Event: pb.Event_EVENT_VALIDATE_PORT, Data: data}
}

func preflightCtx(env *preflightEnv) *app.Context {
	return app.NewContext(context.Background(), env.app)
}

func basePreflightReq() *pb.PreflightRequest {
	return &pb.PreflightRequest{
		ClientId:   lo.ToPtr(testClientID),
		ServerId:   lo.ToPtr(testServerID),
		Protocol:   lo.ToPtr("tcp"),
		RemotePort: lo.ToPtr(uint32(8000)),
		LocalIp:    lo.ToPtr("127.0.0.1"),
		LocalPort:  lo.ToPtr(uint32(8080)),
	}
}

// ---- tests ----

func TestPreflightBothSidesOK(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if !resp.GetServerOk() || !resp.GetClientOk() {
		t.Fatalf("expected both sides ok: server=%v client=%v", resp.GetServerOk(), resp.GetClientOk())
	}
	if resp.GetErrorCode() != "" {
		t.Fatalf("unexpected error code: %s", resp.GetErrorCode())
	}
	if len(resp.GetServerChecks()) != 1 || len(resp.GetClientChecks()) != 1 {
		t.Fatalf("expected 1 evidence per side, got %d/%d", len(resp.GetServerChecks()), len(resp.GetClientChecks()))
	}
	// server gate must use bind probe, client gate must not
	if len(env.requests) != 2 {
		t.Fatalf("expected 2 validate calls, got %d", len(env.requests))
	}
	if !env.requests[0].GetBindProbe() {
		t.Fatal("server gate must use bind_probe=true")
	}
	if env.requests[1].GetBindProbe() {
		t.Fatal("client gate must use bind_probe=false")
	}
	if env.requests[0].GetPort() != 8000 || env.requests[1].GetPort() != 8080 {
		t.Fatalf("wrong ports in requests: %d / %d", env.requests[0].GetPort(), env.requests[1].GetPort())
	}
}

func TestPreflightServerConflict(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, defs.ErrorCodePortRemoteInUse, pb.PortState_STATE_LISTENING, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetServerOk() {
		t.Fatal("server gate must fail on occupied port")
	}
	if resp.GetErrorCode() != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodePortRemoteInUse)
	}
	if len(resp.GetServerChecks()) != 1 {
		t.Fatalf("conflict evidence missing: %+v", resp.GetServerChecks())
	}
	if len(env.requests) != 1 {
		t.Fatalf("client gate must not run when server gate fails, got %d calls", len(env.requests))
	}
}

func TestPreflightClientNotListeningWarnsButPasses(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, defs.ErrorCodePortLocalNotListening, pb.PortState_STATE_AVAILABLE, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	// 2026-08 user decision: the client gate is WARN-graded — nothing
	// listening locally must NOT block rule creation.
	if !resp.GetClientOk() {
		t.Fatal("client gate must PASS (warn-graded) when nothing listens locally")
	}
	if !resp.GetServerOk() {
		t.Fatal("server gate must pass")
	}
	if resp.GetErrorCode() != defs.ErrorCodePortLocalNotListening {
		t.Fatalf("error code = %q, want advisory %s", resp.GetErrorCode(), defs.ErrorCodePortLocalNotListening)
	}
}

func TestPreflightServerUnknownRejects(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, defs.ErrorCodePortScanUnavailable, pb.PortState_STATE_UNKNOWN, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetServerOk() {
		t.Fatal("UNKNOWN must reject the server gate")
	}
	if resp.GetErrorCode() != defs.ErrorCodePortScanUnavailable {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodePortScanUnavailable)
	}
}

func TestPreflightClientOffline(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})
	// client agent not registered
	env.app.cm.Remove(testClientID)

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetClientOk() {
		t.Fatal("offline client must reject")
	}
	if resp.GetErrorCode() != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodeAgentOffline)
	}
}

func TestPreflightServerTimeout(t *testing.T) {
	old := preflightTimeout
	preflightTimeout = 200 * time.Millisecond
	defer func() { preflightTimeout = old }()

	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return nil // never replies
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetServerOk() {
		t.Fatal("unresponsive server must reject")
	}
	if resp.GetErrorCode() != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s (timeout = offline)", resp.GetErrorCode(), defs.ErrorCodeAgentOffline)
	}
	// session cleanup must have happened for the timed-out call
	leaked := 0
	env.app.recv.Range(func(k, v interface{}) bool {
		leaked++
		return false
	})
	if leaked != 0 {
		t.Fatalf("session map leaked %d entries after timeout", leaked)
	}
}

func TestPreflightIndependentPerSideBudget(t *testing.T) {
	// spec §8.4 gives each agent RPC its own full budget: a shared
	// context would let a slow server side starve the client gate.
	// Verify structurally that Preflight creates one independent
	// timeout context per side — deterministically, without wall-clock
	// sleeps — by injecting a recording context factory.
	oldFactory := newPreflightContext
	defer func() { newPreflightContext = oldFactory }()

	var mu sync.Mutex
	type ctxCall struct {
		parent  context.Context
		timeout time.Duration
		ctx     context.Context
	}
	var calls []ctxCall
	newPreflightContext = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent) // independent instance, never auto-expires
		mu.Lock()
		calls = append(calls, ctxCall{parent: parent, timeout: timeout, ctx: ctx})
		mu.Unlock()
		return ctx, cancel
	}

	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	resp, err := Preflight(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if !resp.GetServerOk() || !resp.GetClientOk() {
		t.Fatalf("both gates must pass: server=%v client=%v err=%q",
			resp.GetServerOk(), resp.GetClientOk(), resp.GetErrorMessage())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("expected exactly 2 per-side contexts, got %d", len(calls))
	}
	if calls[0].timeout != preflightTimeout || calls[1].timeout != preflightTimeout {
		t.Fatalf("each side must get the full budget %v, got %v / %v",
			preflightTimeout, calls[0].timeout, calls[1].timeout)
	}
	if calls[0].ctx == calls[1].ctx {
		t.Fatal("server and client must not share one context")
	}
	if calls[0].parent != calls[1].parent {
		t.Fatal("both per-side contexts must derive from the same request context")
	}
}

func TestPreflightUnsupportedProxyType(t *testing.T) {
	env := newPreflightEnv(t)
	req := &pb.PreflightRequest{
		ClientId:  lo.ToPtr(testClientID),
		ServerId:  lo.ToPtr(testServerID),
		ProxyType: lo.ToPtr("http"),
		Config:    []byte("[[proxies]]\nname = \"web\"\ntype = \"http\"\nlocalPort = 8080\ncustomDomains = [\"example.com\"]\n"),
	}
	resp, err := Preflight(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetErrorCode() != defs.ErrorCodeUnsupportedProxyType {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodeUnsupportedProxyType)
	}
}

func TestPreflightInvalidConfig(t *testing.T) {
	env := newPreflightEnv(t)
	req := &pb.PreflightRequest{
		ClientId: lo.ToPtr(testClientID),
		ServerId: lo.ToPtr(testServerID),
		Config:   []byte("not a toml [[["),
	}
	resp, err := Preflight(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetErrorCode() != defs.ErrorCodeConfigValidationFailed {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodeConfigValidationFailed)
	}
}

func TestPreflightMissingNodeIDs(t *testing.T) {
	env := newPreflightEnv(t)
	resp, err := Preflight(preflightCtx(env), &pb.PreflightRequest{})
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetErrorCode() != defs.ErrorCodeConfigValidationFailed {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodeConfigValidationFailed)
	}
}

func TestPreflightExplicitFieldsWinOverConfig(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "udp", "0.0.0.0", 9000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "udp", "127.0.0.1", 9090)
	})
	req := basePreflightReq()
	req.Protocol = lo.ToPtr("udp")
	req.RemotePort = lo.ToPtr(uint32(9000))
	req.LocalPort = lo.ToPtr(uint32(9090))
	// conflicting config bytes must be ignored
	req.Config = []byte("[[proxies]]\nname = \"x\"\ntype = \"tcp\"\nlocalPort = 1\nremotePort = 1\n")

	resp, err := Preflight(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if !resp.GetServerOk() || !resp.GetClientOk() {
		t.Fatalf("expected ok: server=%v client=%v", resp.GetServerOk(), resp.GetClientOk())
	}
	if env.requests[0].GetProtocol() != "udp" || env.requests[0].GetPort() != 9000 {
		t.Fatalf("explicit fields ignored: %+v", env.requests[0])
	}
}
