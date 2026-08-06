package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// countingGateApp embeds app.Application (nil) and records every call to the
// downstream getters the feature-gated handlers would touch. Any embedded
// method that is NOT overridden panics when called — the failpoint that
// proves the gate returned before the handler ran.
type countingGateApp struct {
	app.Application
	cfg conf.Config

	mu    sync.Mutex
	calls map[string]int

	masterCli    app.MasterClient
	workersMgr   app.WorkersManager
	wgMgr        app.WireGuardManager
	rpcHandler   app.ClientRPCHandler
	clientCtrl   app.ClientController
	clientBase   *pb.ClientBase
	clientLogMgr app.ClientLogManager
	shellPTYMgr  app.ShellPTYMgr
}

func newCountingGateApp(cfg conf.Config) *countingGateApp {
	return &countingGateApp{cfg: cfg, calls: map[string]int{}}
}

func (a *countingGateApp) GetConfig() conf.Config { return a.cfg }
func (a *countingGateApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}
func (a *countingGateApp) bump(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls[name]++
}
func (a *countingGateApp) callCount(name string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[name]
}
func (a *countingGateApp) allZero() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, n := range a.calls {
		if n != 0 {
			return false
		}
	}
	return true
}

func (a *countingGateApp) GetMasterCli() app.MasterClient {
	a.bump("GetMasterCli")
	return a.masterCli
}
func (a *countingGateApp) GetWorkersManager() app.WorkersManager {
	a.bump("GetWorkersManager")
	return a.workersMgr
}
func (a *countingGateApp) GetWorkerExecManager() app.WorkerExecManager {
	a.bump("GetWorkerExecManager")
	return nil
}
func (a *countingGateApp) GetWireGuardManager() app.WireGuardManager {
	a.bump("GetWireGuardManager")
	return a.wgMgr
}
func (a *countingGateApp) GetClientRPCHandler() app.ClientRPCHandler {
	a.bump("GetClientRPCHandler")
	return a.rpcHandler
}
func (a *countingGateApp) GetClientController() app.ClientController {
	a.bump("GetClientController")
	return a.clientCtrl
}
func (a *countingGateApp) GetClientBase() *pb.ClientBase {
	a.bump("GetClientBase")
	return a.clientBase
}
func (a *countingGateApp) GetClientLogManager() app.ClientLogManager {
	a.bump("GetClientLogManager")
	return a.clientLogMgr
}
func (a *countingGateApp) GetShellPTYMgr() app.ShellPTYMgr {
	a.bump("GetShellPTYMgr")
	return a.shellPTYMgr
}

// recordingWorkersManager records RunWorker invocations (EVENT_CREATE_WORKER
// legacy path).
type recordingWorkersManager struct {
	app.WorkersManager
	mu        sync.Mutex
	runCalls  int
	stopCalls int
}

func (m *recordingWorkersManager) RunWorker(ctx *app.Context, id string, w app.WorkerController) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runCalls++
	return nil
}
func (m *recordingWorkersManager) StopWorker(ctx *app.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalls++
	return nil
}

// recordingWireGuardManager records CreateService (EVENT_CREATE_WIREGUARD
// legacy path) and returns a stubbed service whose Start succeeds.
type recordingWireGuardManager struct {
	app.WireGuardManager
	mu          sync.Mutex
	createCalls int
}

func (m *recordingWireGuardManager) CreateService(cfg *defs.WireGuardConfig) (app.WireGuard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCalls++
	return &fakeWireGuard{}, nil
}

// GetService completes the embedded WireGuardManager contract (the nil
// embedded interface would panic if a legacy pre-check ever consulted it):
// the fresh manager has no services, so every lookup reports not-found and
// the legacy pull proceeds to CreateService.
func (m *recordingWireGuardManager) GetService(interfaceName string) (app.WireGuard, bool) {
	return nil, false
}

type fakeWireGuard struct {
	app.WireGuard
}

func (w *fakeWireGuard) Start() error { return nil }

// ---- minimal RPC chain for the PTY legacy path (EVENT_START_PTY_CONNECT) ----

type fakePTYRPCHandler struct {
	app.ClientRPCHandler
	cli app.MasterClient
}

func (h *fakePTYRPCHandler) GetCli() app.MasterClient { return h.cli }

type fakePTYMasterClient struct {
	app.MasterClient
	pb pb.MasterClient
}

func (m *fakePTYMasterClient) Call() pb.MasterClient { return m.pb }

type fakePTYPBClient struct {
	pb.MasterClient
	ptyErr error
}

func (c *fakePTYPBClient) PTYConnect(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pb.PTYClientMessage, pb.PTYServerMessage], error) {
	return nil, c.ptyErr
}

// ---- configs ----

func offClientCfg() conf.Config { return conf.Config{} }

func onFunctionsCfg() conf.Config {
	cfg := conf.Config{}
	cfg.Client.Features.EnableFunctions = true
	return cfg
}

func onRemoteShellCfg() conf.Config {
	cfg := conf.Config{}
	cfg.Client.Features.EnableRemoteShell = true
	return cfg
}

func onWireGuardCfg() conf.Config {
	cfg := conf.Config{}
	cfg.Client.Features.EnableWireGuard = true
	return cfg
}

// gatedEventBlocks maps every feature-gated event to the exact fail-closed
// EVENT_ERROR data it must produce while its feature is disabled.
var gatedEventBlocks = []struct {
	event pb.Event
	data  string
}{
	{pb.Event_EVENT_START_PTY_CONNECT, "feature disabled: remote shell"},
	{pb.Event_EVENT_CREATE_WORKER, "feature disabled: functions"},
	{pb.Event_EVENT_REMOVE_WORKER, "feature disabled: functions"},
	{pb.Event_EVENT_GET_WORKER_STATUS, "feature disabled: functions"},
	{pb.Event_EVENT_INSTALL_WORKERD, "feature disabled: functions"},
	{pb.Event_EVENT_CREATE_WIREGUARD, "feature disabled: wireguard"},
	{pb.Event_EVENT_DELETE_WIREGUARD, "feature disabled: wireguard"},
	{pb.Event_EVENT_UPDATE_WIREGUARD, "feature disabled: wireguard"},
	{pb.Event_EVENT_GET_WIREGUARD_RUNTIME_INFO, "feature disabled: wireguard"},
	{pb.Event_EVENT_RESTART_WIREGUARD, "feature disabled: wireguard"},
}

// safeEvents is the whitelist that must NEVER be intercepted by the feature
// gate: frpc/frps lifecycle + ack, stream log, proxy info, upgrade, port
// inspector/preflight (spec §4.2, S5b whitelist).
var safeEvents = []pb.Event{
	pb.Event_EVENT_UPDATE_FRPC,
	pb.Event_EVENT_REMOVE_FRPC,
	pb.Event_EVENT_UPDATE_FRPS,
	pb.Event_EVENT_REMOVE_FRPS,
	pb.Event_EVENT_PING,
	pb.Event_EVENT_PONG,
	pb.Event_EVENT_STOP_FRPC,
	pb.Event_EVENT_START_FRPC,
	pb.Event_EVENT_STOP_FRPS,
	pb.Event_EVENT_START_FRPS,
	pb.Event_EVENT_START_STREAM_LOG,
	pb.Event_EVENT_STOP_STREAM_LOG,
	pb.Event_EVENT_GET_PROXY_INFO,
	pb.Event_EVENT_UPGRADE_FRPP,
	pb.Event_EVENT_INSPECT_PORTS,
	pb.Event_EVENT_VALIDATE_PORT,
}

// TestFeatureGateBlockWhitelist: the pure gate decision table — every gated
// event is blocked (with the exact data) when its feature is off and allowed
// when on; every safe event is allowed even with all features off.
func TestFeatureGateBlockWhitelist(t *testing.T) {
	for _, tc := range gatedEventBlocks {
		msg := featureGateBlock(offClientCfg(), tc.event)
		require.NotNil(t, msg, "event %s must be gated while feature is off", tc.event)
		require.Equal(t, pb.Event_EVENT_ERROR, msg.GetEvent())
		require.Equal(t, tc.data, string(msg.GetData()))
	}

	for _, e := range safeEvents {
		require.Nil(t, featureGateBlock(offClientCfg(), e), "safe event %s must never be gated", e)
	}

	for _, tc := range gatedEventBlocks {
		cfg := offClientCfg()
		switch tc.event {
		case pb.Event_EVENT_START_PTY_CONNECT:
			cfg = onRemoteShellCfg()
		case pb.Event_EVENT_CREATE_WORKER, pb.Event_EVENT_REMOVE_WORKER, pb.Event_EVENT_GET_WORKER_STATUS, pb.Event_EVENT_INSTALL_WORKERD:
			cfg = onFunctionsCfg()
		default:
			cfg = onWireGuardCfg()
		}
		require.Nil(t, featureGateBlock(cfg, tc.event), "event %s must dispatch while feature is on", tc.event)
	}
}

// TestHandleServerMessageGatedEventsOff: with all features disabled, every
// gated event must return the exact EVENT_ERROR and the downstream handler
// must never be reached (zero app getter calls).
func TestHandleServerMessageGatedEventsOff(t *testing.T) {
	for _, tc := range gatedEventBlocks {
		t.Run(tc.event.String(), func(t *testing.T) {
			appInst := newCountingGateApp(offClientCfg())
			resp := HandleServerMessage(appInst, &pb.ServerMessage{Event: tc.event})
			require.NotNil(t, resp, "event %s must produce a response", tc.event)
			require.Equal(t, pb.Event_EVENT_ERROR, resp.GetEvent())
			require.Equal(t, tc.data, string(resp.GetData()))
			require.True(t, appInst.allZero(), "downstream app getters must not be called for gated event %s", tc.event)
		})
	}
}

// TestHandleServerMessageWorkerOnDispatches: EnableFunctions=true keeps the
// legacy worker dispatch — the real CreateWorker handler runs to completion.
func TestHandleServerMessageWorkerOnDispatches(t *testing.T) {
	appInst := newCountingGateApp(onFunctionsCfg())
	wm := &recordingWorkersManager{}
	appInst.workersMgr = wm

	reqData, err := proto.Marshal(&pb.CreateWorkerRequest{Worker: &pb.Worker{WorkerId: lo.ToPtr("w1")}})
	require.NoError(t, err)
	resp := HandleServerMessage(appInst, &pb.ServerMessage{Event: pb.Event_EVENT_CREATE_WORKER, Data: reqData})

	require.NotNil(t, resp)
	require.Equal(t, pb.Event_EVENT_CREATE_WORKER, resp.GetEvent())
	require.Equal(t, 1, wm.runCalls, "legacy CreateWorker handler must run")
	require.Equal(t, 1, appInst.callCount("GetWorkersManager"))
}

// TestHandleServerMessageWireGuardOnDispatches: EnableWireGuard=true keeps
// the legacy wireguard dispatch — the real CreateWireGuard handler runs.
func TestHandleServerMessageWireGuardOnDispatches(t *testing.T) {
	appInst := newCountingGateApp(onWireGuardCfg())
	wgMgr := &recordingWireGuardManager{}
	appInst.wgMgr = wgMgr

	reqData, err := proto.Marshal(&pb.CreateWireGuardRequest{WireguardConfig: &pb.WireGuardConfig{InterfaceName: "wg0"}})
	require.NoError(t, err)
	resp := HandleServerMessage(appInst, &pb.ServerMessage{Event: pb.Event_EVENT_CREATE_WIREGUARD, Data: reqData})

	require.NotNil(t, resp)
	require.Equal(t, pb.Event_EVENT_CREATE_WIREGUARD, resp.GetEvent())
	require.Equal(t, 1, wgMgr.createCalls, "legacy CreateWireGuard handler must run")
	require.Equal(t, 1, appInst.callCount("GetWireGuardManager"))
}

// TestHandleServerMessagePTYOnReachesHandler: EnableRemoteShell=true lets the
// PTY event through the gate — the handler is reached (it fails at the fake
// gRPC connect, proving dispatch happened, not the gate message).
func TestHandleServerMessagePTYOnReachesHandler(t *testing.T) {
	appInst := newCountingGateApp(onRemoteShellCfg())
	appInst.rpcHandler = &fakePTYRPCHandler{
		cli: &fakePTYMasterClient{pb: &fakePTYPBClient{ptyErr: errors.New("fake pty connect error")}},
	}

	resp := HandleServerMessage(appInst, &pb.ServerMessage{Event: pb.Event_EVENT_START_PTY_CONNECT})

	require.NotNil(t, resp)
	require.Equal(t, pb.Event_EVENT_ERROR, resp.GetEvent())
	require.Equal(t, "fake pty connect error", string(resp.GetData()),
		"the gate must not block PTY when EnableRemoteShell is on")
	require.Equal(t, 1, appInst.callCount("GetClientRPCHandler"))
}

// TestHandleServerMessageSafeEventsNotGated: with ALL features off, safe
// events still dispatch — PING answers PONG and FRPC update runs the real
// handler to completion.
func TestHandleServerMessageSafeEventsNotGated(t *testing.T) {
	// PING is handled inline: with a zero config it must still answer PONG.
	appInst := newCountingGateApp(offClientCfg())
	resp := HandleServerMessage(appInst, &pb.ServerMessage{Event: pb.Event_EVENT_PING})
	require.NotNil(t, resp)
	require.Equal(t, pb.Event_EVENT_PONG, resp.GetEvent())

	// EVENT_UPDATE_FRPC must reach the real UpdateFrpcHander (switchTestEnv
	// wires the fake controller + handler constructor from update_tunnel_test).
	env := newSwitchTestEnv(t)
	reqData, err := proto.Marshal(updateReq("cli-child", "srv-1", "cli-origin", []byte(validFrpcTOML)))
	require.NoError(t, err)
	resp = HandleServerMessage(env.app, &pb.ServerMessage{Event: pb.Event_EVENT_UPDATE_FRPC, Data: reqData})
	require.NotNil(t, resp)
	require.Equal(t, pb.Event_EVENT_UPDATE_FRPC, resp.GetEvent(), "FRPC lifecycle must dispatch with all features off")

	// Port inspector / preflight must not be intercepted: the handler is
	// reached (GetClientBase is consulted) and the response is not the gate
	// message even when the underlying scan errors.
	for _, e := range []pb.Event{pb.Event_EVENT_INSPECT_PORTS, pb.Event_EVENT_VALIDATE_PORT} {
		scanApp := newCountingGateApp(offClientCfg())
		scanApp.clientBase = &pb.ClientBase{}
		r := HandleServerMessage(scanApp, &pb.ServerMessage{Event: e})
		require.NotNil(t, r, "event %s must dispatch", e)
		require.Equal(t, 1, scanApp.callCount("GetClientBase"), "port handler must be reached for %s", e)
		require.False(t, r.GetEvent() == pb.Event_EVENT_ERROR && strings.HasPrefix(string(r.GetData()), "feature disabled:"),
			"port/preflight event %s must not be gate-blocked, got: %s", e, string(r.GetData()))
	}
}

// ---- PullWireGuards / ReportWireGuardRuntimeInfo gates ----

type fakeListMasterClient struct {
	app.MasterClient
	resp *pb.ListClientWireGuardsResponse
}

func (m *fakeListMasterClient) Call() pb.MasterClient {
	return &fakeListPBMasterClient{resp: m.resp}
}

type fakeListPBMasterClient struct {
	pb.MasterClient
	resp *pb.ListClientWireGuardsResponse
}

func (m *fakeListPBMasterClient) ListClientWireGuards(ctx context.Context, in *pb.ListClientWireGuardsRequest, opts ...grpc.CallOption) (*pb.ListClientWireGuardsResponse, error) {
	return m.resp, nil
}

// TestPullWireGuardsOffSkipsMasterAndManager: EnableWireGuard=false must
// return immediately — the Master RPC and the WireGuard manager are never
// touched (a broken gate would trip the nil-embedded failpoint).
func TestPullWireGuardsOffSkipsMasterAndManager(t *testing.T) {
	appInst := newCountingGateApp(offClientCfg())
	err := PullWireGuards(appInst, "cli-1", "secret-1")
	require.NoError(t, err)
	require.Zero(t, appInst.callCount("GetMasterCli"))
	require.Zero(t, appInst.callCount("GetWireGuardManager"))
}

// TestPullWireGuardsOnRunsLegacy: EnableWireGuard=true keeps the legacy path —
// the Master list RPC is called (and an empty answer short-circuits cleanly).
func TestPullWireGuardsOnRunsLegacy(t *testing.T) {
	appInst := newCountingGateApp(onWireGuardCfg())
	appInst.masterCli = &fakeListMasterClient{
		resp: &pb.ListClientWireGuardsResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS}},
	}
	err := PullWireGuards(appInst, "cli-1", "secret-1")
	require.NoError(t, err)
	require.Equal(t, 1, appInst.callCount("GetMasterCli"), "legacy PullWireGuards must call the master")
	require.Zero(t, appInst.callCount("GetWireGuardManager"), "empty list must short-circuit before the manager")
}

// TestPullWireGuardsOnCreatesService: with a non-empty master answer the
// legacy path must create and start the wireguard service.
func TestPullWireGuardsOnCreatesService(t *testing.T) {
	appInst := newCountingGateApp(onWireGuardCfg())
	wgMgr := &recordingWireGuardManager{}
	appInst.wgMgr = wgMgr
	appInst.masterCli = &fakeListMasterClient{
		resp: &pb.ListClientWireGuardsResponse{
			Status:           &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS},
			WireguardConfigs: []*pb.WireGuardConfig{{InterfaceName: "wg0"}},
		},
	}
	err := PullWireGuards(appInst, "cli-1", "secret-1")
	require.NoError(t, err)
	require.Equal(t, 1, appInst.callCount("GetMasterCli"))
	require.Equal(t, 1, appInst.callCount("GetWireGuardManager"))
	require.Equal(t, 1, wgMgr.createCalls, "legacy PullWireGuards must create the service")
}

type fakeEmptyWGManager struct {
	app.WireGuardManager
}

func (m *fakeEmptyWGManager) GetAllServices() []app.WireGuard { return nil }

// TestReportWireGuardRuntimeInfoOffSkipsEverything: EnableWireGuard=false
// must return immediately without touching the master client or the manager.
func TestReportWireGuardRuntimeInfoOffSkipsEverything(t *testing.T) {
	appInst := newCountingGateApp(offClientCfg())
	err := ReportWireGuardRuntimeInfo(appInst, "cli-1", "secret-1")
	require.NoError(t, err)
	require.Zero(t, appInst.callCount("GetMasterCli"))
	require.Zero(t, appInst.callCount("GetWireGuardManager"))
}

// TestReportWireGuardRuntimeInfoOnRunsLegacy: EnableWireGuard=true keeps the
// legacy path — the manager is consulted (and with zero services nothing is
// reported).
func TestReportWireGuardRuntimeInfoOnRunsLegacy(t *testing.T) {
	appInst := newCountingGateApp(onWireGuardCfg())
	appInst.wgMgr = &fakeEmptyWGManager{}
	err := ReportWireGuardRuntimeInfo(appInst, "cli-1", "secret-1")
	require.NoError(t, err)
	require.Equal(t, 1, appInst.callCount("GetWireGuardManager"), "legacy report must consult the manager")
}
