package client

import (
	"context"
	"sync"
	"testing"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/fatedier/frp/client/proxy"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// ---- fakes: a recording ClientController + a lightweight ClientHandler
// (the real frp client Service must never be constructed in tests — its
// Run would dial the configured server) ----

type fakeClientHandler struct {
	mu          sync.Mutex
	common      *v1.ClientCommonConfig
	running     bool
	runCalls    int
	stopCalls   int
	updateCalls int
}

func (f *fakeClientHandler) Run() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = true
	f.runCalls++
}
func (f *fakeClientHandler) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = false
	f.stopCalls++
}
func (f *fakeClientHandler) Wait()         {}
func (f *fakeClientHandler) Running() bool { return f.running }
func (f *fakeClientHandler) Update([]v1.ProxyConfigurer, []v1.VisitorConfigurer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
}
func (f *fakeClientHandler) GetCommonCfg() *v1.ClientCommonConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.common
}
func (f *fakeClientHandler) GetProxyCfgs() map[string]v1.ProxyConfigurer     { return nil }
func (f *fakeClientHandler) GetVisitorCfgs() map[string]v1.VisitorConfigurer { return nil }
func (f *fakeClientHandler) AddProxy(v1.ProxyConfigurer)                     {}
func (f *fakeClientHandler) AddVisitor(v1.VisitorConfigurer)                 {}
func (f *fakeClientHandler) RemoveProxy(v1.ProxyConfigurer)                  {}
func (f *fakeClientHandler) RemoveVisitor(v1.VisitorConfigurer)              {}
func (f *fakeClientHandler) GetProxyStatus(string) (*proxy.WorkingStatus, bool) {
	return nil, false
}

// fakeClientController is a minimal thread-safe ClientController recording
// every handler transition the agent handler performs.
type fakeClientController struct {
	mu       sync.Mutex
	handlers map[string]map[string]app.ClientHandler // clientID -> serverID -> handler
}

func newFakeClientController() *fakeClientController {
	return &fakeClientController{handlers: map[string]map[string]app.ClientHandler{}}
}

func (c *fakeClientController) Add(clientID, serverID string, h app.ClientHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		c.handlers[clientID] = map[string]app.ClientHandler{}
	}
	if old, ok := c.handlers[clientID][serverID]; ok {
		old.Stop()
	}
	c.handlers[clientID][serverID] = h
}
func (c *fakeClientController) Get(clientID, serverID string) app.ClientHandler {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		return nil
	}
	return c.handlers[clientID][serverID]
}
func (c *fakeClientController) Delete(clientID, serverID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		return
	}
	if h, ok := c.handlers[clientID][serverID]; ok {
		h.Stop()
		delete(c.handlers[clientID], serverID)
	}
}
func (c *fakeClientController) Set(clientID, serverID string, h app.ClientHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		c.handlers[clientID] = map[string]app.ClientHandler{}
	}
	c.handlers[clientID][serverID] = h
}
func (c *fakeClientController) Run(clientID, serverID string) {
	if h := c.Get(clientID, serverID); h != nil {
		h.Run()
	}
}
func (c *fakeClientController) Stop(clientID, serverID string) {
	if h := c.Get(clientID, serverID); h != nil {
		h.Stop()
	}
}
func (c *fakeClientController) GetByClient(clientID string) *utils.SyncMap[string, app.ClientHandler] {
	return nil
}
func (c *fakeClientController) DeleteByClient(string) {}
func (c *fakeClientController) RunByClient(string)    {}
func (c *fakeClientController) StopByClient(string)   {}
func (c *fakeClientController) StopAll()              {}
func (c *fakeClientController) DeleteAll()            {}
func (c *fakeClientController) RunAll()               {}
func (c *fakeClientController) List() []string        { return nil }

// validFrpcTOML is a parseable frpc client config (the agent handler
// normalizes it via utils.LoadClientConfig).
const validFrpcTOML = `
serverAddr = "127.0.0.1"
serverPort = 7000

[[proxies]]
name = "ssh"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 24
`

// switchTestEnv wires the REAL UpdateFrpcHander to a fake controller whose
// handler construction is injected (the production constructor would build
// a live frp client Service).
type switchTestEnv struct {
	t    *testing.T
	app  app.Application
	ctrl *fakeClientController
}

func newSwitchTestEnv(t *testing.T) *switchTestEnv {
	t.Helper()
	old := newClientHandler
	newClientHandler = func(c *v1.ClientCommonConfig, p []v1.ProxyConfigurer, v []v1.VisitorConfigurer) app.ClientHandler {
		return &fakeClientHandler{common: c}
	}
	t.Cleanup(func() { newClientHandler = old })

	ctrl := newFakeClientController()
	appInst := app.NewApp()
	appInst.SetClientController(ctrl)
	return &switchTestEnv{t: t, app: appInst, ctrl: ctrl}
}

func (e *switchTestEnv) ctx() *app.Context {
	return app.NewContext(context.Background(), e.app)
}

// seedHandler registers a running fake handler under (clientID, serverID).
func (e *switchTestEnv) seedHandler(clientID, serverID string) *fakeClientHandler {
	e.t.Helper()
	h := &fakeClientHandler{common: &v1.ClientCommonConfig{ServerAddr: "127.0.0.1", ServerPort: 7000}}
	e.ctrl.Add(clientID, serverID, h)
	e.ctrl.Run(clientID, serverID)
	require.True(e.t, e.ctrl.Get(clientID, serverID).Running(), "seeded handler must be running")
	return h
}

func (e *switchTestEnv) handler(clientID, serverID string) *fakeClientHandler {
	e.t.Helper()
	h := e.ctrl.Get(clientID, serverID)
	require.NotNil(e.t, h, "expected handler for %s/%s", clientID, serverID)
	return h.(*fakeClientHandler)
}

func updateReq(clientID, serverID, replaceID string, config []byte) *pb.UpdateFRPCRequest {
	return &pb.UpdateFRPCRequest{
		ClientId:        lo.ToPtr(clientID),
		ServerId:        lo.ToPtr(serverID),
		Config:          config,
		ReplaceClientId: lo.ToPtr(replaceID),
	}
}

// TestUpdateFrpcParseFailureLeavesOriginUntouched: a candidate that fails to
// parse must be rejected BEFORE any controller mutation — the replaced
// handler keeps running.
func TestUpdateFrpcParseFailureLeavesOriginUntouched(t *testing.T) {
	env := newSwitchTestEnv(t)
	origin := env.seedHandler("cli-origin", "srv-1")

	resp, err := UpdateFrpcHander(env.ctx(), updateReq("cli-child", "srv-1", "cli-origin", []byte("not a config {{{")))
	require.Error(t, err, "parse failure must surface an error")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())

	require.NotNil(t, env.ctrl.Get("cli-origin", "srv-1"), "origin handler must survive a parse failure")
	require.Zero(t, origin.stopCalls, "origin handler must never be stopped on a parse failure")
	require.True(t, origin.running, "origin handler must stay running")
	require.Nil(t, env.ctrl.Get("cli-child", "srv-1"), "no child handler may be created from an invalid config")
}

// TestUpdateFrpcFirstShadowSwitchStopsOriginAndRunsChild: the first-shadow
// forward apply must stop+delete ONLY the (origin, server) handler and then
// add+run the child handler — the origin handler is never left stale.
func TestUpdateFrpcFirstShadowSwitchStopsOriginAndRunsChild(t *testing.T) {
	env := newSwitchTestEnv(t)
	origin := env.seedHandler("cli-origin", "srv-1")

	resp, err := UpdateFrpcHander(env.ctx(), updateReq("cli-child", "srv-1", "cli-origin", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Nil(t, env.ctrl.Get("cli-origin", "srv-1"), "origin handler must be deleted by the directed switch")
	require.False(t, origin.running, "origin handler must be stopped")
	require.GreaterOrEqual(t, origin.stopCalls, 1, "origin handler must be stopped (explicit Stop + Delete's own Stop)")
	child := env.handler("cli-child", "srv-1")
	require.True(t, child.running, "child handler must be running")
	require.Equal(t, 1, child.runCalls)
}

// TestUpdateFrpcExistingChildLeavesOriginUntouched: an update addressed at
// an existing child (no replacement) must not touch the origin handler at
// all.
func TestUpdateFrpcExistingChildLeavesOriginUntouched(t *testing.T) {
	env := newSwitchTestEnv(t)
	origin := env.seedHandler("cli-origin", "srv-1")
	env.seedHandler("cli-child", "srv-1")

	resp, err := UpdateFrpcHander(env.ctx(), updateReq("cli-child", "srv-1", "", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.NotNil(t, env.ctrl.Get("cli-origin", "srv-1"), "origin handler must stay registered")
	require.Zero(t, origin.stopCalls, "origin handler must never be stopped")
	child := env.handler("cli-child", "srv-1")
	require.True(t, child.running, "child handler must stay running")
}

// TestUpdateFrpcReplaceMissingIsIdempotentSuccess: replacing a handler that
// is not running must still succeed — the directed switch tolerates a
// missing replace target (RPC timeout uncertainty makes the inverse
// operation non-atomic, so it must be idempotent).
func TestUpdateFrpcReplaceMissingIsIdempotentSuccess(t *testing.T) {
	env := newSwitchTestEnv(t)

	resp, err := UpdateFrpcHander(env.ctx(), updateReq("cli-child", "srv-1", "cli-ghost", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	child := env.handler("cli-child", "srv-1")
	require.True(t, child.running, "target handler must be running after a no-op replacement")
}

// TestUpdateFrpcSameTargetAndReplaceDoesNotSelfDelete: when replace_client_id
// equals client_id the handler must NOT delete itself — the update branch
// owns the target.
func TestUpdateFrpcSameTargetAndReplaceDoesNotSelfDelete(t *testing.T) {
	env := newSwitchTestEnv(t)
	env.seedHandler("cli-x", "srv-1")

	resp, err := UpdateFrpcHander(env.ctx(), updateReq("cli-x", "srv-1", "cli-x", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	h := env.handler("cli-x", "srv-1")
	require.True(t, h.running, "identical target/replace must not self-delete the handler")
}
