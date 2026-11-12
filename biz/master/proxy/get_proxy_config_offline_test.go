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
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// These tests cover the tunnel status the panel shows when the frpc process is
// NOT running. Before the fix every failure collapsed into "error" after a 30s
// RPC timeout, so the operator could not tell "start the client" from "the
// tunnel config is wrong".

// offlineStatusEnv seeds one live rule for the child client and gives the
// master a ClientsManager that holds NO session for it — exactly what the
// master sees once the frpc process exits.
type offlineStatusEnv struct {
	*coordTestEnv
	manager *mapClientsManager
	stream  *fakeStream
}

func newOfflineStatusEnv(t *testing.T, stopped bool) *offlineStatusEnv {
	t.Helper()
	e := newCoordTestEnv(t)
	manager := &mapClientsManager{conns: map[string]*defs.Connector{}}
	e.appInst.SetClientsManager(manager)
	e.appInst.SetClientRecvMap(&sync.Map{})
	e.seedShadowedPair(stopped, tcpProxy("magpie", 23425, 3425))

	child, err := dao.NewQuery(e.ctx).GetClientByClientID(e.userInfo, coordChildID)
	require.NoError(t, err)
	require.NoError(t, dao.NewMutation(e.ctx).RebuildProxyConfigFromClient(e.userInfo, child))

	return &offlineStatusEnv{coordTestEnv: e, manager: manager}
}

// connectAgent wires a live session for the agent that actually owns the
// seeded rule so the RPC path is real. The rule lives on the SHADOWED child
// (origin id + "@1"), and the master addresses the agent by its origin id.
func (e *offlineStatusEnv) connectAgent(t *testing.T) {
	t.Helper()
	target := e.onlineOriginID()
	stream := &fakeStream{
		ctx:  context.Background(),
		sent: make(chan *pb.ServerMessage, 16),
		recv: make(chan *pb.ClientMessage, 16),
	}
	e.stream = stream
	e.manager.conns[target] = &defs.Connector{CliID: target, Conn: stream, CliType: "frpc"}
	_ = rpc.Recv(e.appInst, target)
	t.Cleanup(func() { close(stream.recv) })
}

// onlineOriginID is the agent id the master would have a session for if the
// frpc process of that host were running.
func (e *offlineStatusEnv) onlineOriginID() string {
	e.t.Helper()
	resp, err := getProxyConfigWithSessionCheck(e.ctx, coordChildID, testServerID, "magpie")
	require.NoError(e.t, err)
	require.NotEmpty(e.t, resp.GetProxyConfig().GetOriginClientId())
	return resp.GetProxyConfig().GetOriginClientId()
}

// The value itself is part of the UI contract: the frontend switches on this
// literal, so renaming it silently would disable the offline hint.
func TestAgentOfflineStatusValueIsStable(t *testing.T) {
	require.Equal(t, "agent_offline", proxyStatusAgentOffline)
}

// A missing agent session must report agent_offline immediately, with a hint
// that names the client and tells the operator to start it.
func TestGetProxyConfigReportsAgentOfflineWithoutSession(t *testing.T) {
	e := newOfflineStatusEnv(t, false)

	start := time.Now()
	resp, err := getProxyConfigWithSessionCheck(e.ctx, coordChildID, testServerID, "magpie")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, resp.GetWorkingStatus())
	require.Equal(t, proxyStatusAgentOffline, resp.GetWorkingStatus().GetStatus())
	originID := resp.GetProxyConfig().GetOriginClientId()
	require.Equal(t, e.onlineOriginID(), originID,
		"the seeded rule must live on the shadowed child of the origin client")
	require.Contains(t, resp.GetWorkingStatus().GetErr(), originID,
		"the hint must name the client the operator has to start")
	require.Less(t, elapsed, 2*time.Second,
		"an offline agent must be detected without spending the RPC timeout")
}

// A session that exists but never answers is NOT offline: the process may be
// running. Reporting it as offline would invite a pointless restart.
//
// Skipped under -short: this is the only test that must burn the full RPC
// budget (30s), because "the agent never answers" is precisely the case the
// product has to wait out.
func TestGetProxyConfigSessionWithoutReplyIsNotOffline(t *testing.T) {
	if testing.Short() {
		t.Skip("burns the 30s RPC timeout by design")
	}
	e := newOfflineStatusEnv(t, false)
	e.connectAgent(t) // the scripted agent never replies

	resp, err := getProxyConfigWithSessionCheck(e.ctx, coordChildID, testServerID, "magpie")

	require.NoError(t, err)
	require.NotNil(t, resp.GetWorkingStatus())
	require.NotEqual(t, proxyStatusAgentOffline, resp.GetWorkingStatus().GetStatus())
	require.Equal(t, proxyStatusError, resp.GetWorkingStatus().GetStatus())
}

// A live agent must keep reporting its own phase, so the offline check cannot
// mask the real tunnel state.
func TestGetProxyConfigLiveAgentKeepsAgentPhase(t *testing.T) {
	e := newOfflineStatusEnv(t, false)
	e.connectAgent(t)

	go func() {
		for req := range e.stream.sent {
			body := &pb.GetProxyConfigResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "success"},
				WorkingStatus: &pb.ProxyWorkingStatus{
					Name:       lo.ToPtr("tester.magpie"),
					Type:       lo.ToPtr("tcp"),
					Status:     lo.ToPtr("running"),
					RemoteAddr: lo.ToPtr(":23425"),
				},
			}
			data, _ := proto.Marshal(body)
			e.stream.recv <- &pb.ClientMessage{
				Event:     pb.Event_EVENT_GET_PROXY_INFO,
				Data:      data,
				SessionId: req.GetSessionId(),
			}
		}
	}()

	resp, err := getProxyConfigWithSessionCheck(e.ctx, coordChildID, testServerID, "magpie")

	require.NoError(t, err)
	require.Equal(t, "running", resp.GetWorkingStatus().GetStatus())
	require.Equal(t, ":23425", resp.GetWorkingStatus().GetRemoteAddr())
}

// A paused rule must stay "stopped" and must not probe the agent at all.
func TestGetProxyConfigStoppedRuleSkipsAgentProbe(t *testing.T) {
	e := newOfflineStatusEnv(t, false)
	// RebuildProxyConfigFromClient does not carry the stopped flag, so pause
	// the rule afterwards — this is what the panel's "stop" button persists.
	require.NoError(t, e.db.Model(&models.ProxyConfig{}).
		Where("name = ?", "magpie").
		Update("stopped", true).Error)

	resp, err := getProxyConfigWithSessionCheck(e.ctx, coordChildID, testServerID, "magpie")

	require.NoError(t, err)
	require.Equal(t, proxyStatusStopped, resp.GetWorkingStatus().GetStatus())
}
