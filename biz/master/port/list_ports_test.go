package port

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/glebarez/sqlite"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---- test doubles ----

// callRecord captures one outbound EVENT_INSPECT_PORTS RPC.
type callRecord struct {
	ctx    context.Context
	nodeID string
	event  pb.Event
	msg    proto.Message
}

// callStep scripts one reply (resp or err) for a node.
type callStep struct {
	resp *pb.ClientMessage
	err  error
}

// scriptedCaller is the injectable inspectCaller: it records every call
// and serves a per-node FIFO of scripted replies. An unexpected call fails
// loudly so tests can prove zero-RPC behavior.
type scriptedCaller struct {
	mu    sync.Mutex
	steps map[string][]callStep
	calls []callRecord
}

func newScriptedCaller() *scriptedCaller {
	return &scriptedCaller{steps: map[string][]callStep{}}
}

func (s *scriptedCaller) script(nodeID string, steps ...callStep) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.steps[nodeID] = append(s.steps[nodeID], steps...)
}

func (s *scriptedCaller) Call(ctx context.Context, appInst app.Application, nodeID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, callRecord{ctx: ctx, nodeID: nodeID, event: event, msg: msg})
	q := s.steps[nodeID]
	if len(q) == 0 {
		return nil, fmt.Errorf("unexpected rpc call to node [%s]", nodeID)
	}
	step := q[0]
	s.steps[nodeID] = q[1:]
	return step.resp, step.err
}

func (s *scriptedCaller) callsFor(nodeID string) []callRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []callRecord
	for _, c := range s.calls {
		if c.nodeID == nodeID {
			out = append(out, c)
		}
	}
	return out
}

func (s *scriptedCaller) totalCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// fakeResolver is the injectable nodeResolver.
type fakeResolver struct {
	nodes        map[string]*visibleNode
	list         []*visibleNode
	listErr      error
	resolveCalls []string
	listCalls    int
}

func (f *fakeResolver) Resolve(userInfo models.UserInfo, nodeID string) (*visibleNode, error) {
	f.resolveCalls = append(f.resolveCalls, nodeID)
	n, ok := f.nodes[nodeID]
	if !ok {
		return nil, errNodeNotVisible
	}
	return n, nil
}

func (f *fakeResolver) List(userInfo models.UserInfo) ([]*visibleNode, error) {
	f.listCalls++
	return f.list, f.listErr
}

// swapDeps installs the fake caller + resolver + zero backoff for one test
// and restores the production values. NOT parallel-safe by design (the
// seams are package-global).
func swapDeps(t *testing.T, caller *scriptedCaller, resolver *fakeResolver) {
	t.Helper()
	oldCaller := defaultInspectCaller
	oldResolver := newNodeResolver
	oldBackoff := backoffDelay
	defaultInspectCaller = caller
	newNodeResolver = func(*app.Context) nodeResolver { return resolver }
	backoffDelay = func() time.Duration { return time.Nanosecond }
	t.Cleanup(func() {
		defaultInspectCaller = oldCaller
		newNodeResolver = oldResolver
		backoffDelay = oldBackoff
	})
}

// useRealDAOResolver points newNodeResolver at the production DAO-backed
// resolver for one test.
func useRealDAOResolver(t *testing.T) {
	t.Helper()
	old := newNodeResolver
	newNodeResolver = func(ctx *app.Context) nodeResolver { return &daoNodeResolver{ctx: ctx} }
	t.Cleanup(func() { newNodeResolver = old })
}

// portTestCtx builds a handler context carrying a valid user. The app
// instance is nil because the fake caller never touches it.
func portTestCtx(user models.UserInfo) *app.Context {
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), nil)
}

func testUser() *models.UserEntity {
	return &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1, Token: "t"}
}

// inspectResp wraps a list of observations into a SUCCESS agent reply.
func inspectResp(obs ...*pb.PortObservation) *pb.ClientMessage {
	data, _ := proto.Marshal(&pb.InspectPortsResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Observations: obs,
	})
	return &pb.ClientMessage{Event: pb.Event_EVENT_INSPECT_PORTS, Data: data}
}

func inspectObs(nodeID, protocol string, port uint32, address string) *pb.PortObservation {
	return &pb.PortObservation{
		NodeId:       nodeID,
		Protocol:     protocol,
		LocalPort:    port,
		LocalAddress: address,
		State:        pb.PortState_STATE_LISTENING,
		ObservedAt:   time.Now().Unix(),
		Source:       "test",
	}
}

// mustInspectReq extracts the InspectPortsRequest from a recorded call.
func mustInspectReq(t *testing.T, rec callRecord) *pb.InspectPortsRequest {
	t.Helper()
	req, ok := rec.msg.(*pb.InspectPortsRequest)
	require.True(t, ok, "expected *pb.InspectPortsRequest, got %T", rec.msg)
	return req
}

func requireStatus(t *testing.T, resp *pb.ListPortsResponse, code pb.RespCode) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, code, resp.GetStatus().GetCode())
}

// ---- tests ----

// TestListPortsClientRequestShape: an explicit client node must produce
// exactly one EVENT_INSPECT_PORTS call carrying ONLY the client_id locating
// field, the normalized protocol, the effective port range and
// include_docker=true, under a ~5s context deadline (spec §8.4).
func TestListPortsClientRequestShape(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1", callStep{resp: inspectResp()})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{
		NodeId:   lo.ToPtr("cli-1"),
		Protocol: lo.ToPtr("tcp4"),
		PortMin:  lo.ToPtr(uint32(100)),
		PortMax:  lo.ToPtr(uint32(200)),
	})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)

	calls := caller.callsFor("cli-1")
	require.Len(t, calls, 1)
	require.Equal(t, pb.Event_EVENT_INSPECT_PORTS, calls[0].event)
	req := mustInspectReq(t, calls[0])
	require.Equal(t, "cli-1", req.GetClientId())
	require.Empty(t, req.GetServerId(), "a client node must not carry a server_id locating field")
	require.Equal(t, "tcp", req.GetProtocol(), "tcp4 must be normalized to tcp")
	require.Equal(t, uint32(100), req.GetPortMin())
	require.Equal(t, uint32(200), req.GetPortMax())
	require.True(t, req.GetIncludeDocker())

	// the per-node RPC context must carry a ~5s deadline
	dl, ok := calls[0].ctx.Deadline()
	require.True(t, ok, "per-node RPC context must have a deadline")
	until := time.Until(dl)
	require.Greater(t, until, 4*time.Second)
	require.LessOrEqual(t, until, 5*time.Second)
}

// TestListPortsServerRequestShape: an explicit server node must use the
// server_id locating field and never a client_id.
func TestListPortsServerRequestShape(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("srv-1", callStep{resp: inspectResp()})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"srv-1": {ID: "srv-1", Kind: nodeKindServer},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{
		NodeId:   lo.ToPtr("srv-1"),
		Protocol: lo.ToPtr("udp6"),
	})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)

	calls := caller.callsFor("srv-1")
	require.Len(t, calls, 1)
	req := mustInspectReq(t, calls[0])
	require.Equal(t, "srv-1", req.GetServerId())
	require.Empty(t, req.GetClientId(), "a server node must not carry a client_id locating field")
	require.Equal(t, "udp", req.GetProtocol(), "udp6 must be normalized to udp")
	// default range 0/0 means everything: 1..65535
	require.Equal(t, uint32(1), req.GetPortMin())
	require.Equal(t, uint32(65535), req.GetPortMax())
	require.True(t, req.GetIncludeDocker())
}

// TestListPortsProtocolValidation: only ""/tcp/tcp4/tcp6/udp/udp4/udp6 are
// accepted; anything else is a structured INVALID with zero RPC.
func TestListPortsProtocolValidation(t *testing.T) {
	cases := []struct {
		name     string
		protocol string
		want     string // "" = invalid expected
	}{
		{name: "empty means all", protocol: "", want: ""},
		{name: "tcp", protocol: "tcp", want: "tcp"},
		{name: "tcp4 normalized", protocol: "tcp4", want: "tcp"},
		{name: "tcp6 normalized", protocol: "tcp6", want: "tcp"},
		{name: "udp", protocol: "udp", want: "udp"},
		{name: "udp4 normalized", protocol: "udp4", want: "udp"},
		{name: "udp6 normalized", protocol: "udp6", want: "udp"},
		{name: "sctp rejected", protocol: "sctp"},
		{name: "garbage rejected", protocol: "ssh"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caller := newScriptedCaller()
			caller.script("cli-1", callStep{resp: inspectResp()})
			swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
				"cli-1": {ID: "cli-1", Kind: nodeKindClient},
			}})

			resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{
				NodeId:   lo.ToPtr("cli-1"),
				Protocol: lo.ToPtr(tc.protocol),
			})
			require.NoError(t, err)
			if tc.want == "" && tc.protocol != "" {
				requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
				require.Zero(t, caller.totalCalls(), "invalid protocol must never reach an agent")
				return
			}
			requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
			calls := caller.callsFor("cli-1")
			require.Len(t, calls, 1)
			require.Equal(t, tc.want, mustInspectReq(t, calls[0]).GetProtocol())
		})
	}
}

// TestListPortsPortRangeValidation: 0/0 = all, one-sided 0 is an open end,
// the effective range must be 1..65535 with min<=max.
func TestListPortsPortRangeValidation(t *testing.T) {
	cases := []struct {
		name    string
		min     uint32
		max     uint32
		wantMin uint32
		wantMax uint32
		invalid bool
	}{
		{name: "zero zero means all", min: 0, max: 0, wantMin: 1, wantMax: 65535},
		{name: "min zero is open start", min: 0, max: 5000, wantMin: 1, wantMax: 5000},
		{name: "max zero is open end", min: 100, max: 0, wantMin: 100, wantMax: 65535},
		{name: "explicit range", min: 8000, max: 9000, wantMin: 8000, wantMax: 9000},
		{name: "single port", min: 24, max: 24, wantMin: 24, wantMax: 24},
		{name: "min above max", min: 200, max: 100, invalid: true},
		{name: "min over 65535", min: 70000, max: 0, invalid: true},
		{name: "max over 65535", min: 0, max: 70000, invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caller := newScriptedCaller()
			caller.script("cli-1", callStep{resp: inspectResp()})
			swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
				"cli-1": {ID: "cli-1", Kind: nodeKindClient},
			}})

			resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{
				NodeId:  lo.ToPtr("cli-1"),
				PortMin: lo.ToPtr(tc.min),
				PortMax: lo.ToPtr(tc.max),
			})
			require.NoError(t, err)
			if tc.invalid {
				requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
				require.Zero(t, caller.totalCalls(), "invalid port range must never reach an agent")
				return
			}
			requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
			req := mustInspectReq(t, caller.callsFor("cli-1")[0])
			require.Equal(t, tc.wantMin, req.GetPortMin())
			require.Equal(t, tc.wantMax, req.GetPortMax())
		})
	}
}

// TestListPortsNodeNotVisibleZeroRPC: an unknown node id fails closed with
// NOT_FOUND and performs zero RPC (no existence leak).
func TestListPortsNodeNotVisibleZeroRPC(t *testing.T) {
	caller := newScriptedCaller()
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("ghost")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_NOT_FOUND)
	require.Zero(t, caller.totalCalls(), "an invisible node must never be contacted")
}

// TestListPortsInvalidUser: a handler without a valid user context is
// rejected before any DAO or RPC.
func TestListPortsInvalidUser(t *testing.T) {
	caller := newScriptedCaller()
	swapDeps(t, caller, &fakeResolver{})

	resp, err := ListPorts(portTestCtx(nil), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Zero(t, caller.totalCalls())
}

// TestListPortsAgentLiesEnforced: the master must never trust agent-owned
// fields — node_id is pinned to the authorized target and observations
// outside the requested protocol/port range (nil, 0, out-of-range,
// non-tcp/udp) are dropped (spec D2c ruling 6).
func TestListPortsAgentLiesEnforced(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1", callStep{resp: inspectResp(
		inspectObs("evil-node", "tcp", 22, "127.0.0.1"),   // node lie -> keep, pinned
		inspectObs("evil-node", "sctp", 53, "0.0.0.0"),    // protocol lie -> drop
		inspectObs("evil-node", "udp", 0, "0.0.0.0"),      // port 0 -> drop
		inspectObs("evil-node", "udp", 99999, "0.0.0.0"),  // out of range -> drop
		inspectObs("evil-node", "udp", 80, "0.0.0.0"),     // out of requested range -> drop
		inspectObs("evil-node", "tcp4", 443, "127.0.0.1"), // tcp4 -> normalized, kept
		nil, // nil observation -> drop
	)})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{
		NodeId:   lo.ToPtr("cli-1"),
		Protocol: lo.ToPtr("tcp"),
		PortMin:  lo.ToPtr(uint32(1)),
		PortMax:  lo.ToPtr(uint32(1024)),
	})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)

	obs := resp.GetObservations()
	require.Len(t, obs, 2, "only the pinned tcp/22 and normalized tcp/443 survive")
	require.Equal(t, "cli-1", obs[0].GetNodeId(), "agent node_id must be pinned to the authorized node")
	require.Equal(t, "cli-1", obs[1].GetNodeId())
	require.Equal(t, "tcp", obs[0].GetProtocol())
	require.Equal(t, "tcp", obs[1].GetProtocol(), "tcp4 must be normalized to tcp")
	require.Equal(t, uint32(22), obs[0].GetLocalPort())
	require.Equal(t, uint32(443), obs[1].GetLocalPort())
}

// TestListPortsMalformedAgentResponse: corrupt protobuf fails closed with
// INVALID and a UNKNOWN evidence observation (source scan-unavailable).
func TestListPortsMalformedAgentResponse(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1", callStep{resp: &pb.ClientMessage{Event: pb.Event_EVENT_INSPECT_PORTS, Data: []byte{0xde, 0xad, 0xbe, 0xef}}})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	obs := resp.GetObservations()
	require.Len(t, obs, 1)
	require.Equal(t, "cli-1", obs[0].GetNodeId())
	require.Equal(t, pb.PortState_STATE_UNKNOWN, obs[0].GetState())
	require.Equal(t, sourceScanUnavailable, obs[0].GetSource())
	require.Greater(t, obs[0].GetObservedAt(), int64(0))
}

// TestListPortsNonSuccessAgentResponse: an agent INVALID status fails
// closed (no partial trust).
func TestListPortsNonSuccessAgentResponse(t *testing.T) {
	caller := newScriptedCaller()
	data, _ := proto.Marshal(&pb.InspectPortsResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "permission denied"},
		Observations: []*pb.PortObservation{inspectObs("cli-1", "tcp", 22, "127.0.0.1")},
	})
	caller.script("cli-1", callStep{resp: &pb.ClientMessage{Event: pb.Event_EVENT_INSPECT_PORTS, Data: data}})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, resp.GetObservations(), 1)
	require.Equal(t, sourceScanUnavailable, resp.GetObservations()[0].GetSource())
}

// TestListPortsAgentTimeoutFailsClosed: a timeout is agent-offline
// evidence, never a success.
func TestListPortsAgentTimeoutFailsClosed(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1",
		callStep{err: context.DeadlineExceeded},
		callStep{err: context.DeadlineExceeded},
	)
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, resp.GetObservations(), 1)
	require.Equal(t, sourceAgentOffline, resp.GetObservations()[0].GetSource())
	require.Equal(t, "cli-1", resp.GetObservations()[0].GetNodeId())
}

// TestListPortsAgentOfflineFailsClosed: an offline agent ("cannot get
// client") fails closed with agent-offline evidence.
func TestListPortsAgentOfflineFailsClosed(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1",
		callStep{err: fmt.Errorf("cannot get client, id: [cli-1]")},
		callStep{err: fmt.Errorf("cannot get client, id: [cli-1]")},
	)
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, resp.GetObservations(), 1)
	require.Equal(t, sourceAgentOffline, resp.GetObservations()[0].GetSource())
}

// TestListPortsRetryOnceSucceeds: a transient read-scan failure is retried
// exactly once (spec §8.4) and the retry succeeds.
func TestListPortsRetryOnceSucceeds(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1",
		callStep{err: fmt.Errorf("cannot get client, id: [cli-1]")}, // transient
		callStep{resp: inspectResp(inspectObs("cli-1", "tcp", 22, "127.0.0.1"))},
	)
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Len(t, resp.GetObservations(), 1)
	require.Len(t, caller.callsFor("cli-1"), 2, "one retry after the transient failure")
}

// TestListPortsRetrySecondFailureStops: after the single retry also fails
// the scan stops (never a third attempt) and fails closed.
func TestListPortsRetrySecondFailureStops(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1",
		callStep{err: fmt.Errorf("cannot get client, id: [cli-1]")},
		callStep{err: fmt.Errorf("cannot get client, id: [cli-1]")},
	)
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, caller.callsFor("cli-1"), 2, "exactly two attempts: initial + one retry")
}

// TestListPortsFanoutDedupAndDeterministicOrder: with no node_id every
// visible node is scanned (deduplicated by id, clients win), and the
// response is sorted by (node_id, protocol, port, address).
func TestListPortsFanoutDedupAndDeterministicOrder(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-b", callStep{resp: inspectResp(
		inspectObs("cli-b", "tcp", 8080, "0.0.0.0"),
		inspectObs("cli-b", "udp", 5353, "0.0.0.0"),
	)})
	caller.script("srv-a", callStep{resp: inspectResp(
		inspectObs("srv-a", "tcp", 22, "0.0.0.0"),
	)})
	resolver := &fakeResolver{list: []*visibleNode{
		{ID: "cli-b", Kind: nodeKindClient},
		{ID: "srv-a", Kind: nodeKindServer},
		{ID: "cli-b", Kind: nodeKindClient}, // duplicate id must be scanned once
	}}
	swapDeps(t, caller, resolver)

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Equal(t, 1, resolver.listCalls)

	require.Len(t, caller.callsFor("cli-b"), 1, "duplicate ids must be deduplicated")
	require.Len(t, caller.callsFor("srv-a"), 1)
	require.Equal(t, "cli-b", mustInspectReq(t, caller.callsFor("cli-b")[0]).GetClientId())
	require.Equal(t, "srv-a", mustInspectReq(t, caller.callsFor("srv-a")[0]).GetServerId())

	obs := resp.GetObservations()
	require.Len(t, obs, 3)
	// sorted by node_id first ("cli-b" < "srv-a"), then protocol
	// ("tcp" < "udp"), then port
	require.Equal(t, "cli-b", obs[0].GetNodeId())
	require.Equal(t, "tcp", obs[0].GetProtocol())
	require.Equal(t, uint32(8080), obs[0].GetLocalPort())
	require.Equal(t, "cli-b", obs[1].GetNodeId())
	require.Equal(t, "udp", obs[1].GetProtocol())
	require.Equal(t, uint32(5353), obs[1].GetLocalPort())
	require.Equal(t, "srv-a", obs[2].GetNodeId())
	require.Equal(t, "tcp", obs[2].GetProtocol())
	require.Equal(t, uint32(22), obs[2].GetLocalPort())
}

// TestListPortsZeroVisibleNodesEmpty: no visible nodes -> SUCCESS with an
// empty observation list and zero RPC (spec D2c ruling 3).
func TestListPortsZeroVisibleNodesEmpty(t *testing.T) {
	caller := newScriptedCaller()
	swapDeps(t, caller, &fakeResolver{list: []*visibleNode{}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Empty(t, resp.GetObservations())
	require.Zero(t, caller.totalCalls())
}

// TestListPortsPartialFanout: when some nodes fail and some succeed the
// response is SUCCESS message="partial", keeps the successful observations
// and pins a UNKNOWN evidence observation per failed node.
func TestListPortsPartialFanout(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-a", callStep{resp: inspectResp(inspectObs("cli-a", "tcp", 8080, "0.0.0.0"))})
	caller.script("srv-b", callStep{err: fmt.Errorf("cannot get client, id: [srv-b]")},
		callStep{err: fmt.Errorf("cannot get client, id: [srv-b]")})
	caller.script("cli-c", callStep{resp: inspectResp(inspectObs("cli-c", "tcp", 22, "0.0.0.0"))})
	swapDeps(t, caller, &fakeResolver{list: []*visibleNode{
		{ID: "cli-a", Kind: nodeKindClient},
		{ID: "srv-b", Kind: nodeKindServer},
		{ID: "cli-c", Kind: nodeKindClient},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Equal(t, "partial", resp.GetStatus().GetMessage())

	obs := resp.GetObservations()
	require.Len(t, obs, 3)
	// sorted by node_id: cli-a, cli-c, then srv-b (UNKNOWN evidence)
	require.Equal(t, "cli-a", obs[0].GetNodeId())
	require.Equal(t, uint32(8080), obs[0].GetLocalPort())
	require.Equal(t, "cli-c", obs[1].GetNodeId())
	require.Equal(t, uint32(22), obs[1].GetLocalPort())
	// failed node carries a UNKNOWN evidence observation
	require.Equal(t, "srv-b", obs[2].GetNodeId())
	require.Equal(t, pb.PortState_STATE_UNKNOWN, obs[2].GetState())
	require.Equal(t, sourceAgentOffline, obs[2].GetSource())
	require.Greater(t, obs[2].GetObservedAt(), int64(0))
}

// TestListPortsAllNodesFail: every node failing yields INVALID.
func TestListPortsAllNodesFail(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-a", callStep{err: fmt.Errorf("cannot get client, id: [cli-a]")},
		callStep{err: fmt.Errorf("cannot get client, id: [cli-a]")})
	caller.script("srv-b", callStep{err: fmt.Errorf("boom")}, callStep{err: fmt.Errorf("boom")})
	swapDeps(t, caller, &fakeResolver{list: []*visibleNode{
		{ID: "cli-a", Kind: nodeKindClient},
		{ID: "srv-b", Kind: nodeKindServer},
	}})

	resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, resp.GetObservations(), 2)
}

// TestListPortsFanoutBoundedConcurrency: the all-nodes fanout never runs
// more than maxConcurrentScans RPCs at once.
func TestListPortsFanoutBoundedConcurrency(t *testing.T) {
	const nodes = 16
	var entered int32
	entry := make(chan struct{}, nodes)
	release := make(chan struct{})
	caller := newScriptedCaller()
	resolver := &fakeResolver{}
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n-%02d", i)
		caller.script(id, callStep{resp: inspectResp()})
		resolver.list = append(resolver.list, &visibleNode{ID: id, Kind: nodeKindClient})
	}

	// gated caller: every RPC blocks until the gate opens and signals each
	// entry, so the test can observe exactly how many run concurrently.
	gated := &gatedCaller{inner: caller, entered: &entered, entry: entry, release: release}
	oldCaller := defaultInspectCaller
	defaultInspectCaller = gated
	oldResolver := newNodeResolver
	newNodeResolver = func(*app.Context) nodeResolver { return resolver }
	oldBackoff := backoffDelay
	backoffDelay = func() time.Duration { return time.Nanosecond }
	t.Cleanup(func() {
		defaultInspectCaller = oldCaller
		newNodeResolver = oldResolver
		backoffDelay = oldBackoff
	})

	done := make(chan *pb.ListPortsResponse, 1)
	go func() {
		resp, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{})
		require.NoError(t, err)
		done <- resp
	}()

	// the first batch must fill exactly maxConcurrentScans slots
	for i := 0; i < maxConcurrentScans; i++ {
		select {
		case <-entry:
		case <-time.After(3 * time.Second):
			t.Fatal("fanout did not reach the concurrency bound")
		}
	}
	// while the gate is closed NO further call may enter
	select {
	case <-entry:
		t.Fatal("fanout exceeded the concurrency bound")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case resp := <-done:
		requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	case <-time.After(3 * time.Second):
		t.Fatal("fanout did not finish after releasing the gate")
	}
	require.Equal(t, int32(nodes), atomic.LoadInt32(&entered), "every visible node must be scanned exactly once")
}

// gatedCaller blocks every call on release and signals each entry.
type gatedCaller struct {
	inner   *scriptedCaller
	entered *int32
	entry   chan struct{}
	release chan struct{}
}

func (g *gatedCaller) Call(ctx context.Context, appInst app.Application, nodeID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
	atomic.AddInt32(g.entered, 1)
	g.entry <- struct{}{}
	<-g.release
	return g.inner.Call(ctx, appInst, nodeID, event, msg)
}

// ---- real DAO env: tenant scoping + read-only guarantee ----

type daoEnv struct {
	t       *testing.T
	ctx     *app.Context
	appInst app.Application
	db      *gorm.DB
	user    models.UserInfo
}

func newDAOEnv(t *testing.T, user models.UserInfo) *daoEnv {
	t.Helper()
	dsn := fmt.Sprintf("file:port-list-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Client{}, &models.Server{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	ctx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), appInst)
	return &daoEnv{t: t, ctx: ctx, appInst: appInst, db: db, user: user}
}

func (e *daoEnv) countClients() int64 {
	var n int64
	require.NoError(e.t, e.db.Model(&models.Client{}).Count(&n).Error)
	return n
}

func (e *daoEnv) countServers() int64 {
	var n int64
	require.NoError(e.t, e.db.Model(&models.Server{}).Count(&n).Error)
	return n
}

// TestListPortsCrossTenantNotVisible: a node owned by another tenant is
// indistinguishable from a missing node (NOT_FOUND) and never contacted.
func TestListPortsCrossTenantNotVisible(t *testing.T) {
	caller := newScriptedCaller()
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	owner := &models.UserEntity{UserID: 1, UserName: "owner", Email: "o@x.com", TenantID: 1, Token: "t"}
	env := newDAOEnv(t, owner)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(owner, &models.ClientEntity{ClientID: "cli-1", ConnectSecret: "s"}))
	require.NoError(t, dao.NewMutation(env.ctx).CreateServer(owner, &models.ServerEntity{ServerID: "srv-1", ConnectSecret: "s"}))

	other := &models.UserEntity{UserID: 2, UserName: "intruder", Email: "i@x.com", TenantID: 2, Token: "t"}
	otherCtx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, other), env.appInst)

	resp, err := ListPorts(otherCtx, &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_NOT_FOUND)
	require.Zero(t, caller.totalCalls(), "a cross-tenant node must never be contacted")

	// the same lookup from the OWNING tenant succeeds and scans
	caller.script("cli-1", callStep{resp: inspectResp()})
	resp, err = ListPorts(env.ctx, &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Len(t, caller.callsFor("cli-1"), 1)
}

// TestListPortsRealDAOSeesOwnTenantNodes: with the REAL DAO resolver an
// explicit own-tenant client and server both resolve and are scanned.
func TestListPortsRealDAOSeesOwnTenantNodes(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1", callStep{resp: inspectResp(inspectObs("cli-1", "tcp", 22, "0.0.0.0"))})
	caller.script("srv-1", callStep{resp: inspectResp(inspectObs("srv-1", "tcp", 7000, "0.0.0.0"))})
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	env := newDAOEnv(t, testUser())
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{ClientID: "cli-1", ConnectSecret: "s"}))
	require.NoError(t, dao.NewMutation(env.ctx).CreateServer(testUser(), &models.ServerEntity{ServerID: "srv-1", ConnectSecret: "s"}))

	resp, err := ListPorts(env.ctx, &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Len(t, caller.callsFor("cli-1"), 1)
	require.Len(t, caller.callsFor("srv-1"), 1)
	require.Equal(t, "cli-1", mustInspectReq(t, caller.callsFor("cli-1")[0]).GetClientId())
	require.Equal(t, "srv-1", mustInspectReq(t, caller.callsFor("srv-1")[0]).GetServerId())
	require.Len(t, resp.GetObservations(), 2)
}

// TestListPortsNoDBWrites: running ListPorts through the real DAO never
// changes the clients/servers tables (read-only handler, spec ruling 7).
func TestListPortsNoDBWrites(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-1", callStep{resp: inspectResp()})
	caller.script("srv-1", callStep{resp: inspectResp()})
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	env := newDAOEnv(t, testUser())
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{ClientID: "cli-1", ConnectSecret: "s"}))
	require.NoError(t, dao.NewMutation(env.ctx).CreateServer(testUser(), &models.ServerEntity{ServerID: "srv-1", ConnectSecret: "s"}))

	beforeClients, beforeServers := env.countClients(), env.countServers()
	resp, err := ListPorts(env.ctx, &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Equal(t, beforeClients, env.countClients(), "ListPorts must not write clients")
	require.Equal(t, beforeServers, env.countServers(), "ListPorts must not write servers")
	require.Len(t, caller.callsFor("cli-1"), 1)
	require.Len(t, caller.callsFor("srv-1"), 1)
}

// TestListPortsAllNodesCanonicalizesShadowClients: a shadow child is a
// logical address, not a second physical agent. The all-visible scan must
// resolve it to the origin and issue exactly one RPC to that origin.
func TestListPortsAllNodesCanonicalizesShadowClients(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-origin", callStep{resp: inspectResp(inspectObs("spoofed", "tcp", 8080, "0.0.0.0"))})
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	env := newDAOEnv(t, testUser())
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{
		ClientID: "cli-origin", ConnectSecret: "s",
	}))
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{
		ClientID: "cli-shadow", OriginClientID: "cli-origin", ConnectSecret: "s",
	}))

	resp, err := ListPorts(env.ctx, &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	require.Len(t, caller.callsFor("cli-origin"), 1)
	require.Empty(t, caller.callsFor("cli-shadow"), "a logical shadow must never be treated as a second physical agent")
	require.Len(t, resp.GetObservations(), 1)
	require.Equal(t, "cli-origin", resp.GetObservations()[0].GetNodeId(), "Master pins evidence to the physical origin")
}

// TestListPortsAllNodesIncludesDefaultAndTenantServers: ListServers exposes
// the default server in addition to tenant rows. The resolver must allocate
// room for that extra visible row instead of truncating the last tenant
// server or omitting the default when CountServers is zero.
func TestListPortsAllNodesIncludesDefaultAndTenantServers(t *testing.T) {
	caller := newScriptedCaller()
	for _, id := range []string{"srv-a", "srv-b", defs.DefaultServerID} {
		caller.script(id, callStep{resp: inspectResp(inspectObs(id, "tcp", 7000, "0.0.0.0"))})
	}
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	env := newDAOEnv(t, testUser())
	for _, id := range []string{"srv-a", "srv-b", defs.DefaultServerID} {
		require.NoError(t, dao.NewMutation(env.ctx).CreateServer(testUser(), &models.ServerEntity{
			ServerID: id, ConnectSecret: "s",
		}))
	}

	resp, err := ListPorts(env.ctx, &pb.ListPortsRequest{})
	require.NoError(t, err)
	requireStatus(t, resp, pb.RespCode_RESP_CODE_SUCCESS)
	for _, id := range []string{"srv-a", "srv-b", defs.DefaultServerID} {
		require.Len(t, caller.callsFor(id), 1, "visible server %s must be scanned", id)
	}
}

func TestListPortsUnexpectedEventRejected(t *testing.T) {
	caller := newScriptedCaller()
	resp := inspectResp(inspectObs("cli-1", "tcp", 22, "0.0.0.0"))
	resp.Event = pb.Event_EVENT_DATA
	caller.script("cli-1", callStep{resp: resp})
	swapDeps(t, caller, &fakeResolver{nodes: map[string]*visibleNode{
		"cli-1": {ID: "cli-1", Kind: nodeKindClient},
	}})

	out, err := ListPorts(portTestCtx(testUser()), &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-1")})
	require.NoError(t, err)
	requireStatus(t, out, pb.RespCode_RESP_CODE_INVALID)
	require.Len(t, out.GetObservations(), 1)
	require.Equal(t, pb.PortState_STATE_UNKNOWN, out.GetObservations()[0].GetState())
}

func TestListPortsExplicitShadowUsesCanonicalOrigin(t *testing.T) {
	caller := newScriptedCaller()
	caller.script("cli-origin", callStep{resp: inspectResp(inspectObs("spoofed", "tcp", 8080, "0.0.0.0"))})
	swapDeps(t, caller, &fakeResolver{})
	useRealDAOResolver(t)

	env := newDAOEnv(t, testUser())
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{
		ClientID: "cli-origin", ConnectSecret: "s",
	}))
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(testUser(), &models.ClientEntity{
		ClientID: "cli-shadow", OriginClientID: "cli-origin", ConnectSecret: "s",
	}))

	out, err := ListPorts(env.ctx, &pb.ListPortsRequest{NodeId: lo.ToPtr("cli-shadow")})
	require.NoError(t, err)
	requireStatus(t, out, pb.RespCode_RESP_CODE_SUCCESS)
	require.Len(t, caller.callsFor("cli-origin"), 1)
	require.Empty(t, caller.callsFor("cli-shadow"))
	require.Equal(t, "cli-origin", out.GetObservations()[0].GetNodeId())
}
