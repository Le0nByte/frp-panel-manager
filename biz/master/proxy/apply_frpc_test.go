package proxy

import (
	"context"
	"errors"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ackMsg builds a raw agent ACK carrying the given status code.
func ackMsg(t *testing.T, code pb.RespCode, message string) *pb.ClientMessage {
	t.Helper()
	raw, err := proto.Marshal(&pb.UpdateFRPCResponse{
		Status: &pb.Status{Code: code, Message: message},
	})
	require.NoError(t, err)
	return &pb.ClientMessage{Event: pb.Event_EVENT_UPDATE_FRPC, Data: raw}
}

// TestClassifyApplyResponsePinsErrorCodeSemantics (round-8 Minor: agent
// rejections were mapped to APPLY_TIMEOUT — behavior correct, code
// misleading). A rejection is a real answer and must carry
// APPLY_REJECTED; only a missing answer (deadline/cancel) is a timeout.
func TestClassifyApplyResponsePinsErrorCodeSemantics(t *testing.T) {
	t.Run("success-ack", func(t *testing.T) {
		code, err := classifyApplyResponse(ackMsg(t, pb.RespCode_RESP_CODE_SUCCESS, "ok"), nil)
		require.NoError(t, err)
		require.Empty(t, code)
	})

	t.Run("agent-explicit-rejection-is-apply-rejected", func(t *testing.T) {
		code, err := classifyApplyResponse(ackMsg(t, pb.RespCode_RESP_CODE_INVALID, "config rejected"), nil)
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeApplyRejected, code, "an explicit agent rejection must never be disguised as APPLY_TIMEOUT")
		require.Contains(t, err.Error(), "config rejected")
	})

	t.Run("nil-status-ack-is-apply-rejected", func(t *testing.T) {
		raw, merr := proto.Marshal(&pb.UpdateFRPCResponse{})
		require.NoError(t, merr)
		code, err := classifyApplyResponse(&pb.ClientMessage{Data: raw}, nil)
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeApplyRejected, code)
	})

	t.Run("unparseable-ack-is-apply-rejected", func(t *testing.T) {
		code, err := classifyApplyResponse(&pb.ClientMessage{Data: []byte("garbage")}, nil)
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeApplyRejected, code, "a malformed ACK is a real answer, not a timeout")
	})

	t.Run("deadline-is-apply-timeout", func(t *testing.T) {
		code, err := classifyApplyResponse(nil, context.DeadlineExceeded)
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeApplyTimeout, code)
	})

	t.Run("offline-is-agent-offline", func(t *testing.T) {
		code, err := classifyApplyResponse(nil, errors.New("cannot get client, id: [cli-1]"))
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeAgentOffline, code)
	})

	t.Run("broken-connection-is-agent-offline", func(t *testing.T) {
		code, err := classifyApplyResponse(nil, errors.New("client return error: connection lost"))
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeAgentOffline, code)
	})

	t.Run("nil-ack-without-error-is-agent-offline", func(t *testing.T) {
		code, err := classifyApplyResponse(nil, nil)
		require.Error(t, err)
		require.Equal(t, defs.ErrorCodeAgentOffline, code)
	})
}

// TestBuildUpdateFRPCRequestCarriesReplaceClientID pins the wire payload of
// the directed switch: replace_client_id must travel to the agent, and an
// empty replacement must arrive as the empty string (never a stale value).
func TestBuildUpdateFRPCRequestCarriesReplaceClientID(t *testing.T) {
	req := buildUpdateFRPCRequest(ApplyRequest{
		PhysicalAgentID: "agent-1",
		TargetClientID:  "cli-child",
		ReplaceClientID: "cli-origin",
		ServerID:        "srv-1",
		Config:          []byte("cfg"),
	})
	require.Equal(t, "cli-child", req.GetClientId())
	require.Equal(t, "srv-1", req.GetServerId())
	require.Equal(t, "cli-origin", req.GetReplaceClientId(), "the directed switch must travel in replace_client_id")
	require.Equal(t, []byte("cfg"), req.GetConfig())

	plain := buildUpdateFRPCRequest(ApplyRequest{
		PhysicalAgentID: "agent-1",
		TargetClientID:  "cli-child",
		ServerID:        "srv-1",
		Config:          []byte("cfg"),
	})
	require.Empty(t, plain.GetReplaceClientId(), "an empty replacement must stay empty")
	require.NotNil(t, plain.ReplaceClientId, "the field must be present so GetReplaceClientId is deterministic")
}

// TestApplyRequestRejectsEmptyPhysicalAgent: the apply must fail closed
// when the physical agent key is missing (it is the ClientsManager lookup
// dimension — an empty key would address the wrong/no agent).
func TestApplyRequestRejectsEmptyPhysicalAgent(t *testing.T) {
	r := rpcApplyService{}
	code, err := r.Apply(context.Background(), nil, ApplyRequest{TargetClientID: "cli-child", ServerID: "srv-1", Config: []byte("cfg")})
	require.Error(t, err)
	require.Equal(t, defs.ErrorCodeAgentOffline, code)
	_ = lo.ToPtr(1) // keep lo import when assertions change
}
