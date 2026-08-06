package client

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/originlock"
	"github.com/stretchr/testify/require"
)

// DeleteClientHandler is a live legacy writer. It must share the same
// (tenant, origin) lock as SafeCreate and must not return success until the
// physical agent has ACKed EVENT_REMOVE_FRPC. The DB-first ordering remains
// documented debt for the D2 transaction migration; this suite pins the
// interim concurrency and ACK boundary.
func TestDeleteClientHandlerSyncACKRejectedReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	rejectData := mustMarshal(t, &pb.RemoveFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "refused"},
	})

	acked := make(chan struct{})
	go func() {
		defer close(acked)
		select {
		case req := <-stream.sent:
			stream.recv <- &pb.ClientMessage{Event: pb.Event_EVENT_REMOVE_FRPC, SessionId: req.GetSessionId(), Data: rejectData}
		case <-time.After(5 * time.Second):
			return
		}
	}()

	_, err := DeleteClientHandler(env.ctx, &pb.DeleteClientRequest{ClientId: strPtr("cli-origin")})
	<-acked
	require.Error(t, err, "an agent rejection must surface instead of returning success")
	require.Contains(t, err.Error(), "refused")
	require.Zero(t, env.recvMapLen(), "the RPC session must be cleaned after rejection")
}

func TestDeleteClientHandlerUsesSharedOriginLock(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	successData := mustMarshal(t, &pb.RemoveFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	})

	go func() {
		select {
		case req := <-stream.sent:
			stream.recv <- &pb.ClientMessage{Event: pb.Event_EVENT_REMOVE_FRPC, SessionId: req.GetSessionId(), Data: successData}
		case <-time.After(5 * time.Second):
		}
	}()

	rec := &recordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	resp, err := DeleteClientHandler(env.ctx, &pb.DeleteClientRequest{ClientId: strPtr("cli-origin")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []string{originlock.Key(1, "cli-origin")}, rec.keys)
	require.Equal(t, rec.keys, rec.unlocks)
}

func TestDeleteClientHandlerRejectsChildIDWithoutMutation(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair()

	_, err := DeleteClientHandler(env.ctx, &pb.DeleteClientRequest{ClientId: strPtr("cli-child")})
	require.Error(t, err, "the physical-agent delete endpoint must reject a shadow child id")

	_, qerr := dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-child")
	require.NoError(t, qerr, "rejecting a child delete must leave the child row intact")
	_, qerr = dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-origin")
	require.NoError(t, qerr, "rejecting a child delete must leave the origin row intact")
}

func strPtr(v string) *string { return &v }
