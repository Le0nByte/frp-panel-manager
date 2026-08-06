package client

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func startLifecycleACK(stream *fakeAgentStream, code pb.RespCode, message string) <-chan struct{} {
	done := make(chan struct{})
	data, err := proto.Marshal(&pb.RemoveFRPCResponse{Status: &pb.Status{Code: code, Message: message}})
	if err != nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		select {
		case req := <-stream.sent:
			stream.recv <- &pb.ClientMessage{Event: req.GetEvent(), SessionId: req.GetSessionId(), Data: data}
		case <-time.After(5 * time.Second):
		}
	}()
	return done
}

func TestRemoveFrpcHandlerSyncACKRejectedReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	ackDone := startLifecycleACK(stream, pb.RespCode_RESP_CODE_INVALID, "remove refused")
	_, err := RemoveFrpcHandler(env.ctx, &pb.RemoveFRPCRequest{ClientId: strPtr("cli-origin")})
	<-ackDone
	require.Error(t, err, "REMOVE_FRPC rejection must not be reported as success")
	require.Contains(t, err.Error(), "remove refused")
	require.Zero(t, env.recvMapLen(), "the lifecycle RPC session must be cleaned")
}

func TestStartFRPCHandlerSyncACKRejectedReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	ackDone := startLifecycleACK(stream, pb.RespCode_RESP_CODE_INVALID, "start refused")
	_, err := StartFRPCHandler(env.ctx, &pb.StartFRPCRequest{ClientId: strPtr("cli-origin")})
	<-ackDone
	require.Error(t, err, "START_FRPC rejection must not be reported as success")
	require.Contains(t, err.Error(), "start refused")
	require.Zero(t, env.recvMapLen())
}

func TestStopFRPCHandlerSyncACKRejectedReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	ackDone := startLifecycleACK(stream, pb.RespCode_RESP_CODE_INVALID, "stop refused")
	_, err := StopFRPCHandler(env.ctx, &pb.StopFRPCRequest{ClientId: strPtr("cli-origin")})
	<-ackDone
	require.Error(t, err, "STOP_FRPC rejection must not be reported as success")
	require.Contains(t, err.Error(), "stop refused")
	require.Zero(t, env.recvMapLen())
}

func TestPhysicalLifecycleHandlersUseSameOriginLock(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	old := liveOriginLocker
	rec := &recordingLocker{}
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for i := 0; i < 3; i++ {
			select {
			case req := <-stream.sent:
				data, _ := proto.Marshal(&pb.RemoveFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}})
				stream.recv <- &pb.ClientMessage{Event: req.GetEvent(), SessionId: req.GetSessionId(), Data: data}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	_, err := StartFRPCHandler(env.ctx, &pb.StartFRPCRequest{ClientId: strPtr("cli-origin")})
	require.NoError(t, err)
	_, err = StopFRPCHandler(env.ctx, &pb.StopFRPCRequest{ClientId: strPtr("cli-origin")})
	require.NoError(t, err)
	_, err = RemoveFrpcHandler(env.ctx, &pb.RemoveFRPCRequest{ClientId: strPtr("cli-origin")})
	require.NoError(t, err)
	<-ackDone

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.keys, 3)
	require.Equal(t, rec.keys[0], rec.keys[1])
	require.Equal(t, rec.keys[1], rec.keys[2])
	require.Equal(t, rec.keys, rec.unlocks)
}

// ---- origin-only gating ----

// EVENT_REMOVE_FRPC terminates the whole physical Agent process after its
// ACK; START/STOP are physical-Agent-level StartAll/StopAll. A shadow child
// is only ONE logical handler inside that process, so routing a child id
// through these endpoints would take every sibling down. The legacy
// physical lifecycle endpoints must reject child ids BEFORE any DB
// mutation, returning INVALID + error with zero DB delta.

func TestRemoveFrpcHandlerRejectsChildIDWithoutMutation(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair()

	resp, err := RemoveFrpcHandler(env.ctx, &pb.RemoveFRPCRequest{ClientId: strPtr("cli-child")})
	require.Error(t, err, "the physical-agent delete endpoint must reject a shadow child id")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())

	_, qerr := dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-child")
	require.NoError(t, qerr, "rejecting a child remove must leave the child row intact")
	_, qerr = dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-origin")
	require.NoError(t, qerr, "rejecting a child remove must leave the origin row intact")
	require.Zero(t, env.recvMapLen(), "no RPC session may be created for a rejected child")
}

func TestStartFRPCHandlerRejectsChildIDWithoutMutation(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair()
	env.setOriginStopped(true) // a child-addressed start must NOT flip it back to false

	resp, err := StartFRPCHandler(env.ctx, &pb.StartFRPCRequest{ClientId: strPtr("cli-child")})
	require.Error(t, err, "the physical start endpoint must reject a shadow child id")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())

	cli, qerr := dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-origin")
	require.NoError(t, qerr)
	require.True(t, cli.ClientEntity.Stopped, "child-addressed start must not touch the origin's Stopped flag")
	require.Zero(t, env.recvMapLen(), "no RPC session may be created for a rejected child")
}

func TestStopFRPCHandlerRejectsChildIDWithoutMutation(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair() // origin starts Stopped=false

	resp, err := StopFRPCHandler(env.ctx, &pb.StopFRPCRequest{ClientId: strPtr("cli-child")})
	require.Error(t, err, "the physical stop endpoint must reject a shadow child id")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())

	cli, qerr := dao.NewQuery(env.ctx).GetClientByClientID(env.user, "cli-origin")
	require.NoError(t, qerr)
	require.False(t, cli.ClientEntity.Stopped, "child-addressed stop must not flip the origin's Stopped flag")
	require.Zero(t, env.recvMapLen(), "no RPC session may be created for a rejected child")
}

// ---- hold-until-ACK + offline/timeout boundaries ----

// TestLifecycleHandlersHoldUntilAgentACK pins the synchronous-ACK contract
// per route: after the agent event is SENT the handler MUST NOT return
// while the ACK is withheld, and MUST return nil once SUCCESS is ACKed.
// Deterministic — the event send is observed first, at which point the
// handler is provably blocked on the ACK (nothing else can release it).
func TestLifecycleHandlersHoldUntilAgentACK(t *testing.T) {
	cases := []struct {
		name  string
		event pb.Event
		call  func(env *masterClientEnv) error
		ack   proto.Message
	}{
		{
			name:  "start",
			event: pb.Event_EVENT_START_FRPC,
			call: func(env *masterClientEnv) error {
				_, err := StartFRPCHandler(env.ctx, &pb.StartFRPCRequest{ClientId: strPtr("cli-origin")})
				return err
			},
			ack: &pb.StartFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}},
		},
		{
			name:  "stop",
			event: pb.Event_EVENT_STOP_FRPC,
			call: func(env *masterClientEnv) error {
				_, err := StopFRPCHandler(env.ctx, &pb.StopFRPCRequest{ClientId: strPtr("cli-origin")})
				return err
			},
			ack: &pb.StopFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}},
		},
		{
			name:  "remove",
			event: pb.Event_EVENT_REMOVE_FRPC,
			call: func(env *masterClientEnv) error {
				_, err := RemoveFrpcHandler(env.ctx, &pb.RemoveFRPCRequest{ClientId: strPtr("cli-origin")})
				return err
			},
			ack: &pb.RemoveFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newMasterClientEnv(t)
			stream := env.registerAgent("cli-origin")
			env.seedShadowedPair()

			result := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				result <- tc.call(env)
			}()
			t.Cleanup(func() {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("handler goroutine did not finish — ACK was never delivered")
				}
			})

			var req *pb.ServerMessage
			select {
			case req = <-stream.sent:
				require.Equal(t, tc.event, req.GetEvent(), "the handler must send its own physical-level event")
			case <-time.After(5 * time.Second):
				t.Fatal("no agent event was sent")
			}

			// Event is held: the handler is blocked waiting for the ACK and
			// MUST NOT have returned. This is deterministic — the handler's
			// only release paths are our ACK or the 15s agentAckTimeout.
			select {
			case <-result:
				t.Fatal("handler returned before the agent ACKed")
			default:
			}

			stream.recv <- &pb.ClientMessage{Event: tc.event, SessionId: req.GetSessionId(), Data: mustMarshal(t, tc.ack)}

			select {
			case err := <-result:
				require.NoError(t, err, "a SUCCESS ACK must make the handler return nil")
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not return after the ACK")
			}
			require.Zero(t, env.recvMapLen(), "the RPC session must be cleaned after the ACK")
			require.Empty(t, stream.sent, "no event may be pending after the handler returned")
		})
	}
}

func TestStopFRPCHandlerSyncACKTimeoutReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	env.registerAgent("cli-origin") // agent never replies
	env.seedShadowedPair()

	old := agentAckTimeout
	agentAckTimeout = 200 * time.Millisecond
	defer func() { agentAckTimeout = old }()

	start := time.Now()
	_, err := StopFRPCHandler(env.ctx, &pb.StopFRPCRequest{ClientId: strPtr("cli-origin")})
	require.Error(t, err, "a missing ACK must surface as an error")
	require.Less(t, time.Since(start), 5*time.Second, "timeout must be bounded by the injected ACK budget")
	require.Zero(t, env.recvMapLen(), "the RPC session must be cleaned after a timeout")
}

func TestStartFRPCHandlerSyncACKOfflineReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair() // no agent registered for cli-origin

	_, err := StartFRPCHandler(env.ctx, &pb.StartFRPCRequest{ClientId: strPtr("cli-origin")})
	require.Error(t, err, "an offline physical agent must surface as an error")
	require.Zero(t, env.recvMapLen())
}
