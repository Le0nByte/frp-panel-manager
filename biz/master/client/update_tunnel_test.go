package client

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/originlock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestUpdateFrpcHanderFirstShadowReplacesOriginWithChildIdentity(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedUnshadowedOrigin()

	result := make(chan error, 1)
	go func() {
		_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-origin", "srv-1", []byte(validFrpcTOML)))
		result <- err
	}()

	wire := env.waitForEvent(stream)
	// Always release the production RPC/lock before assertions. A RED
	// assertion must not strand the handler goroutine and poison later tests.
	env.ackUpdateFRPC(stream, wire, pb.RespCode_RESP_CODE_SUCCESS, "ok")
	handlerErr := <-result

	require.Equal(t, pb.Event_EVENT_UPDATE_FRPC, wire.GetEvent())
	var update pb.UpdateFRPCRequest
	require.NoError(t, proto.Unmarshal(wire.GetData(), &update))
	require.NotEqual(t, "cli-origin", update.GetClientId(), "first shadow must target the generated child")
	require.Equal(t, "cli-origin", update.GetReplaceClientId(), "the Agent must remove the stale origin handler")

	var payload struct {
		Metadatas map[string]string `json:"metadatas"`
	}
	require.NoError(t, json.Unmarshal(update.GetConfig(), &payload))
	require.Equal(t, update.GetClientId(), payload.Metadatas[defs.FRPClientIDKey],
		"the FRP config identity must match the target child, not the original request id")
	require.NoError(t, handlerErr)
}

func TestUpdateFrpcHanderExistingChildUsesIdempotentOriginReplace(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	result := make(chan error, 1)
	go func() {
		_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
		result <- err
	}()

	wire := env.waitForEvent(stream)
	env.ackUpdateFRPC(stream, wire, pb.RespCode_RESP_CODE_SUCCESS, "ok")
	handlerErr := <-result

	var update pb.UpdateFRPCRequest
	require.NoError(t, proto.Unmarshal(wire.GetData(), &update))
	require.Equal(t, "cli-child", update.GetClientId())
	require.Equal(t, "cli-origin", update.GetReplaceClientId(),
		"idempotent replace cleans historical stale origin handlers")
	require.NoError(t, handlerErr)
}

// The master-side UpdateFrpcHander is the legacy frpc update route (and
// the agent-update helper of every live proxy writer). Round-8 I3: its
// DB-first body fired the agent call in a goroutine and returned SUCCESS
// before the agent had applied anything — silent DB/agent drift. These
// tests pin the synchronous-ACK contract: the handler returns only after
// the agent ACKed (or failed), the ACK payload must be a clean
// UpdateFRPCResponse SUCCESS, and every failure surfaces as an error.

func TestUpdateFrpcHanderSyncACKSuccess(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	env.ackUpdateFRPCLoop(stream, 1)

	resp, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// the RPC completed before the handler returned: exactly one event
	// was delivered and ACKed
	require.Empty(t, stream.sent, "no event may still be pending after the handler returned")
	require.Zero(t, env.recvMapLen(), "no session map entries may leak")
}

func TestUpdateFrpcHanderSyncACKRejectedReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()

	go func() {
		select {
		case req := <-stream.sent:
			env.ackUpdateFRPC(stream, req, pb.RespCode_RESP_CODE_INVALID, "nope")
		case <-time.After(5 * time.Second):
			// handler failed before sending; the test fails elsewhere
		}
	}()

	_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.Error(t, err, "an agent rejection must surface as an error, never a silent success")
	require.Contains(t, err.Error(), "nope")
	require.Zero(t, env.recvMapLen())
}

func TestUpdateFrpcHanderSyncACKTimeoutReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	env.registerAgent("cli-origin") // agent never replies
	env.seedShadowedPair()

	old := agentAckTimeout
	agentAckTimeout = 200 * time.Millisecond
	defer func() { agentAckTimeout = old }()

	start := time.Now()
	_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.Error(t, err, "a missing ACK must surface as an error")
	require.Less(t, time.Since(start), 5*time.Second, "timeout must be bounded by the injected ACK budget")
	require.Zero(t, env.recvMapLen(), "session map must be cleaned up after a timeout")
}

func TestUpdateFrpcHanderSyncACKOfflineReturnsError(t *testing.T) {
	env := newMasterClientEnv(t)
	env.seedShadowedPair() // no agent registered for cli-origin

	_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.Error(t, err, "an offline physical agent must surface as an error")
	require.Zero(t, env.recvMapLen())
}

// TestUpdateFrpcHanderStoppedOriginSkipsAgentCall: a stopped physical
// client keeps the upstream skip semantics — the DB is updated but no
// agent event is sent and the handler still succeeds.
func TestUpdateFrpcHanderStoppedOriginSkipsAgentCall(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	env.setOriginStopped(true)

	resp, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	select {
	case m := <-stream.sent:
		t.Fatalf("no event may be sent to a stopped origin, got: %s", m.GetEvent())
	default:
	}
	cfg := env.childConfig()
	require.NotEmpty(t, cfg.Proxies, "child config must still be updated in the DB")
}

// TestUpdateFrpcHanderLocksSameOriginKeyForChildAndOriginInput pins the
// key resolution wiring: both a child-addressed and an origin-addressed
// request must contend the SAME (tenant, origin) lock key.
func TestUpdateFrpcHanderLocksSameOriginKeyForChildAndOriginInput(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	env.ackUpdateFRPCLoop(stream, 2)

	rec := &recordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	// child-addressed request
	_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.NoError(t, err)

	// origin-addressed request
	_, err = UpdateFrpcHander(env.ctx, updateFrpcReq("cli-origin", "srv-1", []byte(validFrpcTOML)))
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.keys, 2, "exactly two Lock calls across the two requests")
	require.Equal(t, rec.keys[0], rec.keys[1], "child- and origin-addressed requests must lock the SAME key")
	require.Equal(t, originlock.Key(1, "cli-origin"), rec.keys[0])
	require.Equal(t, len(rec.keys), len(rec.unlocks), "every Lock must be paired with an Unlock")
}

// TestUpdateFrpcHanderLockedDoesNotLockAgain pins the reentrancy split:
// UpdateFrpcHanderLocked is the internal helper the proxy handlers call
// while ALREADY holding the lock — it must never lock again (a second
// Lock on the same key from the same goroutine would deadlock).
func TestUpdateFrpcHanderLockedDoesNotLockAgain(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedShadowedPair()
	env.ackUpdateFRPCLoop(stream, 1)

	rec := &recordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	_, err := UpdateFrpcHanderLocked(env.ctx, updateFrpcReq("cli-child", "srv-1", []byte(validFrpcTOML)))
	require.NoError(t, err)
	require.Empty(t, rec.keys, "UpdateFrpcHanderLocked must not acquire the lock")
}

// recordingLocker records every Lock/Unlock key (no blocking) — used to
// assert WHICH key a handler contends on without any concurrency.
type recordingLocker struct {
	mu      sync.Mutex
	keys    []string
	unlocks []string
}

func (r *recordingLocker) Lock(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
}

func (r *recordingLocker) Unlock(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unlocks = append(r.unlocks, key)
}

// TestRequireAckSuccessErrorMessages sanity-checks the ACK helper's
// failure classification (the helper itself is exercised through the
// handler tests above).
func TestRequireAckSuccessErrorMessages(t *testing.T) {
	require.Error(t, requireAckSuccess(nil, &pb.UpdateFRPCResponse{}), "nil response must fail")

	bad := &pb.ClientMessage{Data: []byte("not a proto")}
	require.Error(t, requireAckSuccess(bad, &pb.UpdateFRPCResponse{}), "unparseable ACK must fail")

	rej := &pb.ClientMessage{Data: mustMarshal(t, &pb.UpdateFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "rejected"}})}
	err := requireAckSuccess(rej, &pb.UpdateFRPCResponse{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "rejected"), "the agent message must reach the caller")

	ok := &pb.ClientMessage{Data: mustMarshal(t, &pb.UpdateFRPCResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS}})}
	require.NoError(t, requireAckSuccess(ok, &pb.UpdateFRPCResponse{}))
}
