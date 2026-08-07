package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// ---- RunServerProbe: single server-side bind-probe gate (A4a) ----

func TestRunServerProbeCapturesSingleRPC(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if !res.Ok {
		t.Fatalf("AVAILABLE must pass the server gate: %+v", res)
	}
	if res.ErrorCode != "" || res.ErrorMessage != "" {
		t.Fatalf("no error expected on success, got %q/%q", res.ErrorCode, res.ErrorMessage)
	}
	if len(env.requests) != 1 {
		t.Fatalf("expected exactly 1 validate RPC, got %d", len(env.requests))
	}
	req := env.requests[0]
	if req.GetNodeId() != testServerID {
		t.Fatalf("server gate must target the server node, got %q", req.GetNodeId())
	}
	if req.GetPort() != 8000 || req.GetProtocol() != "tcp" {
		t.Fatalf("server gate must probe remote port/protocol, got %d/%q", req.GetPort(), req.GetProtocol())
	}
	if !req.GetBindProbe() {
		t.Fatal("server gate must use bind_probe=true")
	}
	if req.GetLocalAddress() != "0.0.0.0" {
		t.Fatalf("server gate must bind 0.0.0.0, got %q", req.GetLocalAddress())
	}
	if !req.GetIncludeDocker() {
		t.Fatal("server gate must include docker listeners")
	}
	// the client agent must stay untouched: a single gate is a single RPC
	select {
	case m := <-env.client.sent:
		t.Fatalf("RunServerProbe must not contact the client agent, got %+v", m)
	default:
	}
	if res.Observation == nil || res.Observation.GetState() != pb.PortState_STATE_AVAILABLE {
		t.Fatalf("AVAILABLE observation must be preserved: %+v", res.Observation)
	}
}

func TestRunServerProbeListeningFallsBackToRemoteInUse(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, "", pb.PortState_STATE_LISTENING, "tcp", "0.0.0.0", 8000)
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if res.Ok {
		t.Fatal("LISTENING must reject the server gate")
	}
	if res.ErrorCode != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortRemoteInUse)
	}
	if res.ErrorMessage != "remote port is not available on the server" {
		t.Fatalf("default message expected, got %q", res.ErrorMessage)
	}
	if res.Observation == nil || res.Observation.GetState() != pb.PortState_STATE_LISTENING {
		t.Fatalf("conflict observation must be preserved as evidence: %+v", res.Observation)
	}
}

func TestRunServerProbeUnknownFallsBackToRemoteInUse(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, "", pb.PortState_STATE_UNKNOWN, "tcp", "0.0.0.0", 8000)
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if res.Ok {
		t.Fatal("UNKNOWN must reject the server gate")
	}
	if res.ErrorCode != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s (remote-in-use fallback)", res.ErrorCode, defs.ErrorCodePortRemoteInUse)
	}
}

func TestRunServerProbeAgentErrorCodeWins(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, defs.ErrorCodePortScanUnavailable, pb.PortState_STATE_UNKNOWN, "tcp", "0.0.0.0", 8000)
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if res.Ok {
		t.Fatal("UNKNOWN must reject the server gate")
	}
	if res.ErrorCode != defs.ErrorCodePortScanUnavailable {
		t.Fatalf("agent-provided code must win, got %q want %s", res.ErrorCode, defs.ErrorCodePortScanUnavailable)
	}
}

func TestRunServerProbeForeignListenerNeverAccepted(t *testing.T) {
	// Even a lying agent (Ok=true on an occupied port) must fail closed:
	// the gate requires the AVAILABLE state, so no arbitrary listener —
	// regardless of PID/process — can ever pass. Owner proof is A4b's
	// Verifier job, not this gate's.
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		obs := &pb.PortObservation{
			NodeId: "node", Protocol: "tcp", IpFamily: "v4",
			LocalAddress: "0.0.0.0", LocalPort: 8000,
			State: pb.PortState_STATE_LISTENING, Pid: 12345, ProcessName: "foreign-frps",
		}
		data, _ := proto.Marshal(&pb.ValidatePortResponse{
			Status:      &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
			Observation: obs,
			Ok:          lo.ToPtr(true),
		})
		return &pb.ClientMessage{Event: pb.Event_EVENT_VALIDATE_PORT, Data: data}
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if res.Ok {
		t.Fatal("a foreign listener must never pass the server gate")
	}
	if res.ErrorCode != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortRemoteInUse)
	}
	if res.Observation == nil || res.Observation.GetPid() != 12345 {
		t.Fatalf("conflict evidence must be preserved: %+v", res.Observation)
	}
}

func TestRunServerProbeActiveStillRejected(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_ACTIVE, "tcp", "0.0.0.0", 8000)
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunServerProbe error: %v", err)
	}
	if res.Ok {
		t.Fatal("ACTIVE means the public port is occupied and must never pass a bind probe")
	}
	if res.ErrorCode != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortRemoteInUse)
	}
}

func TestRunServerProbeOfflineAgent(t *testing.T) {
	env := newPreflightEnv(t)
	env.app.cm.Remove(testServerID)

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("transport failure must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("offline server must reject")
	}
	if res.ErrorCode != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeAgentOffline)
	}
	if res.ErrorMessage == "" {
		t.Fatal("offline must carry a message")
	}
	if len(env.requests) != 0 {
		t.Fatalf("offline lookup must not emit an RPC, got %d", len(env.requests))
	}
}

func TestRunServerProbeTimeout(t *testing.T) {
	old := preflightTimeout
	preflightTimeout = 200 * time.Millisecond
	defer func() { preflightTimeout = old }()

	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return nil // never replies
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("timeout must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("unresponsive server must reject")
	}
	if res.ErrorCode != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s (timeout = offline)", res.ErrorCode, defs.ErrorCodeAgentOffline)
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

func TestRunServerProbeBadProtobuf(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return &pb.ClientMessage{Event: pb.Event_EVENT_VALIDATE_PORT, Data: []byte("garbage-not-protobuf")}
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("unparseable reply must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("unparseable reply must reject")
	}
	if res.ErrorCode != defs.ErrorCodePortScanUnavailable {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortScanUnavailable)
	}
}

func TestRunServerProbeAgentNonSuccessStatus(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		data, _ := proto.Marshal(&pb.ValidatePortResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "boom"},
		})
		return &pb.ClientMessage{Event: pb.Event_EVENT_VALIDATE_PORT, Data: data}
	})

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("agent non-success must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("agent non-success must reject")
	}
	if res.ErrorCode != defs.ErrorCodePortScanUnavailable {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortScanUnavailable)
	}
	if res.ErrorMessage != "agent validation failed: boom" {
		t.Fatalf("unexpected message: %q", res.ErrorMessage)
	}
}

// ---- RunClient: single client-side local-listen gate (A4a) ----

func TestRunClientCapturesSingleRPC(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunClient error: %v", err)
	}
	if !res.Ok {
		t.Fatalf("LISTENING must pass the client gate: %+v", res)
	}
	if res.ErrorCode != "" || res.ErrorMessage != "" {
		t.Fatalf("no error expected on success, got %q/%q", res.ErrorCode, res.ErrorMessage)
	}
	if len(env.requests) != 1 {
		t.Fatalf("expected exactly 1 validate RPC, got %d", len(env.requests))
	}
	req := env.requests[0]
	if req.GetNodeId() != testClientID {
		t.Fatalf("client gate must target the client node, got %q", req.GetNodeId())
	}
	if req.GetPort() != 8080 || req.GetProtocol() != "tcp" {
		t.Fatalf("client gate must probe local port/protocol, got %d/%q", req.GetPort(), req.GetProtocol())
	}
	if req.GetLocalAddress() != "127.0.0.1" {
		t.Fatalf("client gate must probe the local ip, got %q", req.GetLocalAddress())
	}
	if req.GetBindProbe() {
		t.Fatal("client gate must use bind_probe=false")
	}
	// the server agent must stay untouched
	select {
	case m := <-env.server.sent:
		t.Fatalf("RunClient must not contact the server agent, got %+v", m)
	default:
	}
	if res.Observation == nil || res.Observation.GetState() != pb.PortState_STATE_LISTENING {
		t.Fatalf("LISTENING observation must be preserved: %+v", res.Observation)
	}
}

func TestRunClientActivePasses(t *testing.T) {
	// Port inspector upgrades a healthy listener to ACTIVE when it observes
	// live connections. ACTIVE therefore satisfies the client-side
	// "local service exists" gate just like LISTENING.
	env := newPreflightEnv(t)
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_ACTIVE, "tcp", "127.0.0.1", 8080)
	})

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunClient error: %v", err)
	}
	if !res.Ok {
		t.Fatalf("ACTIVE healthy local service must pass the client gate: %+v", res)
	}
	if res.Observation == nil || res.Observation.GetState() != pb.PortState_STATE_ACTIVE {
		t.Fatalf("ACTIVE observation must be preserved: %+v", res.Observation)
	}
}

func TestRunClientAvailableWarnsButPasses(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, "", pb.PortState_STATE_AVAILABLE, "tcp", "127.0.0.1", 8080)
	})

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunClient error: %v", err)
	}
	// 2026-08 user decision: AVAILABLE is advisory, not a rejection.
	if !res.Ok {
		t.Fatal("AVAILABLE must PASS the client gate (warn-graded)")
	}
	if res.ErrorCode != defs.ErrorCodePortLocalNotListening {
		t.Fatalf("error code = %q, want advisory %s", res.ErrorCode, defs.ErrorCodePortLocalNotListening)
	}
	if res.ErrorMessage == "" {
		t.Fatal("warning message expected")
	}
	if res.Observation == nil || res.Observation.GetState() != pb.PortState_STATE_AVAILABLE {
		t.Fatalf("observation must be preserved as evidence: %+v", res.Observation)
	}
}

func TestRunClientUnknownRejects(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, "", pb.PortState_STATE_UNKNOWN, "tcp", "127.0.0.1", 8080)
	})

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunClient error: %v", err)
	}
	if res.Ok {
		t.Fatal("UNKNOWN must reject the client gate")
	}
	if res.ErrorCode != defs.ErrorCodePortScanUnavailable {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodePortScanUnavailable)
	}
}

func TestRunClientAvailablePassesWithWarningEvenWhenAgentSaysOk(t *testing.T) {
	// A lying agent (Ok=true while nothing listens) still yields the
	// advisory warning: AVAILABLE passes (warn-graded) but MUST carry
	// PORT_LOCAL_NOT_LISTENING so the UI shows the warning.
	env := newPreflightEnv(t)
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		obs := &pb.PortObservation{
			NodeId: "node", Protocol: "tcp", IpFamily: "v4",
			LocalAddress: "127.0.0.1", LocalPort: 8080,
			State: pb.PortState_STATE_AVAILABLE,
		}
		data, _ := proto.Marshal(&pb.ValidatePortResponse{
			Status:      &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
			Observation: obs,
			Ok:          lo.ToPtr(true),
		})
		return &pb.ClientMessage{Event: pb.Event_EVENT_VALIDATE_PORT, Data: data}
	})

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("RunClient error: %v", err)
	}
	if !res.Ok {
		t.Fatal("AVAILABLE must pass the client gate (warn-graded)")
	}
	if res.ErrorCode != defs.ErrorCodePortLocalNotListening {
		t.Fatalf("error code = %q, want advisory %s", res.ErrorCode, defs.ErrorCodePortLocalNotListening)
	}
	if res.ErrorMessage == "" {
		t.Fatal("warning message expected")
	}
}

func TestRunClientOfflineAgent(t *testing.T) {
	env := newPreflightEnv(t)
	env.app.cm.Remove(testClientID)

	var runner preflightRunner
	res, err := runner.RunClient(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("transport failure must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("offline client must reject")
	}
	if res.ErrorCode != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeAgentOffline)
	}
	if res.ErrorMessage == "" {
		t.Fatal("offline must carry a message")
	}
}

// ---- input handling: explicit fields / single config / rejections ----

func TestRunServerProbeExplicitFieldsWinOverConfig(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "udp", "0.0.0.0", 9000)
	})
	req := basePreflightReq()
	req.Protocol = lo.ToPtr("udp")
	req.RemotePort = lo.ToPtr(uint32(9000))
	req.LocalPort = lo.ToPtr(uint32(9090))
	// conflicting config bytes must be ignored
	req.Config = []byte("[[proxies]]\nname = \"x\"\ntype = \"tcp\"\nlocalPort = 1\nremotePort = 1\n")

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), req)
	if err != nil || !res.Ok {
		t.Fatalf("explicit fields must win: err=%v res=%+v", err, res)
	}
	if env.requests[0].GetProtocol() != "udp" || env.requests[0].GetPort() != 9000 {
		t.Fatalf("explicit fields ignored: %+v", env.requests[0])
	}
}

func TestRunServerProbeParsesSingleConfig(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_AVAILABLE, "tcp", "0.0.0.0", 8000)
	})
	req := &pb.PreflightRequest{
		ClientId: lo.ToPtr(testClientID),
		ServerId: lo.ToPtr(testServerID),
		Config:   candidateWebTOML,
	}

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), req)
	if err != nil || !res.Ok {
		t.Fatalf("single config must parse: err=%v res=%+v", err, res)
	}
	if env.requests[0].GetPort() != 8000 || env.requests[0].GetProtocol() != "tcp" {
		t.Fatalf("config target ignored: %+v", env.requests[0])
	}
}

func TestRunServerProbeUnsupportedType(t *testing.T) {
	env := newPreflightEnv(t)
	req := &pb.PreflightRequest{
		ClientId:  lo.ToPtr(testClientID),
		ServerId:  lo.ToPtr(testServerID),
		ProxyType: lo.ToPtr("http"),
		Config:    []byte("[[proxies]]\nname = \"web\"\ntype = \"http\"\nlocalPort = 8080\ncustomDomains = [\"example.com\"]\n"),
	}

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("input rejection must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("http must be rejected by the port-managed gates")
	}
	if res.ErrorCode != defs.ErrorCodeUnsupportedProxyType {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeUnsupportedProxyType)
	}
	if len(env.requests) != 0 {
		t.Fatalf("no RPC may be emitted on input rejection, got %d", len(env.requests))
	}
}

func TestRunServerProbeMultipleConfigs(t *testing.T) {
	env := newPreflightEnv(t)
	req := &pb.PreflightRequest{
		ClientId: lo.ToPtr(testClientID),
		ServerId: lo.ToPtr(testServerID),
		Config: []byte("[[proxies]]\nname = \"a\"\ntype = \"tcp\"\nlocalPort = 1\nremotePort = 1\n" +
			"[[proxies]]\nname = \"b\"\ntype = \"tcp\"\nlocalPort = 2\nremotePort = 2\n"),
	}

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("input rejection must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("multiple rules must be rejected")
	}
	if res.ErrorCode != defs.ErrorCodeConfigValidationFailed {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeConfigValidationFailed)
	}
	if len(env.requests) != 0 {
		t.Fatalf("no RPC may be emitted on input rejection, got %d", len(env.requests))
	}
}

func TestRunServerProbeMissingNodeIDs(t *testing.T) {
	env := newPreflightEnv(t)

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), &pb.PreflightRequest{})
	if err != nil {
		t.Fatalf("input rejection must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("missing node ids must be rejected")
	}
	if res.ErrorCode != defs.ErrorCodeConfigValidationFailed {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeConfigValidationFailed)
	}
	if res.ErrorMessage != "client_id and server_id are required" {
		t.Fatalf("unexpected message: %q", res.ErrorMessage)
	}
	if len(env.requests) != 0 {
		t.Fatalf("no RPC may be emitted on input rejection, got %d", len(env.requests))
	}
}

func TestRunServerProbeNoTarget(t *testing.T) {
	env := newPreflightEnv(t)
	req := &pb.PreflightRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID)}

	var runner preflightRunner
	res, err := runner.RunServerProbe(preflightCtx(env), req)
	if err != nil {
		t.Fatalf("input rejection must be structured, got error %v", err)
	}
	if res.Ok {
		t.Fatal("request with neither fields nor config must be rejected")
	}
	if res.ErrorCode != defs.ErrorCodeConfigValidationFailed {
		t.Fatalf("error code = %q, want %s", res.ErrorCode, defs.ErrorCodeConfigValidationFailed)
	}
}

// ---- public Preflight regression through the runner interface ----

func TestPreflightRunnerRunServerFailSkipsClient(t *testing.T) {
	env := newPreflightEnv(t)
	env.pump(t, testServerID, env.server, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(false, "", pb.PortState_STATE_LISTENING, "tcp", "0.0.0.0", 8000)
	})
	env.pump(t, testClientID, env.client, func(req *pb.ServerMessage) *pb.ClientMessage {
		return validateResp(true, "", pb.PortState_STATE_LISTENING, "tcp", "127.0.0.1", 8080)
	})

	var runner preflightRunner
	resp, err := runner.Run(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetServerOk() {
		t.Fatal("server gate must fail on occupied port")
	}
	if resp.GetErrorCode() != defs.ErrorCodePortRemoteInUse {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodePortRemoteInUse)
	}
	if resp.GetErrorMessage() != "remote port is not available on the server" {
		t.Fatalf("default message expected, got %q", resp.GetErrorMessage())
	}
	if resp.ClientOk != nil {
		t.Fatal("client gate must not be evaluated when the server gate fails")
	}
	if len(resp.GetServerChecks()) != 1 {
		t.Fatalf("server evidence missing: %+v", resp.GetServerChecks())
	}
	if len(env.requests) != 1 {
		t.Fatalf("client gate must not run when server gate fails, got %d RPCs", len(env.requests))
	}
}

func TestPreflightRunnerRunBothSidesOKWithPerSideBudgets(t *testing.T) {
	oldFactory := newPreflightContext
	defer func() { newPreflightContext = oldFactory }()

	var mu sync.Mutex
	var timeouts []time.Duration
	var ctxs []context.Context
	newPreflightContext = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		mu.Lock()
		timeouts = append(timeouts, timeout)
		ctxs = append(ctxs, ctx)
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

	var runner preflightRunner
	resp, err := runner.Run(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if !resp.GetServerOk() || !resp.GetClientOk() {
		t.Fatalf("both gates must pass: server=%v client=%v err=%q", resp.GetServerOk(), resp.GetClientOk(), resp.GetErrorMessage())
	}
	if resp.GetErrorCode() != "" {
		t.Fatalf("unexpected error code: %s", resp.GetErrorCode())
	}
	if len(resp.GetServerChecks()) != 1 || len(resp.GetClientChecks()) != 1 {
		t.Fatalf("expected 1 evidence per side, got %d/%d", len(resp.GetServerChecks()), len(resp.GetClientChecks()))
	}
	if len(env.requests) != 2 {
		t.Fatalf("expected 2 validate calls, got %d", len(env.requests))
	}
	if !env.requests[0].GetBindProbe() || env.requests[1].GetBindProbe() {
		t.Fatal("server gate must bind-probe, client gate must not")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(timeouts) != 2 {
		t.Fatalf("expected exactly 2 per-side contexts, got %d", len(timeouts))
	}
	if timeouts[0] != preflightTimeout || timeouts[1] != preflightTimeout {
		t.Fatalf("each side must get the full budget %v, got %v / %v", preflightTimeout, timeouts[0], timeouts[1])
	}
	if ctxs[0] == ctxs[1] {
		t.Fatal("server and client must not share one context")
	}
}

func TestPreflightRunnerRunServerOffline(t *testing.T) {
	env := newPreflightEnv(t)
	env.app.cm.Remove(testServerID)

	var runner preflightRunner
	resp, err := runner.Run(preflightCtx(env), basePreflightReq())
	if err != nil {
		t.Fatalf("Preflight error: %v", err)
	}
	if resp.GetServerOk() {
		t.Fatal("offline server must reject")
	}
	if resp.GetErrorCode() != defs.ErrorCodeAgentOffline {
		t.Fatalf("error code = %q, want %s", resp.GetErrorCode(), defs.ErrorCodeAgentOffline)
	}
	if resp.GetErrorMessage() != "cannot get client, id: [srv-1]" {
		t.Fatalf("unexpected message: %q", resp.GetErrorMessage())
	}
	if resp.ClientOk != nil {
		t.Fatal("client gate must not be evaluated when the server transport fails")
	}
}

// ---- fakePreflightRunner: Run unchanged, gate counters independent ----

func TestFakePreflightRunnerGateCountsIndependent(t *testing.T) {
	f := &fakePreflightRunner{serverOK: true, clientOK: true}
	ctx := preflightCtx(newPreflightEnv(t))

	if _, err := f.Run(ctx, basePreflightReq()); err != nil {
		t.Fatalf("fake Run error: %v", err)
	}
	res, err := f.RunServerProbe(ctx, basePreflightReq())
	if err != nil {
		t.Fatalf("fake RunServerProbe error: %v", err)
	}
	if !res.Ok {
		t.Fatalf("scripted server gate must be ok: %+v", res)
	}
	res2, err := f.RunClient(ctx, basePreflightReq())
	if err != nil {
		t.Fatalf("fake RunClient error: %v", err)
	}
	if !res2.Ok {
		t.Fatalf("scripted client gate must be ok: %+v", res2)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 1 {
		t.Fatalf("Run must count independently, got %d", f.calls)
	}
	if f.serverProbeCalls != 1 || f.clientGateCalls != 1 {
		t.Fatalf("gate counters must count independently: server=%d client=%d",
			f.serverProbeCalls, f.clientGateCalls)
	}
}

func TestFakePreflightRunnerScriptableGateResults(t *testing.T) {
	ctx := preflightCtx(newPreflightEnv(t))

	f := &fakePreflightRunner{
		serverProbeResult: &PreflightGateResult{Ok: false, ErrorCode: defs.ErrorCodePortRemoteInUse, ErrorMessage: "occupied"},
	}
	res, err := f.RunServerProbe(ctx, basePreflightReq())
	if err != nil {
		t.Fatalf("fake RunServerProbe error: %v", err)
	}
	if res.Ok || res.ErrorCode != defs.ErrorCodePortRemoteInUse || res.ErrorMessage != "occupied" {
		t.Fatalf("scripted gate result must be returned verbatim: %+v", res)
	}
	// scripted result must not leak into Run
	f.mu.Lock()
	run := f.calls
	f.mu.Unlock()
	if run != 0 {
		t.Fatalf("RunServerProbe must not touch the Run counter, got %d", run)
	}
}
