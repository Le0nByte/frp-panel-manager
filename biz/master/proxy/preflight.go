package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// preflightTimeout bounds each agent RPC (spec §8.4: port scan 5s).
// Overridable in tests.
var preflightTimeout = 5 * time.Second

// newPreflightContext creates the per-side RPC context for Preflight.
// It is a variable so tests can verify per-side independence without
// wall-clock sleeps; the production default is context.WithTimeout and
// is never changed by tests.
var newPreflightContext = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// preflightTarget is the resolved candidate rule for the two-sided gate.
type preflightTarget struct {
	Protocol   string // tcp / udp
	RemotePort uint32
	LocalIP    string
	LocalPort  uint32
	ProxyName  string
}

// PreflightGateResult is the structured outcome of ONE side of the
// two-sided port gate (spec §7.3). Every reachable failure — transport,
// agent decision, observation evidence — is expressible in
// ErrorCode/ErrorMessage, so gate calls never need a non-nil error for
// business rejections. Observation carries the evidence the UI renders
// (node/protocol/port/owner) and is preserved on both success and
// rejection.
type PreflightGateResult struct {
	Ok           bool
	ErrorCode    string
	ErrorMessage string
	Observation  *pb.PortObservation
}

// gateResultError builds the structured transport/agent failure result.
func gateResultError(code, msg string) *PreflightGateResult {
	return &PreflightGateResult{Ok: false, ErrorCode: code, ErrorMessage: msg}
}

// observedState extracts the port state evidence from an agent reply;
// a missing observation is treated as UNKNOWN (fail closed).
func observedState(check *pb.ValidatePortResponse) pb.PortState {
	if obs := check.GetObservation(); obs != nil {
		return obs.GetState()
	}
	return pb.PortState_STATE_UNKNOWN
}

// resolvePreflightTarget validates the request shape and resolves the
// candidate rule exactly once, mapping business input errors to their
// stable codes (CONFIG_VALIDATION_FAILED / UNSUPPORTED_PROXY_TYPE).
// ok=false means the caller must reject with code/msg and touch no agent.
func resolvePreflightTarget(req *pb.PreflightRequest) (target preflightTarget, code, msg string, ok bool) {
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 {
		return preflightTarget{}, defs.ErrorCodeConfigValidationFailed, "client_id and server_id are required", false
	}
	target, err := extractPreflightTargets(req)
	if err != nil {
		code := defs.ErrorCodeConfigValidationFailed
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			code = defs.ErrorCodeUnsupportedProxyType
		}
		return preflightTarget{}, code, err.Error(), false
	}
	return target, "", "", true
}

// Preflight runs the two-sided gate (spec §5.4/§5.5, §7.3) without
// modifying any config:
//
//  1. Server gate: EVENT_VALIDATE_PORT with bind_probe=true on the remote
//     port. Savable only when AVAILABLE (a fresh bind probe succeeds).
//  2. Client gate: EVENT_VALIDATE_PORT with bind_probe=false on the local
//     ip:port. Savable when LISTENING or ACTIVE (a real service exists;
//     ACTIVE means the listener currently has traffic/connections).
//
// UNKNOWN, agent offline and timeout all reject the save. The response
// carries per-side evidence (observation + error code) for the UI to render
// the specific node/protocol/port/owner (spec §7.3).
//
// A4a: the request shape is validated and the candidate target resolved
// exactly once; both sides then reuse the same single-gate helpers that
// preflightRunner.RunServerProbe / RunClient expose individually. The
// public behavior is unchanged: business rejections return (resp, nil).
func Preflight(c *app.Context, req *pb.PreflightRequest) (*pb.PreflightResponse, error) {
	target, code, msg, ok := resolvePreflightTarget(req)
	if !ok {
		return &pb.PreflightResponse{
			Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
			ErrorCode:    lo.ToPtr(code),
			ErrorMessage: lo.ToPtr(msg),
		}, nil
	}

	resp := &pb.PreflightResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}

	// 1. server gate: the public port must be bindable on the server node
	serverRes := runServerGate(c, req, target)
	resp.ServerOk = lo.ToPtr(serverRes.Ok)
	if serverRes.Observation != nil {
		resp.ServerChecks = []*pb.PortObservation{serverRes.Observation}
	}
	if !serverRes.Ok {
		resp.ErrorCode = lo.ToPtr(serverRes.ErrorCode)
		resp.ErrorMessage = lo.ToPtr(serverRes.ErrorMessage)
		return resp, nil
	}

	// 2. client gate: a real local service must be listening on the target
	clientRes := runClientGate(c, req, target)
	resp.ClientOk = lo.ToPtr(clientRes.Ok)
	if clientRes.Observation != nil {
		resp.ClientChecks = []*pb.PortObservation{clientRes.Observation}
	}
	if !clientRes.Ok {
		resp.ErrorCode = lo.ToPtr(clientRes.ErrorCode)
		resp.ErrorMessage = lo.ToPtr(clientRes.ErrorMessage)
		return resp, nil
	}

	return resp, nil
}

// runServerGate executes the server-side bind-probe gate (A4a) against the
// resolved target. Every outcome is folded into a structured
// PreflightGateResult; the client node is never contacted. Only the
// AVAILABLE state passes — LISTENING (any listener, regardless of owner),
// UNKNOWN and transport failures all reject with the existing
// PORT_REMOTE_IN_USE / AGENT_OFFLINE / PORT_SCAN_UNAVAILABLE priority.
func runServerGate(c *app.Context, req *pb.PreflightRequest, target preflightTarget) *PreflightGateResult {
	serverVReq := &pb.ValidatePortRequest{
		NodeId:        lo.ToPtr(req.GetServerId()),
		Protocol:      lo.ToPtr(target.Protocol),
		Port:          lo.ToPtr(target.RemotePort),
		LocalAddress:  lo.ToPtr("0.0.0.0"),
		BindProbe:     lo.ToPtr(true),
		IncludeDocker: lo.ToPtr(true),
	}
	serverCtx, serverCancel := newPreflightContext(c, preflightTimeout)
	defer serverCancel()
	serverCheck, serverErrCode, err := validateOneSide(serverCtx, c, req.GetServerId(), serverVReq)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("preflight server gate failed, server: [%s]", req.GetServerId())
		return gateResultError(serverErrCode, err.Error())
	}
	res := &PreflightGateResult{
		Ok:          serverCheck.GetOk() && observedState(serverCheck) == pb.PortState_STATE_AVAILABLE,
		Observation: serverCheck.GetObservation(),
	}
	if !res.Ok {
		res.ErrorCode = pickCode(serverCheck.GetErrorCode(), defs.ErrorCodePortRemoteInUse)
		res.ErrorMessage = serverCheck.GetErrorMessage()
		if res.ErrorMessage == "" {
			res.ErrorMessage = "remote port is not available on the server"
		}
	}
	return res
}

// runClientGate executes the client-side local-listen gate (A4a) against
// the resolved target. Every outcome is folded into a structured
// PreflightGateResult; the server node is never contacted. Only the
// LISTENING or ACTIVE passes; AVAILABLE (nothing listening), UNKNOWN and
// transport failures reject with PORT_LOCAL_NOT_LISTENING /
// AGENT_OFFLINE / PORT_SCAN_UNAVAILABLE.
func runClientGate(c *app.Context, req *pb.PreflightRequest, target preflightTarget) *PreflightGateResult {
	clientVReq := &pb.ValidatePortRequest{
		NodeId:        lo.ToPtr(req.GetClientId()),
		Protocol:      lo.ToPtr(target.Protocol),
		Port:          lo.ToPtr(target.LocalPort),
		LocalAddress:  lo.ToPtr(target.LocalIP),
		BindProbe:     lo.ToPtr(false),
		IncludeDocker: lo.ToPtr(true),
	}
	clientCtx, clientCancel := newPreflightContext(c, preflightTimeout)
	defer clientCancel()
	clientCheck, clientErrCode, err := validateOneSide(clientCtx, c, req.GetClientId(), clientVReq)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("preflight client gate failed, client: [%s]", req.GetClientId())
		return gateResultError(clientErrCode, err.Error())
	}
	state := observedState(clientCheck)
	res := &PreflightGateResult{
		Ok: clientCheck.GetOk() &&
			(state == pb.PortState_STATE_LISTENING || state == pb.PortState_STATE_ACTIVE),
		Observation: clientCheck.GetObservation(),
	}
	if !res.Ok {
		res.ErrorCode = pickCode(clientCheck.GetErrorCode(), defs.ErrorCodePortLocalNotListening)
		res.ErrorMessage = clientCheck.GetErrorMessage()
		if res.ErrorMessage == "" {
			res.ErrorMessage = "no local service is listening on the client"
		}
	}
	return res
}

// RunServerProbe runs ONLY the server-side bind-probe gate (A4a) with its
// own full RPC budget. The request is validated and the candidate target
// resolved once; business input errors and transport failures are returned
// as structured gate results with a nil error. The client node is never
// contacted — callers compose gates explicitly (A4b SafeUpdate).
func (preflightRunner) RunServerProbe(c *app.Context, req *pb.PreflightRequest) (*PreflightGateResult, error) {
	target, code, msg, ok := resolvePreflightTarget(req)
	if !ok {
		return gateResultError(code, msg), nil
	}
	return runServerGate(c, req, target), nil
}

// RunClient runs ONLY the client-side local-listen gate (A4a) with its own
// full RPC budget. The request is validated and the candidate target
// resolved once; business input errors and transport failures are returned
// as structured gate results with a nil error. The server node is never
// contacted.
func (preflightRunner) RunClient(c *app.Context, req *pb.PreflightRequest) (*PreflightGateResult, error) {
	target, code, msg, ok := resolvePreflightTarget(req)
	if !ok {
		return gateResultError(code, msg), nil
	}
	return runClientGate(c, req, target), nil
}

// extractPreflightTargets resolves the candidate rule. Explicit request
// fields (protocol/remote_port/local_ip/local_port) win over the candidate
// config bytes. Only TCP/UDP is supported (spec §7.3).
func extractPreflightTargets(req *pb.PreflightRequest) (preflightTarget, error) {
	t := preflightTarget{
		Protocol:   req.GetProtocol(),
		RemotePort: req.GetRemotePort(),
		LocalIP:    req.GetLocalIp(),
		LocalPort:  req.GetLocalPort(),
		ProxyName:  req.GetProxyName(),
	}
	if t.Protocol != "" || t.RemotePort != 0 || t.LocalPort != 0 {
		return normalizeTarget(t)
	}
	if len(req.GetConfig()) == 0 {
		return t, fmt.Errorf("preflight request has neither explicit fields nor config")
	}
	cfgs, err := utils.LoadProxiesFromContent(req.GetConfig())
	if err != nil {
		return t, fmt.Errorf("cannot parse candidate config: %w", err)
	}
	if len(cfgs) != 1 {
		return t, fmt.Errorf("preflight requires exactly one proxy rule, got %d", len(cfgs))
	}
	cfg := cfgs[0]
	switch p := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		t.Protocol = "tcp"
		t.RemotePort = uint32(p.RemotePort)
		t.LocalIP = p.LocalIP
		t.LocalPort = uint32(p.LocalPort)
		t.ProxyName = p.Name
	case *v1.UDPProxyConfig:
		t.Protocol = "udp"
		t.RemotePort = uint32(p.RemotePort)
		t.LocalIP = p.LocalIP
		t.LocalPort = uint32(p.LocalPort)
		t.ProxyName = p.Name
	default:
		return t, fmt.Errorf("%w: %s", defs.ErrUnsupportedProxyType, cfg.Type)
	}
	return normalizeTarget(t)
}

func normalizeTarget(t preflightTarget) (preflightTarget, error) {
	switch t.Protocol {
	case "tcp", "tcp4", "tcp6":
		t.Protocol = "tcp"
	case "udp", "udp4", "udp6":
		t.Protocol = "udp"
	default:
		return t, fmt.Errorf("unsupported protocol %q, only tcp/udp are supported", t.Protocol)
	}
	if t.RemotePort == 0 || t.RemotePort > 65535 {
		return t, fmt.Errorf("remote port must be in 1..65535, got %d", t.RemotePort)
	}
	if t.LocalPort == 0 || t.LocalPort > 65535 {
		return t, fmt.Errorf("local port must be in 1..65535, got %d", t.LocalPort)
	}
	return t, nil
}

// validateOneSide sends EVENT_VALIDATE_PORT to one agent and classifies
// transport failures into spec error codes (§7.3): offline/unresponsive
// agents yield AGENT_OFFLINE, anything else on the wire is
// PORT_SCAN_UNAVAILABLE.
func validateOneSide(ctx context.Context, c *app.Context, nodeID string, vreq *pb.ValidatePortRequest) (*pb.ValidatePortResponse, string, error) {
	resp, err := rpc.CallClientWithContext(ctx, c.GetApp(), nodeID, pb.Event_EVENT_VALIDATE_PORT, vreq)
	if err != nil {
		return nil, classifyAgentError(err), err
	}
	out := &pb.ValidatePortResponse{}
	if err := proto.Unmarshal(resp.GetData(), out); err != nil {
		return nil, defs.ErrorCodePortScanUnavailable, err
	}
	if out.GetStatus() == nil || out.GetStatus().GetCode() != pb.RespCode_RESP_CODE_SUCCESS {
		return out, defs.ErrorCodePortScanUnavailable, fmt.Errorf("agent validation failed: %s", out.GetStatus().GetMessage())
	}
	return out, "", nil
}

func classifyAgentError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if strings.Contains(msg, "cannot get client") {
		return defs.ErrorCodeAgentOffline
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return defs.ErrorCodeAgentOffline
	}
	return defs.ErrorCodePortScanUnavailable
}

// pickCode prefers the agent-provided error code, falling back to the
// gate default.
func pickCode(agentCode, fallback string) string {
	if agentCode != "" {
		return agentCode
	}
	return fallback
}
