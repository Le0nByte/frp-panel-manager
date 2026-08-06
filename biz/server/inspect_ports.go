package server

import (
	"os"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/portinspector"
	"github.com/samber/lo"
)

// markEmbeddedFRPS marks an observation whose socket is owned by the
// embedded fatedier frps: PID == THIS process AND state LISTENING/ACTIVE.
// The frps server runs inside the Server Agent process, so its listeners
// carry the agent's own PID; any other PID (an external frps, sshd, ...)
// is never marked (spec §8.1 step 9 / P1-2).
func markEmbeddedFRPS(o *pb.PortObservation) {
	if o.GetPid() == int32(os.Getpid()) &&
		(o.GetState() == pb.PortState_STATE_LISTENING || o.GetState() == pb.PortState_STATE_ACTIVE) {
		o.ServiceName = portinspector.ServiceEmbeddedFRPS
	}
}

// markEmbeddedFRPSAll applies markEmbeddedFRPS to a batch of observations.
func markEmbeddedFRPSAll(obs []*pb.PortObservation) {
	for _, o := range obs {
		markEmbeddedFRPS(o)
	}
}

// InspectPortsHandler scans local sockets on the server node (spec §4.2).
func InspectPortsHandler(c *app.Context, req *pb.InspectPortsRequest) (*pb.InspectPortsResponse, error) {
	inspector := portinspector.New()
	obs, err := inspector.ScanPorts(c, portinspector.ScanOptions{
		NodeID:        c.GetApp().GetServerBase().GetServerId(),
		Protocol:      req.GetProtocol(),
		PortMin:       req.GetPortMin(),
		PortMax:       req.GetPortMax(),
		IncludeDocker: req.GetIncludeDocker(),
	})
	if err != nil {
		return &pb.InspectPortsResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
		}, err
	}
	observations := portinspector.ToObservations(obs)
	markEmbeddedFRPSAll(observations)
	return &pb.InspectPortsResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Observations: observations,
	}, nil
}

// ValidatePortHandler validates a single port on the server node (spec
// §4.2 EVENT_VALIDATE_PORT). Gate semantics are identical to the client
// handler: delegated to the shared portinspector.EvaluateGate and driven
// by the request's BindProbe flag.
func ValidatePortHandler(c *app.Context, req *pb.ValidatePortRequest) (*pb.ValidatePortResponse, error) {
	inspector := portinspector.New()
	obs, err := inspector.ValidatePort(c, portinspector.ValidateOptions{
		NodeID:        c.GetApp().GetServerBase().GetServerId(),
		Protocol:      req.GetProtocol(),
		Port:          req.GetPort(),
		LocalAddress:  req.GetLocalAddress(),
		BindProbe:     req.GetBindProbe(),
		IncludeDocker: req.GetIncludeDocker(),
	})
	if err != nil {
		return &pb.ValidatePortResponse{
			Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
			Ok:           lo.ToPtr(false),
			ErrorCode:    lo.ToPtr(defs.ErrorCodePortScanUnavailable),
			ErrorMessage: lo.ToPtr(err.Error()),
		}, err
	}

	resp := &pb.ValidatePortResponse{
		Status:      &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Observation: obs.ToProto(),
	}
	// Only THIS process's own LISTENING/ACTIVE socket may carry the
	// embedded-frps marker; the Master verifier refuses applies without it.
	markEmbeddedFRPS(resp.Observation)
	gate := portinspector.EvaluateGate(obs.State, req.GetBindProbe())
	resp.Ok = lo.ToPtr(gate.OK)
	if gate.ErrorCode != "" {
		resp.ErrorCode = lo.ToPtr(gate.ErrorCode)
		resp.ErrorMessage = lo.ToPtr(gate.ErrorMessage)
	}
	return resp, nil
}
