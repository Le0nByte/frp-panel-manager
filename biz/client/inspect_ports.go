package client

import (
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/portinspector"
	"github.com/samber/lo"
)

// InspectPortsHandler scans local sockets on the client node (spec §4.2).
func InspectPortsHandler(c *app.Context, req *pb.InspectPortsRequest) (*pb.InspectPortsResponse, error) {
	inspector := portinspector.New()
	obs, err := inspector.ScanPorts(c, portinspector.ScanOptions{
		NodeID:        c.GetApp().GetClientBase().GetClientId(),
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
	return &pb.InspectPortsResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Observations: portinspector.ToObservations(obs),
	}, nil
}

// ValidatePortHandler validates a single port on the client node and
// returns verifiable evidence (spec §4.2 EVENT_VALIDATE_PORT).
//
// The gate semantics follow the request's BindProbe flag and are
// implemented by the shared portinspector.EvaluateGate:
//   - BindProbe=false (client-side local gate): ok when a real local
//     service answers, i.e. LISTENING or ACTIVE (established
//     connections).
//   - BindProbe=true (server-side remote gate): ok when the port is
//     AVAILABLE (a fresh bind probe succeeds).
//
// UNKNOWN never yields ok=true: it must reject a save.
func ValidatePortHandler(c *app.Context, req *pb.ValidatePortRequest) (*pb.ValidatePortResponse, error) {
	inspector := portinspector.New()
	obs, err := inspector.ValidatePort(c, portinspector.ValidateOptions{
		NodeID:        c.GetApp().GetClientBase().GetClientId(),
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
	gate := portinspector.EvaluateGate(obs.State, req.GetBindProbe())
	resp.Ok = lo.ToPtr(gate.OK)
	if gate.ErrorCode != "" {
		resp.ErrorCode = lo.ToPtr(gate.ErrorCode)
		resp.ErrorMessage = lo.ToPtr(gate.ErrorMessage)
	}
	return resp, nil
}
