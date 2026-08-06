package portinspector

import "github.com/VaalaCat/frp-panel/pb"

// ToProto converts an Observation to the wire type (spec §5.2).
func (o Observation) ToProto() *pb.PortObservation {
	state := pb.PortState_STATE_UNKNOWN
	switch o.State {
	case StateAvailable:
		state = pb.PortState_STATE_AVAILABLE
	case StateListening:
		state = pb.PortState_STATE_LISTENING
	case StateActive:
		state = pb.PortState_STATE_ACTIVE
	case StateConflict:
		state = pb.PortState_STATE_CONFLICT
	case StateUnknown:
		state = pb.PortState_STATE_UNKNOWN
	}
	return &pb.PortObservation{
		NodeId:              o.NodeID,
		Protocol:            o.Protocol,
		IpFamily:            o.IPFamily,
		LocalAddress:        o.LocalAddress,
		LocalPort:           o.LocalPort,
		State:               state,
		Pid:                 o.PID,
		ProcessName:         o.ProcessName,
		Executable:          o.Executable,
		ServiceName:         o.ServiceName,
		DockerContainerId:   o.DockerContainerID,
		DockerContainerName: o.DockerContainerName,
		FrpProxyName:        o.FRPProxyName,
		ActiveConnections:   o.ActiveConnections,
		ObservedAt:          o.ObservedAt,
		Source:              o.Source,
	}
}

// ToObservations converts a slice of Observations.
func ToObservations(obs []Observation) []*pb.PortObservation {
	out := make([]*pb.PortObservation, 0, len(obs))
	for _, o := range obs {
		out = append(out, o.ToProto())
	}
	return out
}
