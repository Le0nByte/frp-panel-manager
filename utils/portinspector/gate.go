package portinspector

import "github.com/VaalaCat/frp-panel/defs"

// GateResult is the decision of one side of the two-sided port gate
// (spec §5.4/§5.5). A rejection always carries a stable, non-empty
// ErrorCode and ErrorMessage for the UI (spec §7.3).
type GateResult struct {
	OK           bool
	ErrorCode    string
	ErrorMessage string
}

// EvaluateGate classifies a single-port observation against the gate of
// the side that owns it (pure function, no I/O):
//
//   - serverGate=true is the server-side (remote) gate, driven by a bind
//     probe: the public port must be AVAILABLE. Any listener — LISTENING,
//     ACTIVE (established connections) or CONFLICT — means the remote
//     port is taken and must reject with PORT_REMOTE_IN_USE.
//   - serverGate=false is the client-side (local) gate: a real local
//     service must answer, i.e. LISTENING or ACTIVE. AVAILABLE (nothing
//     listening) and CONFLICT reject with PORT_LOCAL_NOT_LISTENING.
//
// UNKNOWN and any unrecognised/future state reject with
// PORT_SCAN_UNAVAILABLE: a failure is always safer than a false accept.
func EvaluateGate(state State, serverGate bool) GateResult {
	reject := func(code, msg string) GateResult {
		return GateResult{OK: false, ErrorCode: code, ErrorMessage: msg}
	}
	switch state {
	case StateListening:
		if serverGate {
			return reject(defs.ErrorCodePortRemoteInUse, "remote port is already listening")
		}
		return GateResult{OK: true}
	case StateAvailable:
		if serverGate {
			return GateResult{OK: true}
		}
		return reject(defs.ErrorCodePortLocalNotListening, "no local service is listening on the target port")
	case StateActive:
		if serverGate {
			return reject(defs.ErrorCodePortRemoteInUse, "remote port has active connections")
		}
		return GateResult{OK: true}
	case StateConflict:
		if serverGate {
			return reject(defs.ErrorCodePortRemoteInUse, "remote port is occupied by another process/container")
		}
		return reject(defs.ErrorCodePortLocalNotListening, "target port is ambiguous (multiple processes)")
	default: // StateUnknown and any future/unknown state
		return reject(defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)")
	}
}
