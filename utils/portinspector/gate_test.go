package portinspector

import (
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
)

// TestEvaluateGate covers the full two-sided gate table (spec §5.4/§5.5):
// every state × {client gate, server gate}. A rejection must always carry
// a stable, non-empty error code and message; unknown or future states
// must reject rather than false-accept.
func TestEvaluateGate(t *testing.T) {
	cases := []struct {
		name       string
		state      State
		serverGate bool
		wantOK     bool
		wantCode   string
		wantMsg    string
	}{
		// server gate (BindProbe=true): only AVAILABLE passes.
		{"available/server", StateAvailable, true, true, "", ""},
		{"listening/server", StateListening, true, false, defs.ErrorCodePortRemoteInUse, "remote port is already listening"},
		{"active/server", StateActive, true, false, defs.ErrorCodePortRemoteInUse, "remote port has active connections"},
		{"conflict/server", StateConflict, true, false, defs.ErrorCodePortRemoteInUse, "remote port is occupied by another process/container"},
		{"unknown/server", StateUnknown, true, false, defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)"},
		// client gate (BindProbe=false): a live local service passes.
		{"available/client", StateAvailable, false, false, defs.ErrorCodePortLocalNotListening, "no local service is listening on the target port"},
		{"listening/client", StateListening, false, true, "", ""},
		{"active/client", StateActive, false, true, "", ""},
		{"conflict/client", StateConflict, false, false, defs.ErrorCodePortLocalNotListening, "target port is ambiguous (multiple processes)"},
		{"unknown/client", StateUnknown, false, false, defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)"},
		// default / future states must never false-accept and never
		// produce an empty error code.
		{"empty-state/client", State(""), false, false, defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)"},
		{"empty-state/server", State(""), true, false, defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)"},
		{"future-state/server", State("SOMETHING_NEW"), true, false, defs.ErrorCodePortScanUnavailable, "port scan unavailable (permission denied, timeout or parse failure)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateGate(tc.state, tc.serverGate)
			if got.OK != tc.wantOK {
				t.Fatalf("EvaluateGate(%q, serverGate=%v).OK = %v, want %v", tc.state, tc.serverGate, got.OK, tc.wantOK)
			}
			if got.ErrorCode != tc.wantCode {
				t.Fatalf("EvaluateGate(%q, serverGate=%v).ErrorCode = %q, want %q", tc.state, tc.serverGate, got.ErrorCode, tc.wantCode)
			}
			if got.ErrorMessage != tc.wantMsg {
				t.Fatalf("EvaluateGate(%q, serverGate=%v).ErrorMessage = %q, want %q", tc.state, tc.serverGate, got.ErrorMessage, tc.wantMsg)
			}
			if !got.OK && (got.ErrorCode == "" || got.ErrorMessage == "") {
				t.Fatalf("EvaluateGate(%q, serverGate=%v) rejected without a non-empty error code/message", tc.state, tc.serverGate)
			}
		})
	}
}
