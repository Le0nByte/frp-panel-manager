package proxy

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/utils/portinspector"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vresp(ok bool, pid int32, service string) *pb.ValidatePortResponse {
	resp := &pb.ValidatePortResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Ok:     lo.ToPtr(ok),
	}
	if pid != 0 || service != "" {
		resp.Observation = &pb.PortObservation{Pid: pid, ServiceName: service}
	}
	return resp
}

// TestVerifyAppliedAcceptable pins the P1-2 owner contract: a verification
// only proves the apply when the gate passed AND the observation shows a
// live PID owned by the embedded frps of the Server Agent process. A
// foreign listener (ok=true but no/wrong owner) or a PID-less observation
// must keep the verifier polling and ultimately fail with VERIFY_FAILED.
func TestVerifyAppliedAcceptable(t *testing.T) {
	cases := []struct {
		name   string
		resp   *pb.ValidatePortResponse
		expect bool
	}{
		{"frps owner passes", vresp(true, 1234, portinspector.ServiceEmbeddedFRPS), true},
		{"ok=true but no observation", vresp(true, 0, ""), false},
		{"ok=true but pid zero", vresp(true, 0, portinspector.ServiceEmbeddedFRPS), false},
		{"ok=true but external listener (no service)", vresp(true, 4321, ""), false},
		{"ok=true but wrong service (foreign frps)", vresp(true, 4321, "some-other-frps"), false},
		{"gate rejected", vresp(false, 1234, portinspector.ServiceEmbeddedFRPS), false},
		{"nil response", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, verifyAppliedAcceptable(tc.resp), tc.name)
		})
	}
}

// TestDefaultVerificationBudget pins spec §8.4: run-state verification
// polls every 1s, at most 30 attempts, inside a 30s total context. Writes
// (apply) are never retried — only the read-side verification polls.
func TestDefaultVerificationBudget(t *testing.T) {
	d := defaultDeps()
	require.Equal(t, 30, defaultVerifyAttempts)
	require.Equal(t, 30*time.Second, defaultVerifyTimeout)
	require.Equal(t, time.Second, defaultVerifyInterval)
	require.Equal(t, 30, d.VerifyAttempts, "the production coordinator must inherit the 30-attempt budget")
	require.Equal(t, time.Second, d.VerifyInterval)
}
