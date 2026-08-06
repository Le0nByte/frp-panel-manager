package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/portinspector"
	"github.com/samber/lo"
)

// Verifier confirms the server-side effect of an apply (spec §8.1 step 9,
// §8.4: run-state verification polls every 1s, at most 30s total).
type Verifier interface {
	// VerifyApplied confirms the remote port is now held by frps: the server
	// gate (bind_probe=false) must report LISTENING/ACTIVE for the port.
	VerifyApplied(ctx context.Context, c *app.Context, serverID, protocol string, port uint32) error
	// VerifyReleased confirms the remote port is free again after a
	// compensation: the server bind probe must succeed (AVAILABLE).
	VerifyReleased(ctx context.Context, c *app.Context, serverID, protocol string, port uint32) error
}

// rpcVerifier is the production verifier: it reuses the agent-side
// EVENT_VALIDATE_PORT handler (the same read path as preflight) with the
// appropriate gate flag, retrying up to Attempts times with Interval pauses
// inside the caller-provided ctx budget (the coordinator supplies a 30s
// timeout — spec §8.4).
type rpcVerifier struct {
	Attempts int
	Interval time.Duration
}

func (v *rpcVerifier) VerifyApplied(ctx context.Context, c *app.Context, serverID, protocol string, port uint32) error {
	return v.verify(ctx, c, serverID, protocol, port, false)
}

func (v *rpcVerifier) VerifyReleased(ctx context.Context, c *app.Context, serverID, protocol string, port uint32) error {
	return v.verify(ctx, c, serverID, protocol, port, true)
}

// verifyAppliedAcceptable reports whether a validation response proves the
// port is served by the embedded frps of the Server Agent process (spec
// §8.1 step 9 / P1-2): the gate must pass AND the observation must show a
// live PID whose ServiceName is the panel's embedded-frps marker. A
// foreign listener (ok=true but no/wrong owner) or a PID-less observation
// is NOT acceptable — the apply must keep polling and finally fail with
// VERIFY_FAILED rather than trust a partial state.
func verifyAppliedAcceptable(resp *pb.ValidatePortResponse) bool {
	if resp == nil || !resp.GetOk() {
		return false
	}
	obs := resp.GetObservation()
	if obs == nil {
		return false
	}
	if obs.GetPid() <= 0 {
		return false
	}
	return obs.GetServiceName() == portinspector.ServiceEmbeddedFRPS
}

// verify probes the server node. wantFree=true uses bind_probe=true and
// accepts AVAILABLE; wantFree=false uses bind_probe=false and accepts
// LISTENING/ACTIVE ONLY when the observation proves the port is served by
// the embedded frps (verifyAppliedAcceptable). Transport failures between
// attempts are retried; after the last attempt any failure is reported as
// VERIFY_FAILED (the apply already happened — the transaction must
// compensate, never trust a partial state).
func (v *rpcVerifier) verify(ctx context.Context, c *app.Context, serverID, protocol string, port uint32, wantFree bool) error {
	attempts := v.Attempts
	if attempts < 1 {
		attempts = 1
	}
	interval := v.Interval
	if interval <= 0 {
		interval = time.Second
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return fmt.Errorf("%s: verify aborted: %w", defs.ErrorCodeVerifyFailed, ctx.Err())
			}
		}

		vreq := &pb.ValidatePortRequest{
			NodeId:        lo.ToPtr(serverID),
			Protocol:      lo.ToPtr(protocol),
			Port:          lo.ToPtr(port),
			LocalAddress:  lo.ToPtr("0.0.0.0"),
			BindProbe:     lo.ToPtr(wantFree),
			IncludeDocker: lo.ToPtr(false),
		}
		resp, _, err := validateOneSide(ctx, c, serverID, vreq)
		if err != nil {
			lastErr = fmt.Errorf("%s: probe failed: %w", defs.ErrorCodeVerifyFailed, err)
			continue
		}
		if wantFree {
			// compensation verify: only the bind probe result matters
			if resp.GetOk() {
				return nil
			}
			msg := resp.GetErrorMessage()
			if msg == "" {
				msg = "server gate still reports the port occupied"
			}
			lastErr = fmt.Errorf("%s: %s", defs.ErrorCodeVerifyFailed, msg)
			continue
		}
		// apply verify: the port must be served by OUR embedded frps
		if verifyAppliedAcceptable(resp) {
			return nil
		}
		owner := "none"
		if obs := resp.GetObservation(); obs != nil {
			owner = fmt.Sprintf("pid=%d service=%q", obs.GetPid(), obs.GetServiceName())
		}
		lastErr = fmt.Errorf("%s: port not served by the embedded frps (owner: %s)", defs.ErrorCodeVerifyFailed, owner)
	}
	return lastErr
}
