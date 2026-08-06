package rpcguard

import (
	"context"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDisabledGuardAllowsEverything (I3-C): an explicitly disabled guard
// must pass every admission check — no rate limit, no lock, no IP ban.
// This is the knob operators use to turn the guard off without touching
// the listener wiring.
func TestDisabledGuardAllowsEverything(t *testing.T) {
	g := NewGuard(WithEnabled(false))
	peer := "203.0.113.60:7000"

	for i := 0; i < 5*DefaultConnectionRatePerMin; i++ {
		require.True(t, g.AllowConnection(peer), "disabled guard must never deny a connection")
	}
	for i := 0; i < 5*DefaultRegisterRatePerMin; i++ {
		require.True(t, g.AllowRegister(peer, "node-1"), "disabled guard must never deny a register")
	}
	require.True(t, g.AllowSecretAttempt(peer, "node-1"), "disabled guard must never deny a secret attempt")
}

// TestDisabledGuardRecordsNothing (I3-C): with the guard disabled every
// Record* call is a no-op and Flush/Run produce no audit rows at all.
func TestDisabledGuardRecordsNothing(t *testing.T) {
	g := NewGuard(WithEnabled(false))
	g.RecordHandshakeFailureLayer("203.0.113.61:7000", LayerPreTLS)
	g.RecordHandshakeFailure("203.0.113.61:7000")
	g.RecordSecretFailure("203.0.113.61:7000", "node-1", LayerClient)
	g.RecordSecretSuccess("203.0.113.61:7000", "node-1")

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	assert.Empty(t, w.snapshot(), "a disabled guard must never emit audit rows")

	// The audit loop must also terminate immediately (no ticker churn).
	done := make(chan struct{})
	go func() {
		g.Run(context.Background(), w)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run on a disabled guard must return immediately (no goroutine leak)")
	}
}

// TestRecordHandshakeFailureLayerClassifies (I3-C): the layer argument
// separates pre-TLS (0 bytes) from tls_handshake failures in the flushed
// rows, while the legacy RecordHandshakeFailure keeps the empty layer.
// Both feed the SAME per-IP failure counter: 20 mixed failures still lock
// the IP.
func TestRecordHandshakeFailureLayerClassifies(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	peer := "203.0.113.62:7000"

	g.RecordHandshakeFailureLayer(peer, LayerPreTLS)
	g.RecordHandshakeFailureLayer(peer, LayerTLSHandshake)
	g.RecordHandshakeFailure(peer) // legacy: empty layer

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 3, "each layer must aggregate into its own row")
	summaries := map[string]bool{}
	for _, ev := range evs {
		require.Equal(t, models.AuditEventTypeRPCHandshakeFailure, ev.EventType)
		summaries[ev.Summary] = true
	}
	assert.Contains(t, summaries, "rpc.handshake_failure: ip=203.0.113.62 node= layer=pre_tls count=1")
	assert.Contains(t, summaries, "rpc.handshake_failure: ip=203.0.113.62 node= layer=tls_handshake count=1")
	assert.Contains(t, summaries, "rpc.handshake_failure: ip=203.0.113.62 node= layer= count=1")

	// 20 failures across ANY mix of layers lock the IP: further connection
	// attempts are denied.
	g2 := NewGuard(WithClock(clk))
	for i := 0; i < DefaultHandshakeFailureThreshold; i++ {
		if i%2 == 0 {
			g2.RecordHandshakeFailureLayer(peer, LayerPreTLS)
		} else {
			g2.RecordHandshakeFailure(peer)
		}
	}
	require.False(t, g2.AllowConnection(peer), "20 mixed-layer handshake failures must lock the IP")
}

// TestLayerConstantsFixed (I3-C): the layer strings are part of the audit
// contract surfaced in the listener summary; operators rely on them.
func TestLayerConstantsFixed(t *testing.T) {
	assert.Equal(t, "pre_tls", LayerPreTLS)
	assert.Equal(t, "tls_handshake", LayerTLSHandshake)
}
