package rpcguard

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

// TestContextWithGuardFromContextRoundTrip: a guard stored via
// ContextWithGuard must be retrievable via FromContext as the SAME
// pointer, and a context without one must yield nil.
func TestContextWithGuardFromContextRoundTrip(t *testing.T) {
	g := NewGuard()
	ctx := ContextWithGuard(context.Background(), g)
	require.Same(t, g, FromContext(ctx), "FromContext must return the exact injected pointer")
	require.Nil(t, FromContext(context.Background()), "no guard in ctx => nil")
	require.Nil(t, FromContext(nil), "nil ctx => nil")
}

// TestContextWithGuardNillStoresNil: wrapping a nil guard must round-trip
// to nil (legacy mode), not to a non-nil pointer.
func TestContextWithGuardNilStoresNil(t *testing.T) {
	ctx := ContextWithGuard(context.Background(), nil)
	require.Nil(t, FromContext(ctx))
}

// TestPeerFromContext: a gRPC context carrying a peer must yield the
// transport address string; a missing peer must yield "unknown".
func TestPeerFromContext(t *testing.T) {
	p := &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 7000}}
	ctx := peer.NewContext(context.Background(), p)
	require.Equal(t, "203.0.113.7:7000", PeerFromContext(ctx))

	require.Equal(t, "unknown", PeerFromContext(context.Background()))
	require.Equal(t, "unknown", PeerFromContext(nil))
}

// TestPeerFromContextSurvivesGuardWrap: the peer must remain visible when
// the guard context wraps the peer context (the production layering in
// the master service).
func TestPeerFromContextSurvivesGuardWrap(t *testing.T) {
	g := NewGuard()
	p := &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("198.51.100.4"), Port: 9000}}
	ctx := ContextWithGuard(peer.NewContext(context.Background(), p), g)
	require.Equal(t, "198.51.100.4:9000", PeerFromContext(ctx))
	require.Same(t, g, FromContext(ctx))
}

// TestPeerFromContextAddrNil: a peer with a nil Addr must degrade to
// "unknown" instead of panicking.
func TestPeerFromContextAddrNil(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{})
	require.Equal(t, "unknown", PeerFromContext(ctx))
}
