package rpcguard

import (
	"context"

	"google.golang.org/grpc/peer"
)

// guardCtxKey is the unexported, typed key used to carry the Guard in a
// context. Unexported + distinct type = no accidental collisions with any
// other value in the chain.
type guardCtxKey struct{}

// ContextWithGuard returns a copy of ctx carrying g. A nil guard is
// stored as nil (legacy mode: FromContext yields nil and callers keep
// their pre-I3-B behavior). A nil ctx is promoted to context.Background()
// so callers can safely wrap stream contexts.
func ContextWithGuard(ctx context.Context, g *Guard) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, guardCtxKey{}, g)
}

// FromContext returns the Guard carried by ctx, or nil when absent.
// Callers MUST treat nil as "no guard configured" and fall back to the
// legacy behavior; nil is also what a nil-typed guard stored by
// ContextWithGuard yields.
func FromContext(ctx context.Context) *Guard {
	if ctx == nil {
		return nil
	}
	g, _ := ctx.Value(guardCtxKey{}).(*Guard)
	return g
}

// PeerFromContext returns the transport peer address string carried by a
// gRPC context, or "unknown" when no peer is present (or its Addr is
// nil). The string is passed to Guard methods, which normalize it — a
// malformed or missing peer can never produce an empty or aliased key.
func PeerFromContext(ctx context.Context) string {
	if ctx == nil {
		return "unknown"
	}
	if p, ok := peer.FromContext(ctx); ok && p != nil && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}
