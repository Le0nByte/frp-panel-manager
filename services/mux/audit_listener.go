package mux

// I3-C L0/L1: the audit listener gates every accepted connection against
// the per-IP connection-rate ceiling and wraps allowed connections in a
// trackingConn that classifies TLS failures at Close time. This file holds
// NO package-global state — the listener and its tracker share state only
// through the injected *rpcguard.Guard and the owning MuxServer.

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"

	"github.com/VaalaCat/frp-panel/services/rpcguard"
)

// trackerKey is the request-context key carrying the *trackingConn of the
// connection currently being served (set by the http.Server ConnContext
// hook).
type trackerKey struct{}

// auditListener wraps a net.Listener so that every accepted connection is
// first checked against the guard's per-IP connection rate ceiling and,
// when allowed, handed to the http.Server wrapped in a trackingConn.
type auditListener struct {
	lis   net.Listener
	guard *rpcguard.Guard

	// stopping is set by the owning MuxServer.Stop BEFORE the http.Server
	// is closed: connections torn down by shutdown are teardown, never
	// handshake failures.
	stopping atomic.Bool
}

func newAuditListener(lis net.Listener, guard *rpcguard.Guard) *auditListener {
	return &auditListener{lis: lis, guard: guard}
}

// Accept returns ONLY connections the guard allowed. A denied connection
// is closed immediately (no address/secret is ever logged — the guard has
// already recorded the audit bucket) and the accept loop continues, so the
// http.Server only ever sees allowed connections.
func (l *auditListener) Accept() (net.Conn, error) {
	for {
		c, err := l.lis.Accept()
		if err != nil {
			return nil, err
		}
		if !l.guard.AllowConnection(c.RemoteAddr().String()) {
			_ = c.Close()
			continue
		}
		return &trackingConn{
			Conn:   c,
			guard:  l.guard,
			parent: l,
		}, nil
	}
}

func (l *auditListener) Close() error   { return l.lis.Close() }
func (l *auditListener) Addr() net.Addr { return l.lis.Addr() }

// setStopping marks the shutdown phase; afterwards closed connections are
// never classified as failures.
func (l *auditListener) setStopping() { l.stopping.Store(true) }

// trackingConn is a net.Conn wrapper that counts the raw bytes read (the
// TLS record stream: the http.Server reads through the *tls.Conn which
// wraps this conn) and classifies the connection on Close:
//
//   - 0 bytes read and no HTTP request seen → pre_tls (TCP-only probe)
//   - >0 bytes read but no HTTP request seen → tls_handshake (the
//     ClientHello arrived but the handshake failed or was abandoned)
//   - an HTTP request was seen → never a failure
//   - the server is stopping (shutdown teardown) → never a failure
//
// Honest boundary (spec §10.5): the application can only observe
// "pre-TLS" vs "did not reach HTTP". TLS alert details live inside
// net/http's internal handshake and are not retrievable from here, and
// connections that never complete TCP accept (SYN floods) are invisible
// to the application entirely — the kernel SYN queue and a host firewall
// are responsible for those. Close is safe for concurrent use and runs
// its classification exactly once.
type trackingConn struct {
	net.Conn
	guard  *rpcguard.Guard
	parent *auditListener

	closeOnce sync.Once
	closeErr  error

	bytes    atomic.Int64 // raw bytes read (ClientHello and beyond)
	httpSeen atomic.Bool  // an HTTP request reached the handler
}

func (c *trackingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.bytes.Add(int64(n))
	return n, err
}

func (c *trackingConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		c.classify()
	})
	return c.closeErr
}

// markHTTPSeen records that a real HTTP request arrived on this
// connection: from then on it can never be classified as a failure.
func (c *trackingConn) markHTTPSeen() { c.httpSeen.Store(true) }

func (c *trackingConn) classify() {
	if c.parent != nil && c.parent.stopping.Load() {
		return // shutdown teardown, not an attack
	}
	if c.httpSeen.Load() {
		return // a real request arrived; never a failure
	}
	peer := c.Conn.RemoteAddr().String()
	if c.bytes.Load() == 0 {
		c.guard.RecordHandshakeFailureLayer(peer, rpcguard.LayerPreTLS)
		return
	}
	c.guard.RecordHandshakeFailureLayer(peer, rpcguard.LayerTLSHandshake)
}

// findTracker recursively unwraps a net.Conn looking for the trackingConn.
// http.Server hands ConnContext the *tls.Conn; tls.Conn.NetConn returns
// the wrapped raw conn (which may itself be nested), so walk the chain.
// Non-tracking conns (e.g. the loopback HTTP mux) yield nil.
func findTracker(c net.Conn) *trackingConn {
	for c != nil {
		if tc, ok := c.(*trackingConn); ok {
			return tc
		}
		tlsC, ok := c.(*tls.Conn)
		if !ok {
			return nil
		}
		c = tlsC.NetConn()
	}
	return nil
}

// trackerFromRequest returns the trackingConn stashed in a request context
// by the ConnContext hook, or nil.
func trackerFromRequest(ctx context.Context) *trackingConn {
	if ctx == nil {
		return nil
	}
	tr, _ := ctx.Value(trackerKey{}).(*trackingConn)
	return tr
}
