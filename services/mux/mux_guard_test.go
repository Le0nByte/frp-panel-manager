package mux

// I3-C tests for the public 9101 TLS listener: connection-rate gating at
// accept time (L0), TLS-failure classification via trackingConn (L1), the
// real TLS loopback classification, and the Run/Stop lifecycle. All peers
// are TEST-NET addresses (203.0.113.x) — never real public IPs; no secret
// material is involved anywhere.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// muxStubWriter collects the system rows drained by guard.Flush.
type muxStubWriter struct {
	events []*models.AuditEvent
}

func (w *muxStubWriter) LogSystem(ev *models.AuditEvent) error {
	w.events = append(w.events, ev)
	return nil
}

func (w *muxStubWriter) snapshot() []*models.AuditEvent { return w.events }

// ---- fakes ---------------------------------------------------------------

// scriptConn is a net.Conn whose Read/Close are scriptable; used to drive
// trackingConn classification deterministically.
type scriptConn struct {
	net.Conn
	mu       atomic.Int64 // bytes left to serve
	closed   atomic.Bool
	remoteIP string
}

func (c *scriptConn) Read(p []byte) (int, error) {
	left := c.mu.Load()
	if left <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > left {
		n = left
	}
	c.mu.Add(-n)
	for i := int64(0); i < n; i++ {
		p[i] = 0x16 // fake TLS record bytes
	}
	return int(n), nil
}

func (c *scriptConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *scriptConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(c.remoteIP), Port: 7000}
}

func newScriptConn(remoteIP string, bytes int64) *scriptConn {
	c := &scriptConn{remoteIP: remoteIP}
	c.mu.Store(bytes)
	return c
}

// failAfterListener accepts up to n conns then returns a plain (non-net)
// error — the "other Serve error" path Run must log and survive.
type failAfterListener struct {
	lis    net.Listener
	left   atomic.Int64
	fail   error
	closed atomic.Bool
}

func (l *failAfterListener) Accept() (net.Conn, error) {
	if l.left.Add(-1) < 0 {
		return nil, l.fail
	}
	return l.lis.Accept()
}
func (l *failAfterListener) Close() error {
	l.closed.Store(true)
	return l.lis.Close()
}
func (l *failAfterListener) Addr() net.Addr { return l.lis.Addr() }

// ---- construction / wiring -------------------------------------------------

func TestNewMuxWrapsOnlyWhenCredsAndGuard(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	trivial := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	creds := &tls.Config{Certificates: []tls.Certificate{{}}}
	g := rpcguard.NewGuard()

	m := NewMux(trivial, trivial, lis, creds, g).(*muxImpl)
	require.NotNil(t, m.audit, "creds+guard must wrap the listener")
	require.Same(t, g, m.audit.guard, "the wrapped listener must hold the SAME guard pointer")

	m2 := NewMux(trivial, trivial, lis, nil, g).(*muxImpl)
	assert.Nil(t, m2.audit, "the loopback HTTP mux must never wrap")

	m3 := NewMux(trivial, trivial, lis, creds).(*muxImpl)
	assert.Nil(t, m3.audit, "creds without a guard must not wrap (legacy mode)")

	m4 := NewMux(trivial, trivial, lis, nil).(*muxImpl)
	assert.Nil(t, m4.audit, "plain mux must not wrap")
}

// TestNewMuxNilListenerWithCredsAndGuard (I3-C): creds+guard with a NIL
// listener (the TLS bind failed at startup) must never wrap the nil
// listener into a non-nil auditListener — Run and Stop stay fail-safe and
// never panic.
func TestNewMuxNilListenerWithCredsAndGuard(t *testing.T) {
	trivial := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	creds := &tls.Config{Certificates: []tls.Certificate{{}}}
	g := rpcguard.NewGuard()

	m := NewMux(trivial, trivial, nil, creds, g).(*muxImpl)
	require.Nil(t, m.audit, "a nil listener must never be wrapped into an auditListener")
	require.Nil(t, m.lis, "m.lis must stay nil so Run takes the fail-safe path")

	done := make(chan struct{})
	go func() {
		m.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run with nil listener + creds + guard must return without panicking")
	}
	m.Stop() // must not panic either
}

// TestNewMuxClonesTLSConfig (I3-C): the tls.Config handed to the http
// Server must be a CLONE — ServeTLS and the TLS listener are allowed to
// mutate their own copy, never the caller's original object.
func TestNewMuxClonesTLSConfig(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	trivial := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	creds := &tls.Config{Certificates: []tls.Certificate{{}}}
	m := NewMux(trivial, trivial, lis, creds).(*muxImpl)
	require.NotNil(t, m.srv.TLSConfig)
	assert.NotSame(t, creds, m.srv.TLSConfig, "the server must own a cloned TLSConfig")
}

// ---- L0: accept-time connection gating --------------------------------------

// TestAuditListenerRejectsBeyondLimit (I3-C): the 61st connection from one
// IP is closed immediately at accept; the http.Server only ever sees the
// allowed ones. Uses a tiny ceiling so the test stays fast.
func TestAuditListenerRejectsBeyondLimit(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	cfg := rpcguard.DefaultConfig()
	cfg.ConnectionRatePerMin = 3
	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))
	audit := newAuditListener(lis, g)

	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := audit.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	var conns []net.Conn
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", lis.Addr().String())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for i := 0; i < 3; i++ {
		select {
		case <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatal("allowed connection was not accepted")
		}
	}

	// The 4th connection from the same IP is rejected: closed before the
	// http.Server ever sees it.
	rejected, err := net.Dial("tcp", lis.Addr().String())
	require.NoError(t, err)
	_ = rejected.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = rejected.Read(buf)
	require.Error(t, err, "a rejected connection must be closed immediately (read fails)")

	// No more conns may surface from the accept loop.
	select {
	case c := <-accepted:
		t.Fatalf("a rejected connection leaked to the accept caller: %v", c.RemoteAddr())
	case <-time.After(300 * time.Millisecond):
	}

	for _, c := range conns {
		_ = c.Close()
	}
	_ = audit.Close()
}

// TestAuditListenerRejectsAtDefaultCeiling (I3-C): pins the PRODUCTION
// boundary — with the default config the 61st connection from one IP is
// closed immediately at accept, exactly as the 4th was in the scaled-down
// test above. 60 loopback dials are cheap, so this stays fast without
// fixed sleeps: acceptance is observed on the channel, rejection via the
// read deadline.
func TestAuditListenerRejectsAtDefaultCeiling(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	g := rpcguard.NewGuard() // production defaults: 60 conns/IP/min
	audit := newAuditListener(lis, g)

	accepted := make(chan net.Conn, 128)
	go func() {
		for {
			c, err := audit.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	var conns []net.Conn
	for i := 0; i < rpcguard.DefaultConnectionRatePerMin; i++ {
		c, err := net.Dial("tcp", lis.Addr().String())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for i := 0; i < rpcguard.DefaultConnectionRatePerMin; i++ {
		select {
		case <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatalf("allowed connection %d was not accepted", i+1)
		}
	}

	rejected, err := net.Dial("tcp", lis.Addr().String())
	require.NoError(t, err)
	_ = rejected.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, err = rejected.Read(buf)
	require.Error(t, err, "the 61st connection must be closed immediately (read fails)")

	select {
	case c := <-accepted:
		t.Fatalf("the 61st connection leaked to the accept caller: %v", c.RemoteAddr())
	case <-time.After(300 * time.Millisecond):
	}

	for _, c := range conns {
		_ = c.Close()
	}
	_ = audit.Close()
}

// ---- L1: trackingConn classification -----------------------------------------

// flushHandshakeSummaries drains the guard and returns handshake-failure
// summaries seen so far.
func flushHandshakeSummaries(t *testing.T, g *rpcguard.Guard) []string {
	t.Helper()
	w := &muxStubWriter{}
	require.NoError(t, g.Flush(w))
	var out []string
	for _, ev := range w.snapshot() {
		if ev.EventType == "rpc.handshake_failure" {
			out = append(out, ev.Summary)
		}
	}
	return out
}

// waitForHandshake polls (flushing each time) until at least one summary
// matches the layer predicate or the timeout expires.
func waitForHandshake(t *testing.T, g *rpcguard.Guard, wantLayer string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, s := range flushHandshakeSummaries(t, g) {
			if wantLayer == "" || containsLayer(s, wantLayer) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no handshake_failure row with layer=%q within %v", wantLayer, timeout)
}

func containsLayer(summary, layer string) bool {
	// The real fixed summary is "... layer=%s count=%d" — the layer field is
	// followed by " count=N", so it never sits at the very END of the string.
	// Match the bounded field (space-delimited on both sides) so a summary
	// can never alias another layer ("pre_tls" is not a prefix of
	// "tls_handshake", and the trailing " count=" guard keeps this honest).
	return strings.Contains(summary, " layer="+layer+" ")
}

// TestTrackingConnClassification (I3-C): 0-byte close → pre_tls; bytes but
// no HTTP → tls_handshake; httpSeen → never a failure; stopping → never a
// failure; double/concurrent Close classifies exactly once.
func TestTrackingConnClassification(t *testing.T) {
	g := rpcguard.NewGuard()
	audit := &auditListener{guard: g}

	t.Run("pre_tls_zero_bytes", func(t *testing.T) {
		c := &trackingConn{Conn: newScriptConn("203.0.113.71", 0), guard: g, parent: audit}
		require.NoError(t, c.Close())
		require.True(t, c.Conn.(*scriptConn).closed.Load())
		summaries := flushHandshakeSummaries(t, g)
		require.Len(t, summaries, 1)
		assert.Contains(t, summaries[0], "layer=pre_tls")
	})

	t.Run("tls_handshake_bytes_no_http", func(t *testing.T) {
		c := &trackingConn{Conn: newScriptConn("203.0.113.72", 16), guard: g, parent: audit}
		buf := make([]byte, 16)
		_, _ = c.Read(buf) // simulate the TLS stack reading the ClientHello
		require.NoError(t, c.Close())
		summaries := flushHandshakeSummaries(t, g)
		require.Len(t, summaries, 1)
		assert.Contains(t, summaries[0], "layer=tls_handshake")
	})

	t.Run("http_seen_never_failure", func(t *testing.T) {
		c := &trackingConn{Conn: newScriptConn("203.0.113.73", 16), guard: g, parent: audit}
		buf := make([]byte, 16)
		_, _ = c.Read(buf)
		c.markHTTPSeen()
		require.NoError(t, c.Close())
		assert.Empty(t, flushHandshakeSummaries(t, g), "a connection that served HTTP is never a failure")
	})

	t.Run("stopping_never_failure", func(t *testing.T) {
		c := &trackingConn{Conn: newScriptConn("203.0.113.74", 16), guard: g, parent: audit}
		buf := make([]byte, 16)
		_, _ = c.Read(buf)
		audit.setStopping()
		require.NoError(t, c.Close())
		assert.Empty(t, flushHandshakeSummaries(t, g), "shutdown teardown is never a failure")
		audit.stopping.Store(false)
	})

	t.Run("close_once_concurrent", func(t *testing.T) {
		c := &trackingConn{Conn: newScriptConn("203.0.113.75", 0), guard: g, parent: audit}
		done := make(chan struct{})
		for i := 0; i < 8; i++ {
			go func() {
				_ = c.Close()
				done <- struct{}{}
			}()
		}
		for i := 0; i < 8; i++ {
			<-done
		}
		summaries := flushHandshakeSummaries(t, g)
		require.Len(t, summaries, 1, "concurrent Close must classify exactly once")
	})
}

// TestFindTrackerUnwrapsTLS (I3-C): ConnContext receives the *tls.Conn;
// findTracker must walk tls.Conn.NetConn back to the trackingConn, and
// return nil for unrelated conns.
func TestFindTrackerUnwrapsTLS(t *testing.T) {
	raw := newScriptConn("203.0.113.76", 0)
	tc := &trackingConn{Conn: raw, guard: rpcguard.NewGuard()}
	tlsConn := tls.Server(tc, &tls.Config{}) // no handshake needed for NetConn()
	require.Same(t, tc, findTracker(tlsConn))

	plain := newScriptConn("203.0.113.77", 0)
	assert.Nil(t, findTracker(plain), "a conn with no tracker must return nil")
	assert.Nil(t, findTracker(nil), "nil must return nil")
}

// ---- real TLS loopback ------------------------------------------------------

// testTLSCert builds a self-signed ECDSA certificate for loopback tests.
func testTLSCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "frp-panel-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return cert
}

// TestRealTLSLoopbackClassifiesConnections (I3-C): through the REAL
// http.Server + ServeTLS stack —
//   - a TCP-only connect+close counts 1 pre_tls;
//   - a garbage (non-TLS) ClientHello counts 1 tls_handshake;
//   - a successful TLS handshake + HTTP request counts 0 failures;
//   - Stop tears down cleanly without a Fatal.
func TestRealTLSLoopbackClassifiesConnections(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	grpc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not grpc", http.StatusBadRequest)
	})
	g := rpcguard.NewGuard()
	m := NewMux(grpc, api, lis, &tls.Config{Certificates: []tls.Certificate{testTLSCert(t)}}, g)

	done := make(chan struct{})
	go func() {
		m.Run()
		close(done)
	}()
	t.Cleanup(func() {
		m.Stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("mux Run did not exit after Stop")
		}
	})

	addr := lis.Addr().String()

	// (a) connect and close without a single byte: pre_tls.
	c0, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_ = c0.Close()
	waitForHandshake(t, g, "pre_tls", 3*time.Second)

	// (b) garbage bytes that are NOT an HTTP request: tls_handshake.
	c1, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_, err = c1.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x00}) // fake TLS record, no ClientHello body
	require.NoError(t, err)
	_ = c1.Close()
	waitForHandshake(t, g, "tls_handshake", 3*time.Second)

	// (c) real TLS handshake + one HTTP request: never a failure.
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test-only loopback
		},
		Timeout: 3 * time.Second,
	}
	req, err := http.NewRequest(http.MethodGet, "https://"+addr+"/ping", nil)
	require.NoError(t, err)
	req.Header.Set("Connection", "close") // server closes the conn right after the response
	resp, err := client.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, "ok", string(body))

	// markHTTPSeen runs synchronously inside the handler wrapper BEFORE the
	// response is written, so by the time the client reads "ok" this conn is
	// already a success — its later Close can only classify as success. No
	// fixed sleep is needed: at every instant this conn contributes nothing.
	//
	// (d) Prove it deterministically: a fresh garbage conn must be the ONLY
	// handshake row (count=1). If the HTTP conn had ever been classified a
	// failure it would have merged into the same per-IP bucket and made the
	// count 2, no matter when its Close actually ran.
	c2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_, _ = c2.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x00}) // fake TLS record, no ClientHello body
	_ = c2.Close()
	require.Eventually(t, func() bool {
		w := &muxStubWriter{}
		if err := g.Flush(w); err != nil {
			return false
		}
		for _, ev := range w.snapshot() {
			if ev.EventType == models.AuditEventTypeRPCHandshakeFailure {
				return strings.Contains(ev.Summary, " layer=tls_handshake count=1")
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond)
}

// TestRealTLSLoopbackStopDoesNotMiscount (I3-C): a connection that is
// mid-handshake when Stop is called must NEVER be classified as a failure
// (shutdown teardown). Through the REAL ServeTLS stack: complete a TLS
// handshake, keep the conn open, Stop, and verify the guard flushes empty.
// Deterministic — client.Handshake() returns only after the server has
// accepted AND tracked the conn (net/http registers the conn at accept
// time, before the handshake), so by the time Stop runs the conn is
// guaranteed to be closed by srv.Close and classified under the stopping
// flag.
func TestRealTLSLoopbackStopDoesNotMiscount(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	grpc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	g := rpcguard.NewGuard()
	m := NewMux(grpc, api, lis, &tls.Config{Certificates: []tls.Certificate{testTLSCert(t)}}, g)

	done := make(chan struct{})
	go func() {
		m.Run()
		close(done)
	}()

	// A fully handshaken, still-open conn: bytes WERE read (ClientHello), so
	// without the stopping flag this would classify as tls_handshake.
	raw, err := net.Dial("tcp", lis.Addr().String())
	require.NoError(t, err)
	tclient := tls.Client(raw, &tls.Config{InsecureSkipVerify: true}) // test-only loopback
	require.NoError(t, tclient.Handshake())
	t.Cleanup(func() { _ = tclient.Close() })

	m.Stop()
	m.Stop() // idempotent
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not exit after Stop")
	}
	assert.Empty(t, flushHandshakeSummaries(t, g),
		"a connection torn down by Stop must never be classified as a failure")
}

// ---- lifecycle ---------------------------------------------------------------

// TestMuxRunStopLifecycle (I3-C): Run exits normally on Stop (no
// log.Fatal), Stop is idempotent, and a nil listener is fail-safe.
func TestMuxRunStopLifecycle(t *testing.T) {
	t.Run("stop_exits_run_no_fatal", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		m := NewMux(api, api, lis, nil)

		done := make(chan struct{})
		go func() {
			m.Run()
			close(done)
		}()

		// The server must serve before we stop it.
		client := &http.Client{Timeout: 2 * time.Second}
		require.Eventually(t, func() bool {
			resp, err := client.Get("http://" + lis.Addr().String() + "/")
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 3*time.Second, 50*time.Millisecond)

		m.Stop()
		m.Stop() // idempotent
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after Stop (would log.Fatal the process)")
		}
	})

	t.Run("nil_listener_failsafe", func(t *testing.T) {
		api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		m := NewMux(api, api, nil, nil)
		done := make(chan struct{})
		go func() {
			m.Run()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run with a nil listener must return without panicking")
		}
		m.Stop() // must not panic either
	})

	t.Run("other_serve_error_logged_not_fatal", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		fl := &failAfterListener{lis: lis, fail: errors.New("injected accept failure")}
		// left=0: the FIRST Accept immediately returns the injected non-normal
		// error (no client is ever dialed), so Serve returns it right away
		// instead of blocking on the underlying listener.
		fl.left.Store(0)
		api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		m := NewMux(api, api, fl, nil)

		done := make(chan struct{})
		go func() {
			m.Run()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Run must log and return on a non-shutdown Serve error (never log.Fatal)")
		}
	})
}
