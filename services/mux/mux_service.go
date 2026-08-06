package mux

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type MuxServer interface {
	Run()
	Stop()
}

type muxImpl struct {
	srv      *http.Server
	lis      net.Listener
	tls      bool
	audit    *auditListener
	stopOnce sync.Once
}

// NewMux builds the mux server. When creds (TLS) AND a guard are both
// present — the production public 9101 listener — the raw listener is
// wrapped in an auditListener so every accepted connection is gated by the
// guard and TLS failures are classified into the audit trail. The loopback
// HTTP mux (creds == nil) is NEVER wrapped. guards is variadic for legacy
// callers; production fx always passes the shared guard explicitly.
func NewMux(grpcServer, apiServer http.Handler, lis net.Listener, creds *tls.Config, guards ...*rpcguard.Guard) MuxServer {
	var guard *rpcguard.Guard
	if len(guards) > 0 {
		guard = guards[0]
	}

	tlsServer := grpcHandlerFunc(grpcServer, apiServer)
	if creds != nil {
		// The TLSConfig handed to the http.Server is a CLONE: ServeTLS and
		// the TLS listener are allowed to mutate the config they own
		// (NextProtos adjustment, session ticket keys), and concurrent
		// mutation of the caller's original object would be a data race.
		tlsServer.TLSConfig = creds.Clone()
	}

	m := &muxImpl{
		srv: tlsServer,
		lis: lis,
		tls: creds != nil,
	}

	// Wrap ONLY when there is a real listener to wrap: with creds+guard but
	// a nil lis (the TLS listener failed to bind), m.lis stays nil so Run
	// takes its nil-listener fail-safe path instead of panicking inside the
	// audit listener's Accept.
	if creds != nil && guard != nil && lis != nil {
		audit := newAuditListener(lis, guard)
		m.lis = audit
		m.audit = audit

		// Connect the *tls.Conn (which ConnContext receives) back to its
		// trackingConn so the handler wrapper can mark "an HTTP request
		// arrived" on the exact connection.
		tlsServer.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
			if tr := findTracker(c); tr != nil {
				return context.WithValue(ctx, trackerKey{}, tr)
			}
			return ctx
		}

		// Mark httpSeen at the start of every HTTP request. The h2c
		// handler dispatches both HTTP/1.1 and HTTP/2 here, so one wrapper
		// covers every successful path.
		orig := tlsServer.Handler
		tlsServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tr := trackerFromRequest(r.Context()); tr != nil {
				tr.markHTTPSeen()
			}
			orig.ServeHTTP(w, r)
		})
	}

	return m
}

func (m *muxImpl) Run() {
	if m.lis == nil {
		// Fail-safe: a nil listener (e.g. the API listener failed to bind)
		// must never panic the accept loop.
		return
	}
	if m.tls {
		if err := m.srv.ServeTLS(m.lis, "", ""); err != nil {
			if isNormalServeExit(err) {
				return
			}
			logger.Logger(context.Background()).WithError(err).Errorf("tls mux serve error")
		}
		return
	}
	if err := m.srv.Serve(m.lis); err != nil {
		if isNormalServeExit(err) {
			return
		}
		logger.Logger(context.Background()).WithError(err).Errorf("http mux serve error")
	}
}

// isNormalServeExit reports whether Serve/ServeTLS returned because the
// server was stopped (http.ErrServerClosed) or the listener was closed
// (net.ErrClosed) — both are normal shutdown, never errors.
func isNormalServeExit(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed)
}

// Stop shuts the mux down: the shutdown phase is marked FIRST so no
// connection torn down by the close is ever classified as a handshake
// failure, then the http.Server (which closes the listener and every
// active connection) is closed. Idempotent.
func (m *muxImpl) Stop() {
	m.stopOnce.Do(func() {
		if m.audit != nil {
			m.audit.setStopping()
		}
		if m.srv != nil {
			_ = m.srv.Close()
		}
		if m.lis != nil {
			_ = m.lis.Close()
		}
	})
}

func grpcHandlerFunc(grpcServer http.Handler, httpHandler http.Handler) *http.Server {
	return &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("proto major: %d,  %s , %s\n", r.ProtoMajor, r.RequestURI, r.Header.Get("Content-Type"))
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			httpHandler.ServeHTTP(w, r)
		}
	}), &http2.Server{}), ReadHeaderTimeout: time.Second * 30}
}
