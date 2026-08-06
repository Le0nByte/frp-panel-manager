package master

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/cache"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"gorm.io/gorm"
)

// ---- guard wiring ----------------------------------------------------------

// TestNewServerCarriesGuard: the production server constructor must carry
// the injected guard; without one the guard stays nil (legacy mode).
func TestNewServerCarriesGuard(t *testing.T) {
	g := rpcguard.NewGuard()
	srv := newServer(app.NewApp(), g)
	require.Same(t, g, srv.guard, "newServer must store the exact injected pointer")
	require.Nil(t, newServer(app.NewApp()).guard, "no guard => nil (legacy)")
}

// TestNewMasterServiceCarriesGuard: NewMasterService must thread the
// guard through newRpcServer into the registered server implementation.
func TestNewMasterServiceCarriesGuard(t *testing.T) {
	g := rpcguard.NewGuard()
	m := NewMasterService(app.NewApp(), insecure.NewCredentials(), g).(*master)
	require.Same(t, g, m.guard, "NewMasterService must carry the guard into the server")
	m2 := NewMasterService(app.NewApp(), insecure.NewCredentials()).(*master)
	require.Nil(t, m2.guard, "legacy call without guard must stay nil")
}

// TestMasterServicesShareOneGuard: the three production entry points that
// fx builds (TLS, HTTP and WS MasterServices) must receive the SAME
// pointer when constructed from one guard — the whole point of wiring the
// guard through fx rather than per-service singletons.
func TestMasterServicesShareOneGuard(t *testing.T) {
	g := rpcguard.NewGuard()
	tlsSvc := NewMasterService(app.NewApp(), credentials.NewTLS(&tls.Config{}), g)
	httpSvc := NewMasterService(app.NewApp(), insecure.NewCredentials(), g)
	wsSvc := NewMasterService(app.NewApp(), insecure.NewCredentials(), g)
	require.Same(t, g, tlsSvc.(*master).guard)
	require.Same(t, g, httpSvc.(*master).guard)
	require.Same(t, g, wsSvc.(*master).guard)
}

// ---- inbound context wrapping ----------------------------------------------

// auditRecorder collects the fixed-structure system rows drained by Flush.
type auditRecorder struct {
	events []*models.AuditEvent
}

func (w *auditRecorder) LogSystem(ev *models.AuditEvent) error {
	w.events = append(w.events, ev)
	return nil
}

// peerCtx attaches a transport peer, like a real gRPC request context.
func peerCtx(ip string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 7000},
	})
}

// guardSrv builds a server whose app panics on any DAO access (nil
// embedded Application), so a successful typed rejection proves the guard
// fired before the DAO. Feature gates are enabled unless disabled.
func guardSrv(t *testing.T, cfg *conf.Config) (*server, *rpcguard.Guard) {
	t.Helper()
	c := conf.Config{}
	if cfg != nil {
		c = *cfg
	}
	c.Master.Features.EnableWorker = true
	c.Master.Features.EnableWireGuard = true
	g := rpcguard.NewGuard()
	return newServer(&gateSrvApp{cfg: c}, g), g
}

// flushSecretFailures drains the guard and returns the (eventType, ip,
// layer, count) tuples of its secret-failure rows.
func flushSecretFailures(t *testing.T, g *rpcguard.Guard) []string {
	t.Helper()
	w := &auditRecorder{}
	require.NoError(t, g.Flush(w))
	var out []string
	for _, ev := range w.events {
		if ev.EventType == models.AuditEventTypeRPCSecretFailure {
			out = append(out, ev.Summary)
		}
	}
	return out
}

func requireOneFailure(t *testing.T, g *rpcguard.Guard, ip, layer string) {
	t.Helper()
	summaries := flushSecretFailures(t, g)
	require.Len(t, summaries, 1, "exactly one secret-failure row expected, got %v", summaries)
	require.Contains(t, summaries[0], "ip="+ip, "failure must carry the request peer")
	require.Contains(t, summaries[0], "layer="+layer)
}

// TestUnaryMethodsReachGuard: every unary inbound method must wrap the
// ORIGINAL request context (peer preserved) with the guard before the
// helper runs, so an empty base is rejected with the typed sentinel and a
// guard failure recorded — with zero DAO access.
func TestUnaryMethodsReachGuard(t *testing.T) {
	ip := "203.0.113.30"
	cases := []struct {
		name    string
		invoke  func(s *server, ctx context.Context) error
		wantErr error
		layer   string
	}{
		{
			name: "ReportWireGuardRuntimeInfo",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.ReportWireGuardRuntimeInfo(ctx, &pb.ReportWireGuardRuntimeInfoReq{Base: &pb.ClientBase{}})
				return err
			},
			wantErr: dao.ErrInvalidClientSecret,
			layer:   rpcguard.LayerClient,
		},
		{
			name: "ListClientWireGuards",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.ListClientWireGuards(ctx, &pb.ListClientWireGuardsRequest{Base: &pb.ClientBase{}})
				return err
			},
			wantErr: dao.ErrInvalidClientSecret,
			layer:   rpcguard.LayerClient,
		},
		{
			name: "ListClientWorkers",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.ListClientWorkers(ctx, &pb.ListClientWorkersRequest{Base: &pb.ClientBase{}})
				return err
			},
			wantErr: dao.ErrInvalidClientSecret,
			layer:   rpcguard.LayerClient,
		},
		{
			name: "PullClientConfig",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.PullClientConfig(ctx, &pb.PullClientConfigReq{Base: &pb.ClientBase{}})
				return err
			},
			wantErr: dao.ErrInvalidClientSecret,
			layer:   rpcguard.LayerClient,
		},
		{
			name: "PullServerConfig",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.PullServerConfig(ctx, &pb.PullServerConfigReq{Base: &pb.ServerBase{}})
				return err
			},
			wantErr: dao.ErrInvalidServerSecret,
			layer:   rpcguard.LayerServer,
		},
		{
			name: "PushProxyInfo",
			invoke: func(s *server, ctx context.Context) error {
				_, err := s.PushProxyInfo(ctx, &pb.PushProxyInfoReq{Base: &pb.ServerBase{}})
				return err
			},
			wantErr: dao.ErrInvalidServerSecret,
			layer:   rpcguard.LayerServer,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, g := guardSrv(t, nil)
			require.ErrorIs(t, tc.invoke(s, peerCtx(ip)), tc.wantErr,
				"empty base must be rejected with the typed sentinel")
			requireOneFailure(t, g, ip, tc.layer)
		})
	}
}

// ---- stream method context wrapping ----------------------------------------

// fakeClientLogStream implements pb.Master_PushClientStreamLogServer with
// a peer-carrying context and a fixed message queue.
type fakeClientLogStream struct {
	ctx  context.Context
	msgs []*pb.PushClientStreamLogReq
}

func (f *fakeClientLogStream) Send(*pb.PushStreamLogResp) error { return nil }
func (f *fakeClientLogStream) SendAndClose(*pb.PushStreamLogResp) error {
	return nil
}
func (f *fakeClientLogStream) Recv() (*pb.PushClientStreamLogReq, error) {
	if len(f.msgs) == 0 {
		return nil, io.EOF
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return m, nil
}
func (f *fakeClientLogStream) Context() context.Context     { return f.ctx }
func (f *fakeClientLogStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeClientLogStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeClientLogStream) SetTrailer(metadata.MD)       {}
func (f *fakeClientLogStream) SendMsg(interface{}) error    { return nil }
func (f *fakeClientLogStream) RecvMsg(interface{}) error    { return nil }

// fakeServerLogStream implements pb.Master_PushServerStreamLogServer.
type fakeServerLogStream struct {
	ctx  context.Context
	msgs []*pb.PushServerStreamLogReq
}

func (f *fakeServerLogStream) Send(*pb.PushStreamLogResp) error { return nil }
func (f *fakeServerLogStream) SendAndClose(*pb.PushStreamLogResp) error {
	return nil
}
func (f *fakeServerLogStream) Recv() (*pb.PushServerStreamLogReq, error) {
	if len(f.msgs) == 0 {
		return nil, io.EOF
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return m, nil
}
func (f *fakeServerLogStream) Context() context.Context     { return f.ctx }
func (f *fakeServerLogStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerLogStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerLogStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerLogStream) SendMsg(interface{}) error    { return nil }
func (f *fakeServerLogStream) RecvMsg(interface{}) error    { return nil }

// fakePTYStream implements pb.Master_PTYConnectServer.
type fakePTYStream struct {
	ctx  context.Context
	msgs []*pb.PTYClientMessage
}

func (f *fakePTYStream) Send(*pb.PTYServerMessage) error { return nil }
func (f *fakePTYStream) Recv() (*pb.PTYClientMessage, error) {
	if len(f.msgs) == 0 {
		return nil, io.EOF
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return m, nil
}
func (f *fakePTYStream) Context() context.Context     { return f.ctx }
func (f *fakePTYStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakePTYStream) SendHeader(metadata.MD) error { return nil }
func (f *fakePTYStream) SetTrailer(metadata.MD)       {}
func (f *fakePTYStream) SendMsg(interface{}) error    { return nil }
func (f *fakePTYStream) RecvMsg(interface{}) error    { return nil }

// TestStreamMethodsReachGuard: the stream handlers must build their
// app.Context from sender.Context() (not context.Background()), so the
// peer survives into the guard and per-message validation failures are
// recorded.
func TestStreamMethodsReachGuard(t *testing.T) {
	ip := "203.0.113.31"

	t.Run("PushClientStreamLog", func(t *testing.T) {
		s, g := guardSrv(t, nil)
		sender := &fakeClientLogStream{
			ctx:  peerCtx(ip),
			msgs: []*pb.PushClientStreamLogReq{{Base: &pb.ClientBase{}}},
		}
		err := s.PushClientStreamLog(sender)
		require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
		requireOneFailure(t, g, ip, rpcguard.LayerClient)
	})

	t.Run("PushServerStreamLog", func(t *testing.T) {
		s, g := guardSrv(t, nil)
		sender := &fakeServerLogStream{
			ctx:  peerCtx(ip),
			msgs: []*pb.PushServerStreamLogReq{{Base: &pb.ServerBase{}}},
		}
		err := s.PushServerStreamLog(sender)
		require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
		requireOneFailure(t, g, ip, rpcguard.LayerServer)
	})

	t.Run("PTYConnect", func(t *testing.T) {
		s, g := guardSrv(t, nil)
		sender := &fakePTYStream{
			ctx: peerCtx(ip),
			msgs: []*pb.PTYClientMessage{{
				Base: &pb.PTYClientMessage_ClientBase{ClientBase: &pb.ClientBase{}},
			}},
		}
		err := s.PTYConnect(sender)
		require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
		requireOneFailure(t, g, ip, rpcguard.LayerClient)
	})
}

// TestFRPCAuthWrappedContextNoRegression: FRPCAuth is not a ConnectSecret
// flow, but its context must still be guard-wrapped — the RPC keeps
// working end-to-end (cache + real user row) through the wrapped context.
func TestFRPCAuthWrappedContextNoRegression(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:master-frpc-auth?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.User{}))
	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID: 1, UserName: "alice", Email: "alice@example.com", Token: "tok-1",
	}}).Error)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	a := app.NewApp()
	a.SetDBManager(dbm)

	cacheCfg := conf.Config{}
	cacheCfg.Master.CacheSize = 1
	cache.InitCache(cacheCfg)

	g := rpcguard.NewGuard()
	s := newServer(a, g)
	resp, err := s.FRPCAuth(peerCtx("203.0.113.32"), &pb.FRPAuthRequest{User: "alice", Token: "tok-1"})
	require.NoError(t, err)
	require.True(t, resp.GetOk())
	// FRPCAuth never validates a ConnectSecret: no failure rows, no panic.
	require.Empty(t, flushSecretFailures(t, g))
}

// TestServerSendEOFWithoutPeerContext: a stream that ends without a
// register must terminate cleanly through the wrapped sender context.
func TestServerSendEOFWithoutPeerContext(t *testing.T) {
	s, _ := guardSrv(t, nil)
	sender := &fakeServerSendStream{ctx: peerCtx("203.0.113.33"), recv: make(chan *pb.ClientMessage)}
	close(sender.recv)
	require.NoError(t, s.ServerSend(sender), "empty stream must end with EOF, not panic")
}

// ---- helpers ---------------------------------------------------------------

// fakeServerSendStream implements pb.Master_ServerSendServer. sent is a
// buffered channel so the handler goroutine never blocks on it and tests
// read with timeouts.
type fakeServerSendStream struct {
	ctx  context.Context
	recv chan *pb.ClientMessage
	sent chan *pb.ServerMessage
}

func (f *fakeServerSendStream) Send(m *pb.ServerMessage) error {
	f.sent <- m
	return nil
}
func (f *fakeServerSendStream) Recv() (*pb.ClientMessage, error) {
	m, ok := <-f.recv
	if !ok {
		return nil, io.EOF
	}
	return m, nil
}
func (f *fakeServerSendStream) Context() context.Context     { return f.ctx }
func (f *fakeServerSendStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerSendStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerSendStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerSendStream) SendMsg(interface{}) error    { return nil }
func (f *fakeServerSendStream) RecvMsg(interface{}) error    { return nil }
