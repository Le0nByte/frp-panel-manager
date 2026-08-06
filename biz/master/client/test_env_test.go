package client

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/fatedier/frp/pkg/config/v1"
	"github.com/glebarez/sqlite"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---- agent stream + manager fakes (master side of the bidi stream) ----

type fakeAgentStream struct {
	ctx  context.Context
	sent chan *pb.ServerMessage // messages the master sent to the agent
	recv chan *pb.ClientMessage // messages the agent replies with
}

func (f *fakeAgentStream) Send(m *pb.ServerMessage) error {
	f.sent <- m
	return nil
}

func (f *fakeAgentStream) Recv() (*pb.ClientMessage, error) {
	m, ok := <-f.recv
	if !ok {
		return nil, io.EOF
	}
	return m, nil
}

func (f *fakeAgentStream) Context() context.Context     { return f.ctx }
func (f *fakeAgentStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeAgentStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeAgentStream) SetTrailer(metadata.MD)       {}
func (f *fakeAgentStream) SendMsg(m interface{}) error  { return nil }
func (f *fakeAgentStream) RecvMsg(m interface{}) error  { return nil }

// fakeAgentManager is the thread-safe ClientsManager replacement: the
// rpc.Recv pump reads Get concurrently with the handler's send path, so
// the map MUST be synchronized (a bare map panics under churn).
type fakeAgentManager struct {
	mu    sync.RWMutex
	conns map[string]*defs.Connector
}

func (m *fakeAgentManager) Get(cliID string) *defs.Connector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[cliID]
}

func (m *fakeAgentManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {}
func (m *fakeAgentManager) Remove(cliID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, cliID)
}
func (m *fakeAgentManager) ClientAddr(cliID string) string { return "" }
func (m *fakeAgentManager) ConnectTime(cliID string) (time.Time, bool) {
	return time.Time{}, false
}
func (m *fakeAgentManager) UpdateLastSeenAt(cliID string) {}
func (m *fakeAgentManager) GetLastSeenAt(cliID string) (time.Time, bool) {
	return time.Time{}, false
}

// ---- environment ----

var masterClientDSNCounter uint64

// masterClientEnv wires the REAL DAO (in-memory SQLite) + the REAL RPC
// layer to a fake agent stream per physical client id, so the handlers
// run their full DB-first flow and block on the real agent ACK.
type masterClientEnv struct {
	t       *testing.T
	ctx     *app.Context
	appInst app.Application
	manager *fakeAgentManager
	db      *gorm.DB
	user    models.UserInfo
	streams map[string]*fakeAgentStream
}

func newMasterClientEnv(t *testing.T) *masterClientEnv {
	t.Helper()
	n := atomic.AddUint64(&masterClientDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:master-client-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Client{}, &models.Server{}, &models.ProxyConfig{}))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	appInst.SetClientRecvMap(&sync.Map{})

	user := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1, Token: "test-token"}
	ctx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), appInst)

	env := &masterClientEnv{
		t:       t,
		ctx:     ctx,
		appInst: appInst,
		manager: &fakeAgentManager{conns: map[string]*defs.Connector{}},
		db:      db,
		user:    user,
		streams: map[string]*fakeAgentStream{},
	}
	appInst.SetClientsManager(env.manager)
	return env
}

// registerAgent attaches a fake agent stream for a physical client id and
// starts the production Recv pump. The stream's recv channel is closed at
// test end to stop the pump (EOF), so no pump goroutine outlives the test.
func (e *masterClientEnv) registerAgent(clientID string) *fakeAgentStream {
	e.t.Helper()
	stream := &fakeAgentStream{
		ctx:  context.Background(),
		sent: make(chan *pb.ServerMessage, 16),
		recv: make(chan *pb.ClientMessage, 16),
	}
	e.manager.conns[clientID] = &defs.Connector{CliID: clientID, Conn: stream, CliType: "frpc"}
	e.streams[clientID] = stream
	_ = rpc.Recv(e.appInst, clientID)
	e.t.Cleanup(func() { close(stream.recv) })
	return stream
}

// waitForEvent blocks until exactly one ServerMessage arrives and returns it.
func (e *masterClientEnv) waitForEvent(stream *fakeAgentStream) *pb.ServerMessage {
	e.t.Helper()
	select {
	case m := <-stream.sent:
		return m
	case <-time.After(5 * time.Second):
		e.t.Fatal("no ServerMessage sent to the agent")
		return nil
	}
}

// ackUpdateFRPC replies to the pending UPDATE_FRPC event with the given
// status, wrapped in the real response type the agent uses.
func (e *masterClientEnv) ackUpdateFRPC(stream *fakeAgentStream, req *pb.ServerMessage, code pb.RespCode, msg string) {
	e.t.Helper()
	data, err := proto.Marshal(&pb.UpdateFRPCResponse{Status: &pb.Status{Code: code, Message: msg}})
	require.NoError(e.t, err)
	stream.recv <- &pb.ClientMessage{Event: pb.Event_EVENT_UPDATE_FRPC, SessionId: req.GetSessionId(), Data: data}
}

// ackUpdateFRPCLoop ACKs exactly count UPDATE_FRPC events with SUCCESS.
// The cleanup asserts the loop finished: an event that never arrives
// (handler failed before sending) surfaces as a test error, and no ack
// goroutine outlives the test. Never calls t.Fatal from the loop
// goroutine (testing.T is not goroutine-safe for FailNow).
func (e *masterClientEnv) ackUpdateFRPCLoop(stream *fakeAgentStream, count int) {
	e.t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < count; i++ {
			var req *pb.ServerMessage
			select {
			case req = <-stream.sent:
			case <-time.After(5 * time.Second):
				return // handler failed before sending; the test fails elsewhere
			}
			e.ackUpdateFRPC(stream, req, pb.RespCode_RESP_CODE_SUCCESS, "ok")
		}
	}()
	e.t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			e.t.Error("ack loop did not finish — an expected agent event never arrived")
		}
	})
}

// seedShadowedPair creates a shadowed origin + a configured child, so the
// UpdateFrpcHander flow resolves the child, skips every shadow branch and
// goes straight to the DB write + agent ACK.
func (e *masterClientEnv) seedShadowedPair(proxies ...v1.TypedProxyConfig) {
	e.t.Helper()
	child := &models.ClientEntity{
		ClientID:       "cli-child",
		OriginClientID: "cli-origin",
		ServerID:       "srv-1",
		IsShadow:       false,
		Stopped:        false,
		ConnectSecret:  "child-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = proxies
	require.NoError(e.t, child.SetConfigContent(*cfg))
	require.NoError(e.t, dao.NewMutation(e.ctx).CreateClient(e.user, child))

	origin := &models.ClientEntity{
		ClientID:      "cli-origin",
		IsShadow:      true,
		Stopped:       false,
		ConnectSecret: "origin-secret",
	}
	require.NoError(e.t, dao.NewMutation(e.ctx).CreateClient(e.user, origin))

	srv := &models.ServerEntity{
		ServerID:      "srv-1",
		ServerIP:      "127.0.0.1",
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret",
	}
	require.NoError(e.t, dao.NewMutation(e.ctx).CreateServer(e.user, srv))
}

// seedUnshadowedOrigin creates the legacy pre-port-manager shape: one
// physical origin row owns the complete client config. The first update must
// move that config to a child and direct the Agent to replace the origin
// handler with the generated child handler.
func (e *masterClientEnv) seedUnshadowedOrigin() {
	e.t.Helper()
	cfg, err := utils.LoadClientConfigNormal([]byte(validFrpcTOML), true)
	require.NoError(e.t, err)
	origin := &models.ClientEntity{
		ClientID:      "cli-origin",
		ServerID:      "srv-1",
		Stopped:       false,
		ConnectSecret: "origin-secret",
	}
	require.NoError(e.t, origin.SetConfigContent(*cfg))
	require.NoError(e.t, dao.NewMutation(e.ctx).CreateClient(e.user, origin))

	srv := &models.ServerEntity{
		ServerID:      "srv-1",
		ServerIP:      "127.0.0.1",
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret",
	}
	require.NoError(e.t, dao.NewMutation(e.ctx).CreateServer(e.user, srv))
}

// setOriginStopped flips the origin's Stopped flag (the sync agent call
// must be skipped for a stopped physical client, upstream semantics).
func (e *masterClientEnv) setOriginStopped(stopped bool) {
	e.t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByClientID(e.user, "cli-origin")
	require.NoError(e.t, err)
	cli.ClientEntity.Stopped = stopped
	require.NoError(e.t, dao.NewMutation(e.ctx).UpdateClient(e.user, cli.ClientEntity))
}

func (e *masterClientEnv) childConfig() *v1.ClientConfig {
	e.t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByClientID(e.user, "cli-child")
	require.NoError(e.t, err)
	cfg, err := cli.ClientEntity.GetConfigContent()
	require.NoError(e.t, err)
	return cfg
}

func (e *masterClientEnv) recvMapLen() int {
	e.t.Helper()
	n := 0
	e.appInst.GetClientRecvMap().Range(func(k, v interface{}) bool {
		n++
		return true
	})
	return n
}

// validFrpcTOML is a parseable frpc client config with one TCP proxy.
const validFrpcTOML = `
serverAddr = "127.0.0.1"
serverPort = 7000

[[proxies]]
name = "ssh"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 24
`

func updateFrpcReq(clientID, serverID string, config []byte) *pb.UpdateFRPCRequest {
	return &pb.UpdateFRPCRequest{
		ClientId: lo.ToPtr(clientID),
		ServerId: lo.ToPtr(serverID),
		Config:   config,
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(m)
	require.NoError(t, err)
	return data
}
