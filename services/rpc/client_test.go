package rpc

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// fakeStream simulates the master-side bidi stream to one agent.
type fakeStream struct {
	ctx  context.Context
	sent chan *pb.ServerMessage // messages the master sent to the agent
	recv chan *pb.ClientMessage // messages the agent replies with
}

func (f *fakeStream) Send(m *pb.ServerMessage) error {
	f.sent <- m
	return nil
}

func (f *fakeStream) Recv() (*pb.ClientMessage, error) {
	m, ok := <-f.recv
	if !ok {
		return nil, io.EOF
	}
	return m, nil
}

func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}
func (f *fakeStream) SendMsg(m interface{}) error  { return nil }
func (f *fakeStream) RecvMsg(m interface{}) error  { return nil }

type fakeClientsManager struct {
	conn *defs.Connector
}

func (f *fakeClientsManager) Get(cliID string) *defs.Connector                                { return f.conn }
func (f *fakeClientsManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {}
func (f *fakeClientsManager) Remove(cliID string)                                             { f.conn = nil }
func (f *fakeClientsManager) ClientAddr(cliID string) string                                  { return "" }
func (f *fakeClientsManager) ConnectTime(cliID string) (time.Time, bool)                      { return time.Time{}, false }
func (f *fakeClientsManager) UpdateLastSeenAt(cliID string)                                   {}
func (f *fakeClientsManager) GetLastSeenAt(cliID string) (time.Time, bool)                    { return time.Time{}, false }

// fakeApp implements only the Application surface used by CallClient*.
type fakeApp struct {
	app.Application
	cm   app.ClientsManager
	recv *sync.Map
}

func (f *fakeApp) GetClientsManager() app.ClientsManager { return f.cm }
func (f *fakeApp) GetClientRecvMap() *sync.Map           { return f.recv }
func (f *fakeApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

func newFakeEnv(t *testing.T) (*fakeApp, *fakeStream, *app.Context) {
	t.Helper()
	stream := &fakeStream{
		ctx:  context.Background(),
		sent: make(chan *pb.ServerMessage, 16),
		recv: make(chan *pb.ClientMessage, 16),
	}
	cm := &fakeClientsManager{conn: &defs.Connector{CliID: "c1", Conn: stream, CliType: "frpc"}}
	fa := &fakeApp{cm: cm, recv: &sync.Map{}}
	return fa, stream, app.NewContext(context.Background(), fa)
}

// waitSent blocks until the master has sent exactly one ServerMessage and
// returns it.
func waitSent(t *testing.T, stream *fakeStream) *pb.ServerMessage {
	t.Helper()
	select {
	case m := <-stream.sent:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no ServerMessage sent")
		return nil
	}
}

func TestCallClientOffline(t *testing.T) {
	fa, _, ctx := newFakeEnv(t)
	fa.cm = &fakeClientsManager{} // no connector
	_, err := CallClientWithContext(ctx, fa, "ghost", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
	if err == nil {
		t.Fatal("expected error for offline client")
	}
}

func TestCallClientSuccess(t *testing.T) {
	fa, stream, ctx := newFakeEnv(t)
	_ = Recv(fa, "c1") // production pump: delivers agent replies to sessions
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := waitSent(t, stream)
		stream.recv <- &pb.ClientMessage{Event: pb.Event_EVENT_INSPECT_PORTS, SessionId: req.GetSessionId(), Data: []byte("ok")}
	}()
	resp, err := CallClientWithContext(ctx, fa, "c1", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
	<-done
	if err != nil {
		t.Fatalf("CallClientWithContext error: %v", err)
	}
	if string(resp.GetData()) != "ok" {
		t.Fatalf("resp data = %q, want ok", resp.GetData())
	}
	if _, ok := fa.recv.Load(resp.GetSessionId()); ok {
		t.Fatal("session map entry not cleaned after success")
	}
}

func TestCallClientEventError(t *testing.T) {
	fa, stream, ctx := newFakeEnv(t)
	_ = Recv(fa, "c1")
	go func() {
		req := waitSent(t, stream)
		stream.recv <- &pb.ClientMessage{Event: pb.Event_EVENT_ERROR, SessionId: req.GetSessionId(), Data: []byte("boom")}
	}()
	_, err := CallClientWithContext(ctx, fa, "c1", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
	if err == nil {
		t.Fatal("expected error for EVENT_ERROR response")
	}
	fa.recv.Range(func(k, v interface{}) bool {
		t.Fatalf("session map not cleaned after EVENT_ERROR: %v", k)
		return false
	})
}

func TestCallClientTimeout(t *testing.T) {
	fa, stream, _ := newFakeEnv(t)
	callCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := CallClientWithContext(callCtx, fa, "c1", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
		done <- err
	}()
	req := waitSent(t, stream)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CallClientWithContext did not return after timeout")
	}
	if _, ok := fa.recv.Load(req.GetSessionId()); ok {
		t.Fatal("session map entry leaked after timeout")
	}
}

func TestCallClientLateResponseSafe(t *testing.T) {
	// The race: Recv() (or the test here) already holds the session
	// channel, then the timeout fires and the call cleans up. A late
	// response must be discarded safely — never a send-on-closed panic.
	fa, stream, _ := newFakeEnv(t)
	callCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := CallClientWithContext(callCtx, fa, "c1", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
		done <- err
	}()
	req := waitSent(t, stream)

	// hold the channel exactly like Recv() would before delivering
	chAny, ok := fa.recv.Load(req.GetSessionId())
	if !ok {
		t.Fatal("session channel not registered")
	}
	respCh := chAny.(chan *pb.ClientMessage)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CallClientWithContext did not return after timeout")
	}

	// late response into the held channel: must not panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("late response caused panic: %v", r)
		}
	}()
	select {
	case respCh <- &pb.ClientMessage{Event: pb.Event_EVENT_INSPECT_PORTS, SessionId: req.GetSessionId()}:
		// delivered into buffer, nobody reads it — fine
	case <-time.After(2 * time.Second):
		t.Fatal("late response blocked (channel closed or full)")
	}
	if _, ok := fa.recv.Load(req.GetSessionId()); ok {
		t.Fatal("session map entry leaked after timeout")
	}
}

func TestCallClientCleanupAfterSendError(t *testing.T) {
	fa, _, ctx := newFakeEnv(t)
	// a stream whose Send always errors
	broken := &fakeStream{ctx: context.Background(), sent: make(chan *pb.ServerMessage, 16), recv: make(chan *pb.ClientMessage, 16)}
	cm := &fakeClientsManager{conn: &defs.Connector{CliID: "c1", Conn: errorStream{broken}, CliType: "frpc"}}
	fa.cm = cm
	_, err := CallClientWithContext(ctx, fa, "c1", pb.Event_EVENT_INSPECT_PORTS, &pb.InspectPortsRequest{})
	if err == nil {
		t.Fatal("expected send error")
	}
	fa.recv.Range(func(k, v interface{}) bool {
		t.Fatalf("session map entry leaked after send error: %v", k)
		return false
	})
}

type errorStream struct{ *fakeStream }

func (errorStream) Send(m *pb.ServerMessage) error { return io.ErrClosedPipe }

// countingClientsManager counts Get calls and starts with no connector,
// simulating an agent that is not registered (yet).
type countingClientsManager struct {
	mu       sync.Mutex
	getCalls int
	conn     *defs.Connector
}

func (c *countingClientsManager) Get(cliID string) *defs.Connector {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getCalls++
	return c.conn
}
func (c *countingClientsManager) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getCalls
}
func (c *countingClientsManager) setConn(conn *defs.Connector) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
}
func (c *countingClientsManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {}
func (c *countingClientsManager) Remove(cliID string)                                             {}
func (c *countingClientsManager) ClientAddr(cliID string) string                                  { return "" }
func (c *countingClientsManager) ConnectTime(cliID string) (time.Time, bool) {
	return time.Time{}, false
}
func (c *countingClientsManager) UpdateLastSeenAt(cliID string) {}
func (c *countingClientsManager) GetLastSeenAt(cliID string) (time.Time, bool) {
	return time.Time{}, false
}

// TestRecvOfflinePollsBounded proves the Recv pump does not busy-loop
// while the agent is not registered: the number of manager Get calls in
// a fixed window must be bounded. Once the agent comes online the pump
// must resume and stop cleanly on EOF.
func TestRecvOfflinePollsBounded(t *testing.T) {
	cm := &countingClientsManager{}
	fa := &fakeApp{cm: cm, recv: &sync.Map{}}
	done := Recv(fa, "ghost")

	time.Sleep(200 * time.Millisecond)

	if calls := cm.calls(); calls > 20 {
		t.Fatalf("offline Recv busy-loops: %d manager Get calls in 200ms", calls)
	}

	// agent comes online; ending the stream must stop the pump
	stream := &fakeStream{ctx: context.Background(), sent: make(chan *pb.ServerMessage, 16), recv: make(chan *pb.ClientMessage, 16)}
	cm.setConn(&defs.Connector{CliID: "ghost", Conn: stream, CliType: "frpc"})
	close(stream.recv)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not stop after EOF")
	}
}
