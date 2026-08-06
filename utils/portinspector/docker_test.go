package portinspector

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// fakeDockerServer serves a minimal read-only docker Engine API over TCP so
// the unix-socket client can be exercised on any host.
type fakeDockerServer struct {
	ln      net.Listener
	handler http.Handler
}

func newFakeDockerServer(t *testing.T, list []dockerContainerListEntry, inspect map[string]dockerContainerInspect) *fakeDockerServer {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/containers/json", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
	})
	mux.HandleFunc("/containers/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		id := strings.TrimSuffix(r.URL.Path[len("/containers/"):], "/json")
		if id == "json" {
			http.NotFound(w, r)
			return
		}
		info, ok := inspect[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return &fakeDockerServer{ln: ln, handler: mux}
}

func TestDockerContainerByPID(t *testing.T) {
	fake := newFakeDockerServer(t,
		[]dockerContainerListEntry{
			{ID: "abc123", Names: []string{"/web"}},
			{ID: "def456", Names: []string{"/db"}},
		},
		map[string]dockerContainerInspect{
			"abc123": {State: struct {
				Pid     int  `json:"Pid"`
				Running bool `json:"Running"`
			}{Pid: 1111, Running: true}},
			"def456": {State: struct {
				Pid     int  `json:"Pid"`
				Running bool `json:"Running"`
			}{Pid: 2222, Running: false}},
		},
	)
	client := newDockerClient()
	client.baseURL = "http://" + fake.ln.Addr().String()
	client.socket = "" // unused: dialer overridden below
	client.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", fake.ln.Addr().String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.containerByPID(ctx)
	if err != nil {
		t.Fatalf("containerByPID error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d containers, want 1 (stopped one excluded)", len(got))
	}
	info, ok := got[1111]
	if !ok {
		t.Fatalf("pid 1111 not mapped: %+v", got)
	}
	if info.ID != "abc123" || info.Name != "web" {
		t.Fatalf("container = %+v, want abc123/web", info)
	}
}

func TestDockerClientDaemonAbsent(t *testing.T) {
	// no daemon anywhere: must be an empty map, not a panic
	client := newDockerClient()
	client.baseURL = "http://127.0.0.1:1"
	client.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", "127.0.0.1:1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := client.containerByPID(ctx)
	if err == nil {
		if len(got) != 0 {
			t.Fatalf("expected empty map, got %+v", got)
		}
	} else {
		// connection refused is acceptable; the caller ignores the error
		t.Logf("daemon absent reported error (acceptable): %v", err)
	}
}
