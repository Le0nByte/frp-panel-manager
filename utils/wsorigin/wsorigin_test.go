package wsorigin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// newOriginRequest builds a GET request whose Host header matches host and
// whose Origin header is origin ("" = no Origin header at all). The origin
// scheme is not compared against the connection scheme: the master API is
// fronted by a reverse proxy, so legitimate browser requests arrive as plain
// HTTP with an https Origin (spec §10.10, proxy-fronted deployment).
func newOriginRequest(host, origin string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "https://"+host+"/wsgrpc", nil)
	req.Host = host
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}

func TestCheckOriginSameOrigin(t *testing.T) {
	cases := []struct {
		name        string
		host        string
		origin      string
		allowEmpty  bool
		wantAllowed bool
	}{
		{"https host with port", "panel.example.com:9443", "https://panel.example.com:9443", true, true},
		{"http host with port", "panel.example.com:8080", "http://panel.example.com:8080", false, true},
		{"host case insensitive", "PANEL.EXAMPLE.COM:9443", "https://panel.example.com:9443", false, true},
		{"origin scheme case insensitive", "panel.example.com:9443", "HTTPS://panel.example.com:9443", false, true},
		{"origin trailing slash", "panel.example.com:9443", "https://panel.example.com:9443/", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CheckOrigin(newOriginRequest(tc.host, tc.origin), tc.allowEmpty)
			if got != tc.wantAllowed {
				t.Errorf("CheckOrigin(host=%q, origin=%q, allowEmpty=%v) = %v, want %v",
					tc.host, tc.origin, tc.allowEmpty, got, tc.wantAllowed)
			}
		})
	}
}

func TestCheckOriginRejectsDifferentOrigin(t *testing.T) {
	cases := []struct {
		name   string
		host   string
		origin string
	}{
		{"different host", "panel.example.com:9443", "https://evil.example.com:9443"},
		{"different port", "panel.example.com:9443", "https://panel.example.com:9444"},
		{"origin without port", "panel.example.com:9443", "https://panel.example.com"},
		{"origin with default port", "panel.example.com", "https://panel.example.com:443"},
		{"host suffix contains attack", "panel.example.com:9443", "https://panel.example.com:9443.evil.example.com"},
		{"subdomain is not same origin", "panel.example.com:9443", "https://sub.panel.example.com:9443"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CheckOrigin(newOriginRequest(tc.host, tc.origin), true); got {
				t.Errorf("CheckOrigin(host=%q, origin=%q, allowEmpty=true) = true, want false",
					tc.host, tc.origin)
			}
		})
	}
}

func TestCheckOriginRejectsMalformed(t *testing.T) {
	malformed := []string{
		"not a url",
		"https://",
		"://panel.example.com:9443",
		"null",
		"ftp://panel.example.com:9443",
		"file:///etc/passwd",
		"ws://panel.example.com:9443",
		"wss://panel.example.com:9443",
	}
	for _, origin := range malformed {
		t.Run(origin, func(t *testing.T) {
			if got := CheckOrigin(newOriginRequest("panel.example.com:9443", origin), true); got {
				t.Errorf("CheckOrigin(origin=%q, allowEmpty=true) = true, want false", origin)
			}
		})
	}
}

func TestCheckOriginRejectsPresentButEmptyValue(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://panel.example.com:9443/ws", nil)
	req.Host = "panel.example.com:9443"
	req.Header.Set("Origin", "")
	if got := CheckOrigin(req, true); got {
		t.Error("present-but-empty Origin header must be rejected even with allowEmpty=true")
	}
}

func TestCheckOriginEmptyHeaderPolicy(t *testing.T) {
	// /wsgrpc: non-browser agents may omit Origin entirely.
	if got := CheckOrigin(newOriginRequest("panel.example.com:9443", ""), true); !got {
		t.Error("missing Origin with allowEmpty=true must be allowed (wsgrpc agent)")
	}
	// shell/PTY: browser clients must always send a same-origin Origin.
	if got := CheckOrigin(newOriginRequest("panel.example.com:9443", ""), false); got {
		t.Error("missing Origin with allowEmpty=false must be rejected (browser PTY)")
	}
}

func TestCheckOriginRejectsMultipleValues(t *testing.T) {
	// Two separate Origin header lines: never produced by a real browser,
	// must be rejected instead of scanning values for a substring match.
	req := httptest.NewRequest(http.MethodGet, "https://panel.example.com:9443/ws", nil)
	req.Host = "panel.example.com:9443"
	req.Header.Add("Origin", "https://panel.example.com:9443")
	req.Header.Add("Origin", "https://evil.example.com:9443")
	if got := CheckOrigin(req, true); got {
		t.Error("multiple Origin header values must be rejected")
	}

	// Comma-joined values inside a single header line must not pass either.
	req2 := httptest.NewRequest(http.MethodGet, "https://panel.example.com:9443/ws", nil)
	req2.Host = "panel.example.com:9443"
	req2.Header.Set("Origin", "https://panel.example.com:9443,https://evil.example.com")
	if got := CheckOrigin(req2, true); got {
		t.Error("comma-joined Origin header must be rejected")
	}
}

// startOriginServer runs a real HTTP server that upgrades WebSocket
// handshakes with the given upgrader, mirroring the production router.
func startOriginServer(t *testing.T, up *websocket.Upgrader) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			// gorilla already wrote the rejection response.
			return
		}
		conn.Close()
	}))
	t.Cleanup(srv.Close)
	return srv
}

// dialWS drives a real gorilla client handshake against the server.
func dialWS(t *testing.T, srv *httptest.Server, origin string) error {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if conn != nil {
		conn.Close()
	}
	if err != nil && resp != nil {
		t.Logf("dial %q failed with status %d: %v", origin, resp.StatusCode, err)
	}
	return err
}

// TestGorillaUpgraderRealHandshake drives a real gorilla client and server
// through the exact Upgrade code path the production router uses, so the
// CheckOrigin policy is exercised end-to-end, not via string scanning or
// stubs.
func TestGorillaUpgraderRealHandshake(t *testing.T) {
	t.Run("pty same-origin accepted", func(t *testing.T) {
		srv := startOriginServer(t, &websocket.Upgrader{CheckOrigin: SameOriginRequired()})
		// The httptest server is plain HTTP, so a same-origin browser
		// request carries Origin equal to the server URL, whose host:port
		// matches the request Host the gorilla client sends.
		if err := dialWS(t, srv, srv.URL); err != nil {
			t.Fatalf("same-origin upgrade failed: %v", err)
		}
	})

	t.Run("pty cross-origin rejected", func(t *testing.T) {
		srv := startOriginServer(t, &websocket.Upgrader{CheckOrigin: SameOriginRequired()})
		if err := dialWS(t, srv, "http://evil.example.com:9443"); err == nil {
			t.Fatal("cross-origin upgrade succeeded, want rejection")
		}
	})

	t.Run("pty missing origin rejected", func(t *testing.T) {
		srv := startOriginServer(t, &websocket.Upgrader{CheckOrigin: SameOriginRequired()})
		if err := dialWS(t, srv, ""); err == nil {
			t.Fatal("missing-origin upgrade succeeded, want rejection")
		}
	})

	t.Run("wsgrpc agent missing origin accepted", func(t *testing.T) {
		srv := startOriginServer(t, &websocket.Upgrader{CheckOrigin: SameOriginAllowEmpty()})
		if err := dialWS(t, srv, ""); err != nil {
			t.Fatalf("agent upgrade without Origin failed: %v", err)
		}
	})

	t.Run("wsgrpc agent cross-origin rejected", func(t *testing.T) {
		srv := startOriginServer(t, &websocket.Upgrader{CheckOrigin: SameOriginAllowEmpty()})
		if err := dialWS(t, srv, "http://evil.example.com:9443"); err == nil {
			t.Fatal("agent cross-origin upgrade succeeded, want rejection")
		}
	})
}
