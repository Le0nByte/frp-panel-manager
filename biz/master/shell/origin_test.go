package shell

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// testPTYRequest builds a GET request with the given Host and Origin
// ("" = no Origin header at all).
func testPTYRequest(host, origin string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "https://"+host+"/api/v1/pty/abc", nil)
	req.Host = host
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}

// TestGetUpgraderOriginPolicy asserts the browser PTY upgrader policy (spec
// §10.10): the Origin header is mandatory and must be same-origin with the
// request Host; cross-origin and Origin-less upgrades are rejected.
func TestGetUpgraderOriginPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/pty/abc", nil)
	c.Request.Header.Set("Sec-WebSocket-Protocol", "chat")
	up := getUpgrader(c)

	host := "panel.example.com:9443"

	if !up.CheckOrigin(testPTYRequest(host, "https://"+host)) {
		t.Error("same-origin browser request must be allowed")
	}
	if up.CheckOrigin(testPTYRequest(host, "")) {
		t.Error("browser request without Origin must be rejected")
	}
	if up.CheckOrigin(testPTYRequest(host, "https://evil.example.com:9443")) {
		t.Error("cross-origin browser request must be rejected")
	}

	// Sec-WebSocket-Protocol passthrough must be preserved.
	if len(up.Subprotocols) != 1 || up.Subprotocols[0] != "chat" {
		t.Errorf("Subprotocols = %v, want [chat]", up.Subprotocols)
	}
}
