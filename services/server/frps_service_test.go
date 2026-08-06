package server

import (
	"net"
	"testing"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/stretchr/testify/require"
)

// TestNewServerHandlerBindPortConflictReturnsError pins the resilience fix:
// a frps bindPort that is already in use must return an error instead of
// panicking the whole Master process (upstream called logrus.Panic, which
// took down the Master and caused 502s whenever the configured frps port
// collided with an existing listener).
func TestNewServerHandlerBindPortConflictReturnsError(t *testing.T) {
	// occupy a port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	cfg := &v1.ServerConfig{
		BindAddr: "127.0.0.1",
		BindPort: port,
	}
	// bindAddr 127.0.0.1 conflicts with the occupied listener on the same port
	handler, err := NewServerHandler(cfg)
	require.Error(t, err, "conflicting bindPort must return an error")
	require.Contains(t, err.Error(), "cannot create frps server")
	require.Nil(t, handler)
}

// TestNewServerHandlerValidConfigSucceeds: a free port creates the handler.
func TestNewServerHandlerValidConfigSucceeds(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close()) // free the port for the frps bind

	cfg := &v1.ServerConfig{
		BindAddr: "127.0.0.1",
		BindPort: port,
	}
	handler, err := NewServerHandler(cfg)
	require.NoError(t, err)
	require.NotNil(t, handler)
}