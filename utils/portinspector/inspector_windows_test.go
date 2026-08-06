//go:build windows

package portinspector

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// These tests bind real temporary listeners on the host and verify the
// inspector sees them (spec §5.3-6: real sockets as ground truth).

func freeTCPListener(t *testing.T) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	t.Cleanup(func() { _ = ln.Close() })
	return ln, port
}

func TestScanPortsFindsTCPListener(t *testing.T) {
	_, port := freeTCPListener(t)
	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ScanPorts(ctx, ScanOptions{
		Protocol: "tcp",
		PortMin:  uint32(port),
		PortMax:  uint32(port),
	})
	if err != nil {
		t.Fatalf("ScanPorts error: %v", err)
	}
	var found *Observation
	for i := range obs {
		if obs[i].LocalPort == uint32(port) {
			found = &obs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("listener on port %d not found in %d observations", port, len(obs))
	}
	if found.State != StateListening && found.State != StateActive {
		t.Fatalf("state = %s, want LISTENING/ACTIVE: %+v", found.State, *found)
	}
	if found.Protocol != "tcp" {
		t.Fatalf("protocol = %s", found.Protocol)
	}
}

func TestValidatePortClientGateListening(t *testing.T) {
	_, port := freeTCPListener(t)
	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ValidatePort(ctx, ValidateOptions{
		Protocol:     "tcp",
		Port:         uint32(port),
		LocalAddress: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("ValidatePort error: %v", err)
	}
	if obs.State != StateListening {
		t.Fatalf("state = %s, want LISTENING (obs: %+v)", obs.State, obs)
	}
	if obs.PID <= 0 {
		t.Logf("note: pid not attributed (obs: %+v)", obs)
	}
}

func TestValidatePortServerGateAvailable(t *testing.T) {
	// find a port that is definitely free: listen, note port, close
	ln, port := freeTCPListener(t)
	_ = ln.Close()
	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ValidatePort(ctx, ValidateOptions{
		Protocol:  "tcp",
		Port:      uint32(port),
		BindProbe: true,
	})
	if err != nil {
		t.Fatalf("ValidatePort error: %v", err)
	}
	if obs.State != StateAvailable {
		t.Fatalf("state = %s, want AVAILABLE (obs: %+v)", obs.State, obs)
	}
}

func TestValidatePortServerGateConflict(t *testing.T) {
	_, port := freeTCPListener(t)
	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ValidatePort(ctx, ValidateOptions{
		Protocol:  "tcp",
		Port:      uint32(port),
		BindProbe: true,
	})
	if err != nil {
		t.Fatalf("ValidatePort error: %v", err)
	}
	// occupied port must never be AVAILABLE (Windows SO_REUSEADDR may make
	// the probe succeed, so the listener-evidence rule must reject it)
	if obs.State == StateAvailable {
		t.Fatalf("occupied port %d classified AVAILABLE: %+v", port, obs)
	}
	if obs.State != StateConflict {
		t.Fatalf("state = %s, want CONFLICT (obs: %+v)", obs.State, obs)
	}
}

func TestScanPortsUDPBound(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("udp listen: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	port := pc.LocalAddr().(*net.UDPAddr).Port

	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ScanPorts(ctx, ScanOptions{
		Protocol: "udp",
		PortMin:  uint32(port),
		PortMax:  uint32(port),
	})
	if err != nil {
		t.Fatalf("ScanPorts error: %v", err)
	}
	found := false
	for _, o := range obs {
		if o.LocalPort == uint32(port) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("bound udp port %d not found in %d observations", port, len(obs))
	}
}

func TestValidatePortIPv6Listener(t *testing.T) {
	ln, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("ipv6 loopback unavailable: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	obs, err := ins.ValidatePort(ctx, ValidateOptions{
		Protocol:     "tcp",
		Port:         uint32(port),
		LocalAddress: "::1",
	})
	if err != nil {
		t.Fatalf("ValidatePort error: %v", err)
	}
	if obs.State != StateListening {
		t.Fatalf("state = %s, want LISTENING (obs: %+v)", obs.State, obs)
	}
	if obs.IPFamily != "v6" {
		t.Fatalf("family = %s, want v6", obs.IPFamily)
	}
}

func TestScanPortsPortRangeFilter(t *testing.T) {
	_, port := freeTCPListener(t)
	ins := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// range that excludes the listener
	obs, err := ins.ScanPorts(ctx, ScanOptions{
		Protocol: "tcp",
		PortMin:  uint32(port) + 1,
		PortMax:  uint32(port) + 100,
	})
	if err != nil {
		t.Fatalf("ScanPorts error: %v", err)
	}
	for _, o := range obs {
		if o.LocalPort == uint32(port) {
			t.Fatalf("port %d leaked through range filter", port)
		}
	}
}
