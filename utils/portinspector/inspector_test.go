package portinspector

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseProcNetTCP(t *testing.T) {
	line := "   0: 0100007F:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31407 1 0000000000000000 100 0 0 10 0"
	s, err := parseProcNetLine("tcp", "v4", line)
	if err != nil {
		t.Fatalf("parseProcNetLine error: %v", err)
	}
	if s.Protocol != "tcp" || s.IPFamily != "v4" {
		t.Fatalf("proto/family = %s/%s", s.Protocol, s.IPFamily)
	}
	if s.Address != "127.0.0.1" {
		t.Fatalf("address = %q, want 127.0.0.1", s.Address)
	}
	if s.Port != 22 {
		t.Fatalf("port = %d, want 22", s.Port)
	}
	if s.State != "LISTEN" {
		t.Fatalf("state = %q, want LISTEN", s.State)
	}
	if s.Inode != "31407" {
		t.Fatalf("inode = %q, want 31407", s.Inode)
	}
}

func TestParseProcNetWildcardAndEstablished(t *testing.T) {
	// 0.0.0.0:8080 LISTEN, 172.16.0.5:54321 ESTABLISHED
	lines := []string{
		"   1: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 111 1 0000000000000000 100 0 0 10 0",
		"   2: 050010AC:D431 0100007F:0016 01 00000000:00000000 00:00000000 00000000     0        0 222 1 0000000000000000 20 4 30 10 -1",
	}
	var got []procSocket
	for _, l := range lines {
		s, err := parseProcNetLine("tcp", "v4", l)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		got = append(got, s)
	}
	if got[0].Address != "0.0.0.0" || got[0].Port != 8080 || got[0].State != "LISTEN" {
		t.Fatalf("wildcard listener wrong: %+v", got[0])
	}
	if got[1].Address != "172.16.0.5" || got[1].Port != 54321 || got[1].State != "ESTABLISHED" {
		t.Fatalf("established socket wrong: %+v", got[1])
	}
}

func TestParseProcNetUDP(t *testing.T) {
	line := "   0: 00000000:1F90 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 999 2 0000000000000000 0 0 0 0 0"
	s, err := parseProcNetLine("udp", "v4", line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if s.State != "UNCONN" {
		t.Fatalf("udp state = %q, want UNCONN", s.State)
	}
	if s.Port != 8080 || s.Address != "0.0.0.0" {
		t.Fatalf("udp addr/port wrong: %s:%d", s.Address, s.Port)
	}
}

func TestParseProcNetIPv6(t *testing.T) {
	// [::1]:7000 LISTEN — v6 hex, little-endian words
	line := "   0: 00000000000000000000000001000000:1B58 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 4242 1 0000000000000000 100 0 0 10 0"
	s, err := parseProcNetLine("tcp", "v6", line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if s.Address != "::1" {
		t.Fatalf("address = %q, want ::1", s.Address)
	}
	if s.Port != 7000 || s.State != "LISTEN" {
		t.Fatalf("port/state wrong: %d/%s", s.Port, s.State)
	}
}

func TestParseProcNetIPv6Wildcard(t *testing.T) {
	// [::]:7000 LISTEN
	line := "   0: 00000000000000000000000000000000:1B58 00000000000000000000000000000000:0000 0A 00000000000000000000000000000000 00:00000000 00000000     0        0 4243 1 0000000000000000 100 0 0 10 0"
	s, err := parseProcNetLine("tcp", "v6", line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if s.Address != "::" {
		t.Fatalf("address = %q, want ::", s.Address)
	}
}

func TestParseProcNetV4Mapped(t *testing.T) {
	// ::ffff:127.0.0.1:8080 — appears in tcp6 for dual-stack sockets.
	// The kernel prints each 32-bit word as a little-endian u32 of the
	// in-memory bytes: word2 = 00 00 FF FF -> "FFFF0000",
	// word3 = 7F 00 00 01 -> "0100007F".
	line := "   0: 0000000000000000FFFF00000100007F:1F90 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 5555 1 0000000000000000 100 0 0 10 0"
	s, err := parseProcNetLine("tcp", "v6", line)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if s.Address != "127.0.0.1" {
		t.Fatalf("address = %q, want unmapped 127.0.0.1", s.Address)
	}
}

func TestScanProcNetLinesSkipsHeaderAndMalformed(t *testing.T) {
	body := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n" +
		"garbage line without enough fields\n" +
		"   0: 0100007F:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31407 1 0 100 0 0 10 0\n"
	socks, err := scanProcNetLines("tcp", "v4", bufio.NewScanner(strings.NewReader(body)))
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(socks) != 1 {
		t.Fatalf("got %d sockets, want 1", len(socks))
	}
	if socks[0].Port != 22 {
		t.Fatalf("port = %d, want 22", socks[0].Port)
	}
}

func TestParseProcHexIPErrors(t *testing.T) {
	if _, err := parseProcHexIP("123", "v4"); err == nil {
		t.Fatal("expected error for short v4 hex")
	}
	if _, err := parseProcHexIP("zzzzzzzz", "v4"); err == nil {
		t.Fatal("expected error for non-hex v4")
	}
	if _, err := parseProcHexIP("1234", "v6"); err == nil {
		t.Fatal("expected error for short v6 hex")
	}
}

func TestProbeBind(t *testing.T) {
	// a fresh port must be bindable
	ok, err := ProbeBind("tcp4", "127.0.0.1:0")
	if err != nil || !ok {
		t.Fatalf("ProbeBind(free tcp) = %v/%v, want true/nil", ok, err)
	}
	ok, err = ProbeBind("udp4", "127.0.0.1:0")
	if err != nil || !ok {
		t.Fatalf("ProbeBind(free udp) = %v/%v, want true/nil", ok, err)
	}
	if _, err := ProbeBind("bogus", "127.0.0.1:0"); err == nil {
		t.Fatal("expected error for unsupported network")
	}
}

func TestListenerMatches(t *testing.T) {
	cases := []struct {
		name       string
		obs        Observation
		wantIP     string
		wantFamily string
		want       bool
	}{
		{"exact v4", Observation{IPFamily: "v4", LocalAddress: "127.0.0.1"}, "127.0.0.1", "v4", true},
		{"v4 wildcard covers specific", Observation{IPFamily: "v4", LocalAddress: "0.0.0.0"}, "127.0.0.1", "v4", true},
		{"specific does not cover other", Observation{IPFamily: "v4", LocalAddress: "127.0.0.2"}, "127.0.0.1", "v4", false},
		{"v4 wildcard does not cover v6 want", Observation{IPFamily: "v4", LocalAddress: "0.0.0.0"}, "::1", "v6", false},
		{"v6 wildcard covers v6", Observation{IPFamily: "v6", LocalAddress: "::"}, "::1", "v6", true},
		{"v6 wildcard covers v4 (dual stack)", Observation{IPFamily: "v6", LocalAddress: "::"}, "127.0.0.1", "v4", true},
		{"v6 specific does not cover v4", Observation{IPFamily: "v6", LocalAddress: "::1"}, "127.0.0.1", "v4", false},
		{"empty want matches any", Observation{IPFamily: "v4", LocalAddress: "10.0.0.1"}, "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := listenerMatches(tc.obs, tc.wantIP, tc.wantFamily); got != tc.want {
				t.Fatalf("listenerMatches(%+v, %q, %q) = %v, want %v", tc.obs, tc.wantIP, tc.wantFamily, got, tc.want)
			}
		})
	}
}

func TestValidateFromListenersClientGate(t *testing.T) {
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "127.0.0.1", LocalPort: 8000, PID: 42, ProcessName: "mysvc", Source: "gopsutil"},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "127.0.0.1"})
	if obs.State != StateListening {
		t.Fatalf("state = %s, want LISTENING", obs.State)
	}
	if obs.PID != 42 || obs.ProcessName != "mysvc" {
		t.Fatalf("evidence lost: %+v", obs)
	}
}

func TestValidateFromListenersClientGateWildcardCoverage(t *testing.T) {
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 7},
	}
	// querying a specific address still matches the wildcard listener
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "192.168.1.10"})
	if obs.State != StateListening {
		t.Fatalf("state = %s, want LISTENING", obs.State)
	}
	if obs.LocalAddress != "0.0.0.0" {
		t.Fatalf("evidence address = %q, want 0.0.0.0", obs.LocalAddress)
	}
}

func TestValidateFromListenersNoMatchIsAvailable(t *testing.T) {
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "127.0.0.1", LocalPort: 8000},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 9000, LocalAddress: "0.0.0.0"})
	if obs.State != StateAvailable {
		t.Fatalf("state = %s, want AVAILABLE", obs.State)
	}
	obs = validateFromListeners(listeners, ValidateOptions{Protocol: "udp", Port: 8000, LocalAddress: "0.0.0.0"})
	if obs.State != StateAvailable {
		t.Fatalf("udp on tcp-listened port: state = %s, want AVAILABLE", obs.State)
	}
}

func TestValidateFromListenersMultiPIDConflict(t *testing.T) {
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 1},
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 2},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "0.0.0.0"})
	if obs.State != StateConflict {
		t.Fatalf("state = %s, want CONFLICT", obs.State)
	}
}

func TestValidateFromListenersUnhealthyOwnerClientGate(t *testing.T) {
	// spec §5.4: a listener owned by a zombie/exiting process must NOT
	// satisfy the local gate. UNKNOWN rejects the save; LISTENING/ACTIVE
	// would pass it.
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "127.0.0.1", LocalPort: 8000, PID: 42, ProcessName: "mysvc (zombie)", OwnerUnhealthy: true, Source: "gopsutil"},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "127.0.0.1"})
	if obs.State != StateUnknown {
		t.Fatalf("state = %s, want UNKNOWN for unhealthy owner", obs.State)
	}
	if obs.Source != "scan-unhealthy-owner" {
		t.Fatalf("source = %q, want scan-unhealthy-owner", obs.Source)
	}
	if obs.PID != 42 || obs.ProcessName != "mysvc (zombie)" {
		t.Fatalf("unhealthy-owner evidence lost: %+v", obs)
	}
}

func TestValidateFromListenersUnhealthyOwnerServerGate(t *testing.T) {
	// spec §5.4 also binds the server side: a remote port owned by a
	// zombie must be rejected as UNKNOWN, never reported as a plain
	// CONFLICT (which would also reject, but hides the real cause).
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 7000, PID: 99, ProcessName: "frps (zombie)", OwnerUnhealthy: true},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 7000, LocalAddress: "0.0.0.0", BindProbe: true})
	if obs.State != StateUnknown {
		t.Fatalf("state = %s, want UNKNOWN for unhealthy owner", obs.State)
	}
	if obs.Source != "scan-unhealthy-owner" {
		t.Fatalf("source = %q, want scan-unhealthy-owner", obs.Source)
	}
}

func TestValidateFromListenersServerGateConflict(t *testing.T) {
	// server side: any existing listener on the remote port is a conflict
	listeners := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 7000, PID: 99, ProcessName: "frps"},
	}
	obs := validateFromListeners(listeners, ValidateOptions{Protocol: "tcp", Port: 7000, LocalAddress: "0.0.0.0", BindProbe: true})
	if obs.State != StateConflict {
		t.Fatalf("state = %s, want CONFLICT", obs.State)
	}
	if obs.ProcessName != "frps" {
		t.Fatalf("conflict evidence missing: %+v", obs)
	}
}

func TestProbeAddress(t *testing.T) {
	if got := probeAddress("tcp", "v4", "", 7000); got != "0.0.0.0:7000" {
		t.Fatalf("got %q", got)
	}
	if got := probeAddress("tcp", "v4", "127.0.0.1", 7000); got != "127.0.0.1:7000" {
		t.Fatalf("got %q", got)
	}
	if got := probeAddress("udp", "v6", "", 7000); got != "[::]:7000" {
		t.Fatalf("got %q", got)
	}
	if got := probeAddress("tcp", "v6", "::1", 7000); got != "[::1]:7000" {
		t.Fatalf("got %q", got)
	}
}
