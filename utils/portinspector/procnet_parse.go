package portinspector

import (
	"bufio"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// procSocket is one parsed line of /proc/net/{tcp,tcp6,udp,udp6}.
type procSocket struct {
	Protocol string // tcp / udp
	IPFamily string // v4 / v6
	Address  string // textual IP
	Port     uint32
	State    string // LISTEN / ESTABLISHED / UNCONN / ...
	Inode    string
}

// parseProcNetLine parses one /proc/net line (header skipped by caller).
// Layout (after whitespace split):
//
//	0: 0100007F:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 31407 ...
//
//	sl(0) local(1) remote(2) st(3) tx:rx(4) tr:tm(5) retrnsmt(6) uid(7) timeout(8) inode(9)
func parseProcNetLine(protocol, family, line string) (procSocket, error) {
	fields := strings.Fields(line)
	if len(fields) < 10 {
		return procSocket{}, fmt.Errorf("short /proc/net line: %q", line)
	}
	local := strings.SplitN(fields[1], ":", 2)
	if len(local) != 2 {
		return procSocket{}, fmt.Errorf("bad local address: %q", fields[1])
	}
	port, err := strconv.ParseUint(local[1], 16, 32)
	if err != nil {
		return procSocket{}, fmt.Errorf("bad port %q: %w", local[1], err)
	}
	ip, err := parseProcHexIP(local[0], family)
	if err != nil {
		return procSocket{}, err
	}
	stateHex := fields[3]
	state := "UNKNOWN"
	switch stateHex {
	case "01":
		state = "ESTABLISHED"
	case "02":
		state = "SYN_SENT"
	case "03":
		state = "SYN_RECV"
	case "04":
		state = "FIN_WAIT1"
	case "05":
		state = "FIN_WAIT2"
	case "06":
		state = "TIME_WAIT"
	case "07":
		state = "UNCONN" // udp bound / tcp close-wait-ish
	case "08":
		state = "CLOSE_WAIT"
	case "09":
		state = "LAST_ACK"
	case "0A":
		state = "LISTEN"
	case "0B":
		state = "CLOSING"
	}
	return procSocket{
		Protocol: protocol,
		IPFamily: family,
		Address:  ip,
		Port:     uint32(port),
		State:    state,
		Inode:    fields[9],
	}, nil
}

// parseProcHexIP converts the hex address of /proc/net to a textual IP.
// v4: 8 hex chars, little-endian bytes (0100007F -> 127.0.0.1).
// v6: 32 hex chars, four little-endian 32-bit words.
func parseProcHexIP(hex string, family string) (string, error) {
	if family == "v6" {
		if len(hex) != 32 {
			return "", fmt.Errorf("bad v6 hex address %q", hex)
		}
		var b [16]byte
		for i := 0; i < 4; i++ {
			v, err := strconv.ParseUint(hex[i*8:i*8+8], 16, 32)
			if err != nil {
				return "", fmt.Errorf("bad v6 hex address %q: %w", hex, err)
			}
			// the file stores each 32-bit word little-endian
			b[i*4+0] = byte(v)
			b[i*4+1] = byte(v >> 8)
			b[i*4+2] = byte(v >> 16)
			b[i*4+3] = byte(v >> 24)
		}
		addr := netip.AddrFrom16(b)
		if addr.Is4In6() {
			addr = addr.Unmap()
		}
		return addr.String(), nil
	}
	if len(hex) != 8 {
		return "", fmt.Errorf("bad v4 hex address %q", hex)
	}
	v, err := strconv.ParseUint(hex, 16, 32)
	if err != nil {
		return "", fmt.Errorf("bad v4 hex address %q: %w", hex, err)
	}
	return fmt.Sprintf("%d.%d.%d.%d", byte(v), byte(v>>8), byte(v>>16), byte(v>>24)), nil
}

// scanProcNetLines parses a reader containing a /proc/net file body.
// Used by the Linux/Android fallback path and by fixture tests.
func scanProcNetLines(protocol, family string, scanner *bufio.Scanner) ([]procSocket, error) {
	var out []procSocket
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || strings.HasPrefix(line, "  sl") || strings.HasPrefix(line, "sl ") {
			continue
		}
		s, err := parseProcNetLine(protocol, family, line)
		if err != nil {
			// a single malformed line must not fail the whole scan
			continue
		}
		out = append(out, s)
	}
	return out, scanner.Err()
}
