// Package portinspector implements the Port Inspector (spec §5): it scans
// TCP/UDP sockets on the local node, classifies a single port as
// AVAILABLE/LISTENING/CONFLICT/UNKNOWN and returns verifiable evidence
// (pid, process, docker container, source). It never executes shell
// commands and never modifies iptables/firewall rules.
package portinspector

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// State classifies a single port observation (spec §5.1).
type State string

const (
	// StateAvailable: no OS conflict, no FRP reservation, bind probe ok.
	StateAvailable State = "AVAILABLE"
	// StateListening: TCP is LISTEN; UDP is bound.
	StateListening State = "LISTENING"
	// StateActive: established connections > 0 on the port.
	StateActive State = "ACTIVE"
	// StateConflict: occupied by another process, container or FRP rule.
	StateConflict State = "CONFLICT"
	// StateUnknown: agent offline, permission denied, scan timeout, parse failure.
	StateUnknown State = "UNKNOWN"
)

// Observation is one verifiable piece of evidence for a port, bound to a
// node, protocol and address (spec §5.2). It never carries shell text.
type Observation struct {
	NodeID              string
	Protocol            string // tcp / udp
	IPFamily            string // v4 / v6
	LocalAddress        string
	LocalPort           uint32
	State               State
	PID                 int32
	ProcessName         string
	Executable          string
	ServiceName         string
	OwnerUnhealthy      bool // owner is a zombie/exited/stopped process (spec §5.4)
	DockerContainerID   string
	DockerContainerName string
	FRPProxyName        string
	ActiveConnections   int32
	ObservedAt          int64 // unix seconds
	Source              string
}

// ScanOptions filters the socket scan. Empty protocol/range means
// "everything visible".
type ScanOptions struct {
	NodeID        string
	Protocol      string // tcp / udp / ""
	PortMin       uint32
	PortMax       uint32
	IncludeDocker bool
}

// ValidateOptions describes a single-port validation (spec §5.4/§5.5).
type ValidateOptions struct {
	NodeID        string
	Protocol      string // tcp / udp
	Port          uint32
	LocalAddress  string // "" means any (probe binds 0.0.0.0)
	BindProbe     bool   // server-side gate: transient net.Listen probe
	IncludeDocker bool
}

// Inspector scans local sockets and validates single ports.
type Inspector interface {
	// ScanPorts returns one observation per listening/bound socket in the
	// requested range, enriched with process and (optionally) docker evidence.
	ScanPorts(ctx context.Context, opts ScanOptions) ([]Observation, error)
	// ValidatePort returns a single observation for one port. With
	// BindProbe=true the state is decided by a transient bind probe
	// (server-side gate); with BindProbe=false by listener evidence
	// (client-side gate). Scan failures yield StateUnknown, never a hard
	// error: UNKNOWN must reject, not crash.
	ValidatePort(ctx context.Context, opts ValidateOptions) (Observation, error)
}

// New returns the platform-appropriate Inspector.
func New() Inspector {
	return newScanner()
}

// ProbeBind attempts a transient bind and immediately closes it. A bind
// failure is reported as (false, nil): the port is not bindable. Only an
// unsupported network name is an error.
func ProbeBind(network, address string) (bool, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
		ln, err := net.Listen(network, address)
		if err != nil {
			return false, nil
		}
		_ = ln.Close()
		return true, nil
	case "udp", "udp4", "udp6":
		pc, err := net.ListenPacket(network, address)
		if err != nil {
			return false, nil
		}
		_ = pc.Close()
		return true, nil
	default:
		return false, fmt.Errorf("unsupported network %q", network)
	}
}

// normalizeProtocol maps "tcp4"/"udp6"/"" to tcp/udp/"".
func normalizeProtocol(p string) string {
	switch {
	case strings.HasPrefix(p, "tcp"):
		return "tcp"
	case strings.HasPrefix(p, "udp"):
		return "udp"
	default:
		return ""
	}
}

// networkFor returns the net.Listen network for a protocol/family pair.
func networkFor(protocol, family string) string {
	suffix := "4"
	if family == "v6" {
		suffix = "6"
	}
	if protocol == "udp" {
		return "udp" + suffix
	}
	return "tcp" + suffix
}

// isWildcard reports whether an address binds every address of its family
// (including the empty string, which means "unspecified").
func isWildcard(addr string) bool {
	switch addr {
	case "", "0.0.0.0", "::":
		return true
	}
	return false
}

// addressFamily returns v4/v6 for a textual IP.
func addressFamily(ip string) string {
	if strings.Contains(ip, ":") {
		return "v6"
	}
	return "v4"
}

// probeAddress builds the address string passed to ProbeBind.
func probeAddress(protocol, family, localAddress string, port uint32) string {
	ip := localAddress
	if ip == "" {
		if family == "v6" {
			ip = "::"
		} else {
			ip = "0.0.0.0"
		}
	}
	if strings.Contains(ip, ":") && !strings.HasPrefix(ip, "[") {
		ip = "[" + ip + "]"
	}
	return fmt.Sprintf("%s:%d", ip, port)
}

// listenerMatches reports whether a listener observation covers the wanted
// address/family. A v6 wildcard ("::") is treated as dual-stack: with the
// common Linux default (net.ipv6.bindv6only=0) it also serves v4 traffic,
// so it matches v4 wants as well. A v4 wildcard only matches v4.
func listenerMatches(o Observation, wantIP, wantFamily string) bool {
	if wantFamily != "" && o.IPFamily != wantFamily {
		if !(o.IPFamily == "v6" && isWildcard(o.LocalAddress)) {
			return false
		}
	}
	if wantIP == "" || isWildcard(wantIP) {
		return true
	}
	if o.LocalAddress == wantIP {
		return true
	}
	return isWildcard(o.LocalAddress)
}

// distinctPIDs returns the set of pids in observations (0 excluded).
func distinctPIDs(obs []Observation) map[int32]bool {
	out := map[int32]bool{}
	for _, o := range obs {
		if o.PID > 0 {
			out[o.PID] = true
		}
	}
	return out
}

// mergeEvidence folds a list of matching listener observations into one
// evidence record: prefers the most "wildcard" address, keeps the first
// non-empty process/docker fields, sums active connections and OR-folds
// OwnerUnhealthy (spec §5.4: any zombie owner poisons the merge).
func mergeEvidence(matches []Observation, nodeID, protocol string, port uint32) Observation {
	ev := Observation{
		NodeID:     nodeID,
		Protocol:   protocol,
		LocalPort:  port,
		ObservedAt: time.Now().Unix(),
	}
	// wildcard beats specific; v6 wildcard beats v4 wildcard (covers more)
	best := -1
	for i, m := range matches {
		if best == -1 {
			best = i
			continue
		}
		if wildcardRank(m) > wildcardRank(matches[best]) {
			best = i
		}
	}
	if best >= 0 {
		b := matches[best]
		ev.IPFamily = b.IPFamily
		ev.LocalAddress = b.LocalAddress
		ev.State = b.State
		ev.Source = b.Source
	}
	for _, m := range matches {
		if ev.PID == 0 && m.PID != 0 {
			ev.PID = m.PID
		}
		if ev.ProcessName == "" {
			ev.ProcessName = m.ProcessName
		}
		if ev.Executable == "" {
			ev.Executable = m.Executable
		}
		if ev.ServiceName == "" {
			ev.ServiceName = m.ServiceName
		}
		if ev.DockerContainerID == "" {
			ev.DockerContainerID = m.DockerContainerID
		}
		if ev.DockerContainerName == "" {
			ev.DockerContainerName = m.DockerContainerName
		}
		if ev.FRPProxyName == "" {
			ev.FRPProxyName = m.FRPProxyName
		}
		if m.OwnerUnhealthy {
			ev.OwnerUnhealthy = true
		}
		ev.ActiveConnections += m.ActiveConnections
	}
	return ev
}

func wildcardRank(o Observation) int {
	switch {
	case o.IPFamily == "v6" && isWildcard(o.LocalAddress):
		return 3
	case o.IPFamily == "v4" && isWildcard(o.LocalAddress):
		return 2
	case isWildcard(o.LocalAddress):
		return 2
	default:
		return 1
	}
}

// validateFromListeners decides the final state for ValidatePort.
// With BindProbe=true (server-side gate) any existing listener is a
// conflict regardless of probe outcome (the port is owned by another
// process); with no listener the probe is the ground truth.
// With BindProbe=false (client-side gate) listener evidence decides.
func validateFromListeners(listeners []Observation, opts ValidateOptions) Observation {
	obs := Observation{
		NodeID:     opts.NodeID,
		Protocol:   normalizeProtocol(opts.Protocol),
		LocalPort:  opts.Port,
		ObservedAt: time.Now().Unix(),
	}
	proto := normalizeProtocol(opts.Protocol)
	family := addressFamily(opts.LocalAddress)
	if family == "v6" {
		obs.IPFamily = "v6"
		obs.LocalAddress = opts.LocalAddress
	} else {
		obs.IPFamily = "v4"
		obs.LocalAddress = opts.LocalAddress
		if obs.LocalAddress == "" {
			obs.LocalAddress = "0.0.0.0"
		}
	}

	var matches []Observation
	for _, l := range listeners {
		if l.LocalPort != opts.Port {
			continue
		}
		if proto != "" && normalizeProtocol(l.Protocol) != proto {
			continue
		}
		if !listenerMatches(l, opts.LocalAddress, family) {
			continue
		}
		matches = append(matches, l)
	}

	// spec §5.4: a matched listener owned by a zombie/exited process must
	// reject the gate as UNKNOWN — never LISTENING/ACTIVE (which would
	// pass the client gate) and never CONFLICT (which hides the cause).
	// The merged pid/process evidence is kept for the UI.
	for _, m := range matches {
		if m.OwnerUnhealthy {
			obs = mergeEvidence(matches, opts.NodeID, proto, opts.Port)
			obs.NodeID = opts.NodeID
			obs.Protocol = proto
			obs.LocalPort = opts.Port
			obs.State = StateUnknown
			obs.Source = "scan-unhealthy-owner"
			return obs
		}
	}

	if len(matches) == 0 {
		if opts.BindProbe {
			ok, err := ProbeBind(networkFor(proto, family), probeAddress(proto, family, opts.LocalAddress, opts.Port))
			if err != nil {
				obs.State = StateUnknown
				obs.Source = "bind-probe-error"
				return obs
			}
			if ok {
				obs.State = StateAvailable
				obs.Source = "bind-probe"
				return obs
			}
			obs.State = StateConflict
			obs.Source = "bind-probe"
			return obs
		}
		obs.State = StateAvailable
		obs.Source = "scan"
		return obs
	}

	ev := mergeEvidence(matches, opts.NodeID, proto, opts.Port)
	obs = ev
	obs.NodeID = opts.NodeID
	obs.Protocol = proto
	obs.LocalPort = opts.Port
	if opts.BindProbe {
		obs.State = StateConflict
		obs.Source = "scan+bind-probe"
		return obs
	}
	if len(distinctPIDs(matches)) > 1 {
		obs.State = StateConflict
		obs.Source = "scan"
		return obs
	}
	obs.State = StateListening
	obs.Source = ev.Source
	if ev.Source == "" {
		obs.Source = "scan"
	}
	if obs.ActiveConnections > 0 {
		obs.State = StateActive
	}
	return obs
}
