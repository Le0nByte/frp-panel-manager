package portinspector

import (
	"context"
	"fmt"
	"strings"
	"time"

	psnet "github.com/shirou/gopsutil/v4/net"
	psprocess "github.com/shirou/gopsutil/v4/process"
)

// collectListenersGopsutil scans local sockets through gopsutil v4. It
// returns one observation per listening/bound socket plus the source tag.
// The error is returned to the caller which may fall back to /proc.
func collectListenersGopsutil(ctx context.Context, opts ScanOptions) ([]Observation, string, error) {
	conns, err := psnet.Connections("all")
	if err != nil {
		return nil, "", err
	}
	now := time.Now().Unix()
	var raw []Observation
	estByKey := map[string]int32{}
	for _, c := range conns {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if c.Laddr.Port == 0 {
			continue
		}
		proto := socketProto(c)
		if proto == "" {
			continue
		}
		if opts.Protocol != "" && normalizeProtocol(opts.Protocol) != proto {
			continue
		}
		port := uint32(c.Laddr.Port)
		if opts.PortMin > 0 && port < opts.PortMin {
			continue
		}
		if opts.PortMax > 0 && port > opts.PortMax {
			continue
		}
		ip := c.Laddr.IP
		family := addressFamily(ip)
		key := fmt.Sprintf("%s|%s|%d", proto, ip, port)
		if isListeningConn(c, proto) {
			raw = append(raw, Observation{
				Protocol:     proto,
				IPFamily:     family,
				LocalAddress: ip,
				LocalPort:    port,
				PID:          c.Pid,
				ObservedAt:   now,
				Source:       "gopsutil",
			})
		} else if proto == "tcp" && c.Status == "ESTABLISHED" {
			estByKey[key]++
		}
	}

	// fold established connections into listener observations
	for i := range raw {
		key := fmt.Sprintf("%s|%s|%d", raw[i].Protocol, raw[i].LocalAddress, raw[i].LocalPort)
		if n, ok := estByKey[key]; ok {
			raw[i].ActiveConnections = n
			if n > 0 {
				raw[i].State = StateActive
			}
		}
	}

	out := groupAndEnrich(ctx, raw, opts)
	return out, "gopsutil", nil
}

// socketProto maps a gopsutil connection type to tcp/udp or "".
func socketProto(c psnet.ConnectionStat) string {
	switch c.Type {
	case 1: // SOCK_STREAM
		return "tcp"
	case 2: // SOCK_DGRAM
		return "udp"
	default:
		return ""
	}
}

// isListeningConn reports whether the socket counts as a listener:
// TCP only in LISTEN state, UDP whenever a local port is bound.
func isListeningConn(c psnet.ConnectionStat, proto string) bool {
	if proto == "udp" {
		return true
	}
	return c.Status == "LISTEN"
}

// groupAndEnrich groups raw observations by (protocol, family, address,
// port), marks multi-pid ports as CONFLICT, and attaches process (and
// optionally docker) evidence. Enrichment is best-effort: failures leave
// the field empty rather than failing the scan.
func groupAndEnrich(ctx context.Context, raw []Observation, opts ScanOptions) []Observation {
	type key struct {
		proto, family, addr string
		port                uint32
	}
	groups := map[key][]Observation{}
	var order []key
	for _, o := range raw {
		k := key{o.Protocol, o.IPFamily, o.LocalAddress, o.LocalPort}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], o)
	}

	docker := (*dockerClient)(nil)
	if opts.IncludeDocker {
		docker = newDockerClient()
	}
	var pidToContainer map[int32]dockerContainerInfo
	if docker != nil {
		pidToContainer, _ = docker.containerByPID(ctx)
	}

	out := make([]Observation, 0, len(order))
	for _, k := range order {
		group := groups[k]
		merged := mergeEvidence(group, opts.NodeID, k.proto, k.port)
		merged.IPFamily = k.family
		merged.LocalAddress = k.addr
		if len(distinctPIDs(group)) > 1 {
			merged.State = StateConflict
		} else if merged.State == "" {
			merged.State = StateListening
		}
		merged = enrichProcess(ctx, merged)
		if docker != nil && pidToContainer != nil {
			if info, ok := pidToContainer[merged.PID]; ok {
				merged.DockerContainerID = info.ID
				merged.DockerContainerName = info.Name
			}
		}
		out = append(out, merged)
	}
	return out
}

// enrichProcess attaches process name/executable via gopsutil. Best-effort.
func enrichProcess(ctx context.Context, o Observation) Observation {
	if o.PID <= 0 {
		return o
	}
	p, err := psprocess.NewProcessWithContext(ctx, int32(o.PID))
	if err != nil {
		return o
	}
	if name, err := p.NameWithContext(ctx); err == nil {
		o.ProcessName = name
	}
	if exe, err := p.ExeWithContext(ctx); err == nil {
		o.Executable = exe
	}
	if status, err := p.StatusWithContext(ctx); err == nil && isUnhealthyProcessStatus(status) {
		// zombie/dead/stopped/terminated/exited owners must not pass the
		// gate (spec §5.4).
		o.OwnerUnhealthy = true
	}
	return o
}

// normalizeDisplayIP strips brackets and v4-mapped prefixes for display.
func normalizeDisplayIP(ip string) string {
	ip = strings.TrimPrefix(ip, "[")
	ip = strings.TrimSuffix(ip, "]")
	return ip
}

// filterRawByOpts applies ScanOptions filters to raw observations.
func filterRawByOpts(raw []Observation, opts ScanOptions) []Observation {
	var out []Observation
	for _, o := range raw {
		if opts.Protocol != "" && normalizeProtocol(o.Protocol) != normalizeProtocol(opts.Protocol) {
			continue
		}
		if opts.PortMin > 0 && o.LocalPort < opts.PortMin {
			continue
		}
		if opts.PortMax > 0 && o.LocalPort > opts.PortMax {
			continue
		}
		out = append(out, o)
	}
	return out
}

// validatePortWith implements ValidatePort on top of any scanner: a scan
// failure is reported as a StateUnknown observation (never a hard error),
// because UNKNOWN must reject a save but must not crash the RPC.
func validatePortWith(scanner Inspector, ctx context.Context, opts ValidateOptions) (Observation, error) {
	listeners, err := scanner.ScanPorts(ctx, ScanOptions{
		NodeID:        opts.NodeID,
		Protocol:      opts.Protocol,
		PortMin:       opts.Port,
		PortMax:       opts.Port,
		IncludeDocker: opts.IncludeDocker,
	})
	if err != nil {
		return Observation{
			NodeID:     opts.NodeID,
			Protocol:   normalizeProtocol(opts.Protocol),
			LocalPort:  opts.Port,
			State:      StateUnknown,
			Source:     "scan-error",
			ObservedAt: time.Now().Unix(),
		}, nil
	}
	return validateFromListeners(listeners, opts), nil
}
