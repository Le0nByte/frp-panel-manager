//go:build linux || android

package portinspector

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// readProcNetSockets is the /proc fallback scanner (spec §5.3-2): it parses
// /proc/net/{tcp,tcp6,udp,udp6} and resolves socket inodes to pids via
// /proc/<pid>/fd. It never executes shell commands.
func readProcNetSockets(ctx context.Context) ([]Observation, error) {
	var raw []procSocket
	for _, f := range []struct {
		path, protocol, family string
	}{
		{"/proc/net/tcp", "tcp", "v4"},
		{"/proc/net/tcp6", "tcp", "v6"},
		{"/proc/net/udp", "udp", "v4"},
		{"/proc/net/udp6", "udp", "v6"},
	} {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fh, err := os.Open(f.path)
		if err != nil {
			continue // one unreadable file must not fail the scan
		}
		socks, err := scanProcNetLines(f.protocol, f.family, bufio.NewScanner(fh))
		_ = fh.Close()
		if err != nil {
			continue
		}
		raw = append(raw, socks...)
	}

	now := time.Now().Unix()
	var out []Observation
	for _, s := range raw {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.Port == 0 {
			continue
		}
		if !(s.Protocol == "tcp" && s.State == "LISTEN") && !(s.Protocol == "udp") {
			continue // only listeners/bound sockets
		}
		pid, zombie := procPIDForInode(ctx, s.Inode)
		o := Observation{
			Protocol:     s.Protocol,
			IPFamily:     s.IPFamily,
			LocalAddress: s.Address,
			LocalPort:    s.Port,
			PID:          pid,
			ObservedAt:   now,
			Source:       "/proc",
		}
		if pid > 0 {
			if comm, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(int(pid)), "comm")); err == nil {
				o.ProcessName = strings.TrimSpace(string(comm))
			}
			if exe, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(int(pid)), "exe")); err == nil {
				o.Executable = exe
			}
		}
		o = markZombieOwner(o, zombie)
		out = append(out, o)
	}
	return out, nil
}

// procPIDForInode finds the pid owning a socket inode by walking
// /proc/<pid>/fd symlinks. Returns pid=0 when not found and a zombie flag
// when the owning process is a zombie (spec §5.4: zombies must not satisfy
// the local listener gate).
func procPIDForInode(ctx context.Context, inode string) (int32, bool) {
	if inode == "" {
		return 0, false
	}
	want := "socket:[" + inode + "]"
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return 0, false
		}
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.ParseInt(e.Name(), 10, 32)
		if err != nil || pid <= 0 {
			continue
		}
		fdDir := filepath.Join("/proc", e.Name(), "fd")
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err != nil {
				continue
			}
			if target == want {
				return int32(pid), isZombie(pid)
			}
		}
	}
	return 0, false
}

// isZombie reads /proc/<pid>/stat state field (index 2 after the comm
// field, which itself may contain spaces/parens: parse from the last ')').
func isZombie(pid int64) bool {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.FormatInt(pid, 10), "stat"))
	if err != nil {
		return false
	}
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return false
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 1 {
		return false
	}
	return fields[0] == "Z"
}
