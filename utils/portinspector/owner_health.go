package portinspector

import "strings"

// markZombieOwner flags an observation whose owning process is a zombie
// (spec §5.4: zombies must not satisfy the local/remote gate). The
// process name keeps a readable "(zombie)" suffix as UI evidence. Pure
// helper, unit-testable on any platform.
func markZombieOwner(o Observation, zombie bool) Observation {
	if !zombie {
		return o
	}
	o.OwnerUnhealthy = true
	if !strings.HasSuffix(o.ProcessName, " (zombie)") {
		o.ProcessName = o.ProcessName + " (zombie)"
	}
	return o
}

// isUnhealthyProcessStatus reports whether any gopsutil process status
// is a terminal/exit state that must not own a listening port: zombie,
// dead, stopped, terminated or exited (spec §5.4). "unknown"/empty
// statuses carry no evidence and stay healthy: absence of proof is not
// proof of absence.
func isUnhealthyProcessStatus(statuses []string) bool {
	for _, s := range statuses {
		switch strings.ToLower(s) {
		case "zombie", "dead", "stopped", "terminated", "exited":
			return true
		}
	}
	return false
}
