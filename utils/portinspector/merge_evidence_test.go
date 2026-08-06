package portinspector

import (
	"context"
	"testing"
)

// TestMergeEvidencePropagatesOwnerUnhealthy pins the OR semantics of the
// OwnerUnhealthy fold (spec §5.4): any unhealthy observation in the group
// must poison the merged evidence, while healthy groups stay healthy and
// the existing pid/process evidence is preserved.
func TestMergeEvidencePropagatesOwnerUnhealthy(t *testing.T) {
	healthy := []Observation{
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 11, ProcessName: "svc-a", Source: "gopsutil"},
		{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 22, ProcessName: "svc-b", Source: "/proc"},
	}
	if merged := mergeEvidence(healthy, "node-1", "tcp", 8000); merged.OwnerUnhealthy {
		t.Fatalf("healthy group flagged unhealthy: %+v", merged)
	}

	// a zombie observation anywhere in the group (first/middle/last)
	// poisons the merge; pid/process evidence must not be lost.
	zombie := Observation{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 33, ProcessName: "svc-c (zombie)", OwnerUnhealthy: true, Source: "/proc"}
	for _, i := range []int{0, 1, 2} {
		group := append([]Observation{}, healthy[:i]...)
		group = append(group, zombie)
		group = append(group, healthy[i:]...)
		merged := mergeEvidence(group, "node-1", "tcp", 8000)
		if !merged.OwnerUnhealthy {
			t.Fatalf("zombie at index %d dropped: OwnerUnhealthy=false", i)
		}
		if merged.PID == 0 || merged.ProcessName == "" {
			t.Fatalf("pid/process evidence lost: %+v", merged)
		}
	}
}

// TestProcFallbackZombieReachesClientGate covers the M1b wiring chain
// /proc raw -> groupAndEnrich -> validateFromListeners: the /proc fallback
// flags the zombie owner via markZombieOwner, but the flag used to be
// structurally dropped in mergeEvidence before the gate could reject it.
// Raw observations are shaped exactly like readProcNetSockets output.
func TestProcFallbackZombieReachesClientGate(t *testing.T) {
	t.Run("single zombie owner", func(t *testing.T) {
		raw := []Observation{
			{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 424201, ProcessName: "frpc (zombie)", OwnerUnhealthy: true, Source: "/proc"},
		}
		grouped := groupAndEnrich(context.Background(), raw, ScanOptions{})
		if len(grouped) != 1 {
			t.Fatalf("groupAndEnrich returned %d groups, want 1", len(grouped))
		}
		if !grouped[0].OwnerUnhealthy {
			t.Fatalf("merge dropped OwnerUnhealthy: %+v", grouped[0])
		}
		obs := validateFromListeners(grouped, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "0.0.0.0"})
		if obs.State != StateUnknown {
			t.Fatalf("client gate state = %s, want UNKNOWN for zombie owner", obs.State)
		}
		if obs.Source != "scan-unhealthy-owner" {
			t.Fatalf("client gate source = %q, want scan-unhealthy-owner", obs.Source)
		}
	})

	t.Run("any unhealthy observation in the group", func(t *testing.T) {
		raw := []Observation{
			{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 424202, ProcessName: "frpc", Source: "/proc"},
			{Protocol: "tcp", IPFamily: "v4", LocalAddress: "0.0.0.0", LocalPort: 8000, PID: 424203, ProcessName: "frpc (zombie)", OwnerUnhealthy: true, Source: "/proc"},
		}
		grouped := groupAndEnrich(context.Background(), raw, ScanOptions{})
		if len(grouped) != 1 {
			t.Fatalf("groupAndEnrich returned %d groups, want 1", len(grouped))
		}
		if !grouped[0].OwnerUnhealthy {
			t.Fatalf("group with an unhealthy member dropped the flag: %+v", grouped[0])
		}
		obs := validateFromListeners(grouped, ValidateOptions{Protocol: "tcp", Port: 8000, LocalAddress: "0.0.0.0"})
		if obs.State != StateUnknown {
			t.Fatalf("client gate state = %s, want UNKNOWN for zombie owner", obs.State)
		}
		if obs.Source != "scan-unhealthy-owner" {
			t.Fatalf("client gate source = %q, want scan-unhealthy-owner", obs.Source)
		}
	})
}
