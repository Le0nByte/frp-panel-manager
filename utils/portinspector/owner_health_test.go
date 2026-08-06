package portinspector

import (
	"context"
	"os"
	"testing"
)

func TestMarkZombieOwner(t *testing.T) {
	o := Observation{ProcessName: "mysvc"}
	got := markZombieOwner(o, true)
	if !got.OwnerUnhealthy {
		t.Fatal("zombie owner must be flagged unhealthy")
	}
	if got.ProcessName != "mysvc (zombie)" {
		t.Fatalf("process name = %q, want readable zombie suffix", got.ProcessName)
	}
	if !got.OwnerUnhealthy {
		t.Fatal("flag must survive marking")
	}
	if clean := markZombieOwner(o, false); clean.OwnerUnhealthy {
		t.Fatal("healthy owner must not be flagged")
	}
	// re-marking must not double the suffix
	if again := markZombieOwner(got, true); again.ProcessName != "mysvc (zombie)" {
		t.Fatalf("double mark mangled name: %q", again.ProcessName)
	}
}

func TestIsUnhealthyProcessStatus(t *testing.T) {
	healthy := [][]string{
		{"running"},
		{"sleeping"},
		{"running", "sleeping"},
		{"unknown"}, // no evidence must not be treated as unhealthy
		nil,
		{},
	}
	for _, s := range healthy {
		if isUnhealthyProcessStatus(s) {
			t.Fatalf("status %v must be healthy", s)
		}
	}
	unhealthy := [][]string{
		{"zombie"},
		{"dead"},
		{"stopped"},
		{"terminated"},
		{"exited"},
		{"running", "zombie"}, // any terminal status poisons the owner
		{"Zombie"},            // case-insensitive
	}
	for _, s := range unhealthy {
		if !isUnhealthyProcessStatus(s) {
			t.Fatalf("status %v must be unhealthy", s)
		}
	}
}

func TestEnrichProcessSelfIsHealthy(t *testing.T) {
	// wiring check against a real process: our own running process must
	// never be flagged unhealthy by the gopsutil enrichment path.
	obs := enrichProcess(context.Background(), Observation{PID: int32(os.Getpid())})
	if obs.OwnerUnhealthy {
		t.Fatalf("self process flagged unhealthy: %+v", obs)
	}
}
