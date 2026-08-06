package server

import (
	"strconv"
	"strings"
	"testing"
)

func TestUpdateFRPSReqSummary(t *testing.T) {
	const (
		serverID     = "agent-srv-2"
		frpToken     = "SENTINEL_FRP_TOKEN_1a2b3c4d"
		configString = "serverAddr = \"1.2.3.4\"\nauth.token = \"" + frpToken + "\""
	)
	config := []byte(configString)

	got := UpdateFRPSReqSummary(serverID, config)

	// Only the server ID and the config length may appear (spec §10.9).
	if !strings.Contains(got, serverID) {
		t.Errorf("summary missing server ID %q: %s", serverID, got)
	}
	if !strings.Contains(got, strconv.Itoa(len(configString))) {
		t.Errorf("summary missing config length %d: %s", len(configString), got)
	}

	// The raw FRPS config bytes (which embed the FRP auth token) must never
	// be logged.
	for _, leaked := range []string{frpToken, "auth.token", "serverAddr", configString} {
		if strings.Contains(got, leaked) {
			t.Errorf("summary leaks config content %q: %s", leaked, got)
		}
	}
}
