package server

import (
	"strings"
	"testing"
)

func TestRemoveFRPSReqSummary(t *testing.T) {
	const serverID = "srv-node-9"

	got := RemoveFRPSReqSummary(serverID)

	// The server ID must remain visible for traceability (spec §10.9).
	if !strings.Contains(got, serverID) {
		t.Errorf("summary missing server ID %q: %s", serverID, got)
	}
	if !strings.Contains(got, "server_id") {
		t.Errorf("summary missing server_id field label: %s", got)
	}

	// The summary must never embed the raw request dump: it is derived from
	// the server ID alone, so no request struct formatting may appear.
	for _, leaked := range []string{"%+v", "[%+v]", "RemoveFRPSRequest"} {
		if strings.Contains(got, leaked) {
			t.Errorf("summary leaks raw request formatting %q: %s", leaked, got)
		}
	}
}
