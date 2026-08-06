package server

import (
	"strings"
	"testing"
)

func TestFRPAuthReqSummary(t *testing.T) {
	const (
		user  = "alice"
		token = "SENTINEL_FRP_AUTH_TOKEN_4f3a2b"
	)

	got := FRPAuthReqSummary(user)

	// The user must remain visible for traceability (spec §10.9).
	if !strings.Contains(got, user) {
		t.Errorf("summary missing user %q: %s", user, got)
	}
	if !strings.Contains(got, "user") {
		t.Errorf("summary missing user field label: %s", got)
	}

	// The auth token is never part of the summary — the helper only accepts
	// the user, so the token cannot even be passed in.
	for _, leaked := range []string{token, "%+v", "[%+v]", "FRPAuthRequest"} {
		if strings.Contains(got, leaked) {
			t.Errorf("summary leaks auth material %q: %s", leaked, got)
		}
	}
}
