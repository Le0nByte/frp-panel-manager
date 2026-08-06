// Package wsorigin implements the management-plane WebSocket same-origin
// policy (spec §10.10). Both the /wsgrpc agent channel and the browser PTY
// endpoint must reject cross-origin upgrade attempts instead of accepting
// every Origin unconditionally. The Origin value is never logged or echoed
// back by this package.
package wsorigin

import (
	"net/http"
	"net/url"
	"strings"
)

// CheckOrigin reports whether an incoming WebSocket upgrade request may be
// accepted. A request without an Origin header is allowed only when allowEmpty
// is true (non-browser agents such as the wsgrpc client). When an Origin is
// present it must parse as an absolute http/https URL whose host (including
// port) matches the request Host exactly. Scheme, host and port mismatches,
// malformed values and multi-value Origin headers are rejected.
func CheckOrigin(r *http.Request, allowEmpty bool) bool {
	values, present := r.Header["Origin"]
	if !present || len(values) == 0 {
		return allowEmpty
	}
	// RFC 6454 allows the header to appear multiple times; a legitimate
	// browser sends exactly one value, so anything else is rejected rather
	// than scanning the joined string for a substring match.
	if len(values) != 1 {
		return false
	}
	u, err := url.ParseRequestURI(values[0])
	if err != nil {
		return false
	}
	if scheme := strings.ToLower(u.Scheme); scheme != "http" && scheme != "https" {
		return false
	}
	if u.Host == "" {
		return false
	}
	// The URL parser lowercases the Origin host; the request Host is
	// normalized here for a case-insensitive host:port comparison.
	return strings.EqualFold(u.Host, r.Host)
}

// SameOriginRequired returns a gorilla websocket CheckOrigin func that
// requires a same-origin Origin header, for browser-facing endpoints such as
// the PTY shell.
func SameOriginRequired() func(*http.Request) bool {
	return func(r *http.Request) bool { return CheckOrigin(r, false) }
}

// SameOriginAllowEmpty returns a gorilla websocket CheckOrigin func that
// allows requests without an Origin header (non-browser agents) while still
// rejecting cross-origin requests, for the /wsgrpc agent channel.
func SameOriginAllowEmpty() func(*http.Request) bool {
	return func(r *http.Request) bool { return CheckOrigin(r, true) }
}
