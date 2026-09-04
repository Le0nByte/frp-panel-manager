package utils

import (
	"fmt"
	"net/url"

	"github.com/spf13/cast"
)

// ValidFrpsScheme reports whether scheme is a transport frpc can dial.
// It mirrors frp's own transport.protocol values.
func ValidFrpsScheme(scheme string) bool {
	return scheme == "tcp" ||
		scheme == "kcp" || scheme == "quic" ||
		scheme == "websocket" || scheme == "wss"
}

// ValidateFrpsURL parses and fully validates a client row's frps URL override.
//
// This is the SINGLE validator for both directions. The write path
// (biz/master/proxy.buildFrpcApplyConfig) used to carry its own weaker copy
// that only checked for a non-empty hostname, so the same stored frps_url
// could be rejected by the read path (agent pull) while the write path
// happily pushed `protocol: "bogus"` and `serverPort: 0` to the agent. The
// injector these two share is only byte-compatible if their input validation
// is identical too.
//
// Every check is fail-closed: an override that cannot produce a dialable
// address must be refused rather than silently degraded.
func ValidateFrpsURL(urlStr string) (*url.URL, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("parse frps url error")
	}

	if !ValidFrpsScheme(parsed.Scheme) {
		return nil, fmt.Errorf("invalid frps scheme")
	}

	if len(parsed.Host) == 0 {
		return nil, fmt.Errorf("invalid frps host")
	}

	if len(parsed.Hostname()) == 0 {
		return nil, fmt.Errorf("invalid frps hostname")
	}

	if cast.ToInt(parsed.Port()) == 0 {
		return nil, fmt.Errorf("invalid frps port")
	}

	return parsed, nil
}
