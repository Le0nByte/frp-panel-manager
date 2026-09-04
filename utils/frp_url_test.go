package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The write path used to carry a weaker private copy of this validator that
// only checked for a non-empty hostname, so the same stored frps_url was
// rejected on pull but pushed to the agent on write. One validator, one
// verdict — these cases pin the strict contract both paths now share.
func TestValidateFrpsURLAcceptsEveryDialableTransport(t *testing.T) {
	for _, scheme := range []string{"tcp", "kcp", "quic", "websocket", "wss"} {
		parsed, err := ValidateFrpsURL(scheme + "://frps.example.com:7100")
		require.NoError(t, err, scheme)
		require.Equal(t, scheme, parsed.Scheme)
		require.Equal(t, "frps.example.com", parsed.Hostname())
		require.Equal(t, "7100", parsed.Port())
	}
}

func TestValidateFrpsURLRejectsUndialableOverrides(t *testing.T) {
	cases := map[string]string{
		"unknown scheme":  "bogus://frps.example.com:7100",
		"missing scheme":  "frps.example.com:7100",
		"missing port":    "tcp://frps.example.com",
		"zero port":       "tcp://frps.example.com:0",
		"no host":         "tcp://",
		"empty string":    "",
		"http not a frps": "https://frps.example.com:7100",
		// Host is non-empty (":7100") but Hostname() is empty — only the
		// dedicated hostname check catches this one.
		"port without host": "tcp://:7100",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			parsed, err := ValidateFrpsURL(raw)
			require.Error(t, err, "must not silently degrade to an undialable override")
			require.Nil(t, parsed)
		})
	}
}
