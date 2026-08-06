package rpc

import (
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
)

// TestHTTPCliHonorsTLSInsecureSkipVerify pins that the HTTP client used for
// the cert-bootstrap and join API no longer unconditionally bypasses TLS
// verification (spec §10.5): it mirrors CLIENT_TLS_INSECURE_SKIP_VERIFY.
// No network is touched — the client is only constructed and inspected.
func TestHTTPCliHonorsTLSInsecureSkipVerify(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip bool
		want bool
	}{
		{name: "verify by default", skip: false, want: false},
		{name: "explicit skip", skip: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := conf.Config{}
			cfg.Client.TLSInsecureSkipVerify = tc.skip

			c := httpCli(cfg)
			if c.TLSClientConfig == nil {
				t.Fatal("httpCli: TLSClientConfig must be set")
			}
			if c.TLSClientConfig.InsecureSkipVerify != tc.want {
				t.Errorf("httpCli().TLSClientConfig.InsecureSkipVerify = %v, want %v", c.TLSClientConfig.InsecureSkipVerify, tc.want)
			}
		})
	}
}
