package conf

import (
	"net"
	"testing"
)

// TestGetCertTemplatePublicIPGoesToIPAddressesSAN pins spec §10.4: a public
// IP in MASTER_API_HOST must land in the IPAddresses SAN (never DNSNames),
// with the loopback addresses preserved.
func TestGetCertTemplatePublicIPGoesToIPAddressesSAN(t *testing.T) {
	cfg := Config{}
	cfg.Master.APIHost = "47.109.18.109"

	tmpl := GetCertTemplate(cfg)

	if !containsIP(tmpl.IPAddresses, net.ParseIP(cfg.Master.APIHost)) {
		t.Errorf("GetCertTemplate(%q): public IP missing from IPAddresses SAN: %v", cfg.Master.APIHost, tmpl.IPAddresses)
	}
	for _, dns := range tmpl.DNSNames {
		if dns == cfg.Master.APIHost {
			t.Errorf("GetCertTemplate(%q): public IP must not appear in DNSNames: %v", cfg.Master.APIHost, tmpl.DNSNames)
		}
	}
	if !containsIP(tmpl.IPAddresses, net.ParseIP("127.0.0.1")) {
		t.Errorf("GetCertTemplate(%q): loopback must remain in IPAddresses: %v", cfg.Master.APIHost, tmpl.IPAddresses)
	}
}

// TestGetCertTemplateDomainGoesToDNSNamesSAN guards the domain side of
// spec §10.4: a domain in MASTER_API_HOST lands in DNSNames, never in
// IPAddresses.
func TestGetCertTemplateDomainGoesToDNSNamesSAN(t *testing.T) {
	cfg := Config{}
	cfg.Master.APIHost = "panel.example.com"

	tmpl := GetCertTemplate(cfg)

	if !containsString(tmpl.DNSNames, cfg.Master.APIHost) {
		t.Errorf("GetCertTemplate(%q): domain missing from DNSNames SAN: %v", cfg.Master.APIHost, tmpl.DNSNames)
	}
	for _, ip := range tmpl.IPAddresses {
		if ip.String() == cfg.Master.APIHost {
			t.Errorf("GetCertTemplate(%q): domain must not appear in IPAddresses: %v", cfg.Master.APIHost, tmpl.IPAddresses)
		}
	}
}

// TestGetCertTemplateEmptyHostSafe pins the nil/empty-safety of the SAN
// builder: an empty APIHost must not produce an empty DNSNames entry.
func TestGetCertTemplateEmptyHostSafe(t *testing.T) {
	tmpl := GetCertTemplate(Config{}) // empty APIHost

	for _, dns := range tmpl.DNSNames {
		if dns == "" {
			t.Errorf("GetCertTemplate(empty APIHost): empty DNSNames entry present: %v", tmpl.DNSNames)
		}
	}
	if !containsIP(tmpl.IPAddresses, net.ParseIP("127.0.0.1")) || !containsIP(tmpl.IPAddresses, net.ParseIP("::1")) {
		t.Errorf("GetCertTemplate(empty APIHost): loopback IPs must remain: %v", tmpl.IPAddresses)
	}
}

// TestMasterAPIListenAddrBindsLoopbackOnly pins spec §10.1: the Master API
// listener must bind 127.0.0.1 only (the reverse proxy owns public HTTPS).
func TestMasterAPIListenAddrBindsLoopbackOnly(t *testing.T) {
	cfg := Config{}
	cfg.Master.APIPort = 9100

	if got := MasterAPIListenAddr(cfg); got != "127.0.0.1:9100" {
		t.Errorf("MasterAPIListenAddr() = %q, want %q (spec §10.1 loopback bind)", got, "127.0.0.1:9100")
	}
}

// TestRPCListenAddrStaysWildcard guards the RPC side: the gRPC/mux listener
// keeps binding all interfaces so agents can reach it over the public net.
func TestRPCListenAddrStaysWildcard(t *testing.T) {
	cfg := Config{}
	cfg.Master.RPCPort = 9101

	if got := RPCListenAddr(cfg); got != ":9101" {
		t.Errorf("RPCListenAddr() = %q, want %q (RPC must stay public)", got, ":9101")
	}
}

func containsIP(ips []net.IP, want net.IP) bool {
	for _, ip := range ips {
		if ip != nil && ip.Equal(want) {
			return true
		}
	}
	return false
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
