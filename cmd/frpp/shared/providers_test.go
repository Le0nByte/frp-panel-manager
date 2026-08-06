package shared

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// testCA is a throwaway self-signed CA generated in-test; the tests never
// read real certs or secrets from disk (spec §10.5 test hygiene).
type testCA struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "frp-panel-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}
	return &testCA{
		cert: parsed,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// serverCert mints a TLS server certificate signed by the CA with the
// 127.0.0.1 SAN.
func (ca *testCA) serverCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func tlsServerConfig(cert tls.Certificate) *tls.Config {
	// grpc credentials require ALPN negotiation ("h2"); without it the
	// handshake is rejected before certificate verification runs.
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}
}

// assertCredHandshake performs a real, fully-local TLS handshake between the
// gRPC transport credentials and a server presenting serverCfg, asserting the
// expected outcome. No external network is touched.
func assertCredHandshake(t *testing.T, cred credentials.TransportCredentials, serverCfg *tls.Config, wantOK bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer raw.Close()
		err = tls.Server(raw, serverCfg).Handshake()
		serverErr <- err
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outConn, _, err := cred.ClientHandshake(ctx, "127.0.0.1", raw)
	if outConn != nil {
		defer outConn.Close()
	} else {
		_ = raw.Close()
	}
	<-serverErr

	if wantOK && err != nil {
		t.Errorf("handshake: expected success, got %v", err)
	}
	if !wantOK && err == nil {
		t.Error("handshake: expected failure, got success")
	}
}

// newCertFetchServer serves the /api/v1/auth/cert bootstrap response with the
// master CA, so the provider credentials can be built without any real
// network or secrets.
func newCertFetchServer(t *testing.T, caPEM []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := proto.Marshal(&pb.GetClientCertResponse{Cert: caPEM})
		if err != nil {
			t.Errorf("marshal GetClientCertResponse: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	}))
}

// newProviderApp builds an Application whose config points the cert bootstrap
// at the fake endpoint.
func newProviderApp(t *testing.T, apiURL string, skip bool) app.Application {
	t.Helper()
	cfg := conf.Config{}
	cfg.Client.Secret = "test-secret"
	cfg.Client.APIUrl = apiURL
	cfg.Client.TLSInsecureSkipVerify = skip

	a := app.NewApp()
	a.SetConfig(cfg)
	return a
}

// TestNewServerCredHonorsTLSInsecureSkipVerify pins spec §10.5: the FRPS
// bootstrap credentials verify the server cert when CLIENT_TLS_INSECURE_SKIP_VERIFY
// is false and only skip when it is explicitly true. The server presents a
// cert signed by a DIFFERENT CA than the one returned by the cert endpoint.
func TestNewServerCredHonorsTLSInsecureSkipVerify(t *testing.T) {
	masterCA := newTestCA(t)
	untrusted := newTestCA(t)
	srv := newCertFetchServer(t, masterCA.pem)
	defer srv.Close()

	for _, tc := range []struct {
		name string
		skip bool
	}{
		{name: "verify when false", skip: false},
		{name: "explicit skip when true", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newProviderApp(t, srv.URL, tc.skip)

			cred := NewServerCred(a)
			assertCredHandshake(t, cred, tlsServerConfig(untrusted.serverCert(t)), tc.skip)
		})
	}
}

// TestNewClientCredHonorsTLSInsecureSkipVerify is the FRPC twin of the FRPS
// test above.
func TestNewClientCredHonorsTLSInsecureSkipVerify(t *testing.T) {
	masterCA := newTestCA(t)
	untrusted := newTestCA(t)
	srv := newCertFetchServer(t, masterCA.pem)
	defer srv.Close()

	for _, tc := range []struct {
		name string
		skip bool
	}{
		{name: "verify when false", skip: false},
		{name: "explicit skip when true", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newProviderApp(t, srv.URL, tc.skip)

			cred := NewClientCred(a)
			assertCredHandshake(t, cred, tlsServerConfig(untrusted.serverCert(t)), tc.skip)
		})
	}
}

// TestNewWSUpgraderOriginPolicy asserts the /wsgrpc upgrader policy (spec
// §10.10): non-browser agents may omit Origin, but a present Origin must be
// same-origin with the request Host; cross-origin upgrades are rejected.
func TestNewWSUpgraderOriginPolicy(t *testing.T) {
	up := NewWSUpgrader(nil, conf.Config{})
	if up == nil {
		t.Fatal("NewWSUpgrader returned nil")
	}
	if up.CheckOrigin == nil {
		t.Fatal("NewWSUpgrader must set CheckOrigin")
	}
	host := "panel.example.com:9443"

	req := func(origin string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "https://"+host+"/wsgrpc", nil)
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}

	if !up.CheckOrigin(req("")) {
		t.Error("wsgrpc agent without Origin must be allowed")
	}
	if !up.CheckOrigin(req("https://" + host)) {
		t.Error("same-origin agent must be allowed")
	}
	if up.CheckOrigin(req("https://evil.example.com:9443")) {
		t.Error("cross-origin agent must be rejected")
	}
}
