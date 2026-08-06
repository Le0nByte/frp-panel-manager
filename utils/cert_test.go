package utils_test

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
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/utils"
	"google.golang.org/grpc/credentials"
)

// testCA is a throwaway self-signed CA generated in-test. Tests must never
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
// 127.0.0.1 SAN, so a verifying client with the CA in its RootCAs accepts it.
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

// TestTLSClientCertWithSkipVerifyFalseVerifiesCA pins spec §10.5: with
// skip=false the credentials must verify the server chain against the CA
// (a CA-signed server cert is accepted, an untrusted one is rejected) and
// refuse pre-TLS1.2 servers.
func TestTLSClientCertWithSkipVerifyFalseVerifiesCA(t *testing.T) {
	ca := newTestCA(t)

	cred, err := utils.TLSClientCertWithSkipVerify(ca.pem, false)
	if err != nil {
		t.Fatalf("TLSClientCertWithSkipVerify(false): %v", err)
	}

	// Server cert signed by the trusted CA: verification must succeed.
	assertCredHandshake(t, cred, tlsServerConfig(ca.serverCert(t)), true)

	// Server cert signed by an untrusted CA: verification must reject it.
	other := newTestCA(t)
	assertCredHandshake(t, cred, tlsServerConfig(other.serverCert(t)), false)

	// TLS 1.0/1.1-only server: the client must refuse (TLS1.2 minimum).
	legacy := &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		MinVersion:   tls.VersionTLS10,
		MaxVersion:   tls.VersionTLS11,
		NextProtos:   []string{"h2"},
	}
	assertCredHandshake(t, cred, legacy, false)
}

// TestTLSClientCertWithSkipVerifyTrueSkipsVerification pins the explicit
// opt-out: skip=true must accept a server cert from an untrusted CA (test
// environments only, spec §10.5).
func TestTLSClientCertWithSkipVerifyTrueSkipsVerification(t *testing.T) {
	ca := newTestCA(t)
	other := newTestCA(t)

	cred, err := utils.TLSClientCertWithSkipVerify(ca.pem, true)
	if err != nil {
		t.Fatalf("TLSClientCertWithSkipVerify(true): %v", err)
	}

	assertCredHandshake(t, cred, tlsServerConfig(other.serverCert(t)), true)
}

// TestTLSClientCertDelegatesToValidatingPath pins that the plain helper now
// delegates to the validating path: an untrusted chain is rejected and
// pre-TLS1.2 servers are refused (the old implementation had no TLS1.2
// minimum).
func TestTLSClientCertDelegatesToValidatingPath(t *testing.T) {
	ca := newTestCA(t)
	other := newTestCA(t)

	cred, err := utils.TLSClientCert(ca.pem)
	if err != nil {
		t.Fatalf("TLSClientCert: %v", err)
	}

	// Verifying, not NoValidate: an untrusted chain must be rejected.
	assertCredHandshake(t, cred, tlsServerConfig(other.serverCert(t)), false)

	// TLS1.2 minimum, unlike the old NewClientTLSFromCert path.
	legacy := &tls.Config{
		Certificates: []tls.Certificate{ca.serverCert(t)},
		MinVersion:   tls.VersionTLS10,
		MaxVersion:   tls.VersionTLS11,
		NextProtos:   []string{"h2"},
	}
	assertCredHandshake(t, cred, legacy, false)
}
