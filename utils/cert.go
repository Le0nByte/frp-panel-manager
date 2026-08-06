package utils

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"

	"github.com/VaalaCat/frp-panel/utils/logger"
	"google.golang.org/grpc/credentials"
)

func PublicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

func PemBlockForPrivKey(priv interface{}) *pem.Block {
	ctx := context.Background()
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			logger.Logger(ctx).Fatalf("Unable to marshal ECDSA private key: %v", err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}

func TLSServerCert(certPem, keyPem []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
	}
	return config, nil
}

// TLSClientCert returns gRPC transport credentials that verify the server
// certificate against caPem (spec §10.5: production must verify).
func TLSClientCert(caPem []byte) (credentials.TransportCredentials, error) {
	return TLSClientCertWithSkipVerify(caPem, false)
}

// TLSClientCertWithSkipVerify returns gRPC transport credentials that verify
// the server certificate chain against caPem (RootCAs) with a TLS 1.2
// minimum. When skip is true the certificate is not verified — this is for
// test environments only; production keeps CLIENT_TLS_INSECURE_SKIP_VERIFY
// false (spec §10.5).
func TLSClientCertWithSkipVerify(caPem []byte, skip bool) (credentials.TransportCredentials, error) {
	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPem)

	config := &tls.Config{
		RootCAs:            certpool,
		InsecureSkipVerify: skip,
		MinVersion:         tls.VersionTLS12,
	}

	return credentials.NewTLS(config), nil
}
