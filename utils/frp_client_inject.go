package utils

import (
	"encoding/json"
	"fmt"

	"github.com/VaalaCat/frp-panel/defs"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
)

// FRPClientInjection carries everything needed to turn a STORED candidate
// client config into a config an agent can actually dial with.
//
// Background: the transactional coordinators persist the *candidate* config
// into `clients.config_content` (proxies are real, connection identity is
// still at frp's Complete() defaults — 0.0.0.0:7000, no user, no auth
// metadata) and send the *injected* config to the agent over RPC. Any read
// path that hands `config_content` to an agent must therefore inject the
// connection identity first, or the agent dials 0.0.0.0:7000 unauthenticated.
//
// The struct is deliberately made of plain values (no *app.Context, no DAO)
// so the injection is a pure function usable from both biz layers and unit
// tests without a database.
type FRPClientInjection struct {
	// ServerAddr is the frps address agents dial (the server row's ServerIP).
	ServerAddr string
	// BindPort is the frps TCP bind port; KCPBindPort/QUICBindPort are used
	// only when the candidate selects those transports.
	BindPort     int
	KCPBindPort  int
	QUICBindPort int

	// User is the frp user prefix (the owning account's user name).
	User string
	// AuthToken is the frps auth token carried in the FRP metadata.
	AuthToken string
	// ClientID is the child client id frps must index this connection as.
	// It is REQUIRED: an empty value would let frps attribute the tunnel to
	// the wrong handler.
	ClientID string

	// FrpsURLHost/Port/Protocol override the server-derived address when the
	// client row carries an explicit frps URL. Empty host = no override.
	FrpsURLHost     string
	FrpsURLPort     int
	FrpsURLProtocol string
}

// InjectFRPClientConfig rewrites the connection identity of a stored candidate
// client config and returns the bytes an agent can run.
//
// It is the SINGLE implementation shared by both directions:
// biz/master/proxy.buildFrpcApplyConfig (write path — pushed inside a
// transaction) and biz/master/client.pullableClientConfig (read path — the
// agent's 30s pull loop). They drifted once already: the write path pushed an
// injected config while the read path served the raw candidate, so every pull
// overwrote a working agent config with an undialable 0.0.0.0:7000 one
// (production incident 2026-09-04, client root.c.openWrt). Callers resolve
// inputs; ALL rewriting happens here.
//
// The function is idempotent: injecting an already-injected config rewrites the
// same fields with the same values, which matters because the agent's pull loop
// re-injects every 30s and compares the parsed common config to decide whether
// to recreate its frpc handler. A non-idempotent injection would churn the
// handler forever.
//
// Proxies and visitors are never modified — only the connection identity is.
func InjectFRPClientConfig(candidate []byte, in FRPClientInjection) ([]byte, error) {
	if in.ClientID == "" {
		return nil, fmt.Errorf("inject frp client config: client id is empty")
	}
	// frps' multiuser auth plugin rejects a login whose user or meta token is
	// empty (biz/server/auth_handler.go: "user or meta token can not be
	// empty"). Injecting an empty identity would produce a config that parses
	// fine, dials the right address, and can never log in — the same
	// silent-failure class this injector exists to prevent. Fail closed here
	// so both paths refuse to serve it.
	if in.User == "" {
		return nil, fmt.Errorf("inject frp client config: user is empty")
	}
	if in.AuthToken == "" {
		return nil, fmt.Errorf("inject frp client config: auth token is empty")
	}

	cliCfg, err := LoadClientConfigNormal(candidate, true)
	if err != nil {
		return nil, fmt.Errorf("inject frp client config: cannot load candidate: %w", err)
	}

	cliCfg.ServerAddr = in.ServerAddr
	switch cliCfg.Transport.Protocol {
	case "kcp":
		cliCfg.ServerPort = in.KCPBindPort
	case "quic":
		cliCfg.ServerPort = in.QUICBindPort
	default:
		cliCfg.ServerPort = in.BindPort
	}

	if len(in.FrpsURLHost) > 0 {
		cliCfg.ServerAddr = in.FrpsURLHost
		cliCfg.ServerPort = in.FrpsURLPort
		cliCfg.Transport.Protocol = in.FrpsURLProtocol
	}

	// The selected port must be dialable. A server row whose config lacks the
	// port for the candidate's transport (e.g. transport kcp but no
	// kcpBindPort) would otherwise yield serverPort: 0 — a config that parses,
	// carries valid auth, and can never connect. Same fail-closed reasoning as
	// the identity checks above: refuse rather than serve a dead config.
	if cliCfg.ServerPort <= 0 || cliCfg.ServerPort > 65535 {
		return nil, fmt.Errorf("inject frp client config: no dialable port for transport %q (got %d)",
			cliCfg.Transport.Protocol, cliCfg.ServerPort)
	}
	if cliCfg.ServerAddr == "" {
		return nil, fmt.Errorf("inject frp client config: server address is empty")
	}

	cliCfg.User = in.User
	if cliCfg.Metadatas == nil {
		cliCfg.Metadatas = make(map[string]string)
	}
	cliCfg.Metadatas[defs.FRPAuthTokenKey] = in.AuthToken
	cliCfg.Metadatas[defs.FRPClientIDKey] = in.ClientID

	newCfg := struct {
		v1.ClientCommonConfig
		Proxies  []v1.ProxyConfigurer   `json:"proxies,omitempty"`
		Visitors []v1.VisitorConfigurer `json:"visitors,omitempty"`
	}{
		ClientCommonConfig: cliCfg.ClientCommonConfig,
		Proxies:            lo.Map(cliCfg.Proxies, func(item v1.TypedProxyConfig, _ int) v1.ProxyConfigurer { return item.ProxyConfigurer }),
		Visitors:           lo.Map(cliCfg.Visitors, func(item v1.TypedVisitorConfig, _ int) v1.VisitorConfigurer { return item.VisitorConfigurer }),
	}
	return json.Marshal(newCfg)
}
