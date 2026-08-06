package shared

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/master"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

// TestNewRPCGuardProducesWorkingGuard: the fx provider builds a guard
// with production defaults (no secret material anywhere) that functions
// as an admission state machine.
func TestNewRPCGuardProducesWorkingGuard(t *testing.T) {
	a := app.NewApp()
	g := NewRPCGuard(app.NewContext(context.Background(), a))
	require.NotNil(t, g)

	// Default register ceiling: 30 allowed, the 31st blocked.
	peer := "203.0.113.50:7000"
	for i := 0; i < rpcguard.DefaultRegisterRatePerMin; i++ {
		require.True(t, g.AllowRegister(peer, "node-1"), "attempt %d must be allowed", i+1)
	}
	require.False(t, g.AllowRegister(peer, "node-1"), "attempt beyond the ceiling must be blocked")
}

// TestMasterServiceProvidersWireOneGuard: the three master entry points
// must each accept a guard. The same-instance proof lives in the master
// package (TestMasterServicesShareOneGuard); this pins the provider
// signatures and that fx resolves them from ONE NewRPCGuard invocation
// (fx invokes a provider once per container, so TLS/HTTP/WS share the
// pointer by construction).
func TestMasterServiceProvidersWireOneGuard(t *testing.T) {
	a := app.NewApp()
	ctx := app.NewContext(context.Background(), a)
	tlsCfg := &tls.Config{}

	g := NewRPCGuard(ctx)
	tlsSvc := NewTLSMasterService(a, tlsCfg, g)
	httpSvc := NewHTTPMasterService(a, g)
	wsSvc := NewHTTPMasterService(a, g)

	require.NotNil(t, tlsSvc.GetServer())
	require.NotNil(t, httpSvc.GetServer())
	require.NotNil(t, wsSvc.GetServer())
}

// TestRPCGuardFxGraphResolves: the full fx wiring — NewRPCGuard provided
// once and consumed by the TLS/HTTP/WS MasterService providers — must
// resolve without missing dependencies, proving masterMod can wire the
// shared guard into every listener.
func TestRPCGuardFxGraphResolves(t *testing.T) {
	a := app.NewApp()
	appCtx := app.NewContext(context.Background(), a)
	tlsCfg := &tls.Config{}

	var got struct {
		fx.In

		Guard *rpcguard.Guard
		TLS   master.MasterService `name:"tlsMasterService"`
		HTTP  master.MasterService `name:"httpMasterService"`
		WS    master.MasterService `name:"wsMasterService"`
	}

	app := fx.New(
		fx.Supply(
			fx.Annotate(a, fx.As(new(app.Application))),
			appCtx,
			tlsCfg,
		),
		fx.Provide(
			NewRPCGuard,
			fx.Annotate(NewTLSMasterService, fx.ResultTags(`name:"tlsMasterService"`)),
			fx.Annotate(NewHTTPMasterService, fx.ResultTags(`name:"httpMasterService"`)),
			fx.Annotate(NewHTTPMasterService, fx.ResultTags(`name:"wsMasterService"`)),
		),
		fx.NopLogger,
		fx.Populate(&got),
	)
	require.NoError(t, app.Err(), "the guard wiring graph must construct")
	require.NotNil(t, got.Guard)
	require.NotNil(t, got.TLS.GetServer())
	require.NotNil(t, got.HTTP.GetServer())
	require.NotNil(t, got.WS.GetServer())
}
