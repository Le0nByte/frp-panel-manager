package master

import (
	"context"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// gateSrvApp embeds app.Application (nil): GetConfig is real, Logger is
// harmless, and every other method panics when called — the failpoint that
// proves ListClientWireGuards / ListClientWorkers never touch the DAO or
// ValidateClientRequest while the master feature is off.
type gateSrvApp struct {
	app.Application
	cfg conf.Config
}

func (a *gateSrvApp) GetConfig() conf.Config { return a.cfg }
func (a *gateSrvApp) Logger(ctx context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.New())
}

func newGateSrv(enableWorker, enableWireGuard bool) *server {
	cfg := conf.Config{}
	cfg.Master.Features.EnableWorker = enableWorker
	cfg.Master.Features.EnableWireGuard = enableWireGuard
	return &server{appInstance: &gateSrvApp{cfg: cfg}}
}

func wgListReq() *pb.ListClientWireGuardsRequest {
	return &pb.ListClientWireGuardsRequest{Base: &pb.ClientBase{ClientId: "cli-1", ClientSecret: "secret-1"}}
}

func workerListReq() *pb.ListClientWorkersRequest {
	return &pb.ListClientWorkersRequest{Base: &pb.ClientBase{ClientId: "cli-1", ClientSecret: "secret-1"}}
}

// TestListClientWireGuardsOffReturnsEmptyWithoutDAO: EnableWireGuard=false
// must return a successful empty response BEFORE validation/DAO. The nil
// embedded app would panic if ValidateClientRequest were ever reached.
func TestListClientWireGuardsOffReturnsEmptyWithoutDAO(t *testing.T) {
	s := newGateSrv(false, false)
	resp, err := s.ListClientWireGuards(context.Background(), wgListReq())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetWireguardConfigs(), "gated list must be empty")
}

// TestListClientWireGuardsOffEmptyBaseStillEmpty: even a request with no base
// gets the empty success — the gate fires before any request validation.
func TestListClientWireGuardsOffEmptyBaseStillEmpty(t *testing.T) {
	s := newGateSrv(false, false)
	resp, err := s.ListClientWireGuards(context.Background(), &pb.ListClientWireGuardsRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetWireguardConfigs())
}

// TestListClientWireGuardsOnKeepsLegacyValidation: EnableWireGuard=true must
// NOT short-circuit — the legacy path runs ValidateClientRequest and an
// unauthenticated base fails with the legacy error instead of an empty success.
func TestListClientWireGuardsOnKeepsLegacyValidation(t *testing.T) {
	s := newGateSrv(false, true)
	resp, err := s.ListClientWireGuards(context.Background(), &pb.ListClientWireGuardsRequest{})
	require.Error(t, err, "legacy validation must reject an empty base when the feature is on")
	require.Nil(t, resp, "the gate must not return the gated empty response while on")
}

// TestListClientWorkersOffReturnsEmptyWithoutDAO: EnableWorker=false must
// return a successful empty response before validation/DAO.
func TestListClientWorkersOffReturnsEmptyWithoutDAO(t *testing.T) {
	s := newGateSrv(false, false)
	resp, err := s.ListClientWorkers(context.Background(), workerListReq())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetWorkers(), "gated list must be empty")
}

// TestListClientWorkersOnKeepsLegacyValidation: EnableWorker=true must not
// short-circuit — legacy validation still rejects an empty base.
func TestListClientWorkersOnKeepsLegacyValidation(t *testing.T) {
	s := newGateSrv(true, false)
	resp, err := s.ListClientWorkers(context.Background(), &pb.ListClientWorkersRequest{})
	require.Error(t, err, "legacy validation must reject an empty base when the feature is on")
	require.Nil(t, resp, "the gate must not return the gated empty response while on")
}

// TestListClientWireGuardsOffDoesNotTouchOtherRPCs: the WG gate must not
// leak into unrelated RPCs — ReportWireGuardRuntimeInfo still runs its
// validation (legacy) and rejects an empty base with the legacy error.
func TestListClientWireGuardsOffDoesNotTouchOtherRPCs(t *testing.T) {
	s := newGateSrv(false, false)
	resp, err := s.ReportWireGuardRuntimeInfo(context.Background(), &pb.ReportWireGuardRuntimeInfoReq{})
	require.Error(t, err, "unrelated RPC must keep legacy validation behavior")
	require.Nil(t, resp)
}
