package proxy

import (
	"errors"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// Same-name create contract (the "silent upsert" defect).
//
// create_config is an INSERT: a name that is already taken in the same
// (tenant, origin, server) scope must be REJECTED, never merged into the
// existing row. The old behaviour preserved the row id of the same-name rule
// and rewrote its content, so a second "create" silently moved the public port
// of a live tunnel and left the previous port's committed reservation behind —
// invisible to every list endpoint and impossible to release from the UI, it
// blocked that port with RESERVATION_CONFLICT forever.
//
// overwrite=true is the caller's explicit "replace this rule" intent and keeps
// working; the commit must then reconcile the inherited lease (release the old
// port, adopt the same port).
// ---------------------------------------------------------------------------

func overwriteReq(idemKey string, config []byte) *pb.CreateProxyConfigRequest {
	req := createReq(idemKey, config)
	req.Overwrite = lo.ToPtr(true)
	return req
}

func (e *coordTestEnv) proxyRowByName(name string) *models.ProxyConfig {
	e.t.Helper()
	var row models.ProxyConfig
	err := e.db.Where("name = ?", name).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(e.t, err)
	return &row
}

// assertZeroConfigDelta pins the 零配置变更 contract for a REJECTED create: the
// client rows and every proxy_config row must be byte-identical to the
// pre-request state (the gate runs before any write, so nothing may move).
func assertZeroConfigDelta(t *testing.T, env *coordTestEnv, beforeClients []models.Client, beforeProxies []models.ProxyConfig) {
	t.Helper()
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "client rows must be identical to the pre-request state")
	require.Equal(t, beforeProxies, afterProxies, "proxy_config rows must be identical to the pre-request state")
}

func remotePortOf(t *testing.T, row *models.ProxyConfig) uint32 {
	t.Helper()
	cfg, err := row.GetTypedProxyConfig()
	require.NoError(t, err)
	switch p := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		return uint32(p.RemotePort)
	case *v1.UDPProxyConfig:
		return uint32(p.RemotePort)
	default:
		t.Fatalf("unsupported proxy type %T", cfg.ProxyConfigurer)
		return 0
	}
}

// TestSafeCreateSameNameIsRejectedWithoutOverwrite is the exact defect report:
// create web@8000, then create web@8001. The second request used to succeed and
// rewrite the first tunnel (5099→39130 became 6081→39131) while 39130 stayed
// reserved. It must now fail with PROXY_NAME_CONFLICT and touch nothing.
func TestSafeCreateSameNameIsRejectedWithoutOverwrite(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	first, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, first.GetStatus().GetCode())

	web := env.proxyRowByName("web")
	require.NotNil(t, web)
	require.Equal(t, uint32(8000), remotePortOf(t, web))
	lease := env.reservationByPort(8000)
	require.NotNil(t, lease)
	require.NotNil(t, lease.OwnerProxyID)
	require.Equal(t, web.ID, *lease.OwnerProxyID)

	beforeC, beforeP := env.captureConfigRows()
	txsBefore := env.countTransactions()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateTOML("web", 8001, 8080)))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeProxyNameConflict)

	// ZERO side effects: the gate runs before the transaction row, the lease
	// and the agent apply.
	require.Equal(t, txsBefore, env.countTransactions(), "a rejected create must not open a transaction")
	assertZeroConfigDelta(t, env, beforeC, beforeP)
	require.Equal(t, 1, env.apply.calls, "a rejected create must not reach the agent")
	require.Equal(t, 1, env.countReservations(), "the rejected create must not take a lease")
	require.Nil(t, env.reservationByPort(8001))

	// the existing tunnel still owns its port and its lease
	web = env.proxyRowByName("web")
	require.Equal(t, uint32(8000), remotePortOf(t, web), "the existing rule must not be rewritten")
	lease = env.reservationByPort(8000)
	require.NotNil(t, lease)
	require.Equal(t, models.ReservationStatusCommitted, lease.Status)
	require.Equal(t, web.ID, *lease.OwnerProxyID)
}

// TestSafeCreateOverwriteReleasesPreviousPortLease pins the explicit overwrite
// path: the old port's committed lease must be released in the same commit that
// moves the rule to a new port, otherwise the leak is only reached through a
// different door.
func TestSafeCreateOverwriteReleasesPreviousPortLease(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	first, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, first.GetStatus().GetCode())
	web := env.proxyRowByName("web")
	require.NotNil(t, web)
	oldRowID := web.ID

	resp, err := tc.SafeCreateProxy(env.ctx, overwriteReq(uuidKey(), candidateTOML("web", 8001, 8080)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
	require.Empty(t, resp.GetErrorCode())

	web = env.proxyRowByName("web")
	require.Equal(t, oldRowID, web.ID, "an explicit overwrite keeps the row (same tunnel)")
	require.Equal(t, uint32(8001), remotePortOf(t, web))

	// THE FIX: the previous port's committed lease is gone.
	require.Nil(t, env.reservationByPort(8000),
		"the previous port's committed lease must be released with the port change")
	lease := env.reservationByPort(8001)
	require.NotNil(t, lease)
	require.Equal(t, models.ReservationStatusCommitted, lease.Status)
	require.NotNil(t, lease.OwnerProxyID)
	require.Equal(t, web.ID, *lease.OwnerProxyID)
	require.Equal(t, 1, env.countReservations())

	// regression proof: the freed port is immediately usable again (before the
	// fix this answered RESERVATION_CONFLICT "already reserved by another change")
	resp2, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateTOML("web2", 8000, 8080)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode(), resp2.GetStatus().GetMessage())
}

// TestSafeCreateOverwriteSamePortAdoptsLease pins the same-key overwrite: the
// replaced rule already owns the committed lease at that port, and a committed
// lease is never evictable — the transaction must adopt it instead of trying to
// reserve its own port and failing with a self-conflict.
func TestSafeCreateOverwriteSamePortAdoptsLease(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	first, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, first.GetStatus().GetCode())
	web := env.proxyRowByName("web")
	require.NotNil(t, web)

	// same name, same public port, different backend
	resp, err := tc.SafeCreateProxy(env.ctx, overwriteReq(uuidKey(), candidateTOML("web", 8000, 8081)))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())

	require.Equal(t, 1, env.countReservations(), "adoption must not create a second lease")
	lease := env.reservationByPort(8000)
	require.NotNil(t, lease)
	require.Equal(t, models.ReservationStatusCommitted, lease.Status)
	require.NotNil(t, lease.OwnerProxyID)
	require.Equal(t, web.ID, *lease.OwnerProxyID)

	web = env.proxyRowByName("web")
	require.Equal(t, uint32(8000), remotePortOf(t, web))
	require.Equal(t, 2, env.apply.calls, "the overwrite still applies the new backend")
}
