package proxy

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/biz/master/client"
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
)

// DeleteProxyConfig is the live /delete_config route entry (handler.go
// POST /api/v1/proxy/delete_config). It is a THIN DELEGATE to SafeDeleteProxy
// (the B3c transactional coordinator): the handler itself never validates by
// the legacy contract, never resolves origins, never acquires locks, never
// reads or writes the DB and never sends agent RPCs. All of that —
// canonical-origin resolution, the (tenant, origin) keyed lock, idempotency
// replay, before-snapshots, branch selection (fast DB-only vs full
// apply+verify), CommitDelete and compensation — runs inside the
// coordinator via the package-global safeDeleteCoordinator.
func DeleteProxyConfig(ctx *app.Context, req *pb.DeleteProxyConfigRequest) (*pb.DeleteProxyConfigResponse, error) {
	// I1-B L2 gate: a blocked master answers READ_ONLY here — the
	// coordinator (lock, DB, snapshots, agent RPC) is never reached.
	if readonly.Blocked() {
		return invalidDeleteResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return SafeDeleteProxy(ctx, req)
}

// DeleteProxyConfigLegacy is the LEGACY delete entry, used ONLY by the
// worker-ingress teardown (worker/remove_worker.go) and legacy-lock tests.
// It implements the pre-transactional contract: basic field validation,
// LockLegacyOrigin held for the WHOLE operation (the same originlock.Shared
// instance SafeCreateProxy holds) and the DB-first + async agent flow.
//
// It is NOT the managed transactional /delete_config route (that route is
// the thin delegate above): it does NOT enforce the idempotency/UUID
// contract, does NOT create transactions/snapshots/reservations and MUST
// MUST NOT be used for new managed flows.
func DeleteProxyConfigLegacy(c *app.Context, req *pb.DeleteProxyConfigRequest) (*pb.DeleteProxyConfigResponse, error) {
	// I1-B L2 gate: fires BEFORE validation and before LockLegacyOrigin,
	// so a blocked master performs no lock and no DB work.
	if readonly.Blocked() {
		return nil, readonly.ErrReadOnly
	}
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetName()) == 0 {
		return nil, fmt.Errorf("request invalid")
	}

	_, unlock, err := LockLegacyOrigin(c, req.GetClientId())
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve origin client, id: [%s]", req.GetClientId())
		return nil, err
	}
	defer unlock()

	return deleteProxyConfigLegacyLocked(c, req)
}

// deleteProxyConfigLegacyLocked is the lock-free LEGACY delete body. The
// caller MUST already hold the (tenant, origin) keyed lock for the
// request's resolved origin (DeleteProxyConfigLegacy and
// CreateProxyConfigWithTypedConfigLocked). It is LEGACY/INTERNAL ONLY —
// never call it from the managed transactional route.
func deleteProxyConfigLegacyLocked(c *app.Context, req *pb.DeleteProxyConfigRequest) (*pb.DeleteProxyConfigResponse, error) {
	// I1-B defense-in-depth: this body is production-callable through the
	// legacy create cleanup path, so it gates before any DB access.
	if readonly.Blocked() {
		return nil, readonly.ErrReadOnly
	}
	var (
		userInfo  = common.GetUserInfo(c)
		clientID  = req.GetClientId()
		serverID  = req.GetServerId()
		proxyName = req.GetName()
	)
	q := dao.NewQuery(c)
	m := dao.NewMutation(c)

	cli, err := q.GetClientByClientID(userInfo, clientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get client, id: [%s]", clientID)
		return nil, err
	}
	if cli.ServerID != serverID {
		return nil, fmt.Errorf("client and server not match")
	}

	oldCfg, err := cli.GetConfigContent()
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get client config content, id: [%s]", clientID)
		return nil, err
	}

	oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name != proxyName
	})

	if err := cli.SetConfigContent(*oldCfg); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot set client config, id: [%s]", clientID)
		return nil, err
	}

	if err := m.UpdateClient(userInfo, cli.ClientEntity); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot update client, id: [%s]", clientID)
		return nil, err
	}

	rawCfg, err := cli.MarshalJSONConfig()
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot marshal client config, id: [%s]", clientID)
		return nil, err
	}

	// Locked variant: the caller already holds the (tenant, origin) key;
	// the locking route entry would deadlock this goroutine on its own key.
	_, err = client.UpdateFrpcHanderLocked(c, &pb.UpdateFRPCRequest{
		ClientId: &cli.ClientID,
		ServerId: &serverID,
		Config:   rawCfg,
		Comment:  &cli.Comment,
		FrpsUrl:  &cli.FrpsUrl,
	})
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot update frpc, id: [%s]", clientID)
		return nil, err
	}

	if err := m.DeleteProxyConfig(userInfo, clientID, proxyName); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot delete proxy config, id: [%s]", clientID)
		return nil, err
	}

	logger.Logger(c).Infof("delete proxy config, id: [%s], name: [%s]", clientID, proxyName)

	return &pb.DeleteProxyConfigResponse{}, nil
}
