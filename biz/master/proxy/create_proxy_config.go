package proxy

import (
	"errors"
	"fmt"

	"github.com/VaalaCat/frp-panel/biz/master/client"
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"

	"gorm.io/gorm"
)

// safeCreateCoordinator is the package-wide default coordinator used by the
// CreateProxyConfig handler. It is a REAL implementation (DAO + RPC +
// configured key file), never a stub; tests swap it for a coordinator with
// injected deps and restore it afterwards, mirroring the preflightTimeout
// override pattern.
var safeCreateCoordinator = NewDefaultCoordinator()

// CreateProxyConfig is the live /create_config route entry. I1-B: while
// the master is blocked read-only, the entry answers the structured
// READ_ONLY response itself — the coordinator (and with it any lock, DB
// write, snapshot or agent RPC) is never reached.
func CreateProxyConfig(c *app.Context, req *pb.CreateProxyConfigRequest) (*pb.CreateProxyConfigResponse, error) {
	if readonly.Blocked() {
		return invalidCreateResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return safeCreateCoordinator.SafeCreateProxy(c, req)
}

type CreateProxyConfigWithTypedConfigParam struct {
	ClientID     string
	ServerID     string
	ProxyCfg     v1.TypedProxyConfig
	ClientEntity *models.ClientEntity
	Overwrite    bool
	WorkerID     *string
}

// CreateProxyConfigWithTypedConfig is the legacy worker-ingress path (used by
// create_worker_ingress.go). It retains the upstream DB-first + async agent
// flow. The port-manager CreateProxyConfig handler does NOT use it: it goes
// through the transaction coordinator (SafeCreateProxy).
//
// It is a live legacy writer, so it validates the request, resolves the
// ORIGIN client id and acquires the (tenant, origin) keyed lock — the SAME
// originlock.Shared instance SafeCreateProxy holds) — for the WHOLE
// operation (round-8 I3). Callers that ALREADY hold the lock (worker
// ingress locks at its own entry, before GetClientWithMakeShadow) MUST
// call CreateProxyConfigWithTypedConfigLocked instead — the keyed mutex is
// non-reentrant and this entry would deadlock the same goroutine.
func CreateProxyConfigWithTypedConfig(c *app.Context, param CreateProxyConfigWithTypedConfigParam) error {
	// I1-B L2 gate: fires BEFORE validation and before LockLegacyOrigin,
	// so a blocked master performs no lock and no DB work.
	if readonly.Blocked() {
		return readonly.ErrReadOnly
	}
	if len(param.ClientID) == 0 || len(param.ServerID) == 0 || param.ClientEntity == nil {
		return fmt.Errorf("request invalid")
	}

	_, unlock, err := LockLegacyOrigin(c, param.ClientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve origin client, id: [%s]", param.ClientID)
		return err
	}
	defer unlock()

	return CreateProxyConfigWithTypedConfigLocked(c, param)
}

// CreateProxyConfigWithTypedConfigLocked is the lock-free body of
// CreateProxyConfigWithTypedConfig. The caller MUST already hold the
// (tenant, origin) keyed lock for the request's resolved origin (worker
// ingress holds it across GetClientWithMakeShadow + this call).
func CreateProxyConfigWithTypedConfigLocked(c *app.Context, param CreateProxyConfigWithTypedConfigParam) error {
	// I1-B L2 gate: the worker-ingress writer must not run (and must not
	// cascade into the legacy delete body) while the master is blocked.
	if readonly.Blocked() {
		return readonly.ErrReadOnly
	}
	var (
		userInfo      = common.GetUserInfo(c)
		clientID      = param.ClientID
		serverID      = param.ServerID
		clientEntity  = param.ClientEntity
		typedProxyCfg = param.ProxyCfg
		err           error
		overwrite     = param.Overwrite
	)

	proxyCfg := &models.ProxyConfig{}

	if err := proxyCfg.FillClientConfig(clientEntity); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot fill client config, id: [%s]", clientID)
		return err
	}

	if err := proxyCfg.FillTypedProxyConfig(typedProxyCfg); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot fill typed proxy config")
		return err
	}

	// child→origin normalization: proxy rows are keyed by the ORIGIN
	// client id, so the origin-scoped lookup uses the resolved origin.
	originID, err := dao.NewQuery(c).GetOriginClientID(userInfo, clientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve origin client, id: [%s]", clientID)
		return err
	}

	var existedProxyCfg *models.ProxyConfig
	existedProxyCfg, err = dao.NewQuery(c).GetProxyConfigByOriginClientIDAndName(userInfo, originID, proxyCfg.Name)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		logger.Logger(c).WithError(err).Errorf("cannot get proxy config, id: [%s]", clientID)
		return err
	}

	if !overwrite && err == nil {
		logger.Logger(c).Errorf("proxy config already exist, name: [%s]", proxyCfg.Name)
		return fmt.Errorf("proxy config already exist")
	}

	// update client config
	if oldCfg, err := clientEntity.GetConfigContent(); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get client config, id: [%s]", clientID)
		return err
	} else {
		oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(proxy v1.TypedProxyConfig, _ int) bool {
			return proxy.GetBaseConfig().Name != typedProxyCfg.GetBaseConfig().Name
		})
		oldCfg.Proxies = append(oldCfg.Proxies, typedProxyCfg)

		if err := clientEntity.SetConfigContent(*oldCfg); err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot set client config, id: [%s]", clientID)
			return err
		}
	}

	rawCfg, err := clientEntity.MarshalJSONConfig()
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot marshal client config, id: [%s]", clientID)
		return err
	}

	// Locked variant: the caller already holds the (tenant, origin) key;
	// the locking route entry would deadlock this goroutine on its own key.
	// A failed/warned agent call MUST surface as an error (round-8 I3:
	// fire-and-forget SUCCESS let DB/agent drift go silent).
	_, err = client.UpdateFrpcHanderLocked(c, &pb.UpdateFRPCRequest{
		ClientId: &clientEntity.ClientID,
		ServerId: &serverID,
		Config:   rawCfg,
		Comment:  &clientEntity.Comment,
		FrpsUrl:  &clientEntity.FrpsUrl,
	})
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot update frpc failed, id: [%s]", clientID)
		return err
	}

	if existedProxyCfg != nil && existedProxyCfg.ServerID != serverID {
		logger.Logger(c).Warnf("client and server not match, delete old proxy, client: [%s], server: [%s], proxy: [%s]", clientID, serverID, proxyCfg.Name)
		// Locked variant: the caller already holds the key.
		if _, err := deleteProxyConfigLegacyLocked(c, &pb.DeleteProxyConfigRequest{
			ClientId: lo.ToPtr(existedProxyCfg.ClientID),
			ServerId: lo.ToPtr(existedProxyCfg.ServerID),
			Name:     &proxyCfg.Name,
		}); err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot delete old proxy, client: [%s], server: [%s], proxy: [%s]", clientID, clientEntity.ServerID, proxyCfg.Name)
			return err
		}
	}

	return nil
}
