package worker

import (
	"fmt"
	"strings"

	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func IngressName(worker *models.Worker, cli *models.ClientEntity) string {
	return fmt.Sprintf("ingress-%s-%s", strings.Split(worker.ID, "-")[0], cli.OriginClientID)
}

// CreateWorkerIngress is the live /create_ingress route entry. It locks at
// ITS OWN outermost entry because it calls proxy.GetClientWithMakeShadow,
// which WRITES during resolution (MakeClientShadowed / ChildClientForServer
// on a never-shadowed origin) — that write must run inside the (tenant,
// origin) keyed lock (the SAME originlock.Shared instance SafeCreateProxy
// holds), not after it. It then delegates to the lock-free
// CreateProxyConfigWithTypedConfigLocked; the locking entry would deadlock
// this goroutine on its own key (non-reentrant keyed mutex).
func CreateWorkerIngress(ctx *app.Context, req *pb.CreateWorkerIngressRequest) (*pb.CreateWorkerIngressResponse, error) {

	if err := validateCreateWorkerIngressRequest(req); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("invalid create worker ingress request, origin is: [%s]", req.String())
		return nil, err
	}

	originID, unlock, err := proxy.LockLegacyOrigin(ctx, req.GetClientId())
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot resolve origin client, id: [%s]", req.GetClientId())
		return nil, err
	}
	defer unlock()

	var (
		clientId = req.GetClientId()
		serverId = req.GetServerId()
		workerId = req.GetWorkerId()
		userInfo = common.GetUserInfo(ctx)
	)

	// child→origin normalization: the shadow machinery is keyed by the
	// ORIGIN id.
	clientEntity, err := proxy.GetClientWithMakeShadow(ctx, originID, serverId)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot get client, id: [%s]", clientId)
		return nil, err
	}

	_, err = dao.NewQuery(ctx).GetServerByServerID(userInfo, serverId)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot get server, id: [%s]", serverId)
		return nil, err
	}

	workerToExpose, err := dao.NewQuery(ctx).GetWorkerByWorkerID(userInfo, workerId)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot get worker, id: [%s]", workerId)
		return nil, err
	}

	httpProxyCfg := v1.HTTPProxyConfig{
		ProxyBaseConfig: v1.ProxyBaseConfig{
			Name: IngressName(workerToExpose, clientEntity),
			Type: string(v1.ProxyTypeHTTP),
			Annotations: map[string]string{
				defs.FrpProxyAnnotationsKey_Ingress:  "true",
				defs.FrpProxyAnnotationsKey_WorkerId: workerId,
			},
			ProxyBackend: v1.ProxyBackend{
				Plugin: v1.TypedClientPluginOptions{
					Type: v1.PluginUnixDomainSocket,
					ClientPluginOptions: &v1.UnixDomainSocketPluginOptions{
						Type:     v1.PluginUnixDomainSocket,
						UnixPath: fmt.Sprintf("@%s", strings.TrimPrefix(workerToExpose.Socket.Data.GetAddress(), "unix-abstract:")),
					},
				},
			},
		},
		DomainConfig: v1.DomainConfig{
			SubDomain: workerId,
		},
	}

	httpProxyCfg.LoadBalancer = v1.LoadBalancerConfig{
		Group:    models.HttpIngressLBGroup(workerId, &httpProxyCfg),
		GroupKey: workerId,
	}

	if err := proxy.CreateProxyConfigWithTypedConfigLocked(ctx, proxy.CreateProxyConfigWithTypedConfigParam{
		ClientID: clientId,
		ServerID: serverId,
		ProxyCfg: v1.TypedProxyConfig{
			Type:            string(v1.ProxyTypeHTTP),
			ProxyConfigurer: &httpProxyCfg,
		},
		ClientEntity: clientEntity,
		Overwrite:    true,
	}); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot create proxy config, client id: [%s], server id: [%s], worker id: [%s]", clientId, serverId, workerId)
		return nil, err
	}

	return &pb.CreateWorkerIngressResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

func validateCreateWorkerIngressRequest(req *pb.CreateWorkerIngressRequest) error {
	if req == nil {
		return fmt.Errorf("invalid request")
	}

	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetWorkerId()) == 0 {
		return fmt.Errorf("invalid request, client id: [%s], server id: [%s], worker id: [%s]", req.GetClientId(), req.GetServerId(), req.GetWorkerId())
	}

	return nil
}
