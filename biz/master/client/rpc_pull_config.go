package client

import (
	"fmt"
	"sync"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// pullFailureLog throttles the pull-path fault log.
//
// A config the master cannot make dialable does NOT self-heal, and the agent
// re-pulls every defs.PullConfigDuration (30s). Logging every attempt at error
// level would emit ~2880 identical lines/day per broken client and bury the
// rest of the master's log (production baseline is ~790 lines/h total).
//
// So: the FIRST occurrence of a given failure for a given client logs at error
// level, identical repeats drop to debug, and a successful pull clears the
// memo so a NEW fault is loud again. Visibility is preserved (the error also
// travels to the agent over gRPC and is logged there), volume is not.
//
// Bounded: one short string per client id, dropped on a successful pull
// (clearPullFailure below) and on client deletion (DeleteClientHandler in
// delete_client.go), so a removed client cannot leave a permanent entry.
var pullFailureLog sync.Map // clientID -> last error string

func logPullFailure(ctx *app.Context, clientID string, err error) {
	msg := err.Error()
	prev, seen := pullFailureLog.Load(clientID)
	pullFailureLog.Store(clientID, msg)

	entry := logger.Logger(ctx).WithError(err)
	if seen && prev == msg {
		entry.Debugf("client config still not dialable (repeat), id: [%s]", clientID)
		return
	}
	entry.Errorf("cannot build a dialable config for client, refusing to serve an un-injected one, id: [%s]", clientID)
}

func clearPullFailure(clientID string) {
	pullFailureLog.Delete(clientID)
}

func RPCPullConfig(ctx *app.Context, req *pb.PullClientConfigReq) (*pb.PullClientConfigResp, error) {
	var (
		err       error
		cli       *models.ClientEntity
		clientIDs []string
	)

	if cli, err = ValidateClientRequest(ctx, req.GetBase()); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot validate client request")
		return nil, err
	}

	if err := dao.NewMutation(ctx).AdminUpdateClientLastSeen(cli.ClientID); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("update client last_seen_at time error, req:[%s] clientId:[%s]",
			req.String(), cli.ClientID)
	}

	mgr := ctx.GetApp().GetClientsManager()
	mgr.UpdateLastSeenAt(cli.ClientID)

	if cli.IsShadow {
		proxies, err := dao.NewQuery(ctx).AdminListProxyConfigsWithFilters(&models.ProxyConfigEntity{
			OriginClientID: cli.ClientID,
		})
		if err != nil {
			logger.Logger(ctx).Infof("cannot get client ids in shadow, maybe not a shadow client, id: [%s]", cli.ClientID)
		}
		clientIDs = lo.Map(proxies, func(p *models.ProxyConfig, _ int) string { return p.ClientID })
	}

	if cli.Stopped && cli.IsShadow {
		return &pb.PullClientConfigResp{
			Client: &pb.Client{
				Id:      lo.ToPtr(cli.ClientID),
				Stopped: lo.ToPtr(true),
			},
		}, nil
	}

	// A row that legitimately has no config yet (brand-new client, no tunnels)
	// answers with an empty config — the agent logs "config is empty, wait for
	// server init" and keeps whatever it is running.
	//
	// A row that HAS a config the master cannot make dialable is a real fault
	// and must NOT be masked as "empty": that reads identically to the healthy
	// brand-new case and would leave a broken client silently un-diagnosable.
	// Returning the error propagates it to the agent, which logs "cannot pull
	// client config", keeps its current handlers untouched, and retries on the
	// next tick — visible on both sides, destructive on neither.
	config := ""
	if len(cli.ConfigContent) > 0 {
		if config, err = pullableClientConfig(ctx, cli); err != nil {
			logPullFailure(ctx, cli.ClientID, err)
			return nil, err
		}
	}
	clearPullFailure(cli.ClientID)

	return &pb.PullClientConfigResp{
		Client: &pb.Client{
			Id:             lo.ToPtr(cli.ClientID),
			ServerId:       lo.ToPtr(cli.ServerID),
			Config:         lo.ToPtr(config),
			OriginClientId: lo.ToPtr(cli.OriginClientID),
			ClientIds:      clientIDs,
		},
	}, nil
}

// pullableClientConfig turns the STORED candidate config into the config the
// agent can actually dial with.
//
// Why this exists: the transactional coordinators persist the *candidate*
// config into clients.config_content (real proxies, but the connection
// identity still at frp's Complete() defaults — 0.0.0.0:7000, no user, no
// auth metadata) and push the *injected* config to the agent over RPC inside
// the transaction. That injected config only ever lived in the agent's
// memory: this pull loop (every defs.PullConfigDuration = 30s) used to hand
// the raw candidate back, so the agent overwrote its working config with an
// undialable one. Observed in production 2026-09-04 (client root.c.openWrt):
// the agent dialed 0.0.0.0:7000, reached a foreign unauthenticated frps, and
// exited within seconds because frp's loginFailExit defaults to true.
//
// This is a READ-side transform ONLY: nothing is written back, so the
// plaintext frps token never lands in the client row.
//
// Every failure is returned, never swallowed into an empty config — see the
// caller for why "empty" must stay reserved for "no config yet".
//
// It shares utils.InjectFRPClientConfig with the write path
// (biz/master/proxy.buildFrpcApplyConfig), so a config an agent PULLS is
// byte-compatible with the one a transaction PUSHES. That shared injector is
// what keeps the two paths from drifting again.
func pullableClientConfig(ctx *app.Context, cli *models.ClientEntity) (string, error) {
	// An agent RPC has NO JWT user in its context (it authenticates by client
	// secret), so the owning account must be resolved by id first.
	// common.GetUserInfo(ctx) is nil-safe but would return a nil UserInfo
	// here, which then nil-derefs on GetUserName/GetToken — and an ambient
	// JWT identity would be the wrong authority anyway: the injection must be
	// scoped to the client row's OWNER, not to whoever happens to be calling.
	q := dao.NewQuery(ctx)

	owner, err := q.GetUserByUserID(cli.UserID)
	if err != nil {
		return "", fmt.Errorf("resolve owner user %d: %w", cli.UserID, err)
	}
	if owner == nil {
		return "", fmt.Errorf("resolve owner user %d: no such user", cli.UserID)
	}

	// Tenant-scoped exactly like the write path: the resolved OWNER acts as
	// the UserInfo, so a client can only ever be injected with a server row
	// its own account may read (GetServerByServerID keeps the built-in
	// `default` server reachable for the admin account via its own branch).
	// AdminGetServerByServerID would skip that check and let a client whose
	// server_id points at another tenant's row pull that tenant's frps
	// address.
	srv, err := q.GetServerByServerID(owner, cli.ServerID)
	if err != nil {
		return "", fmt.Errorf("get server %q: %w", cli.ServerID, err)
	}
	if srv == nil || len(srv.ServerIP) == 0 || len(srv.ConfigContent) == 0 {
		return "", fmt.Errorf("server %q is not prepared", cli.ServerID)
	}
	srvConf, err := srv.GetConfigContent()
	if err != nil {
		return "", fmt.Errorf("read server %q config: %w", cli.ServerID, err)
	}
	if srvConf == nil {
		return "", fmt.Errorf("read server %q config: empty", cli.ServerID)
	}

	injection := utils.FRPClientInjection{
		ServerAddr:   srv.ServerIP,
		BindPort:     srvConf.BindPort,
		KCPBindPort:  srvConf.KCPBindPort,
		QUICBindPort: srvConf.QUICBindPort,
		User:         owner.GetUserName(),
		AuthToken:    owner.GetToken(),
		ClientID:     cli.ClientID,
	}

	if len(cli.FrpsUrl) > 0 {
		parsed, err := ValidateFrpsUrl(cli.FrpsUrl)
		if err != nil {
			return "", fmt.Errorf("invalid frps url on client row: %w", err)
		}
		injection.FrpsURLHost = parsed.Hostname()
		injection.FrpsURLPort = cast.ToInt(parsed.Port())
		injection.FrpsURLProtocol = parsed.Scheme
	}

	raw, err := utils.InjectFRPClientConfig(cli.ConfigContent, injection)
	if err != nil {
		return "", fmt.Errorf("inject client config: %w", err)
	}
	return string(raw), nil
}
