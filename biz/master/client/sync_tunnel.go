package client

import (
	"context"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
)

// SyncTunnel normalizes every client row of a user after their name or frps
// token changed.
//
// It used to WRITE the new identity (user + plaintext frps token) into
// clients.config_content. That is now both unnecessary and harmful:
//
//   - Unnecessary: every consumer injects the identity at use time through
//     utils.InjectFRPClientConfig — the agent pull path
//     (RPCPullConfig/pullableClientConfig, every 30s) and the transactional
//     apply path (biz/master/proxy.buildFrpcApplyConfig) both resolve the
//     OWNER's current user name and token from the users row. A rename
//     therefore reaches every agent within one pull cycle with no DB rewrite.
//   - Harmful: it left the frps auth token in plaintext at rest in a column
//     that get_client/list_client hand to the WebUI, violating the "no
//     credential at rest in the client row" invariant (spec §10.9). It also
//     REPLACED the whole metadatas map, silently destroying
//     x-vaala-frp-client-id on any row that had it.
//
// So this now STRIPS the identity instead: it clears the frp user and drops
// only the auth-token metadata key, leaving proxies, visitors, transport and
// every other metadata entry untouched. The result is the canonical candidate
// shape the coordinators persist, so historical rows that still carry a
// plaintext token get cleaned up the next time the owner's identity changes.
func SyncTunnel(ctx *app.Context, userInfo models.UserInfo) error {
	clis, err := dao.NewQuery(ctx).GetAllClients(userInfo)
	if err != nil {
		return err
	}
	lo.ForEach(clis, func(cli *models.ClientEntity, _ int) {
		cfg, err := cli.GetConfigContent()
		if err != nil {
			logger.Logger(context.Background()).WithError(err).Errorf("cannot get client config content, id: [%s]", cli.ClientID)
			return
		}

		// Identity is injected at use time; storage must not carry it.
		cfg.User = ""
		delete(cfg.Metadatas, defs.FRPAuthTokenKey)

		if err := cli.SetConfigContent(*cfg); err != nil {
			logger.Logger(context.Background()).WithError(err).Errorf("cannot set client config content, id: [%s]", cli.ClientID)
			return
		}

		if err := dao.NewMutation(ctx).UpdateClient(userInfo, cli); err != nil {
			logger.Logger(context.Background()).WithError(err).Errorf("cannot update client, id: [%s]", cli.ClientID)
			return
		}
		logger.Logger(ctx).Infof("normalized stored client config, id: [%s]", cli.ClientID)
	})
	return nil
}
