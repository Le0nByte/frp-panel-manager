package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
)

var ErrLegacyFRPSUpdateDisabled = errors.New("legacy raw frps update endpoint is disabled; use the managed proxy API")

// RejectLegacyFRPSUpdate keeps the public route fail-closed. Raw FRPS JSON
// performs a DB-first, asynchronous Agent update and cannot satisfy the
// transaction/snapshot/ACK/rollback contract. Agent-side EVENT_UPDATE_FRPS
// handling remains available to managed internal flows.
func RejectLegacyFRPSUpdate(_ *app.Context, _ *pb.UpdateFRPSRequest) (*pb.UpdateFRPSResponse, error) {
	return &pb.UpdateFRPSResponse{
		Status: &pb.Status{
			Code:    pb.RespCode_RESP_CODE_INVALID,
			Message: "legacy raw frps update endpoint is disabled; use the managed proxy API",
		},
	}, ErrLegacyFRPSUpdateDisabled
}

// UpdateFrpsHandlerSafe is the managed replacement for the raw /frps/update
// route. Unlike the upstream legacy handler (DB-first + async fire-and-forget
// agent push without ACK), it:
//
//   - parses and validates the frps config (LoadServerConfig),
//   - sanity-checks bindPort and the injected auth plugin,
//   - fixes the upstream lo.Filter assignment bug (the multiuser plugin
//     filter result was discarded, so the plugin was never removed),
//   - persists the server row AFTER validation,
//   - records a tenant-scoped audit event (config bytes never logged),
//   - returns success only after the DB write is durable; the built-in
//     default server applies the new config through its pull loop, and
//     external frps agents apply it on their next pull as well.
//
// This satisfies spec §7.1 ("keep routes, but every write must be safe"):
// the route is no longer a raw JSON write, it is a validated, audited,
// persisted update. It is NOT a proxy transaction (no port reservation is
// involved — frps global parameters like bindPort/token have no proxy-level
// equivalent), but it closes the gap that otherwise made the default server
// unconfigurable and blocked tunnel creation.
func UpdateFrpsHandlerSafe(c *app.Context, req *pb.UpdateFRPSRequest) (*pb.UpdateFRPSResponse, error) {
	logger.Logger(c).Info(UpdateFRPSReqSummary(req.GetServerId(), req.GetConfig()))
	var (
		serverID  = req.GetServerId()
		configStr = req.GetConfig()
		userInfo  = common.GetUserInfo(c)
	)

	if len(configStr) == 0 || len(serverID) == 0 {
		return nil, fmt.Errorf("request invalid")
	}

	q := dao.NewQuery(c)
	srv, err := q.GetServerByServerID(userInfo, serverID)
	if srv == nil || err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot get server, id: [%s]", serverID)
		return nil, err
	}

	srvCfg, err := utils.LoadServerConfig(configStr, true)
	if srvCfg == nil || err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot load server config")
		return nil, err
	}
	if srvCfg.BindPort <= 0 || srvCfg.BindPort > 65535 {
		return nil, fmt.Errorf("invalid bindPort: %d", srvCfg.BindPort)
	}

	// Correct the upstream bug: the filter result was never assigned, so the
	// multiuser plugin could never be replaced on repeated updates.
	srvCfg.HTTPPlugins = lo.Filter(srvCfg.HTTPPlugins, func(item v1.HTTPPluginOptions, _ int) bool {
		return item.Name != defs.FRP_Plugin_Multiuser
	})
	srvCfg.HTTPPlugins = append(srvCfg.HTTPPlugins, conf.FRPsAuthOption(c.GetApp().GetConfig()))

	if err := srv.SetConfigContent(srvCfg); err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot set server config")
		return nil, err
	}

	srv.Comment = req.GetComment()
	if len(req.GetServerIp()) > 0 {
		srv.ServerIP = req.GetServerIp()
	}
	if len(req.GetFrpsUrls()) > 0 {
		srv.FrpsUrls = req.GetFrpsUrls()
	}

	if err := dao.NewMutation(c).UpdateServer(userInfo, srv); err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot update server, id: [%s]", serverID)
		return nil, err
	}

	// Audit: tenant/actor are forced from the authenticated user; the summary
	// carries only the server id and config size (spec §10.9 — never the raw
	// config bytes, which embed the FRP auth token).
	if err := dao.NewMutation(c).Log(userInfo, &models.AuditEvent{
		EventType: models.AuditEventTypeUpdate,
		Summary:   fmt.Sprintf("frps config updated: server_id=%s, config_size=%d bytes", serverID, len(configStr)),
		NodeID:    serverID,
		Success:   true,
	}); err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot write audit event for frps update, id: [%s]", serverID)
	}

	logger.Logger(c).Infof("update frps success, id: [%s]", serverID)
	return &pb.UpdateFRPSResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

// UpdateFRPSReqSummary returns a safe log summary of an update-request:
// only the server ID and the config byte length are included. The raw FRPS
// config bytes (which embed the FRP auth token) are never logged (spec
// §10.9). The signature is request-agnostic so sibling handlers (e.g.
// UpdateFRPC) can reuse it.
func UpdateFRPSReqSummary(serverID string, config []byte) string {
	return fmt.Sprintf("update frps, req: {server_id: %q, config_size: %d bytes}", serverID, len(config))
}
