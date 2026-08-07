package firewall

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
)

// OpenFirewallRequest is the JSON body of POST /api/v1/firewall/open.
type OpenFirewallRequest struct {
	ServerID string `json:"server_id"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

// runFirewallCmd executes firewall-cmd with a bounded context. It is a
// package variable so tests can inject a fake executor; the production
// implementation shells out to the system firewall (CentOS 7 / firewalld).
var runFirewallCmd = func(ctx context.Context, args ...string) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cmdCtx, "firewall-cmd", args...).CombinedOutput()
	return string(out), err
}

// OpenFirewallPortHandler opens a TCP/UDP port in the server's firewalld
// (permanent zone + reload). It is a control-plane write:
//
//   - validates the request shape (port 1..65535, protocol tcp/udp),
//   - verifies the target server exists and belongs to the caller,
//   - executes `firewall-cmd --permanent --add-port=<port>/<proto>` then
//     `firewall-cmd --reload`,
//   - writes an audit event (server_id/port/protocol only — never command
//     output),
//   - returns the standard {code,msg,body} envelope.
//
// The route is mounted behind RequireWritable (see handler.go).
func OpenFirewallPortHandler(appInstance app.Application) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req OpenFirewallRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, common.Err("invalid request body"))
			return
		}
		if req.Port < 1 || req.Port > 65535 {
			c.JSON(http.StatusBadRequest, common.Err(fmt.Sprintf("port must be in 1..65535, got %d", req.Port)))
			return
		}
		if req.Protocol != "tcp" && req.Protocol != "udp" {
			c.JSON(http.StatusBadRequest, common.Err(fmt.Sprintf("protocol must be tcp or udp, got %q", req.Protocol)))
			return
		}
		if req.ServerID == "" {
			c.JSON(http.StatusBadRequest, common.Err("server_id is required"))
			return
		}

		ctx := app.NewContext(c, appInstance)
		userInfo := common.GetUserInfo(c)

		srv, err := dao.NewQuery(ctx).GetServerByServerID(userInfo, req.ServerID)
		if err != nil || srv == nil {
			c.JSON(http.StatusNotFound, common.Err(fmt.Sprintf("server not found, id: [%s]", req.ServerID)))
			return
		}

		portSpec := fmt.Sprintf("%d/%s", req.Port, req.Protocol)
		if out, err := runFirewallCmd(c.Request.Context(), "--permanent", "--add-port="+portSpec); err != nil {
			msg := strings.TrimSpace(out)
			if msg == "" {
				msg = err.Error()
			}
			logger.Logger(ctx).WithError(err).Errorf("firewall open failed, server: [%s] port: [%s]", req.ServerID, portSpec)
			_ = dao.NewMutation(ctx).Log(userInfo, &models.AuditEvent{
				EventType: models.AuditEventTypeFirewall,
				Summary:   fmt.Sprintf("firewall open failed: server_id=%s port=%s", req.ServerID, portSpec),
				NodeID:    req.ServerID,
				Success:   false,
			})
			c.JSON(http.StatusOK, common.Err(fmt.Sprintf("firewall open failed: %s", msg)))
			return
		}
		if _, err := runFirewallCmd(c.Request.Context(), "--reload"); err != nil {
			logger.Logger(ctx).WithError(err).Errorf("firewall reload failed, server: [%s] port: [%s]", req.ServerID, portSpec)
			_ = dao.NewMutation(ctx).Log(userInfo, &models.AuditEvent{
				EventType: models.AuditEventTypeFirewall,
				Summary:   fmt.Sprintf("firewall reload failed after open: server_id=%s port=%s", req.ServerID, portSpec),
				NodeID:    req.ServerID,
				Success:   false,
			})
			c.JSON(http.StatusOK, common.Err("firewall rule added but reload failed, rule may not be active yet"))
			return
		}

		if err := dao.NewMutation(ctx).Log(userInfo, &models.AuditEvent{
			EventType: models.AuditEventTypeFirewall,
			Summary:   fmt.Sprintf("firewall open: server_id=%s port=%s", req.ServerID, portSpec),
			NodeID:    req.ServerID,
			Success:   true,
		}); err != nil {
			logger.Logger(ctx).WithError(err).Errorf("cannot write firewall audit event, server: [%s]", req.ServerID)
		}

		logger.Logger(ctx).Infof("firewall open success, server: [%s] port: [%s]", req.ServerID, portSpec)
		c.JSON(http.StatusOK, common.OK("success").WithBody(gin.H{"status": "ok", "port": portSpec}))
	}
}
