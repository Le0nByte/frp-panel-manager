package master

import (
	"embed"
	"net/http"

	"github.com/VaalaCat/frp-panel/biz/master/audit"
	"github.com/VaalaCat/frp-panel/biz/master/auth"
	"github.com/VaalaCat/frp-panel/biz/master/client"
	"github.com/VaalaCat/frp-panel/biz/master/firewall"
	"github.com/VaalaCat/frp-panel/biz/master/platform"
	"github.com/VaalaCat/frp-panel/biz/master/port"
	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/biz/master/server"
	"github.com/VaalaCat/frp-panel/biz/master/shell"
	"github.com/VaalaCat/frp-panel/biz/master/streamlog"
	"github.com/VaalaCat/frp-panel/biz/master/user"
	"github.com/VaalaCat/frp-panel/biz/master/worker"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/middleware"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/gin-gonic/gin"

	wgHandler "github.com/VaalaCat/frp-panel/biz/master/wg"
)

func NewRouter(fs embed.FS, appInstance app.Application) *gin.Engine {
	router := gin.Default()
	HandleStaticFile(fs, router)
	ConfigureRouter(appInstance, router)
	return router
}

func ConfigureRouter(appInstance app.Application, router *gin.Engine) {
	router.POST("/auth", auth.MakeGinHandlerFunc(appInstance, auth.HandleLogin))

	api := router.Group("/api")
	api.POST("/v1/auth/cert", app.Wrapper(appInstance, auth.GetClientCert))
	api.POST("/v1/auth/login", app.Wrapper(appInstance, auth.LoginHandler))
	// I1-B: register creates state, so it is a write surface; login,
	// logout and cert are read-only by spec and stay ungated.
	api.POST("/v1/auth/register", middleware.RequireWritable(), app.Wrapper(appInstance, auth.RegisterHandler))
	api.POST("/v1/auth/logout", auth.RemoveJWTHandler(appInstance))

	// S5a L1: dangerous surfaces are only mounted while their feature flag
	// is on at startup (fail-closed default). Each gated route additionally
	// carries requireFeature so a runtime flip to off yields 403 (L2).
	masterCfg := appInstance.GetConfig()

	v1 := api.Group("/v1", middleware.JWTAuth(appInstance), middleware.AuthCtx(appInstance), middleware.RBAC(appInstance))
	{
		userRouter := v1.Group("/user")
		{
			userRouter.POST("/get", app.Wrapper(appInstance, user.GetUserInfoHandler))
			// I1-B write gate: mutations sit after JWT/AuthCtx/RBAC and
			// before the business handler.
			userRouter.POST("/update", middleware.RequireWritable(), app.Wrapper(appInstance, user.UpdateUserInfoHander))
			userRouter.POST("/sign-token", app.Wrapper(appInstance, user.SignTokenHandler))
		}
		platformRouter := v1.Group("/platform")
		{
			platformRouter.GET("/baseinfo", platform.GetPlatformInfo(appInstance))
			// 当前 Master 二进制自身的版本（前端在侧边栏底部展示）
			platformRouter.GET("/version", platform.GetVersion())
			platformRouter.POST("/clientsstatus", app.Wrapper(appInstance, platform.GetClientsStatus))
		}
		firewallRouter := v1.Group("/firewall")
		{
			// 2026-08-07: open a firewalld port when a tunnel is created
			// (control-plane write; gated by RequireWritable).
			firewallRouter.POST("/open", middleware.RequireWritable(), firewall.OpenFirewallPortHandler(appInstance))
		}
		clientRouter := v1.Group("/client")
		{
			clientRouter.POST("/get", app.Wrapper(appInstance, client.GetClientHandler))
			clientRouter.POST("/init", middleware.RequireWritable(), app.Wrapper(appInstance, client.InitClientHandler))
			clientRouter.POST("/delete", middleware.RequireWritable(), app.Wrapper(appInstance, client.DeleteClientHandler))
			clientRouter.POST("/list", app.Wrapper(appInstance, client.ListClientsHandler))
			// S5a: gate only this single route — never the whole clientRouter
			// (that would break /client/{get,init,delete,list,upgrade}).
			if masterCfg.Master.Features.EnableWorker {
				clientRouter.POST("/install_workerd", requireFeature(appInstance, func(c conf.Config) bool {
					return c.Master.Features.EnableWorker
				}), middleware.RequireWritable(), app.Wrapper(appInstance, worker.InstallWorkerd))
			}
			clientRouter.POST("/upgrade", middleware.RequireWritable(), app.Wrapper(appInstance, client.UpgradeFrppHandler))
		}
		serverRouter := v1.Group("/server")
		{
			serverRouter.POST("/get", app.Wrapper(appInstance, server.GetServerHandler))
			serverRouter.POST("/init", middleware.RequireWritable(), app.Wrapper(appInstance, server.InitServerHandler))
			serverRouter.POST("/delete", middleware.RequireWritable(), app.Wrapper(appInstance, server.DeleteServerHandler))
			serverRouter.POST("/list", app.Wrapper(appInstance, server.ListServersHandler))
		}
		frpcRouter := v1.Group("/frpc")
		{
			frpcRouter.POST("/update", middleware.RequireWritable(), app.Wrapper(appInstance, client.RejectLegacyFRPCUpdate))
			frpcRouter.POST("/delete", middleware.RequireWritable(), app.Wrapper(appInstance, client.RemoveFrpcHandler))
			frpcRouter.POST("/stop", middleware.RequireWritable(), app.Wrapper(appInstance, client.StopFRPCHandler))
			frpcRouter.POST("/start", middleware.RequireWritable(), app.Wrapper(appInstance, client.StartFRPCHandler))
		}
		frpsRouter := v1.Group("/frps")
		{
			frpsRouter.POST("/update", middleware.RequireWritable(), app.Wrapper(appInstance, server.UpdateFrpsHandlerSafe))
			frpsRouter.POST("/delete", middleware.RequireWritable(), app.Wrapper(appInstance, server.RemoveFrpsHandler))
		}
		proxyRouter := v1.Group("/proxy")
		{
			proxyRouter.POST("/preflight", app.Wrapper(appInstance, proxy.Preflight))
			proxyRouter.POST("/get_by_cid", app.Wrapper(appInstance, proxy.GetProxyStatsByClientID))
			proxyRouter.POST("/get_by_sid", app.Wrapper(appInstance, proxy.GetProxyStatsByServerID))
			proxyRouter.POST("/list_configs", app.Wrapper(appInstance, proxy.ListProxyConfigs))
			// I1-B: the five managed mutation entries carry the HTTP gate
			// AND their own L2 entry gates (readonly_gate_test.go); the
			// read surface (preflight/get/list_configs) stays ungated.
			proxyRouter.POST("/create_config", middleware.RequireWritable(), app.Wrapper(appInstance, proxy.CreateProxyConfig))
			proxyRouter.POST("/update_config", middleware.RequireWritable(), app.Wrapper(appInstance, proxy.UpdateProxyConfig))
			proxyRouter.POST("/delete_config", middleware.RequireWritable(), app.Wrapper(appInstance, proxy.DeleteProxyConfig))
			proxyRouter.POST("/get_config", app.Wrapper(appInstance, proxy.GetProxyConfig))
			proxyRouter.POST("/start_proxy", middleware.RequireWritable(), app.Wrapper(appInstance, proxy.StartProxy))
			proxyRouter.POST("/stop_proxy", middleware.RequireWritable(), app.Wrapper(appInstance, proxy.StopProxy))
		}
		portRouter := v1.Group("/port")
		{
			portRouter.POST("/list", app.Wrapper(appInstance, port.ListPorts))
		}
		auditRouter := v1.Group("/audit")
		{
			auditRouter.POST("/list", app.Wrapper(appInstance, audit.ListAudit))
			auditRouter.POST("/get", app.Wrapper(appInstance, audit.GetAudit))
			// D2c-C: manual rollback delegates to the transactional
			// rollback coordinator (transaction_rollback_coordinator.go);
			// the handler validates the boundary only.
			auditRouter.POST("/rollback", middleware.RequireWritable(), app.Wrapper(appInstance, audit.Rollback))
		}
		// S5a: the whole /worker group is gated on EnableWorker.
		if masterCfg.Master.Features.EnableWorker {
			workerHandler := v1.Group("/worker", requireFeature(appInstance, func(c conf.Config) bool {
				return c.Master.Features.EnableWorker
			}))
			{
				workerHandler.POST("/get", app.Wrapper(appInstance, worker.GetWorker))
				workerHandler.POST("/status", app.Wrapper(appInstance, worker.GetWorkerStatus))
				workerHandler.POST("/create", middleware.RequireWritable(), app.Wrapper(appInstance, worker.CreateWorker))
				workerHandler.POST("/list", app.Wrapper(appInstance, worker.ListWorkers))
				workerHandler.POST("/remove", middleware.RequireWritable(), app.Wrapper(appInstance, worker.RemoveWorker))
				workerHandler.POST("/update", middleware.RequireWritable(), app.Wrapper(appInstance, worker.UpdateWorker))
				workerHandler.POST("/redeploy", middleware.RequireWritable(), app.Wrapper(appInstance, worker.RedeployWorker))
				workerHandler.POST("/create_ingress", middleware.RequireWritable(), app.Wrapper(appInstance, worker.CreateWorkerIngress))
				workerHandler.POST("/get_ingress", app.Wrapper(appInstance, worker.GetWorkerIngress))
			}
		}

		// S5a: the whole /wg group is gated on EnableWireGuard.
		if masterCfg.Master.Features.EnableWireGuard {
			wgRouter := v1.Group("/wg", requireFeature(appInstance, func(c conf.Config) bool {
				return c.Master.Features.EnableWireGuard
			}))
			{
				// network
				wgRouter.POST("/network/create", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.CreateNetwork))
				wgRouter.POST("/network/delete", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.DeleteNetwork))
				wgRouter.POST("/network/update", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.UpdateNetwork))
				wgRouter.POST("/network/get", app.Wrapper(appInstance, wgHandler.GetNetwork))
				wgRouter.POST("/network/list", app.Wrapper(appInstance, wgHandler.ListNetworks))
				wgRouter.POST("/network/topology", app.Wrapper(appInstance, wgHandler.GetNetworkTopology))

				// endpoint
				wgRouter.POST("/endpoint/create", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.CreateEndpoint))
				wgRouter.POST("/endpoint/delete", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.DeleteEndpoint))
				wgRouter.POST("/endpoint/update", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.UpdateEndpoint))
				wgRouter.POST("/endpoint/get", app.Wrapper(appInstance, wgHandler.GetEndpoint))
				wgRouter.POST("/endpoint/list", app.Wrapper(appInstance, wgHandler.ListEndpoints))

				// link
				wgRouter.POST("/link/create", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.CreateWireGuardLink))
				wgRouter.POST("/link/delete", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.DeleteWireGuardLink))
				wgRouter.POST("/link/update", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.UpdateWireGuardLink))
				wgRouter.POST("/link/get", app.Wrapper(appInstance, wgHandler.GetWireGuardLink))
				wgRouter.POST("/link/list", app.Wrapper(appInstance, wgHandler.ListWireGuardLinks))

				// wireguard
				wgRouter.POST("/create", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.CreateWireGuard))
				wgRouter.POST("/delete", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.DeleteWireGuard))
				wgRouter.POST("/update", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.UpdateWireGuard))
				wgRouter.POST("/restart", middleware.RequireWritable(), app.Wrapper(appInstance, wgHandler.RestartWireGuard))
				wgRouter.POST("/get", app.Wrapper(appInstance, wgHandler.GetWireGuard))
				wgRouter.POST("/list", app.Wrapper(appInstance, wgHandler.ListWireGuards))
				wgRouter.POST("/runtime/get", app.Wrapper(appInstance, wgHandler.GetWireGuardRuntimeInfo))
			}
		}

		// S5a: the PTY endpoint is gated on EnablePTY; requireFeature runs
		// before shell.PTYHandler, i.e. before the websocket upgrade.
		if masterCfg.Master.Features.EnablePTY {
			v1.GET("/pty/:clientID", requireFeature(appInstance, func(c conf.Config) bool {
				return c.Master.Features.EnablePTY
			}), middleware.RequireWritable(), shell.PTYHandler(appInstance))
		}
		v1.GET("/log", streamlog.GetLogHandler(appInstance))
	}
}

// requireFeature is the S5a L2 request-time gate. It re-reads the live
// master config through appInstance.GetConfig() on every request: a feature
// flipped off after startup aborts with HTTP 403 before any business
// handler (or PTY websocket upgrade) runs. Routes are mounted at startup
// only while the feature is on (L1); this middleware is the runtime
// backstop for a config flip. Middleware ordering is unchanged: it sits
// after JWT/AuthCtx/RBAC and never touches the C5 SameOrigin checks.
func requireFeature(appInstance app.Application, enabled func(conf.Config) bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enabled(appInstance.GetConfig()) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "feature disabled"})
			return
		}
		c.Next()
	}
}
