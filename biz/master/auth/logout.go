package auth

import (
	"net/http"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/middleware"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/gin-gonic/gin"
)

func RemoveJWTHandler(appInstance app.Application) func(c *gin.Context) {
	return func(ctx *gin.Context) {
		cfg := appInstance.GetConfig()
		ctx.SetSameSite(http.SameSiteStrictMode)
		ctx.SetCookie(cfg.App.CookieName,
			"", -1,
			cfg.App.CookiePath,
			cfg.App.CookieDomain,
			cfg.App.CookieSecure,
			cfg.App.CookieHTTPOnly)
		// clear the double-submit CSRF cookie alongside the session
		ctx.SetSameSite(http.SameSiteStrictMode)
		ctx.SetCookie(middleware.CSRFCookieName(cfg.App.CookieName),
			"", -1,
			cfg.App.CookiePath,
			cfg.App.CookieDomain,
			cfg.App.CookieSecure,
			false)
		common.OKResp(ctx, &pb.CommonResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS,
			Message: "ok"}})
	}
}
