package platform

import (
	"net/http"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/gin-gonic/gin"
)

// GetVersion 返回当前 Master 二进制自身的版本信息。
//
// 与 /platform/baseinfo 不同，这里不经过 pb.Response 泛型约束：
// conf.VersionInfo 是普通结构体，可以直接塞进 Result.Body 走 JSON。
// 之所以不复用 GetPlatformInfoResponse，是因为那需要改 .proto 并重新
// 生成 Go / TypeScript 两套 pb 代码，代价远大于收益。
//
// 版本号由构建时 -ldflags "-X ...conf.gitVersion=<tag>" 注入；
// 未注入时 conf 里的默认值是 "dev-build"，前端据此显示为开发构建。
func GetVersion() func(*gin.Context) {
	return func(c *gin.Context) {
		c.Header(defs.TraceIDKey, c.GetString(defs.TraceIDKey))
		c.JSON(http.StatusOK, common.OK(defs.ReqSuccess).WithBody(conf.GetVersion()))
	}
}
