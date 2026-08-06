package middleware

import (
	"net/http"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/gin-gonic/gin"
)

// RequireWritable is the I1-B L2 write gate (spec §6.3.2): while the
// master is blocked in read-only mode — snapshot key fingerprint missing
// or mismatched — every mutation route answers HTTP 403 with the exact
// fixed body below, BEFORE its business handler (or PTY websocket
// upgrade) runs. The body is deliberately constant: it never carries the
// configured reason or any fingerprint material, so a blocked response
// leaks nothing about the key.
//
// Mounting order: after JWT/AuthCtx/RBAC, before the business handler.
// Read surfaces never mount it.
const writableGateMessage = "master is read-only: snapshot key fingerprint missing or mismatched"

func RequireWritable() gin.HandlerFunc {
	return func(c *gin.Context) {
		if readonly.Blocked() {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": writableGateMessage})
			return
		}
		c.Next()
	}
}
