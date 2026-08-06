package middleware

import (
	"errors"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
)

func AuthCtx(appInstance app.Application) func(*gin.Context) {
	return func(c *gin.Context) {
		var err error
		var u *models.UserEntity
		appCtx := app.NewContext(c, appInstance)

		defer func() {
			logger.Logger(c).Debugf("finish auth user middleware")
		}()

		userID, err := cast.ToIntE(c.Value(defs.UserIDKey))
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("invalid user id: %v", c.Value(defs.UserIDKey))
			common.ErrUnAuthorized(c, "token invalid")
			c.Abort()
			return
		}

		u, err = dao.NewQuery(appCtx).GetUserByUserID(userID)
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("get user by user id failed, userID: [%d]", userID)
			common.ErrUnAuthorized(c, "token invalid")
			c.Abort()
			return
		}

		logger.Logger(c).Debugf("auth middleware authed user is: %s", utils.MarshalForJson(u.GetSafeUserInfo()))

		if !u.Valid() {
			logger.Logger(c).Errorf("invalid authorization, auth ctx middleware login failed")
			common.ErrUnAuthorized(c, "token invalid")
			c.Abort()
			return
		}

		// spec §10.7.8 session-version check: the login token carries the user's
		// session version; the local admin reset-password bumps the stored
		// version, so tokens minted before the reset (claim missing or stale)
		// are rejected here. Errors never leak claim values or tokens.
		if err := checkSessionVersion(c, u); err != nil {
			logger.Logger(c).WithError(err).Errorf("session version check failed, userID: [%d]", userID)
			common.ErrUnAuthorized(c, "token invalid")
			c.Abort()
			return
		}

		logger.Logger(c).Debugf("set auth user to context, login success")
		c.Set(defs.UserInfoKey, u)
		c.Next()
		return
	}
}

// checkSessionVersion enforces the per-user session version claim. Legacy
// tokens without the claim are honoured only while the user's effective
// version is still 1 (never reset); after a reset they are rejected exactly
// like stale claimed tokens. The returned error carries no claim values.
func checkSessionVersion(c *gin.Context, u *models.UserEntity) error {
	effective := u.EffectiveSessionVersion()
	claimRaw, present := c.Get(defs.SessionVersionKey)
	if !present {
		if effective <= 1 {
			return nil
		}
		return errors.New("session version claim missing after session reset")
	}
	if cast.ToInt64(claimRaw) == effective {
		return nil
	}
	return errors.New("session version claim mismatch")
}

func AuthAdmin(c *gin.Context) {
	u := common.GetUserInfo(c)
	if u == nil || u.GetRole() != defs.UserRole_Admin {
		common.ErrUnAuthorized(c, "permission denied")
		c.Abort()
		return
	}
	c.Next()
}
