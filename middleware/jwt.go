package middleware

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/cast"
)

// JWTMAuth check if authed and set uid to context
func JWTAuth(appInstance app.Application) func(c *gin.Context) {
	return func(c *gin.Context) {
		defer func() {
			logger.Logger(c).Debugf("finish jwt middleware")
		}()

		// spec §10.6: tokens must never travel in URLs. Any request carrying
		// ?token= is rejected outright — never parsed, never logged, never echoed.
		if _, ok := c.GetQuery(defs.TokenKey); ok {
			logger.Logger(c).Infof("query token authentication rejected")
			common.ErrUnAuthorized(c, "invalid authorization")
			c.Abort()
			return
		}

		// Authorization Bearer header takes priority over the auth cookie: the
		// web UI always sends the header (www/api/http.ts) while the browser
		// always sends the cookie, so a cookie-first order would preempt Bearer
		// and force CSRF on every UI write (review C1). A present header is
		// authoritative — valid → Bearer path (no CSRF, cookie ignored, resign
		// kept); invalid → 401, never falling back to the cookie (no ambiguity).
		if rawHeader := c.Request.Header.Get(defs.AuthorizationKey); rawHeader != "" {
			tokenStr := strings.TrimPrefix(rawHeader, "Bearer ")
			if tokenStr == "" || tokenStr == "null" {
				logger.Logger(c).Infof("authorization header token is empty")
				common.ErrUnAuthorized(c, "invalid authorization")
				c.Abort()
				return
			}
			t, err := utils.ParseToken(conf.JWTSecret(appInstance.GetConfig()), tokenStr)
			if err == nil {
				for k, v := range t {
					c.Set(k, v)
				}
				logger.Logger(c).Debugf("header auth success")
				if err = resignAndPatchCtxJWT(c, appInstance, cast.ToInt(t[defs.UserIDKey]), t, tokenStr); err != nil {
					logger.Logger(c).WithError(err).Errorf("resign jwt error")
					common.ErrUnAuthorized(c, "resign jwt error")
					c.Abort()
					return
				}
				c.Next()
				return
			}
			// §10.9: never log the token plaintext on parse failure.
			logger.Logger(c).WithError(err).Errorf("jwt middleware parse token error")
			common.ErrUnAuthorized(c, "invalid authorization")
			c.Abort()
			return
		}

		// No Authorization header → cookie path. Double-submit CSRF guards
		// state-changing methods; reads (GET/HEAD/OPTIONS) stay exempt.
		cookieToken, err := c.Cookie(appInstance.GetConfig().App.CookieName)
		if err == nil {
			if t, err := utils.ParseToken(conf.JWTSecret(appInstance.GetConfig()), cookieToken); err == nil {
				for k, v := range t {
					c.Set(k, v)
				}
				logger.Logger(c).Debugf("cookie auth success")
				// Double-submit CSRF: cookie-authenticated state changes must
				// carry X-CSRF-Token equal to the CSRF cookie. Bearer-header
				// auth (the web UI) and read methods are exempt.
				if IsStateChangingMethod(c.Request.Method) &&
					!ValidateCSRF(csrfCookieValue(c, appInstance), c.GetHeader(CSRFHeaderName)) {
					logger.Logger(c).Infof("csrf validation failed for cookie-authenticated write")
					common.ErrUnAuthorized(c, "invalid csrf token")
					c.Abort()
					return
				}
				if err = resignAndPatchCtxJWT(c, appInstance, cast.ToInt(t[defs.UserIDKey]), t, cookieToken); err != nil {
					logger.Logger(c).WithError(err).Errorf("resign jwt error")
					common.ErrUnAuthorized(c, "resign jwt error")
					c.Abort()
					return
				}
				c.Next()
				return
			}
			logger.Logger(c).WithError(err).Errorf("jwt middleware parse token error")
			common.ErrUnAuthorized(c, "invalid authorization")
			c.Abort()
			return
		}

		logger.Logger(c).WithError(errors.New("authorization is empty")).Infof("authorization is empty")
		common.ErrUnAuthorized(c, "invalid authorization")
		c.Abort()
	}
}

func resignAndPatchCtxJWT(c *gin.Context, appInstance app.Application, userID int, t jwt.MapClaims, tokenStr string) error {
	tokenExpire, _ := t.GetExpirationTime()
	now := time.Now().Add(time.Duration(appInstance.GetConfig().App.CookieAge/2) * time.Second)
	if now.Before(tokenExpire.Time) {
		logger.Logger(c).Debugf("jwt not going to expire, continue to use old one")
		c.Set(defs.TokenKey, tokenStr)
		return nil
	}

	tokenStr, err := SetToken(c, appInstance, userID, t)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("resign jwt error")
		return err
	}

	PushTokenStr(c, appInstance, tokenStr)

	logger.Logger(c).Debugf("jwt going to expire, resigning token")
	return nil
}

// csrfCookieValue returns the double-submit CSRF cookie value sent by the
// client, or "" when absent.
func csrfCookieValue(c *gin.Context, appInstance app.Application) string {
	v, err := c.Cookie(CSRFCookieName(appInstance.GetConfig().App.CookieName))
	if err != nil {
		return ""
	}
	return v
}

// SetToken 设置新token并写入ctx
func SetToken(c *gin.Context, appInstance app.Application, userID int, payload jwt.MapClaims) (string, error) {
	logger.Logger(c).Debugf("set token for userID:[%d]", userID)
	if payload == nil {
		payload = make(map[string]interface{})
	}

	payload[defs.UserIDKey] = userID

	token, err := conf.GetJWTWithPayload(appInstance.GetConfig(), userID, payload)
	if err != nil {
		return "", err
	}
	c.Set(defs.TokenKey, token)
	return token, nil
}

// PushTokenStr 推送token到客户端
func PushTokenStr(c *gin.Context, appInstance app.Application, tokenStr string) {
	logger.Logger(c).Debugf("push new token to client")
	cfg := appInstance.GetConfig()
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(cfg.App.CookieName,
		tokenStr,
		cfg.App.CookieAge,
		cfg.App.CookiePath,
		cfg.App.CookieDomain,
		cfg.App.CookieSecure,
		cfg.App.CookieHTTPOnly)
	c.Header(defs.SetAuthorizationKey, tokenStr)
	// mint a fresh double-submit CSRF pair on login/renewal
	setCSRFCookie(c, cfg)
}
