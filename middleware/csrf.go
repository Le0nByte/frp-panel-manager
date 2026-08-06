package middleware

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
)

// CSRFHeaderName is the response/request header carrying the double-submit
// CSRF token for cookie-authenticated clients.
const CSRFHeaderName = "X-CSRF-Token"

// CSRFCookieName derives the CSRF cookie name from the auth cookie name.
// The cookie is deliberately NOT HttpOnly so browser JS can read it and
// echo it back as the X-CSRF-Token header (double-submit pattern).
func CSRFCookieName(authCookieName string) string {
	return authCookieName + "-csrf"
}

// NewCSRFToken returns a fresh 256-bit, URL-safe random token.
func NewCSRFToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// IsStateChangingMethod reports whether the HTTP method mutates state and
// therefore needs CSRF protection for cookie-authenticated requests.
func IsStateChangingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// ValidateCSRF implements the double-submit check: the header value must be
// non-empty and equal to the cookie value, compared in constant time.
func ValidateCSRF(cookieValue, headerValue string) bool {
	if cookieValue == "" || headerValue == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cookieValue), []byte(headerValue)) == 1
}

// setCSRFCookie mints a fresh CSRF token, stores it in a SameSite=Strict,
// non-HttpOnly cookie (Secure follows the app config) and echoes it in the
// X-CSRF-Token response header for the client to persist.
func setCSRFCookie(c *gin.Context, cfg conf.Config) {
	csrfToken, err := NewCSRFToken()
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("generate csrf token failed")
		return
	}
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(CSRFCookieName(cfg.App.CookieName),
		csrfToken,
		cfg.App.CookieAge,
		cfg.App.CookiePath,
		cfg.App.CookieDomain,
		cfg.App.CookieSecure,
		false) // HttpOnly=false: JS must read it for the double-submit header
	c.Header(CSRFHeaderName, csrfToken)
}
