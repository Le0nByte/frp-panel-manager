package conf

import (
	"errors"
	"strings"
)

const MinGlobalSecretLength = 32

// ValidateMasterSecurity rejects predictable or missing signing material before
// the Master opens any listener. The error never includes the secret value.
func ValidateMasterSecurity(cfg Config) error {
	if len(strings.TrimSpace(cfg.App.GlobalSecret)) < MinGlobalSecretLength {
		return errors.New("APP_GLOBAL_SECRET must be explicitly set to at least 32 characters")
	}
	return nil
}
