package client

import (
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

// StripStoredCredentials removes the frp identity (frp user + plaintext frps
// auth token) from every stored client config.
//
// Why this exists: before the injector landed, the write paths
// (UpdateFrpcHanderLocked, SyncTunnel) persisted the INJECTED config into
// clients.config_content — leaving the frps auth token in plaintext at rest in
// a column get_client/list_client hand to the WebUI. Those writers now keep the
// candidate shape, but rows written earlier do NOT self-heal: they keep the
// credential until that client happens to pass through a legacy update or its
// owner's identity changes.
//
// This is a one-shot, idempotent startup pass. It is deliberately admin-wide
// (data hygiene, not a user-facing operation) and deliberately narrow: it
// clears ONLY cfg.User and ONLY the auth-token metadata key. Proxies,
// visitors, transport, the x-vaala-frp-client-id metadata and every other
// field are untouched — stripping a credential must never cost a tunnel.
//
// Failure policy: a row we cannot parse or cannot write is logged and SKIPPED,
// never fatal. A hygiene migration must not be able to block Master startup.
// Returns (scanned, cleaned) so the caller can log real numbers instead of
// claiming success blindly.
func StripStoredCredentials(ctx *app.Context) (scanned int, cleaned int, err error) {
	db := ctx.GetApp().GetDBManager().GetDefaultDB()

	var rows []*models.Client
	if err := db.Where("config_content IS NOT NULL AND length(config_content) > 0").
		Find(&rows).Error; err != nil {
		return 0, 0, err
	}

	for _, row := range rows {
		scanned++

		cfg, parseErr := row.GetConfigContent()
		if parseErr != nil {
			logger.Logger(ctx).WithError(parseErr).Warnf(
				"credential strip: cannot parse stored config, leaving row as-is, id: [%s]", row.ClientID)
			continue
		}

		_, hasToken := cfg.Metadatas[defs.FRPAuthTokenKey]
		if cfg.User == "" && !hasToken {
			continue // already canonical — do not rewrite
		}

		cfg.User = ""
		delete(cfg.Metadatas, defs.FRPAuthTokenKey)

		if setErr := row.SetConfigContent(*cfg); setErr != nil {
			logger.Logger(ctx).WithError(setErr).Warnf(
				"credential strip: cannot re-encode config, leaving row as-is, id: [%s]", row.ClientID)
			continue
		}
		if updErr := db.Model(&models.Client{}).
			Where("client_id = ?", row.ClientID).
			Update("config_content", row.ConfigContent).Error; updErr != nil {
			logger.Logger(ctx).WithError(updErr).Warnf(
				"credential strip: cannot persist stripped config, id: [%s]", row.ClientID)
			continue
		}
		cleaned++
		logger.Logger(ctx).Infof("credential strip: removed stored frp identity, id: [%s]", row.ClientID)
	}

	return scanned, cleaned, nil
}
