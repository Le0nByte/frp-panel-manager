package server

import (
	"errors"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"gorm.io/gorm"
)

type ValidateableServerRequest interface {
	GetServerSecret() string
	GetServerId() string
}

// ValidateServerRequest validates a server ConnectSecret. When a Guard is
// carried by ctx (I3-B), every attempt is admission-gated and audited:
//
//   - nil request / empty id / empty secret: a real failure — recorded in
//     the guard's secret-failure accounting (node is the id if present)
//     and rejected with the typed dao.ErrInvalidServerSecret;
//   - the (peer,node) key lock is checked BEFORE any DAO access, so a
//     locked key is rejected without touching the database;
//   - DAO errors count as failures ONLY for the typed sentinel or
//     gorm.ErrRecordNotFound; transient DB errors never count;
//   - a successful validation calls RecordSecretSuccess, clearing any
//     earlier failure state.
//
// Without a guard the function keeps its legacy behavior but still
// returns the typed sentinel for nil/empty inputs.
func ValidateServerRequest(ctx *app.Context, req ValidateableServerRequest) (*models.ServerEntity, error) {
	guard := rpcguard.FromContext(ctx)
	peer := rpcguard.PeerFromContext(ctx)

	if req == nil {
		if guard != nil {
			guard.RecordSecretFailure(peer, "", rpcguard.LayerServer)
		}
		return nil, dao.ErrInvalidServerSecret
	}

	if req.GetServerId() == "" || req.GetServerSecret() == "" {
		if guard != nil {
			guard.RecordSecretFailure(peer, req.GetServerId(), rpcguard.LayerServer)
		}
		return nil, dao.ErrInvalidServerSecret
	}

	if guard != nil && !guard.AllowSecretAttempt(peer, req.GetServerId()) {
		guard.RecordSecretFailure(peer, req.GetServerId(), rpcguard.LayerServer)
		return nil, dao.ErrInvalidServerSecret
	}

	var (
		cli *models.ServerEntity
		err error
	)

	if cli, err = dao.NewQuery(ctx).ValidateServerSecret(req.GetServerId(), req.GetServerSecret()); err != nil {
		if guard != nil && isSecretFailure(err) {
			guard.RecordSecretFailure(peer, req.GetServerId(), rpcguard.LayerServer)
		}
		return nil, err
	}

	if guard != nil {
		guard.RecordSecretSuccess(peer, req.GetServerId())
	}

	return cli, nil
}

// isSecretFailure reports whether a DAO error represents a real
// ConnectSecret validation failure (the typed sentinel or a missing row).
// Anything else — transient DB errors in particular — never counts toward
// the guard's failure accounting.
func isSecretFailure(err error) bool {
	return errors.Is(err, dao.ErrInvalidServerSecret) || errors.Is(err, gorm.ErrRecordNotFound)
}
