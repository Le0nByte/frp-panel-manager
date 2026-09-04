package client

import (
	"errors"
	"net/url"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
	"github.com/tiendc/go-deepcopy"
	"gorm.io/gorm"
)

type ValidateableClientRequest interface {
	GetClientSecret() string
	GetClientId() string
}

// ValidateClientRequest validates a client ConnectSecret. When a Guard is
// carried by ctx (I3-B), every attempt is admission-gated and audited:
//
//   - nil request / empty id / empty secret: a real failure — recorded in
//     the guard's secret-failure accounting (node is the id if present)
//     and rejected with the typed dao.ErrInvalidClientSecret;
//   - the (peer,node) key lock is checked BEFORE any DAO access, so a
//     locked key is rejected without touching the database;
//   - DAO errors count as failures ONLY for the typed sentinel or
//     gorm.ErrRecordNotFound; transient DB errors (SQLite busy/closed,
//     timeouts, ...) never count and never lock;
//   - a successful validation calls RecordSecretSuccess, clearing any
//     earlier failure state.
//
// Without a guard the function keeps its legacy behavior but still
// returns the typed sentinel for nil/empty inputs.
func ValidateClientRequest(ctx *app.Context, req ValidateableClientRequest) (*models.ClientEntity, error) {
	guard := rpcguard.FromContext(ctx)
	peer := rpcguard.PeerFromContext(ctx)

	if req == nil {
		if guard != nil {
			guard.RecordSecretFailure(peer, "", rpcguard.LayerClient)
		}
		return nil, dao.ErrInvalidClientSecret
	}

	if req.GetClientId() == "" || req.GetClientSecret() == "" {
		if guard != nil {
			guard.RecordSecretFailure(peer, req.GetClientId(), rpcguard.LayerClient)
		}
		return nil, dao.ErrInvalidClientSecret
	}

	if guard != nil && !guard.AllowSecretAttempt(peer, req.GetClientId()) {
		guard.RecordSecretFailure(peer, req.GetClientId(), rpcguard.LayerClient)
		return nil, dao.ErrInvalidClientSecret
	}

	var (
		cli *models.ClientEntity
		err error
	)

	if cli, err = dao.NewQuery(ctx).ValidateClientSecret(req.GetClientId(), req.GetClientSecret()); err != nil {
		if guard != nil && isSecretFailure(err) {
			guard.RecordSecretFailure(peer, req.GetClientId(), rpcguard.LayerClient)
		}
		return nil, err
	}

	if guard != nil {
		guard.RecordSecretSuccess(peer, req.GetClientId())
	}

	return cli, nil
}

// isSecretFailure reports whether a DAO error represents a real
// ConnectSecret validation failure (the typed sentinel or a missing row).
// Anything else — transient DB errors in particular — never counts toward
// the guard's failure accounting.
func isSecretFailure(err error) bool {
	return errors.Is(err, dao.ErrInvalidClientSecret) || errors.Is(err, gorm.ErrRecordNotFound)
}

func MakeClientShadowed(c *app.Context, serverID string, clientEntity *models.ClientEntity) (*models.ClientEntity, error) {
	userInfo := common.GetUserInfo(c)
	m := dao.NewMutation(c)

	var clientID = clientEntity.ClientID
	var childClient *models.ClientEntity
	var err error
	if len(clientEntity.ConfigContent) != 0 {
		childClient, _, err = ChildClientForServer(c, serverID, clientEntity)
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot create child client, id: [%s]", clientID)
			return nil, err
		}

		if err := m.RebuildProxyConfigFromClient(userInfo, &models.Client{ClientEntity: childClient}); err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot rebuild proxy config from client, id: [%s]", childClient.ClientID)
			return nil, err
		}
	}

	clientEntity.IsShadow = true
	clientEntity.ConfigContent = nil
	clientEntity.ServerID = ""
	if err := m.UpdateClient(userInfo, clientEntity); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot update client, id: [%s]", clientID)
		return nil, err
	}

	if err := m.DeleteProxyConfigsByClientID(userInfo, clientID); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot delete proxy configs, id: [%s]", clientID)
		return nil, err
	}

	return childClient, nil
}

// ChildClientForServer 支持传入serverID和任意类型client，返回serverID对应的client in shadow，如果不存在则新建
// bool 表示是否新建
func ChildClientForServer(c *app.Context, serverID string, clientEntity *models.ClientEntity) (*models.ClientEntity, bool, error) {
	userInfo := common.GetUserInfo(c)
	q := dao.NewQuery(c)
	m := dao.NewMutation(c)

	originClientID := clientEntity.ClientID
	if len(clientEntity.OriginClientID) != 0 {
		originClientID = clientEntity.OriginClientID
	}

	existClient, err := q.GetClientByFilter(userInfo, &models.ClientEntity{
		ServerID:       serverID,
		OriginClientID: originClientID,
	}, lo.ToPtr(false))
	if err == nil {
		return existClient, false, nil
	}

	shadowCount, err := q.CountClientsInShadow(userInfo, originClientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot count shadow clients, id: [%s]", originClientID)
		return nil, false, err
	}

	copiedClient := &models.ClientEntity{}
	if err := deepcopy.Copy(copiedClient, clientEntity); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot copy client, id: [%s]", originClientID)
		return nil, false, err
	}
	copiedClient.ServerID = serverID
	copiedClient.ClientID = app.ShadowedClientID(originClientID, shadowCount+1)
	copiedClient.OriginClientID = originClientID
	copiedClient.IsShadow = false
	copiedClient.Stopped = false
	if err := m.CreateClient(userInfo, copiedClient); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot create child client, id: [%s]", copiedClient.ClientID)
		return nil, false, err
	}

	return copiedClient, true, nil
}

// ValidFrpsScheme is kept as a package-level alias for existing callers; the
// single implementation lives in utils so the write path (biz/master/proxy)
// and this read path cannot drift apart again.
func ValidFrpsScheme(scheme string) bool {
	return utils.ValidFrpsScheme(scheme)
}

// ValidateFrpsUrl delegates to utils.ValidateFrpsURL — one validator, one
// verdict for both the pull path and the transactional apply path. The write
// path used to carry a weaker private copy that only required a non-empty
// hostname, so the same stored frps_url was rejected here but pushed to the
// agent there (with protocol:"bogus" / port:0).
func ValidateFrpsUrl(urlStr string) (*url.URL, error) {
	return utils.ValidateFrpsURL(urlStr)
}
