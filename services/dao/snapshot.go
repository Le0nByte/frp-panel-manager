package dao

import (
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
)

type SnapshotQuery interface {
	FindByTransaction(userInfo models.UserInfo, txID string) ([]*models.ConfigSnapshot, error)
}

type SnapshotMutation interface {
	// SaveEncrypted encrypts plaintext with the given key and persists the
	// sealed snapshot bound to a transaction side (before/after).
	SaveEncrypted(userInfo models.UserInfo, txID, side, nodeType, nodeID string, plaintext, key []byte) (*models.ConfigSnapshot, error)
}

type snapshotQuery struct{ *queryImpl }
type snapshotMutation struct{ *mutationImpl }

func newSnapshotQuery(base *queryImpl) SnapshotQuery { return &snapshotQuery{base} }
func newSnapshotMutation(base *mutationImpl) SnapshotMutation {
	return &snapshotMutation{base}
}

func (q *snapshotQuery) FindByTransaction(userInfo models.UserInfo, txID string) ([]*models.ConfigSnapshot, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.ConfigSnapshot{}
	err := db.Where("transaction_id = ? AND tenant_id = ?", txID, userInfo.GetTenantID()).
		Order("id ASC").Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *snapshotMutation) SaveEncrypted(userInfo models.UserInfo, txID, side, nodeType, nodeID string, plaintext, key []byte) (*models.ConfigSnapshot, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	ciphertext, nonce, sum, err := snapshot.EncryptSnapshot(plaintext, key)
	if err != nil {
		return nil, err
	}
	snap := &models.ConfigSnapshot{
		TransactionID: txID,
		TenantID:      userInfo.GetTenantID(),
		Side:          side,
		NodeType:      nodeType,
		NodeID:        nodeID,
		Ciphertext:    ciphertext,
		Nonce:         nonce,
		SHA256:        sum,
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	if err := db.Create(snap).Error; err != nil {
		return nil, err
	}
	return snap, nil
}

// DecryptSnapshotForTest exposes snapshot decryption to DAO round-trip tests.
func DecryptSnapshotForTest(snap *models.ConfigSnapshot, key []byte) ([]byte, error) {
	return snapshot.DecryptSnapshot(snap.Ciphertext, snap.Nonce, key)
}
