package models_test

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestConfigSnapshotRoundTripFields(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(uniqueDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.ConfigSnapshot{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	snap := &models.ConfigSnapshot{
		TransactionID: "tx-1",
		Side:          "before",
		NodeType:      "client",
		NodeID:        "k30pro",
		Ciphertext:    []byte{0x01, 0x02, 0x03},
		Nonce:         []byte{0xaa, 0xbb},
		SHA256:        "deadbeef",
	}
	require.NoError(t, db.Create(snap).Error)

	var loaded models.ConfigSnapshot
	require.NoError(t, db.First(&loaded, "transaction_id = ? AND side = ?", "tx-1", "before").Error)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, loaded.Ciphertext)
	assert.Equal(t, "k30pro", loaded.NodeID)
	assert.Equal(t, "config_snapshot", (&models.ConfigSnapshot{}).TableName())
}
