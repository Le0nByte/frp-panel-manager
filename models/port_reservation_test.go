package models_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// uniqueDSNSeq guarantees a fresh DSN per invocation, also across
// repeated runs of the same test (-count=N): t.Name() alone is stable
// between runs, so a shared-cache memory DB would leak rows from the
// previous run into the next one.
var uniqueDSNSeq atomic.Uint64

func uniqueDSN(t *testing.T) string {
	return fmt.Sprintf("file:model-test-%s-%d-%p?mode=memory&cache=shared", t.Name(), uniqueDSNSeq.Add(1), t)
}

func setupModelDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.PortReservation{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	// Close the underlying pool so the shared-cache memory database is
	// dropped and no connection leaks out of the test.
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestPortReservationUniqueConstraint(t *testing.T) {
	db := setupModelDB(t)
	r1 := &models.PortReservation{
		ServerID:      "aliyun-1",
		Protocol:      "tcp",
		RemotePort:    8000,
		TransactionID: "tx-1",
		Status:        models.ReservationStatusPending,
		ExpiresAt:     time.Now().Add(time.Minute),
	}
	require.NoError(t, db.Create(r1).Error)

	r2 := &models.PortReservation{
		ServerID:      "aliyun-1",
		Protocol:      "tcp",
		RemotePort:    8000,
		TransactionID: "tx-2",
		Status:        models.ReservationStatusPending,
		ExpiresAt:     time.Now().Add(time.Minute),
	}
	err := db.Create(r2).Error
	assert.Error(t, err, "expected unique constraint violation for (server_id, protocol, remote_port)")

	// same port, different protocol is allowed
	r3 := &models.PortReservation{
		ServerID:      "aliyun-1",
		Protocol:      "udp",
		RemotePort:    8000,
		TransactionID: "tx-3",
		Status:        models.ReservationStatusPending,
		ExpiresAt:     time.Now().Add(time.Minute),
	}
	require.NoError(t, db.Create(r3).Error)
}

func TestPortReservationIsExpired(t *testing.T) {
	now := time.Now()
	t.Run("pending past expiry is expired", func(t *testing.T) {
		r := &models.PortReservation{
			Status:    models.ReservationStatusPending,
			ExpiresAt: now.Add(-time.Second),
		}
		assert.True(t, r.IsExpired(now))
	})
	t.Run("pending future expiry not expired", func(t *testing.T) {
		r := &models.PortReservation{
			Status:    models.ReservationStatusPending,
			ExpiresAt: now.Add(time.Minute),
		}
		assert.False(t, r.IsExpired(now))
	})
	t.Run("committed never expires", func(t *testing.T) {
		r := &models.PortReservation{
			Status:    models.ReservationStatusCommitted,
			ExpiresAt: now.Add(-time.Hour),
		}
		assert.False(t, r.IsExpired(now))
	})
	t.Run("zero expiry not expired", func(t *testing.T) {
		r := &models.PortReservation{Status: models.ReservationStatusPending}
		assert.False(t, r.IsExpired(now))
	})
}

func TestPortReservationTableName(t *testing.T) {
	assert.Equal(t, "port_reservation", (&models.PortReservation{}).TableName())
}
