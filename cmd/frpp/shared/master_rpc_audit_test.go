package shared

import (
	"context"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRPCAuditLoopStopDrainsPendingRows pins the production shutdown contract:
// stopping the loop must allow its final Flush to use a live writer context,
// persist the pending aggregate, and then return without leaking the goroutine.
func TestRPCAuditLoopStopDrainsPendingRows(t *testing.T) {
	db := masterSetupDB(t)
	require.NoError(t, db.AutoMigrate(&models.AuditEvent{}))
	dbm := masterDBManager(db, true)
	appCtx, _ := masterHarness(t, "", dbm)

	cfg := rpcguard.DefaultConfig()
	cfg.AuditFlushInterval = time.Hour // ensure only Stop triggers the flush
	guard := rpcguard.NewGuard(rpcguard.WithConfig(cfg))
	loop := startRPCAuditLoop(appCtx, guard)

	for i := 0; i < 100; i++ {
		guard.RecordSecretFailure("203.0.113.90:9101", "node-drain", rpcguard.LayerClient)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, loop.Stop(stopCtx))

	var rows []models.AuditEvent
	require.NoError(t, db.Where("event_type = ?", models.AuditEventTypeRPCSecretFailure).Find(&rows).Error)
	require.Len(t, rows, 1)
	assert.Zero(t, rows[0].TenantID)
	assert.Zero(t, rows[0].ActorUserID)
	assert.False(t, rows[0].Success)
	assert.Contains(t, rows[0].Summary, "count=100")
	assert.NotContains(t, rows[0].Summary, "secret=")

	// Stop is idempotent and must not try to drain a second time.
	require.NoError(t, loop.Stop(context.Background()))
}
