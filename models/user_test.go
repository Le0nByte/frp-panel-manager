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

// TestEffectiveSessionVersion pins the legacy-row rule (spec §10.7.8): rows
// written before the session-version feature carry 0 and count as version 1
// (the initial generation); a nil entity also counts as 1.
func TestEffectiveSessionVersion(t *testing.T) {
	require.Equal(t, int64(1), (*models.UserEntity)(nil).EffectiveSessionVersion())
	require.Equal(t, int64(1), (&models.UserEntity{SessionVersion: 0}).EffectiveSessionVersion())
	require.Equal(t, int64(1), (&models.UserEntity{SessionVersion: 1}).EffectiveSessionVersion())
	require.Equal(t, int64(2), (&models.UserEntity{SessionVersion: 2}).EffectiveSessionVersion())
	require.Equal(t, int64(9), (&models.UserEntity{SessionVersion: 9}).EffectiveSessionVersion())
}

// TestUserSessionVersionColumn: a fresh user must land on session version 1
// (the column default) — a zero-value insert must NOT write 0.
func TestUserSessionVersionColumn(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(uniqueDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.User{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   1,
		UserName: "admin",
		Email:    "admin@example.com",
		Role:     "admin",
	}}).Error)

	var loaded models.User
	require.NoError(t, db.First(&loaded, "user_id = ?", 1).Error)
	assert.Equal(t, int64(1), loaded.SessionVersion, "fresh user must default to session version 1")
}

// TestUserSessionVersionMigrationUpgradesExistingTable: AutoMigrate over a
// PRE-FEATURE users table (no session_version column) must add the column
// with DEFAULT 1, so existing installations upgrade in place and their rows
// count as version 1 (legacy tokens stay valid until the first reset).
func TestUserSessionVersionMigrationUpgradesExistingTable(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(uniqueDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	// pre-feature schema: exactly the columns the current model had before
	// session_version existed
	require.NoError(t, db.Exec(`CREATE TABLE users (
		user_id INTEGER PRIMARY KEY,
		user_name TEXT NOT NULL,
		password TEXT,
		email TEXT NOT NULL,
		status INTEGER DEFAULT 0,
		role TEXT,
		tenant_id INTEGER DEFAULT 0,
		token TEXT,
		created_at DATETIME,
		updated_at DATETIME,
		deleted_at DATETIME
	)`).Error)

	require.NoError(t, db.AutoMigrate(&models.User{}))

	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   1,
		UserName: "admin",
		Email:    "admin@example.com",
	}}).Error)

	var loaded models.User
	require.NoError(t, db.First(&loaded, "user_id = ?", 1).Error)
	assert.Equal(t, int64(1), loaded.SessionVersion, "migrated legacy rows must default to session version 1")
}

// TestAuditEventTypeResetPassword pins the reset event kind written by the
// local-only admin reset-password command (spec §10.7.8).
func TestAuditEventTypeResetPassword(t *testing.T) {
	if models.AuditEventTypeResetPassword != "admin.reset_password" {
		t.Fatalf("AuditEventTypeResetPassword = %q, want admin.reset_password", models.AuditEventTypeResetPassword)
	}
}
