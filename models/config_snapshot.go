package models

import (
	"time"

	"gorm.io/gorm"
)

// Snapshot sides.
const (
	SnapshotSideBefore = "before"
	SnapshotSideAfter  = "after"
)

// Snapshot node types.
const (
	SnapshotNodeTypeClient = "client"
	SnapshotNodeTypeServer = "server"
	// SnapshotNodeTypeProxy identifies an encrypted typed-rule snapshot
	// (node_id = the proxy_config row id) — the reversibility evidence for
	// safe updates: before/after typed rules decrypt with the same key.
	SnapshotNodeTypeProxy = "proxy"
)

// ConfigSnapshot stores an AES-GCM encrypted copy of a node configuration so
// that one-click rollback never needs plaintext secrets in the audit trail.
type ConfigSnapshot struct {
	gorm.Model
	TransactionID string    `gorm:"index;size:64;not null"`
	TenantID      int       `gorm:"index"`
	Side          string    `gorm:"size:16;not null"` // before / after
	NodeType      string    `gorm:"size:16;not null"` // client / server
	NodeID        string    `gorm:"size:128;not null"`
	Ciphertext    []byte    `gorm:"not null"`
	Nonce         []byte    `gorm:"not null"`
	SHA256        string    `gorm:"size:64;not null"`
	CreatedAt     time.Time `gorm:"index"`
}

func (*ConfigSnapshot) TableName() string {
	return "config_snapshot"
}
