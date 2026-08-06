package models

import (
	"time"
)

// SnapshotKeyFingerprintMetaKey is the system_metadata key that stores the
// hex SHA-256 fingerprint of the master snapshot encryption key (spec
// §6.3.2). Only the fingerprint is stored — never the key material — so
// startup can detect a missing or mismatched secret file and enter
// read-only mode without ever persisting secrets.
const SnapshotKeyFingerprintMetaKey = "snapshot_key_fingerprint"

// SystemMeta is a database-wide key/value store for system metadata rows:
// rows live in the shared database (visible to every master/worker
// process), not in any single process's memory. The natural string primary
// key keeps the table portable across SQLite, MySQL and PostgreSQL, and
// lets trust-on-first-use inserts use a dialect-neutral ON CONFLICT DO
// NOTHING via gorm's clause API.
type SystemMeta struct {
	Key       string `gorm:"primaryKey;size:64"`
	Value     string `gorm:"size:255"`
	UpdatedAt time.Time
}

func (*SystemMeta) TableName() string {
	return "system_metadata"
}
