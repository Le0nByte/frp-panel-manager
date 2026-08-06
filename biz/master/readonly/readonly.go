// Package readonly owns the master's process-local read-only state and the
// snapshot-key fingerprint verification that feeds it (spec §6.3.2).
//
// Invariants:
//   - The singleton state defaults to unblocked, so the package is a pure
//     additive change for existing callers and tests.
//   - Reasons passed to Set must never contain key material or fingerprint
//     values: they are surfaced to operators and startup logs.
//   - VerifySnapshotKey never records the key, never logs it, and never
//     updates an existing fingerprint row: a mismatched key may only be
//     blocked, never promoted (overwriting would let a wrong key claim the
//     database).
package readonly

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrReadOnly is the sentinel returned by write paths when the master is
// blocked in read-only mode (L2 gates). Startup keeps the read APIs alive;
// only rule writes and rollbacks observe it.
var ErrReadOnly = errors.New("master is read-only: snapshot key fingerprint missing or mismatched")

// verifyBatchSize is the FindInBatches page size used to walk the FULL
// historical snapshot set before a new key may be trusted.
const verifyBatchSize = 200

// Blocked-state reasons. These are intentionally generic: they are shown
// to operators and written to logs, and must never embed the fingerprint
// or any key material.
const (
	reasonKeyMissing  = "snapshot key file is missing or invalid; master runs read-only"
	reasonKeyMismatch = "snapshot key fingerprint does not match the database; master runs read-only"
	reasonHistoryBad  = "snapshot key cannot decrypt the existing snapshots; master runs read-only"
)

var (
	stateMu      sync.RWMutex
	stateBlocked bool
	stateReason  string
)

// Set records the process-local read-only state. reason MUST NOT contain
// the key or the fingerprint (see package doc).
func Set(blocked bool, reason string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	stateBlocked = blocked
	stateReason = reason
}

// Blocked reports whether the master is currently in read-only mode.
func Blocked() bool {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return stateBlocked
}

// Reason returns the human-readable reason for the current state (empty
// when not blocked).
func Reason() string {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return stateReason
}

// ResetForTest restores the default unblocked state. Test-only: production
// startup must derive the state from VerifySnapshotKey, never from a reset.
func ResetForTest() {
	Set(false, "")
}

// VerifySnapshotKey loads the snapshot key file, compares its fingerprint
// against system_metadata and — when the master already owns historical
// snapshots — proves the current key can decrypt EVERY stored snapshot
// before trusting it.
//
// Return contract:
//   - (false, "", nil): key verified, master is writable.
//   - (true, reason, nil): key missing/invalid or mismatched; the master
//     may start in read-only mode and the caller should Set(true, reason).
//   - (true, "", err): a database query/transaction failed; the startup
//     layer decides whether to abort. Errors never contain key material
//     or ciphertext.
//
// Key material is never stored or logged; only the hex SHA-256 fingerprint
// (which is safe to record in deployment manifests) ever touches the DB.
func VerifySnapshotKey(db *gorm.DB, keyPath string) (blocked bool, reason string, err error) {
	key, loadErr := snapshot.LoadKeyFromFile(keyPath)
	if loadErr != nil {
		// A missing/unreadable key must never be TOFU'd into metadata:
		// block and let the master serve read-only instead.
		return true, reasonKeyMissing, nil
	}
	return VerifySnapshotKeyBytes(db, key)
}

// VerifySnapshotKeyBytes verifies the exact key bytes the caller will use.
// This closes a file-replacement TOCTOU window: production callers load the
// file once, verify these same bytes, then use these same bytes for crypto.
func VerifySnapshotKeyBytes(db *gorm.DB, key []byte) (blocked bool, reason string, err error) {
	if len(key) != snapshot.KeySize {
		return true, reasonKeyMissing, nil
	}
	fp := snapshot.Fingerprint(key)

	var meta models.SystemMeta
	err = db.First(&meta, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error
	switch {
	case err == nil:
		if meta.Value == fp {
			return false, "", nil
		}
		// Never overwrite: the stored fingerprint is the manifest truth.
		return true, reasonKeyMismatch, nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		// First sight: TOFU path below.
	default:
		return true, "", fmt.Errorf("read snapshot key fingerprint metadata: %w", err)
	}

	var count int64
	if err := db.Model(&models.ConfigSnapshot{}).Count(&count).Error; err != nil {
		return true, "", fmt.Errorf("count config snapshots: %w", err)
	}
	if count > 0 {
		// Historical evidence: the current key must prove it can decrypt
		// every stored snapshot — and every plaintext must hash to its
		// recorded SHA-256 — before its fingerprint may be recorded.
		if err := verifyHistoricalSnapshots(db, key); err != nil {
			return true, reasonHistoryBad, nil
		}
	}

	matched, err := tofuWriteFingerprint(db, fp)
	if err != nil {
		return true, "", fmt.Errorf("record snapshot key fingerprint: %w", err)
	}
	if !matched {
		// A concurrent writer recorded a different fingerprint first:
		// fail closed, never overwrite.
		return true, reasonKeyMismatch, nil
	}
	return false, "", nil
}

// SnapshotKeyFingerprintRecorded reports whether system_metadata already
// holds a snapshot_key_fingerprint row. The startup layer uses it to tell
// a first-run TOFU record from a restart against existing metadata, so it
// can emit a prominent generic warning on the former without ever printing
// the fingerprint. Read-only helper: it never writes and never logs.
func SnapshotKeyFingerprintRecorded(db *gorm.DB) (bool, error) {
	var count int64
	if err := db.Model(&models.SystemMeta{}).
		Where("key = ?", models.SnapshotKeyFingerprintMetaKey).
		Count(&count).Error; err != nil {
		return false, fmt.Errorf("count snapshot key fingerprint metadata: %w", err)
	}
	return count > 0, nil
}

// verifyHistoricalSnapshots decrypts every config_snapshot row with the
// current key and asserts each plaintext hashes to its stored SHA-256.
// Rows are walked in batches so arbitrarily large histories are covered
// without loading the whole table. Any failure aborts the walk and the
// caller blocks without touching system_metadata.
func verifyHistoricalSnapshots(db *gorm.DB, key []byte) error {
	rows := []models.ConfigSnapshot{}
	return db.Model(&models.ConfigSnapshot{}).
		Order("id ASC").
		FindInBatches(&rows, verifyBatchSize, func(tx *gorm.DB, batch int) error {
			for i := range rows {
				plaintext, err := snapshot.DecryptSnapshot(rows[i].Ciphertext, rows[i].Nonce, key)
				if err != nil {
					return err
				}
				sum := sha256.Sum256(plaintext)
				if hex.EncodeToString(sum[:]) != rows[i].SHA256 {
					return errSnapshotIntegrity
				}
			}
			return nil
		}).Error
}

var errSnapshotIntegrity = errors.New("snapshot plaintext SHA-256 does not match the stored value")

// tofuWriteFingerprint records the fingerprint trust-on-first-use style:
// inside one transaction it inserts the row with a dialect-neutral
// ON CONFLICT DO NOTHING (gorm clause, works on SQLite/MySQL/PostgreSQL)
// and re-reads the stored value. It reports false when a concurrent
// writer won the race with a different fingerprint — the caller must then
// block, never overwrite.
func tofuWriteFingerprint(db *gorm.DB, fp string) (matched bool, err error) {
	err = db.Transaction(func(tx *gorm.DB) error {
		row := models.SystemMeta{Key: models.SnapshotKeyFingerprintMetaKey, Value: fp}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			DoNothing: true,
		}).Create(&row).Error; err != nil {
			return err
		}
		var got models.SystemMeta
		if err := tx.First(&got, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error; err != nil {
			return err
		}
		matched = got.Value == fp
		return nil
	})
	return matched, err
}
