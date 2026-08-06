package proxy

import (
	"context"
	"fmt"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
)

// KeyProvider supplies the AES-256 snapshot encryption key (spec §6.3).
// Implementations must never log the key and must never fall back to a
// hardcoded default: a missing/unreadable key is a hard error so snapshots
// can never be written with a key that cannot be recovered from the secret
// store.
type KeyProvider interface {
	// SnapshotKey returns the 32-byte AES-256 key (snapshot.KeySize).
	SnapshotKey(ctx context.Context) ([]byte, error)
}

// FileKeyProvider reads the key from a root-only secret file on every call.
// The file must contain exactly snapshot.KeySize bytes (a single trailing
// newline is tolerated).
type FileKeyProvider struct {
	Path string
}

func (p *FileKeyProvider) SnapshotKey(_ context.Context) ([]byte, error) {
	if p == nil || p.Path == "" {
		return nil, fmt.Errorf("snapshot key file path is not configured")
	}
	return snapshot.LoadKeyFromFile(p.Path)
}

// ConfigKeyProvider is the production default: it resolves the key file path
// from the master configuration (MASTER_SNAPSHOT_KEY_FILE) at call time, so
// the provider stays correct regardless of when the app config is loaded.
//
// I1-B: after reading the file, every call re-verifies the key fingerprint
// against the system_metadata row via the app-context default DB using the
// exact bytes loaded above (readonly.VerifySnapshotKeyBytes). A key file that
// no longer matches the database is rejected with readonly.ErrReadOnly (fail closed — snapshots
// must never be written with a key the database has not seen), and a DB
// failure is wrapped without ever embedding the key or the fingerprint.
// I1-C: the Master startup path (cmd/frpp/shared) performs the same
// verification BEFORE recovery/listeners and drives the process read-only
// state (readonly.Set) from it; this provider stays the per-write
// fail-closed check on top of that startup decision. Rotation and the
// Bitwarden restore drill remain future work.
type ConfigKeyProvider struct{}

func (ConfigKeyProvider) SnapshotKey(ctx context.Context) ([]byte, error) {
	ac, ok := ctx.(*app.Context)
	if !ok || ac == nil || ac.GetApp() == nil {
		return nil, fmt.Errorf("snapshot key: expected an app context")
	}
	path := ac.GetApp().GetConfig().Master.SnapshotKeyFile
	if path == "" {
		return nil, fmt.Errorf("snapshot key file is not configured (MASTER_SNAPSHOT_KEY_FILE)")
	}
	key, err := snapshot.LoadKeyFromFile(path)
	if err != nil {
		return nil, err
	}

	dbm := ac.GetApp().GetDBManager()
	if dbm == nil {
		return nil, fmt.Errorf("snapshot key: database not available for fingerprint verification")
	}
	db := dbm.GetDefaultDB()
	if db == nil {
		return nil, fmt.Errorf("snapshot key: database not available for fingerprint verification")
	}
	blocked, _, err := readonly.VerifySnapshotKeyBytes(db, key)
	if err != nil {
		// DB errors never carry the key or the fingerprint (readonly
		// contract); the wrapper adds no secret material either.
		return nil, fmt.Errorf("snapshot key fingerprint verification failed: %w", err)
	}
	if blocked {
		return nil, readonly.ErrReadOnly
	}
	return key, nil
}

// MemoryKeyProvider is for tests only: a fixed caller-supplied key. It exists
// so test suites never depend on the filesystem or a config file.
type MemoryKeyProvider struct {
	Key []byte
}

func (p *MemoryKeyProvider) SnapshotKey(_ context.Context) ([]byte, error) {
	if p == nil || len(p.Key) != snapshot.KeySize {
		return nil, snapshot.ErrInvalidKeySize
	}
	return p.Key, nil
}
