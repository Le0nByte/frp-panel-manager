package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(i * 3)
	}
	return key
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := testKey(t)
	plaintext := []byte(`{"serverAddr":"47.109.18.109","auth":{"token":"supersecret"}}`)

	ciphertext, nonce, sum, err := EncryptSnapshot(plaintext, key)
	require.NoError(t, err)
	require.NotEmpty(t, ciphertext)
	require.Len(t, nonce, NonceSize)
	require.NotEmpty(t, sum)

	// ciphertext must not leak plaintext
	assert.False(t, bytes.Contains(ciphertext, []byte("supersecret")))

	dec, err := DecryptSnapshot(ciphertext, nonce, key)
	require.NoError(t, err)
	assert.Equal(t, plaintext, dec)
}

func TestWrongKeyFails(t *testing.T) {
	key := testKey(t)
	wrong := make([]byte, KeySize)
	ciphertext, nonce, _, err := EncryptSnapshot([]byte("hello"), key)
	require.NoError(t, err)
	_, err = DecryptSnapshot(ciphertext, nonce, wrong)
	require.ErrorIs(t, err, ErrDecryptFailed)
}

func TestTamperedCiphertextFails(t *testing.T) {
	key := testKey(t)
	ciphertext, nonce, _, err := EncryptSnapshot([]byte("hello"), key)
	require.NoError(t, err)
	ciphertext[len(ciphertext)-1] ^= 0x01
	_, err = DecryptSnapshot(ciphertext, nonce, key)
	require.ErrorIs(t, err, ErrDecryptFailed)
}

func TestWrongNonceFails(t *testing.T) {
	key := testKey(t)
	ciphertext, nonce, _, err := EncryptSnapshot([]byte("hello"), key)
	require.NoError(t, err)
	badNonce := make([]byte, NonceSize)
	copy(badNonce, nonce)
	badNonce[0] ^= 0xff
	_, err = DecryptSnapshot(ciphertext, badNonce, key)
	require.ErrorIs(t, err, ErrDecryptFailed)
}

func TestKeySizeEnforced(t *testing.T) {
	_, _, _, err := EncryptSnapshot([]byte("x"), make([]byte, 16))
	require.ErrorIs(t, err, ErrInvalidKeySize)
	_, err = DecryptSnapshot([]byte("x"), make([]byte, NonceSize), make([]byte, 16))
	require.ErrorIs(t, err, ErrInvalidKeySize)
}

func TestLargeConfigPerformance(t *testing.T) {
	key := testKey(t)
	plaintext := bytes.Repeat([]byte("cfg-line\n"), 500_000) // ~4.5MB
	start := time.Now()
	ciphertext, nonce, _, err := EncryptSnapshot(plaintext, key)
	require.NoError(t, err)
	encDur := time.Since(start)

	start = time.Now()
	dec, err := DecryptSnapshot(ciphertext, nonce, key)
	require.NoError(t, err)
	decDur := time.Since(start)

	assert.Equal(t, plaintext, dec)
	t.Logf("encrypt %dms decrypt %dms for %d bytes", encDur.Milliseconds(), decDur.Milliseconds(), len(plaintext))
	assert.Less(t, decDur, 5*time.Second)
}

func TestFingerprintStable(t *testing.T) {
	key := testKey(t)
	fp1 := Fingerprint(key)
	fp2 := Fingerprint(key)
	assert.Equal(t, fp1, fp2)
	assert.Len(t, fp1, 64)
	assert.NotEqual(t, Fingerprint(make([]byte, KeySize)), fp1)
}

func TestLoadKeyFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.key")

	// exact 32 bytes
	key := testKey(t)
	require.NoError(t, os.WriteFile(path, key, 0o600))
	loaded, err := LoadKeyFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, key, loaded)

	// with trailing newline
	require.NoError(t, os.WriteFile(path, append(key, '\n'), 0o600))
	loaded, err = LoadKeyFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, key, loaded)

	// wrong size
	require.NoError(t, os.WriteFile(path, []byte("short"), 0o600))
	_, err = LoadKeyFromFile(path)
	require.ErrorIs(t, err, ErrInvalidKeySize)

	// missing file
	_, err = LoadKeyFromFile(filepath.Join(dir, "nope.key"))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "read snapshot key file"))
}
