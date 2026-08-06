// Package snapshot provides AES-256-GCM encryption and SHA-256 integrity for
// config snapshots, so rollback evidence never stores plaintext secrets.
package snapshot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// KeySize is the required AES-256 key length in bytes.
const KeySize = 32

// NonceSize is the AES-GCM nonce length.
const NonceSize = 12

var (
	ErrInvalidKeySize = errors.New("snapshot key must be 32 bytes")
	ErrDecryptFailed  = errors.New("snapshot decrypt failed (wrong key, tampered ciphertext or bad nonce)")
)

// EncryptSnapshot seals plaintext with AES-GCM using a fresh random nonce and
// returns ciphertext, nonce and the hex SHA-256 of the plaintext.
func EncryptSnapshot(plaintext, key []byte) (ciphertext, nonce []byte, sha256sum string, err error) {
	if len(key) != KeySize {
		return nil, nil, "", ErrInvalidKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, "", err
	}
	nonce = make([]byte, NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, "", fmt.Errorf("generate nonce: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, "", err
	}
	ciphertext = aead.Seal(nil, nonce, plaintext, nil)
	sum := sha256.Sum256(plaintext)
	return ciphertext, nonce, hex.EncodeToString(sum[:]), nil
}

// DecryptSnapshot opens ciphertext with the given nonce and key.
func DecryptSnapshot(ciphertext, nonce, key []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, ErrInvalidKeySize
	}
	if len(nonce) != NonceSize {
		return nil, ErrDecryptFailed
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrDecryptFailed
	}
	return plaintext, nil
}

// Fingerprint returns a hex SHA-256 of the key itself. Stored in the database
// and deployment manifests to detect missing/mismatched keys without ever
// storing the key material.
func Fingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:])
}

// LoadKeyFromFile reads a 32-byte snapshot key from a root-only secret file
// (e.g. /opt/frp-panel/secrets/snapshot.key, mode 0600). The file content is
// expected to be exactly KeySize bytes.
func LoadKeyFromFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot key file: %w", err)
	}
	key := raw
	// tolerate a single trailing newline written by editors/echo
	if len(raw) == KeySize+1 && raw[len(raw)-1] == '\n' {
		key = raw[:KeySize]
	}
	if len(key) != KeySize {
		return nil, ErrInvalidKeySize
	}
	return key, nil
}
