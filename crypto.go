package taxvault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// keyLen 是 AES-256 密钥长度。
const keyLen = 32

// errBadCiphertext 表示密文格式非法或校验失败。
var errBadCiphertext = errors.New("taxvault: invalid ciphertext")

// generateKey 生成一把独立的 AES-256 密钥，keyID 为随机十六进制串。
func generateKey() (keyID string, key []byte, err error) {
	key = make([]byte, keyLen)
	if _, err := rand.Read(key); err != nil {
		return "", nil, fmt.Errorf("generate key: %w", err)
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", nil, fmt.Errorf("generate key id: %w", err)
	}
	return hex.EncodeToString(idBytes), key, nil
}

// encrypt 使用 AES-256-GCM 加密明文，返回 nonce||ciphertext 自包含密文。
func encrypt(key, plaintext []byte) ([]byte, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+aead.Overhead())
	out = append(out, nonce...)
	out = aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

// decrypt 解密 encrypt 产出的密文。
func decrypt(key, ciphertext []byte) ([]byte, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(ciphertext) < ns+aead.Overhead() {
		return nil, errBadCiphertext
	}
	plain, err := aead.Open(nil, ciphertext[:ns], ciphertext[ns:], nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errBadCiphertext, err)
	}
	return plain, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != keyLen {
		return nil, fmt.Errorf("taxvault: key length %d, want %d", len(key), keyLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
