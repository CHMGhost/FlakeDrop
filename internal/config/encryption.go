package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	
	"flakedrop/pkg/models"
)

const (
	encryptedPrefix = "ENC["
	encryptedSuffix = "]"
)

// getEncryptionKey derives an encryption key from environment or machine ID
func getEncryptionKey() []byte {
	// First check for explicit encryption key
	if key := os.Getenv("FLAKEDROP_ENCRYPTION_KEY"); key != "" {
		hash := sha256.Sum256([]byte(key))
		return hash[:]
	}
	
	// Fall back to machine-specific key
	// In production, this should use a proper key management service
	hostname, _ := os.Hostname()
	homeDir, _ := os.UserHomeDir()
	machineID := fmt.Sprintf("%s-%s-flakedrop", hostname, homeDir)
	hash := sha256.Sum256([]byte(machineID))
	return hash[:]
}

// EncryptPassword encrypts a password using AES-256-GCM
func EncryptPassword(password string) (string, error) {
	if password == "" {
		return "", nil
	}
	
	// Check if already encrypted
	if IsEncrypted(password) {
		return password, nil
	}
	
	key := getEncryptionKey()
	
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}
	
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}
	
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	
	ciphertext := gcm.Seal(nonce, nonce, []byte(password), nil)
	encoded := base64.StdEncoding.EncodeToString(ciphertext)
	
	return fmt.Sprintf("%s%s%s", encryptedPrefix, encoded, encryptedSuffix), nil
}

// DecryptPassword decrypts a password encrypted with EncryptPassword
func DecryptPassword(encrypted string) (string, error) {
	if encrypted == "" {
		return "", nil
	}
	
	// Check if not encrypted
	if !IsEncrypted(encrypted) {
		return encrypted, nil
	}
	
	// Remove prefix and suffix
	encoded := strings.TrimPrefix(encrypted, encryptedPrefix)
	encoded = strings.TrimSuffix(encoded, encryptedSuffix)
	
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("failed to decode encrypted password: %w", err)
	}
	
	key := getEncryptionKey()
	
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}
	
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}
	
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}
	
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt password: %w", err)
	}
	
	return string(plaintext), nil
}

// IsEncrypted checks if a string is encrypted
func IsEncrypted(value string) bool {
	return strings.HasPrefix(value, encryptedPrefix) && strings.HasSuffix(value, encryptedSuffix)
}

// EncryptConfigPasswords encrypts all passwords in a config
func EncryptConfigPasswords(config *models.Config) error {
	// Encrypt Snowflake password
	if config.Snowflake.Password != "" && !IsEncrypted(config.Snowflake.Password) {
		encrypted, err := EncryptPassword(config.Snowflake.Password)
		if err != nil {
			return fmt.Errorf("failed to encrypt Snowflake password: %w", err)
		}
		config.Snowflake.Password = encrypted
	}
	
	// Currently repositories don't have passwords, but keeping this extensible
	// for future use cases
	
	return nil
}

// DecryptConfigPasswords decrypts all passwords in a config
func DecryptConfigPasswords(config *models.Config) error {
	// Decrypt Snowflake password
	if IsEncrypted(config.Snowflake.Password) {
		decrypted, err := DecryptPassword(config.Snowflake.Password)
		if err != nil {
			return fmt.Errorf("failed to decrypt Snowflake password: %w", err)
		}
		config.Snowflake.Password = decrypted
	}
	
	// Currently repositories don't have passwords, but keeping this extensible
	// for future use cases
	
	return nil
}