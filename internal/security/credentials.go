package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"flakedrop/internal/common"
	"github.com/zalando/go-keyring"
	"golang.org/x/crypto/pbkdf2"
)

const (
	// Keyring service name
	keyringService = "flakedrop"
	// Salt for key derivation
	saltSize = 32
	// Number of iterations for PBKDF2
	pbkdf2Iterations = 100000
	// Key size for AES-256
	keySize = 32
)

// CredentialManager handles secure storage and retrieval of credentials
type CredentialManager struct {
	useKeyring bool
	masterKey  []byte
}

// Credential represents a stored credential
type Credential struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Value     string            `json:"value"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Encrypted bool              `json:"encrypted"`
}

// NewCredentialManager creates a new credential manager
func NewCredentialManager() (*CredentialManager, error) {
	cm := &CredentialManager{
		useKeyring: isKeyringAvailable(),
	}

	// Initialize master key if not using system keyring
	if !cm.useKeyring {
		key, err := cm.getMasterKey()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize master key: %w", err)
		}
		cm.masterKey = key
	}

	return cm, nil
}

// StoreCredential securely stores a credential
func (cm *CredentialManager) StoreCredential(name, credType, value string, metadata map[string]string) error {
	if cm.useKeyring {
		return cm.storeInKeyring(name, credType, value, metadata)
	}
	return cm.storeEncrypted(name, credType, value, metadata)
}

// GetCredential retrieves a stored credential
func (cm *CredentialManager) GetCredential(name string) (*Credential, error) {
	if cm.useKeyring {
		return cm.getFromKeyring(name)
	}
	return cm.getEncrypted(name)
}

// DeleteCredential removes a stored credential
func (cm *CredentialManager) DeleteCredential(name string) error {
	if cm.useKeyring {
		return keyring.Delete(keyringService, name)
	}
	return cm.deleteEncrypted(name)
}

// ListCredentials returns a list of stored credential names
func (cm *CredentialManager) ListCredentials() ([]string, error) {
	if cm.useKeyring {
		// Keyring doesn't support listing, so we maintain a separate index
		return cm.getCredentialIndex()
	}
	return cm.listEncrypted()
}

// Keyring storage methods

func (cm *CredentialManager) storeInKeyring(name, credType, value string, metadata map[string]string) error {
	cred := Credential{
		Name:      name,
		Type:      credType,
		Value:     value,
		Metadata:  metadata,
		Encrypted: false,
	}

	data, err := json.Marshal(cred)
	if err != nil {
		return fmt.Errorf("failed to marshal credential: %w", err)
	}

	if err := keyring.Set(keyringService, name, string(data)); err != nil {
		return fmt.Errorf("failed to store in keyring: %w", err)
	}

	// Update index
	return cm.updateCredentialIndex(name, true)
}

func (cm *CredentialManager) getFromKeyring(name string) (*Credential, error) {
	data, err := keyring.Get(keyringService, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get from keyring: %w", err)
	}

	var cred Credential
	if err := json.Unmarshal([]byte(data), &cred); err != nil {
		return nil, fmt.Errorf("failed to unmarshal credential: %w", err)
	}

	return &cred, nil
}

// Encrypted file storage methods

func (cm *CredentialManager) storeEncrypted(name, credType, value string, metadata map[string]string) error {
	// Encrypt the value
	encrypted, err := cm.encrypt(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt credential: %w", err)
	}

	cred := Credential{
		Name:      name,
		Type:      credType,
		Value:     encrypted,
		Metadata:  metadata,
		Encrypted: true,
	}

	// Store in file
	return cm.saveCredentialFile(name, &cred)
}

func (cm *CredentialManager) getEncrypted(name string) (*Credential, error) {
	cred, err := cm.loadCredentialFile(name)
	if err != nil {
		return nil, err
	}

	if cred.Encrypted {
		decrypted, err := cm.decrypt(cred.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt credential: %w", err)
		}
		cred.Value = decrypted
		cred.Encrypted = false
	}

	return cred, nil
}

func (cm *CredentialManager) deleteEncrypted(name string) error {
	path := cm.getCredentialPath(name)
	return os.Remove(path)
}

func (cm *CredentialManager) listEncrypted() ([]string, error) {
	dir := cm.getCredentialsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".cred") {
			name := strings.TrimSuffix(entry.Name(), ".cred")
			names = append(names, name)
		}
	}

	return names, nil
}

// Encryption methods

func (cm *CredentialManager) encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(cm.masterKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (cm *CredentialManager) decrypt(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(cm.masterKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, encryptedData := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// Helper methods

func (cm *CredentialManager) getMasterKey() ([]byte, error) {
	keyPath := cm.getMasterKeyPath()

	// Validate path against credentials directory
	validatedPath, err := common.ValidatePath(keyPath, cm.getCredentialsDir())
	if err != nil {
		return nil, fmt.Errorf("invalid master key path: %w", err)
	}

	// Check if master key exists
	data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err == nil {
		// Extract the key part (skip the salt)
		if len(data) != saltSize+keySize {
			return nil, fmt.Errorf("invalid master key file size")
		}
		return data[saltSize:], nil
	}

	// Generate new master key
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	// Derive key from machine-specific data
	machineID := getMachineID()
	key := pbkdf2.Key([]byte(machineID), salt, pbkdf2Iterations, keySize, sha256.New)

	// Store salt and key together
	keyData := append(salt, key...)
	if err := os.MkdirAll(cm.getCredentialsDir(), 0700); err != nil {
		return nil, err
	}

	// Validate path before writing
	validatedWritePath, err := common.ValidatePath(keyPath, cm.getCredentialsDir())
	if err != nil {
		return nil, fmt.Errorf("invalid master key path for writing: %w", err)
	}

	if err := os.WriteFile(validatedWritePath, keyData, 0600); err != nil { // #nosec G304
		return nil, err
	}

	return key, nil
}

func (cm *CredentialManager) getCredentialsDir() string {
	home, _ := os.UserHomeDir()
	return fmt.Sprintf("%s/.flakedrop/credentials", home)
}

func (cm *CredentialManager) getCredentialPath(name string) string {
	return fmt.Sprintf("%s/%s.cred", cm.getCredentialsDir(), name)
}

func (cm *CredentialManager) getMasterKeyPath() string {
	return fmt.Sprintf("%s/.master", cm.getCredentialsDir())
}

func (cm *CredentialManager) saveCredentialFile(name string, cred *Credential) error {
	data, err := json.MarshalIndent(cred, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cm.getCredentialsDir(), 0700); err != nil {
		return err
	}

	path := cm.getCredentialPath(name)
	// Validate path against credentials directory
	validatedPath, err := common.ValidatePath(path, cm.getCredentialsDir())
	if err != nil {
		return fmt.Errorf("invalid credential file path: %w", err)
	}
	return os.WriteFile(validatedPath, data, 0600) // #nosec G304
}

func (cm *CredentialManager) loadCredentialFile(name string) (*Credential, error) {
	path := cm.getCredentialPath(name)
	// Validate path against credentials directory
	validatedPath, err := common.ValidatePath(path, cm.getCredentialsDir())
	if err != nil {
		return nil, fmt.Errorf("invalid credential file path: %w", err)
	}
	data, err := os.ReadFile(validatedPath) // #nosec G304
	if err != nil {
		return nil, err
	}

	var cred Credential
	if err := json.Unmarshal(data, &cred); err != nil {
		return nil, err
	}

	return &cred, nil
}

func (cm *CredentialManager) getCredentialIndex() ([]string, error) {
	indexPath := fmt.Sprintf("%s/.index", cm.getCredentialsDir())
	// Validate path against credentials directory
	validatedPath, err := common.ValidatePath(indexPath, cm.getCredentialsDir())
	if err != nil {
		return nil, fmt.Errorf("invalid index file path: %w", err)
	}
	data, err := os.ReadFile(validatedPath) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var index []string
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}

	return index, nil
}

func (cm *CredentialManager) updateCredentialIndex(name string, add bool) error {
	index, err := cm.getCredentialIndex()
	if err != nil {
		return err
	}

	// Update index
	found := false
	newIndex := []string{}
	for _, n := range index {
		if n == name {
			found = true
			if add {
				newIndex = append(newIndex, n)
			}
		} else {
			newIndex = append(newIndex, n)
		}
	}

	if add && !found {
		newIndex = append(newIndex, name)
	}

	// Save index
	data, err := json.Marshal(newIndex)
	if err != nil {
		return err
	}

	indexPath := fmt.Sprintf("%s/.index", cm.getCredentialsDir())
	// Validate path against credentials directory
	validatedPath, err := common.ValidatePath(indexPath, cm.getCredentialsDir())
	if err != nil {
		return fmt.Errorf("invalid index file path: %w", err)
	}
	return os.WriteFile(validatedPath, data, 0600) // #nosec G304
}

// Platform-specific helpers

func isKeyringAvailable() bool {
	// Check if keyring usage is explicitly disabled
	if os.Getenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN") == "false" {
		return false
	}
	
	switch runtime.GOOS {
	case "darwin", "windows":
		return true
	case "linux":
		// Check if a supported keyring backend is available
		if os.Getenv("DISPLAY") != "" || os.Getenv("WAYLAND_DISPLAY") != "" {
			return true
		}
	}
	return false
}

func getMachineID() string {
	// Get machine-specific identifier
	hostname, _ := os.Hostname()
	user := os.Getenv("USER")
	if user == "" {
		user = os.Getenv("USERNAME")
	}
	
	// Combine with other system info
	data := fmt.Sprintf("%s-%s-%s-%s", hostname, user, runtime.GOOS, runtime.GOARCH)
	hash := sha256.Sum256([]byte(data))
	return base64.StdEncoding.EncodeToString(hash[:])
}

// ExportCredentials exports all credentials (for backup)
func (cm *CredentialManager) ExportCredentials(password string) ([]byte, error) {
	names, err := cm.ListCredentials()
	if err != nil {
		return nil, err
	}

	credentials := make(map[string]*Credential)
	for _, name := range names {
		cred, err := cm.GetCredential(name)
		if err != nil {
			return nil, err
		}
		credentials[name] = cred
	}

	// Marshal credentials
	data, err := json.Marshal(credentials)
	if err != nil {
		return nil, err
	}

	// Encrypt with password
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	key := pbkdf2.Key([]byte(password), salt, pbkdf2Iterations, keySize, sha256.New)
	
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	
	// Prepend salt
	result := append(salt, ciphertext...)
	return result, nil
}

// ImportCredentials imports credentials from backup
func (cm *CredentialManager) ImportCredentials(data []byte, password string) error {
	if len(data) < saltSize {
		return fmt.Errorf("invalid backup data")
	}

	// Extract salt
	salt := data[:saltSize]
	ciphertext := data[saltSize:]

	// Derive key
	key := pbkdf2.Key([]byte(password), salt, pbkdf2Iterations, keySize, sha256.New)

	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt: invalid password or corrupted data")
	}

	// Unmarshal credentials
	var credentials map[string]*Credential
	if err := json.Unmarshal(plaintext, &credentials); err != nil {
		return err
	}

	// Import each credential
	for name, cred := range credentials {
		if err := cm.StoreCredential(name, cred.Type, cred.Value, cred.Metadata); err != nil {
			return fmt.Errorf("failed to import credential %s: %w", name, err)
		}
	}

	return nil
}