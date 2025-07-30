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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"gopkg.in/yaml.v3"
)

// SecureConfig handles encrypted configuration management
type SecureConfig struct {
	mu              sync.RWMutex
	configPath      string
	encryptionKey   []byte
	credManager     *CredentialManager
	auditLogger     *AuditLogger
	sensitiveFields []string
}

// ConfigData represents the configuration structure
type ConfigData struct {
	Version    string                 `json:"version" yaml:"version"`
	Encrypted  bool                   `json:"encrypted" yaml:"encrypted"`
	Data       map[string]interface{} `json:"data" yaml:"data"`
	Checksum   string                 `json:"checksum,omitempty" yaml:"checksum,omitempty"`
}

// NewSecureConfig creates a new secure configuration manager
func NewSecureConfig(configPath string, credManager *CredentialManager, auditLogger *AuditLogger) (*SecureConfig, error) {
	if configPath == "" {
		home, _ := os.UserHomeDir()
		configPath = filepath.Join(home, ".flakedrop", "secure-config")
	}

	// Ensure directory exists with proper permissions
	if err := os.MkdirAll(configPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	sc := &SecureConfig{
		configPath:  configPath,
		credManager: credManager,
		auditLogger: auditLogger,
		sensitiveFields: []string{
			"password",
			"token",
			"key",
			"secret",
			"credential",
			"private_key",
			"api_key",
		},
	}

	// Initialize encryption key
	if err := sc.initializeEncryptionKey(); err != nil {
		return nil, fmt.Errorf("failed to initialize encryption key: %w", err)
	}

	return sc, nil
}

// LoadConfig loads and decrypts configuration from file
func (sc *SecureConfig) LoadConfig(filename string) (*ConfigData, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	filepath := sc.getConfigPath(filename)
	
	// Validate the file path
	validatedPath, err := common.ValidatePath(filepath, sc.configPath)
	if err != nil {
		return nil, fmt.Errorf("invalid config file path: %w", err)
	}
	
	data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Determine format based on extension
	var config ConfigData
	ext := strings.ToLower(filepath[strings.LastIndex(filepath, "."):])
	
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON config: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal YAML config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config format: %s", ext)
	}

	// Decrypt if encrypted
	if config.Encrypted {
		if err := sc.decryptConfig(&config); err != nil {
			return nil, fmt.Errorf("failed to decrypt config: %w", err)
		}
	}

	// Verify checksum
	if config.Checksum != "" {
		if !sc.verifyChecksum(&config) {
			return nil, fmt.Errorf("config checksum verification failed")
		}
	}

	// Log config access
	_ = sc.auditLogger.LogAccess(ActionRead, "config:"+filename, "success", nil)

	return &config, nil
}

// SaveConfig encrypts and saves configuration to file
func (sc *SecureConfig) SaveConfig(filename string, config *ConfigData, encrypt bool) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Create a copy to avoid modifying the original
	configCopy := *config
	configCopy.Version = "1.0"
	configCopy.Encrypted = encrypt

	// Calculate checksum before encryption
	configCopy.Checksum = sc.calculateChecksum(&configCopy)

	// Encrypt sensitive fields if requested
	if encrypt {
		if err := sc.encryptConfig(&configCopy); err != nil {
			return fmt.Errorf("failed to encrypt config: %w", err)
		}
	}

	// Marshal based on extension
	filepath := sc.getConfigPath(filename)
	ext := strings.ToLower(filepath[strings.LastIndex(filepath, "."):])
	
	var data []byte
	var err error
	
	switch ext {
	case ".json":
		data, err = json.MarshalIndent(configCopy, "", "  ")
	case ".yaml", ".yml":
		data, err = yaml.Marshal(configCopy)
	default:
		return fmt.Errorf("unsupported config format: %s", ext)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write with secure permissions
	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	// Log config modification
	_ = sc.auditLogger.LogAccess(ActionUpdate, "config:"+filename, "success", 
		map[string]interface{}{"encrypted": encrypt})

	return nil
}

// GetSecureValue retrieves a value from config, decrypting if necessary
func (sc *SecureConfig) GetSecureValue(config *ConfigData, key string) (interface{}, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	value, exists := config.Data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Check if this is a credential reference
	if strVal, ok := value.(string); ok && strings.HasPrefix(strVal, "@credential:") {
		credName := strings.TrimPrefix(strVal, "@credential:")
		cred, err := sc.credManager.GetCredential(credName)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve credential: %w", err)
		}
		return cred.Value, nil
	}

	return value, nil
}

// SetSecureValue sets a value in config, encrypting if sensitive
func (sc *SecureConfig) SetSecureValue(config *ConfigData, key string, value interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if config.Data == nil {
		config.Data = make(map[string]interface{})
	}

	// Check if this is a sensitive field
	if sc.isSensitiveField(key) {
		// Store as credential reference
		credName := fmt.Sprintf("config-%s", key)
		strVal := fmt.Sprintf("%v", value)
		
		if err := sc.credManager.StoreCredential(credName, "config", strVal, 
			map[string]string{"config_key": key}); err != nil {
			return fmt.Errorf("failed to store credential: %w", err)
		}
		
		config.Data[key] = "@credential:" + credName
	} else {
		config.Data[key] = value
	}

	return nil
}

// ValidateConfig validates configuration against schema
func (sc *SecureConfig) ValidateConfig(config *ConfigData, schema map[string]interface{}) error {
	// Basic validation implementation
	// In production, this would use a proper schema validation library
	
	for key, schemaValue := range schema {
		if required, ok := schemaValue.(map[string]interface{})["required"].(bool); ok && required {
			if _, exists := config.Data[key]; !exists {
				return fmt.Errorf("required field missing: %s", key)
			}
		}
	}

	return nil
}

// MergeConfigs merges multiple configurations with precedence
func (sc *SecureConfig) MergeConfigs(configs ...*ConfigData) *ConfigData {
	merged := &ConfigData{
		Version: "1.0",
		Data:    make(map[string]interface{}),
	}

	for _, config := range configs {
		if config == nil {
			continue
		}
		
		for key, value := range config.Data {
			merged.Data[key] = value
		}
	}

	return merged
}

// BackupConfig creates an encrypted backup of configuration
func (sc *SecureConfig) BackupConfig(filename string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Read original config
	config, err := sc.LoadConfig(filename)
	if err != nil {
		return fmt.Errorf("failed to load config for backup: %w", err)
	}

	// Create backup filename
	timestamp := time.Now().Format("20060102-150405")
	backupName := fmt.Sprintf("%s.backup-%s", filename, timestamp)
	
	// Save encrypted backup
	if err := sc.SaveConfig(backupName, config, true); err != nil {
		return fmt.Errorf("failed to save backup: %w", err)
	}

	// Log backup creation
	_ = sc.auditLogger.LogEvent(EventTypeSecurity, "backup", "config:"+filename, "success",
		map[string]interface{}{"backup_file": backupName})

	return nil
}

// RestoreConfig restores configuration from backup
func (sc *SecureConfig) RestoreConfig(backupFile, targetFile string) error {
	// Load backup
	config, err := sc.LoadConfig(backupFile)
	if err != nil {
		return fmt.Errorf("failed to load backup: %w", err)
	}

	// Save to target
	if err := sc.SaveConfig(targetFile, config, config.Encrypted); err != nil {
		return fmt.Errorf("failed to restore config: %w", err)
	}

	// Log restore
	_ = sc.auditLogger.LogEvent(EventTypeSecurity, "restore", "config:"+targetFile, "success",
		map[string]interface{}{"backup_file": backupFile})

	return nil
}

// Helper methods

func (sc *SecureConfig) initializeEncryptionKey() error {
	keyPath := filepath.Join(sc.configPath, ".encryption-key")
	
	// Validate the key path
	validatedPath, err := common.ValidatePath(keyPath, sc.configPath)
	if err != nil {
		return fmt.Errorf("invalid encryption key path: %w", err)
	}
	
	// Check if key exists
	data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err == nil {
		sc.encryptionKey = data
		return nil
	}

	// Generate new key
	key := make([]byte, 32) // AES-256
	if _, err := rand.Read(key); err != nil {
		return err
	}

	// Store key securely
	if err := os.WriteFile(keyPath, key, 0600); err != nil {
		return err
	}

	sc.encryptionKey = key
	return nil
}

func (sc *SecureConfig) encryptConfig(config *ConfigData) error {
	// Encrypt sensitive fields
	for key, value := range config.Data {
		if sc.isSensitiveField(key) {
			encrypted, err := sc.encryptValue(fmt.Sprintf("%v", value))
			if err != nil {
				return err
			}
			config.Data[key] = encrypted
		}
	}

	return nil
}

func (sc *SecureConfig) decryptConfig(config *ConfigData) error {
	// Decrypt sensitive fields
	for key, value := range config.Data {
		if sc.isSensitiveField(key) {
			if strVal, ok := value.(string); ok {
				decrypted, err := sc.decryptValue(strVal)
				if err != nil {
					return err
				}
				config.Data[key] = decrypted
			}
		}
	}

	config.Encrypted = false
	return nil
}

func (sc *SecureConfig) encryptValue(value string) (string, error) {
	block, err := aes.NewCipher(sc.encryptionKey)
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

	ciphertext := gcm.Seal(nonce, nonce, []byte(value), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (sc *SecureConfig) decryptValue(encrypted string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(sc.encryptionKey)
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

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

func (sc *SecureConfig) calculateChecksum(config *ConfigData) string {
	// Create a copy without checksum for calculation
	temp := config.Checksum
	config.Checksum = ""
	
	data, _ := json.Marshal(config.Data)
	hash := sha256.Sum256(data)
	
	config.Checksum = temp
	return base64.StdEncoding.EncodeToString(hash[:])
}

func (sc *SecureConfig) verifyChecksum(config *ConfigData) bool {
	expected := config.Checksum
	actual := sc.calculateChecksum(config)
	return expected == actual
}

func (sc *SecureConfig) isSensitiveField(key string) bool {
	lowerKey := strings.ToLower(key)
	for _, field := range sc.sensitiveFields {
		if strings.Contains(lowerKey, field) {
			return true
		}
	}
	return false
}

func (sc *SecureConfig) getConfigPath(filename string) string {
	if filepath.IsAbs(filename) {
		return filename
	}
	return filepath.Join(sc.configPath, filename)
}

// AddSensitiveField adds a field pattern to be treated as sensitive
func (sc *SecureConfig) AddSensitiveField(field string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	sc.sensitiveFields = append(sc.sensitiveFields, strings.ToLower(field))
}

// GetConfigPath returns the base configuration path
func (sc *SecureConfig) GetConfigPath() string {
	return sc.configPath
}

// ListConfigs returns a list of available configuration files
func (sc *SecureConfig) ListConfigs() ([]string, error) {
	entries, err := os.ReadDir(sc.configPath)
	if err != nil {
		return nil, err
	}

	var configs []string
	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			configs = append(configs, entry.Name())
		}
	}

	return configs, nil
}