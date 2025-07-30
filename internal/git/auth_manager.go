package git

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/zalando/go-keyring"
	"flakedrop/pkg/errors"
)

// AuthManager handles Git authentication with multiple providers
type AuthManager struct {
	credStore     CredentialStore
	sshKeyManager *SSHKeyManager
	tokenManager  *TokenManager
	mu            sync.RWMutex
}

// NewAuthManager creates a new authentication manager
func NewAuthManager() *AuthManager {
	return &AuthManager{
		credStore:     NewCredentialStore(),
		sshKeyManager: NewSSHKeyManager(),
		tokenManager:  NewTokenManager(),
	}
}

// GetAuth returns the appropriate authentication method for a Git URL
func (am *AuthManager) GetAuth(gitURL string) (transport.AuthMethod, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	// Determine auth type based on URL
	if IsSSHURL(gitURL) {
		return am.getSSHAuth(gitURL)
	} else if IsHTTPSURL(gitURL) {
		return am.getHTTPSAuth(gitURL)
	}

	// Local repository, no auth needed
	return nil, nil
}

// getSSHAuth returns SSH authentication
func (am *AuthManager) getSSHAuth(gitURL string) (transport.AuthMethod, error) {
	// Try different SSH key sources in order of preference
	
	// 1. Check for URL-specific SSH key
	if auth := am.sshKeyManager.GetKeyForURL(gitURL); auth != nil {
		return auth, nil
	}

	// 2. Check for SSH agent
	if auth, err := ssh.NewSSHAgentAuth("git"); err == nil {
		return auth, nil
	}

	// 3. Check for default SSH keys
	defaultKeys := []string{
		filepath.Join(os.Getenv("HOME"), ".ssh", "id_ed25519"),
		filepath.Join(os.Getenv("HOME"), ".ssh", "id_rsa"),
		filepath.Join(os.Getenv("HOME"), ".ssh", "id_ecdsa"),
	}

	for _, keyPath := range defaultKeys {
		if _, err := os.Stat(keyPath); err == nil {
			auth, err := ssh.NewPublicKeysFromFile("git", keyPath, "")
			if err == nil {
				return auth, nil
			}
			
			// Key might be password protected
			if strings.Contains(err.Error(), "encrypted") {
				// Try to get passphrase from keyring
				passphrase, _ := am.getSSHKeyPassphrase(keyPath)
				if passphrase != "" {
					auth, err := ssh.NewPublicKeysFromFile("git", keyPath, passphrase)
					if err == nil {
						return auth, nil
					}
				}
			}
		}
	}

	return nil, errors.New(errors.ErrCodeAuthenticationFailed,
		"No SSH authentication method available").
		WithSuggestions(
			"Ensure SSH keys are properly configured",
			"Add your SSH key to the SSH agent with 'ssh-add'",
			"Check that your SSH key has access to the repository",
		)
}

// getHTTPSAuth returns HTTPS authentication
func (am *AuthManager) getHTTPSAuth(gitURL string) (transport.AuthMethod, error) {
	// Extract host from URL
	host := extractHost(gitURL)

	// 1. Check for stored credentials
	if creds := am.credStore.GetCredentials(host); creds != nil {
		return &http.BasicAuth{
			Username: creds.Username,
			Password: creds.Password,
		}, nil
	}

	// 2. Check for personal access tokens
	if token := am.tokenManager.GetToken(host); token != "" {
		return &http.BasicAuth{
			Username: "token",
			Password: token,
		}, nil
	}

	// 3. Check environment variables
	if auth := am.getAuthFromEnv(host); auth != nil {
		return auth, nil
	}

	// 4. Check git credential helper
	if auth := am.getAuthFromGitCredentialHelper(gitURL); auth != nil {
		return auth, nil
	}

	return nil, errors.New(errors.ErrCodeAuthenticationFailed,
		"No HTTPS authentication method available").
		WithContext("host", host).
		WithSuggestions(
			"Configure credentials with 'flakedrop git auth add'",
			"Set GITHUB_TOKEN or GIT_TOKEN environment variable",
			"Use SSH URL instead of HTTPS",
		)
}

// getAuthFromEnv checks environment variables for authentication
func (am *AuthManager) getAuthFromEnv(host string) transport.AuthMethod {
	// Check host-specific tokens first
	hostUpper := strings.ToUpper(strings.ReplaceAll(host, ".", "_"))
	token := os.Getenv(hostUpper + "_TOKEN")
	
	if token == "" {
		// Check generic tokens
		switch {
		case strings.Contains(host, "github"):
			token = os.Getenv("GITHUB_TOKEN")
		case strings.Contains(host, "gitlab"):
			token = os.Getenv("GITLAB_TOKEN")
		case strings.Contains(host, "bitbucket"):
			token = os.Getenv("BITBUCKET_TOKEN")
		default:
			token = os.Getenv("GIT_TOKEN")
		}
	}

	if token != "" {
		return &http.BasicAuth{
			Username: "token",
			Password: token,
		}
	}

	// Check for username/password
	username := os.Getenv("GIT_USERNAME")
	password := os.Getenv("GIT_PASSWORD")
	if username != "" && password != "" {
		return &http.BasicAuth{
			Username: username,
			Password: password,
		}
	}

	return nil
}

// getAuthFromGitCredentialHelper tries to get credentials from git credential helper
func (am *AuthManager) getAuthFromGitCredentialHelper(gitURL string) transport.AuthMethod {
	// This would integrate with git credential helper
	// Simplified implementation for now
	return nil
}

// StoreCredentials stores credentials for a host
func (am *AuthManager) StoreCredentials(host, username, password string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	return am.credStore.StoreCredentials(host, username, password)
}

// StoreToken stores a personal access token for a host
func (am *AuthManager) StoreToken(host, token string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	return am.tokenManager.StoreToken(host, token)
}

// RemoveCredentials removes stored credentials for a host
func (am *AuthManager) RemoveCredentials(host string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	return am.credStore.RemoveCredentials(host)
}

// getSSHKeyPassphrase retrieves SSH key passphrase from secure storage
func (am *AuthManager) getSSHKeyPassphrase(keyPath string) (string, error) {
	service := "snowflake-deploy-ssh"
	account := filepath.Base(keyPath)
	
	passphrase, err := keyring.Get(service, account)
	if err != nil {
		return "", err
	}
	
	return passphrase, nil
}

// GetAuthForRemote returns authentication for a specific remote URL
func (am *AuthManager) GetAuthForRemote(remoteURL string) transport.AuthMethod {
	auth, _ := am.GetAuth(remoteURL)
	return auth
}

// CredentialStore manages credential storage
type CredentialStore struct {
	configPath string
	mu         sync.RWMutex
}

// NewCredentialStore creates a new credential store
func NewCredentialStore() CredentialStore {
	homeDir, _ := os.UserHomeDir()
	return CredentialStore{
		configPath: filepath.Join(homeDir, ".flakedrop", "git-credentials.json"),
	}
}

// Credentials represents stored credentials
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// GetCredentials retrieves credentials for a host
func (cs *CredentialStore) GetCredentials(host string) *Credentials {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// Try to get from system keyring first
	service := "snowflake-deploy-git"
	if creds, err := keyring.Get(service, host); err == nil {
		// Credentials stored as JSON in keyring
		var c Credentials
		if err := json.Unmarshal([]byte(creds), &c); err == nil {
			return &c
		}
	}

	// Fall back to file storage (less secure)
	if data, err := os.ReadFile(cs.configPath); err == nil {
		var store map[string]Credentials
		if err := json.Unmarshal(data, &store); err == nil {
			if creds, ok := store[host]; ok {
				return &creds
			}
		}
	}

	return nil
}

// StoreCredentials stores credentials for a host
func (cs *CredentialStore) StoreCredentials(host, username, password string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	creds := Credentials{
		Username: username,
		Password: password,
	}

	// Try to store in system keyring (secure)
	service := "snowflake-deploy-git"
	if data, err := json.Marshal(creds); err == nil {
		if err := keyring.Set(service, host, string(data)); err == nil {
			return nil
		}
	}

	// Fall back to file storage with warning
	fmt.Fprintf(os.Stderr, "Warning: Storing credentials in file (less secure)\n")
	
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(cs.configPath), 0700); err != nil {
		return err
	}

	// Load existing store
	store := make(map[string]Credentials)
	if data, err := os.ReadFile(cs.configPath); err == nil {
		_ = json.Unmarshal(data, &store)
	}

	// Update store
	store[host] = creds

	// Write back
	if data, err := json.MarshalIndent(store, "", "  "); err == nil {
		return os.WriteFile(cs.configPath, data, 0600)
	}

	return errors.New(errors.ErrCodeInternal, "Failed to store credentials")
}

// RemoveCredentials removes credentials for a host
func (cs *CredentialStore) RemoveCredentials(host string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Try to remove from keyring
	service := "snowflake-deploy-git"
	_ = keyring.Delete(service, host)

	// Also remove from file storage
	if data, err := os.ReadFile(cs.configPath); err == nil {
		var store map[string]Credentials
		if err := json.Unmarshal(data, &store); err == nil {
			delete(store, host)
			if data, err := json.MarshalIndent(store, "", "  "); err == nil {
				return os.WriteFile(cs.configPath, data, 0600)
			}
		}
	}

	return nil
}

// SSHKeyManager manages SSH keys
type SSHKeyManager struct {
	keys map[string]transport.AuthMethod
	mu   sync.RWMutex
}

// NewSSHKeyManager creates a new SSH key manager
func NewSSHKeyManager() *SSHKeyManager {
	return &SSHKeyManager{
		keys: make(map[string]transport.AuthMethod),
	}
}

// GetKeyForURL returns SSH key for a specific URL
func (skm *SSHKeyManager) GetKeyForURL(gitURL string) transport.AuthMethod {
	skm.mu.RLock()
	defer skm.mu.RUnlock()

	// Extract host from URL
	host := extractHost(gitURL)
	return skm.keys[host]
}

// RegisterKey registers an SSH key for a host
func (skm *SSHKeyManager) RegisterKey(host string, keyPath string, passphrase string) error {
	skm.mu.Lock()
	defer skm.mu.Unlock()

	auth, err := ssh.NewPublicKeysFromFile("git", keyPath, passphrase)
	if err != nil {
		return err
	}

	skm.keys[host] = auth
	return nil
}

// TokenManager manages personal access tokens
type TokenManager struct {
	configPath string
	mu         sync.RWMutex
}

// NewTokenManager creates a new token manager
func NewTokenManager() *TokenManager {
	homeDir, _ := os.UserHomeDir()
	return &TokenManager{
		configPath: filepath.Join(homeDir, ".flakedrop", "git-tokens.json"),
	}
}

// GetToken retrieves a token for a host
func (tm *TokenManager) GetToken(host string) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// Try keyring first
	service := "snowflake-deploy-tokens"
	if token, err := keyring.Get(service, host); err == nil {
		return token
	}

	// Fall back to file
	if data, err := os.ReadFile(tm.configPath); err == nil {
		var tokens map[string]string
		if err := json.Unmarshal(data, &tokens); err == nil {
			return tokens[host]
		}
	}

	return ""
}

// StoreToken stores a token for a host
func (tm *TokenManager) StoreToken(host, token string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Try keyring first
	service := "snowflake-deploy-tokens"
	if err := keyring.Set(service, host, token); err == nil {
		return nil
	}

	// Fall back to file
	fmt.Fprintf(os.Stderr, "Warning: Storing token in file (less secure)\n")
	
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(tm.configPath), 0700); err != nil {
		return err
	}

	// Load existing tokens
	tokens := make(map[string]string)
	if data, err := os.ReadFile(tm.configPath); err == nil {
		_ = json.Unmarshal(data, &tokens)
	}

	// Update tokens
	tokens[host] = token

	// Write back
	if data, err := json.MarshalIndent(tokens, "", "  "); err == nil {
		return os.WriteFile(tm.configPath, data, 0600)
	}

	return errors.New(errors.ErrCodeInternal, "Failed to store token")
}

// GetSSHKeyManager returns the SSH key manager for external use
func (am *AuthManager) GetSSHKeyManager() *SSHKeyManager {
	return am.sshKeyManager
}

// extractHost extracts the host from a Git URL
func extractHost(gitURL string) string {
	// Remove protocol
	url := gitURL
	url = strings.TrimPrefix(url, "https://")
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "git@")
	url = strings.TrimPrefix(url, "ssh://")

	// Extract host part
	if idx := strings.Index(url, "/"); idx > 0 {
		url = url[:idx]
	}
	if idx := strings.Index(url, ":"); idx > 0 {
		url = url[:idx]
	}

	return url
}