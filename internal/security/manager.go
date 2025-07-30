package security

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	
	"flakedrop/pkg/models"
)

// Manager provides a unified interface for all security features
type Manager struct {
	credManager   *CredentialManager
	auditLogger   *AuditLogger
	scanner       *Scanner
	accessControl *AccessControl
	secureConfig  *SecureConfig
	initialized   bool
}

// NewManager creates a new security manager
func NewManager() (*Manager, error) {
	home, _ := os.UserHomeDir()
	basePath := filepath.Join(home, ".flakedrop")

	// Initialize audit logger first
	auditLogger, err := NewAuditLogger(filepath.Join(basePath, "audit"))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize audit logger: %w", err)
	}

	// Initialize credential manager
	credManager, err := NewCredentialManager()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize credential manager: %w", err)
	}

	// Initialize scanner
	scanner := NewScanner()

	// Initialize access control
	accessControl, err := NewAccessControl(filepath.Join(basePath, "access"), auditLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize access control: %w", err)
	}

	// Initialize secure config
	secureConfig, err := NewSecureConfig(filepath.Join(basePath, "secure-config"), credManager, auditLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize secure config: %w", err)
	}

	manager := &Manager{
		credManager:   credManager,
		auditLogger:   auditLogger,
		scanner:       scanner,
		accessControl: accessControl,
		secureConfig:  secureConfig,
		initialized:   true,
	}

	// Log initialization
	_ = auditLogger.LogEvent(EventTypeSecurity, "initialize", "security_manager", "success", nil)

	return manager, nil
}

// GetCredentialManager returns the credential manager
func (m *Manager) GetCredentialManager() *CredentialManager {
	return m.credManager
}

// GetAuditLogger returns the audit logger
func (m *Manager) GetAuditLogger() *AuditLogger {
	return m.auditLogger
}

// GetScanner returns the security scanner
func (m *Manager) GetScanner() *Scanner {
	return m.scanner
}

// GetAccessControl returns the access control manager
func (m *Manager) GetAccessControl() *AccessControl {
	return m.accessControl
}

// GetSecureConfig returns the secure configuration manager
func (m *Manager) GetSecureConfig() *SecureConfig {
	return m.secureConfig
}

// SecureSnowflakeConfig secures Snowflake configuration
func (m *Manager) SecureSnowflakeConfig(config *models.Snowflake) error {
	if !m.initialized {
		return fmt.Errorf("security manager not initialized")
	}

	// Store password securely
	if config.Password != "" {
		credName := fmt.Sprintf("snowflake-%s", config.Account)
		if err := m.credManager.StoreCredential(
			credName,
			"snowflake-password",
			config.Password,
			map[string]string{
				"account":  config.Account,
				"username": config.Username,
			},
		); err != nil {
			return fmt.Errorf("failed to store password: %w", err)
		}

		// Replace password with credential reference
		config.Password = "@credential:" + credName

		// Log the action
		_ = m.auditLogger.LogSecurityEvent(
			"secure_credential",
			"snowflake_password",
			"success",
			map[string]interface{}{
				"account": config.Account,
				"user":    config.Username,
			},
		)
	}

	return nil
}

// GetSnowflakePassword retrieves the Snowflake password
func (m *Manager) GetSnowflakePassword(config *models.Snowflake) (string, error) {
	if !m.initialized {
		return "", fmt.Errorf("security manager not initialized")
	}

	// Check if password is a credential reference
	if strings.HasPrefix(config.Password, "@credential:") {
		credName := strings.TrimPrefix(config.Password, "@credential:")
		cred, err := m.credManager.GetCredential(credName)
		if err != nil {
			return "", fmt.Errorf("failed to retrieve password: %w", err)
		}

		// Log access
		_ = m.auditLogger.LogAccess(
			ActionRead,
			"credential:snowflake_password",
			"success",
			map[string]interface{}{
				"account": config.Account,
			},
		)

		return cred.Value, nil
	}

	// Return password as-is (for backward compatibility)
	return config.Password, nil
}

// SecureLicenseKey secures the license key
func (m *Manager) SecureLicenseKey(license *models.License) error {
	if !m.initialized {
		return fmt.Errorf("security manager not initialized")
	}

	if license.Key != "" && !strings.HasPrefix(license.Key, "@credential:") {
		if err := m.credManager.StoreCredential(
			"license-key",
			"license",
			license.Key,
			map[string]string{
				"expires_at": license.ExpiresAt,
			},
		); err != nil {
			return fmt.Errorf("failed to store license key: %w", err)
		}

		license.Key = "@credential:license-key"

		_ = m.auditLogger.LogSecurityEvent(
			"secure_credential",
			"license_key",
			"success",
			nil,
		)
	}

	return nil
}

// GetLicenseKey retrieves the license key
func (m *Manager) GetLicenseKey(license *models.License) (string, error) {
	if !m.initialized {
		return "", fmt.Errorf("security manager not initialized")
	}

	if strings.HasPrefix(license.Key, "@credential:") {
		credName := strings.TrimPrefix(license.Key, "@credential:")
		cred, err := m.credManager.GetCredential(credName)
		if err != nil {
			return "", fmt.Errorf("failed to retrieve license key: %w", err)
		}

		_ = m.auditLogger.LogAccess(
			ActionRead,
			"credential:license_key",
			"success",
			nil,
		)

		return cred.Value, nil
	}

	return license.Key, nil
}

// ScanConfiguration performs security scan on configuration
func (m *Manager) ScanConfiguration(config *models.Config) (*ScanReport, error) {
	if !m.initialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	// Convert config to map for scanning
	configMap := map[string]interface{}{
		"snowflake": map[string]interface{}{
			"account":   config.Snowflake.Account,
			"username":  config.Snowflake.Username,
			"password":  config.Snowflake.Password,
			"role":      config.Snowflake.Role,
			"warehouse": config.Snowflake.Warehouse,
		},
		"repositories": config.Repositories,
		"license": map[string]interface{}{
			"key":        config.License.Key,
			"expires_at": config.License.ExpiresAt,
		},
	}

	report, err := m.scanner.ScanConfig(configMap)
	if err != nil {
		return nil, fmt.Errorf("security scan failed: %w", err)
	}

	// Log scan
	_ = m.auditLogger.LogSecurityEvent(
		"security_scan",
		"configuration",
		"completed",
		map[string]interface{}{
			"findings_count": len(report.Findings),
			"critical_count": report.Summary.Critical,
			"high_count":     report.Summary.High,
		},
	)

	return report, nil
}

// CheckDeploymentAccess checks if current user can perform deployment
func (m *Manager) CheckDeploymentAccess(repository, action string) error {
	if !m.initialized {
		return fmt.Errorf("security manager not initialized")
	}

	// Get current user
	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = os.Getenv("USERNAME")
	}

	// Create access request
	request := AccessRequest{
		UserID:   currentUser,
		Resource: fmt.Sprintf("deployment:%s", repository),
		Action:   action,
		Context: map[string]string{
			"repository": repository,
			"timestamp":  time.Now().Format(time.RFC3339),
		},
	}

	// Check access
	decision := m.accessControl.CheckAccess(request)
	if !decision.Allowed {
		return fmt.Errorf("access denied: %s", decision.Reason)
	}

	return nil
}

// LogDeployment logs a deployment event
func (m *Manager) LogDeployment(repository, commit, status string, details map[string]interface{}) error {
	if !m.initialized {
		return fmt.Errorf("security manager not initialized")
	}

	// Add standard details
	if details == nil {
		details = make(map[string]interface{})
	}
	details["repository"] = repository
	details["commit"] = commit

	return m.auditLogger.LogDeployment(
		"deploy",
		repository,
		status,
		details,
	)
}

// VerifySnowflakeSSL verifies Snowflake SSL certificate
func (m *Manager) VerifySnowflakeSSL(account string) (*CertificateReport, error) {
	if !m.initialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	// Construct Snowflake hostname
	host := fmt.Sprintf("%s.snowflakecomputing.com", account)

	report, err := m.scanner.VerifySSLCertificate(host)
	if err != nil {
		return nil, err
	}

	// Log verification
	_ = m.auditLogger.LogSecurityEvent(
		"ssl_verification",
		host,
		"completed",
		map[string]interface{}{
			"valid":  report.IsValid,
			"issues": len(report.Issues),
		},
	)

	return report, nil
}

// PerformSecurityAudit performs a comprehensive security audit
func (m *Manager) PerformSecurityAudit() (*SecurityAuditReport, error) {
	if !m.initialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	report := &SecurityAuditReport{
		Timestamp: time.Now(),
		Sections:  make(map[string]interface{}),
	}

	// Check credential security
	credList, err := m.credManager.ListCredentials()
	if err == nil {
		report.Sections["credentials"] = map[string]interface{}{
			"total_stored": len(credList),
			"encryption":   "AES-256-GCM",
			"storage":      "keyring/encrypted",
		}
	}

	// Check audit log integrity
	endTime := time.Now()
	startTime := endTime.AddDate(0, -1, 0) // Last month
	integrityReport, err := m.auditLogger.VerifyIntegrity(startTime, endTime)
	if err == nil {
		report.Sections["audit_logs"] = integrityReport
	}

	// Check access control configuration
	users := m.accessControl.ListUsers()
	roles := m.accessControl.ListRoles()
	policies := m.accessControl.ListPolicies()

	report.Sections["access_control"] = map[string]interface{}{
		"users_count":    len(users),
		"roles_count":    len(roles),
		"policies_count": len(policies),
		"active_users":   countActiveUsers(users),
	}

	// Scan configuration files
	configPath := m.secureConfig.GetConfigPath()
	scanReport, err := m.scanner.ScanDirectory(configPath)
	if err == nil {
		report.Sections["configuration_scan"] = scanReport.Summary
	}

	// Log audit
	_ = m.auditLogger.LogSecurityEvent(
		"security_audit",
		"system",
		"completed",
		map[string]interface{}{
			"sections": len(report.Sections),
		},
	)

	return report, nil
}

// SecurityAuditReport contains comprehensive security audit results
type SecurityAuditReport struct {
	Timestamp time.Time                 `json:"timestamp"`
	Sections  map[string]interface{}    `json:"sections"`
	Issues    []string                  `json:"issues,omitempty"`
	Recommendations []string            `json:"recommendations,omitempty"`
}

// ExportSecurityConfig exports security configuration for backup
func (m *Manager) ExportSecurityConfig(password string) ([]byte, error) {
	if !m.initialized {
		return nil, fmt.Errorf("security manager not initialized")
	}

	export := map[string]interface{}{
		"version":    "1.0",
		"exported":   time.Now(),
		"components": make(map[string]interface{}),
	}

	// Export credentials
	credBackup, err := m.credManager.ExportCredentials(password)
	components := export["components"].(map[string]interface{})
	if err == nil {
		components["credentials"] = base64.StdEncoding.EncodeToString(credBackup)
	}

	// Export access control config
	components["access_control"] = map[string]interface{}{
		"users":    m.accessControl.ListUsers(),
		"roles":    m.accessControl.ListRoles(),
		"policies": m.accessControl.ListPolicies(),
	}

	// Log export
	_ = m.auditLogger.LogSecurityEvent(
		"export_config",
		"security_configuration",
		"success",
		nil,
	)

	return json.MarshalIndent(export, "", "  ")
}

// Close cleanly shuts down the security manager
func (m *Manager) Close() error {
	if !m.initialized {
		return nil
	}

	// Close audit logger
	if err := m.auditLogger.Close(); err != nil {
		return fmt.Errorf("failed to close audit logger: %w", err)
	}

	m.initialized = false
	return nil
}

// Helper functions

func countActiveUsers(users []*User) int {
	count := 0
	for _, user := range users {
		if user.Active {
			count++
		}
	}
	return count
}