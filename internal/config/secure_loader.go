package config

import (
	"fmt"
	"os"
	"flakedrop/internal/security"
	"flakedrop/pkg/models"
	"strings"

	"gopkg.in/yaml.v3"
)

// SecureLoader provides secure configuration loading with security manager integration
type SecureLoader struct {
	securityManager *security.Manager
}

// NewSecureLoader creates a new secure configuration loader
func NewSecureLoader() (*SecureLoader, error) {
	secManager, err := security.NewManager()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize security manager: %w", err)
	}

	return &SecureLoader{
		securityManager: secManager,
	}, nil
}

// LoadSecure loads configuration with security features enabled
func (sl *SecureLoader) LoadSecure() (*models.Config, error) {
	// Load base configuration
	config, err := Load()
	if err != nil {
		return nil, err
	}

	// Process secure credentials
	if err := sl.processSecureCredentials(config); err != nil {
		return nil, fmt.Errorf("failed to process secure credentials: %w", err)
	}

	// Perform security scan
	scanReport, err := sl.securityManager.ScanConfiguration(config)
	if err != nil {
		// Log warning but don't fail
		fmt.Fprintf(os.Stderr, "Warning: Security scan failed: %v\n", err)
	} else if scanReport.Summary.Critical > 0 {
		// Report critical issues
		fmt.Fprintf(os.Stderr, "Warning: %d critical security issues found in configuration\n", 
			scanReport.Summary.Critical)
		for _, finding := range scanReport.Findings {
			if finding.Severity == security.SeverityCritical {
				fmt.Fprintf(os.Stderr, "  - %s: %s\n", finding.RuleID, finding.Message)
				if finding.Remediation != "" {
					fmt.Fprintf(os.Stderr, "    Remediation: %s\n", finding.Remediation)
				}
			}
		}
	}

	// Log configuration access
	_ = sl.securityManager.GetAuditLogger().LogAccess(
		security.ActionRead,
		"configuration",
		"success",
		map[string]interface{}{
			"config_file": GetConfigFile(),
		},
	)

	return config, nil
}

// SaveSecure saves configuration with security features
func (sl *SecureLoader) SaveSecure(config *models.Config) error {
	// Secure sensitive data before saving
	if err := sl.secureConfigData(config); err != nil {
		return fmt.Errorf("failed to secure configuration: %w", err)
	}

	// Save configuration
	if err := Save(config); err != nil {
		return err
	}

	// Log configuration update
	_ = sl.securityManager.GetAuditLogger().LogAccess(
		security.ActionUpdate,
		"configuration",
		"success",
		map[string]interface{}{
			"config_file": GetConfigFile(),
		},
	)

	return nil
}

// processSecureCredentials processes credential references in configuration
func (sl *SecureLoader) processSecureCredentials(config *models.Config) error {
	// Process Snowflake password
	if strings.HasPrefix(config.Snowflake.Password, "@credential:") {
		password, err := sl.securityManager.GetSnowflakePassword(&config.Snowflake)
		if err != nil {
			return fmt.Errorf("failed to retrieve Snowflake password: %w", err)
		}
		config.Snowflake.Password = password
	}

	// Process license key
	if strings.HasPrefix(config.License.Key, "@credential:") {
		key, err := sl.securityManager.GetLicenseKey(&config.License)
		if err != nil {
			return fmt.Errorf("failed to retrieve license key: %w", err)
		}
		config.License.Key = key
	}

	return nil
}

// secureConfigData secures sensitive data in configuration before saving
func (sl *SecureLoader) secureConfigData(config *models.Config) error {
	// Secure Snowflake credentials
	if config.Snowflake.Password != "" && !strings.HasPrefix(config.Snowflake.Password, "@credential:") {
		if err := sl.securityManager.SecureSnowflakeConfig(&config.Snowflake); err != nil {
			return fmt.Errorf("failed to secure Snowflake credentials: %w", err)
		}
	}

	// Secure license key
	if config.License.Key != "" && !strings.HasPrefix(config.License.Key, "@credential:") {
		if err := sl.securityManager.SecureLicenseKey(&config.License); err != nil {
			return fmt.Errorf("failed to secure license key: %w", err)
		}
	}

	return nil
}

// GetSecurityManager returns the security manager instance
func (sl *SecureLoader) GetSecurityManager() *security.Manager {
	return sl.securityManager
}

// Close closes the secure loader and associated resources
func (sl *SecureLoader) Close() error {
	if sl.securityManager != nil {
		return sl.securityManager.Close()
	}
	return nil
}

// LoadWithCredentialPrompt loads config and prompts for missing credentials
func (sl *SecureLoader) LoadWithCredentialPrompt() (*models.Config, error) {
	config, err := sl.LoadSecure()
	if err != nil {
		// Check if error is due to missing credentials
		if strings.Contains(err.Error(), "failed to retrieve") {
			fmt.Println("Missing credentials detected. Please provide:")
			
			// Prompt for Snowflake password if needed
			if config.Snowflake.Password == "" || strings.HasPrefix(config.Snowflake.Password, "@credential:") {
				fmt.Printf("Snowflake password for %s@%s: ", 
					config.Snowflake.Username, config.Snowflake.Account)
				
				// In a real implementation, this would use a secure password input
				// For now, we'll just return the error
				return nil, fmt.Errorf("interactive credential input not implemented")
			}
		}
		return nil, err
	}
	
	return config, nil
}

// ExportSecureConfig exports configuration without sensitive data
func (sl *SecureLoader) ExportSecureConfig(config *models.Config, outputPath string) error {
	// Create a copy of the config
	exportConfig := *config
	
	// Replace sensitive data with references
	if exportConfig.Snowflake.Password != "" && !strings.HasPrefix(exportConfig.Snowflake.Password, "@credential:") {
		exportConfig.Snowflake.Password = "@credential:snowflake-" + exportConfig.Snowflake.Account
	}
	
	if exportConfig.License.Key != "" && !strings.HasPrefix(exportConfig.License.Key, "@credential:") {
		exportConfig.License.Key = "@credential:license-key"
	}
	
	// Marshal to YAML
	data, err := yaml.Marshal(exportConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	
	// Write to file
	if err := os.WriteFile(outputPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write export file: %w", err)
	}
	
	// Log export
	_ = sl.securityManager.GetAuditLogger().LogEvent(
		security.EventTypeConfig,
		"export",
		"configuration",
		"success",
		map[string]interface{}{
			"output_path": outputPath,
		},
	)
	
	return nil
}

// ValidateSecureConfig validates configuration with security checks
func (sl *SecureLoader) ValidateSecureConfig(config *models.Config) error {
	// Basic validation
	if config.Snowflake.Account == "" {
		return fmt.Errorf("Snowflake account is required")
	}
	
	if config.Snowflake.Username == "" {
		return fmt.Errorf("Snowflake username is required")
	}
	
	// Check for plaintext passwords
	if config.Snowflake.Password != "" && 
		!strings.HasPrefix(config.Snowflake.Password, "@credential:") &&
		!strings.HasPrefix(config.Snowflake.Password, "enc:") {
		fmt.Fprintf(os.Stderr, "Warning: Plaintext password detected. Consider using secure credential storage.\n")
	}
	
	// Validate repositories
	for i, repo := range config.Repositories {
		if repo.Name == "" {
			return fmt.Errorf("repository[%d]: name is required", i)
		}
		if repo.GitURL == "" {
			return fmt.Errorf("repository[%d]: git_url is required", i)
		}
		if repo.Database == "" {
			return fmt.Errorf("repository[%d]: database is required", i)
		}
		if repo.Schema == "" {
			return fmt.Errorf("repository[%d]: schema is required", i)
		}
	}
	
	return nil
}