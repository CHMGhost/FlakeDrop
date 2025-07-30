package testutil

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"flakedrop/pkg/models"
)

// MockConfigService provides a mock implementation of configuration management
type MockConfigService struct {
	mu sync.Mutex
	
	// Configuration data
	Config       *models.Config
	ConfigPath   string
	LastModified time.Time
	
	// Behavior control
	LoadError    error
	SaveError    error
	ValidateError error
	
	// Operation tracking
	LoadCount    int
	SaveCount    int
	ValidateCount int
}

// NewMockConfigService creates a new mock config service
func NewMockConfigService() *MockConfigService {
	return &MockConfigService{
		Config: &models.Config{
			Repositories: []models.Repository{
				{
					Name:     "default-repo",
					GitURL:   "https://github.com/test/repo.git",
					Branch:   "main",
					Database: "TEST_DB",
					Schema:   "PUBLIC",
				},
			},
			Snowflake: models.Snowflake{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Password:  "testpass",
				Role:      "SYSADMIN",
				Warehouse: "TEST_WH",
			},
			License: models.License{
				Key:       "TEST-LICENSE-KEY",
				ExpiresAt: "2025-12-31",
			},
		},
		LastModified: time.Now(),
	}
}

// Load simulates loading configuration from file
func (m *MockConfigService) Load(path string) (*models.Config, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.LoadCount++
	m.ConfigPath = path
	
	if m.LoadError != nil {
		return nil, m.LoadError
	}
	
	// Simulate file-based loading behavior
	switch filepath.Base(path) {
	case "invalid.yaml":
		return nil, fmt.Errorf("invalid YAML syntax")
	case "missing.yaml":
		return nil, fmt.Errorf("file not found: %s", path)
	case "empty.yaml":
		return &models.Config{}, nil
	}
	
	return m.Config, nil
}

// Save simulates saving configuration to file
func (m *MockConfigService) Save(path string, config *models.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.SaveCount++
	m.ConfigPath = path
	
	if m.SaveError != nil {
		return m.SaveError
	}
	
	// Simulate file permission issues
	if filepath.Base(path) == "readonly.yaml" {
		return fmt.Errorf("permission denied")
	}
	
	m.Config = config
	m.LastModified = time.Now()
	
	return nil
}

// Validate simulates configuration validation
func (m *MockConfigService) Validate(config *models.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.ValidateCount++
	
	if m.ValidateError != nil {
		return m.ValidateError
	}
	
	// Perform basic validation
	if config == nil {
		return fmt.Errorf("config is nil")
	}
	
	if len(config.Repositories) == 0 {
		return fmt.Errorf("no repositories configured")
	}
	
	for _, repo := range config.Repositories {
		if repo.Name == "" {
			return fmt.Errorf("repository name is required")
		}
		if repo.GitURL == "" {
			return fmt.Errorf("repository Git URL is required")
		}
		if repo.Database == "" {
			return fmt.Errorf("repository database is required")
		}
	}
	
	if config.Snowflake.Account == "" {
		return fmt.Errorf("Snowflake account is required")
	}
	
	if config.Snowflake.Username == "" {
		return fmt.Errorf("Snowflake username is required")
	}
	
	return nil
}

// GetRepository returns a repository by name
func (m *MockConfigService) GetRepository(name string) (*models.Repository, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.Config == nil {
		return nil, fmt.Errorf("configuration not loaded")
	}
	
	for _, repo := range m.Config.Repositories {
		if repo.Name == name {
			return &repo, nil
		}
	}
	
	return nil, fmt.Errorf("repository not found: %s", name)
}

// AddRepository adds a new repository to the configuration
func (m *MockConfigService) AddRepository(repo models.Repository) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.Config == nil {
		return fmt.Errorf("configuration not loaded")
	}
	
	// Check for duplicates
	for _, existing := range m.Config.Repositories {
		if existing.Name == repo.Name {
			return fmt.Errorf("repository already exists: %s", repo.Name)
		}
	}
	
	m.Config.Repositories = append(m.Config.Repositories, repo)
	m.LastModified = time.Now()
	
	return nil
}

// RemoveRepository removes a repository from the configuration
func (m *MockConfigService) RemoveRepository(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.Config == nil {
		return fmt.Errorf("configuration not loaded")
	}
	
	found := false
	var filtered []models.Repository
	
	for _, repo := range m.Config.Repositories {
		if repo.Name != name {
			filtered = append(filtered, repo)
		} else {
			found = true
		}
	}
	
	if !found {
		return fmt.Errorf("repository not found: %s", name)
	}
	
	m.Config.Repositories = filtered
	m.LastModified = time.Now()
	
	return nil
}

// GetStats returns mock statistics about the configuration
func (m *MockConfigService) GetStats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	stats := make(map[string]interface{})
	
	if m.Config != nil {
		stats["repositories"] = len(m.Config.Repositories)
		stats["has_license"] = m.Config.License.Key != ""
		stats["snowflake_configured"] = m.Config.Snowflake.Account != ""
	}
	
	stats["load_count"] = m.LoadCount
	stats["save_count"] = m.SaveCount
	stats["validate_count"] = m.ValidateCount
	stats["last_modified"] = m.LastModified
	
	return stats
}

// Reset resets the mock to initial state
func (m *MockConfigService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.LoadCount = 0
	m.SaveCount = 0
	m.ValidateCount = 0
	m.LoadError = nil
	m.SaveError = nil
	m.ValidateError = nil
}

// MockConfigBuilder provides a fluent interface for building test configurations
type MockConfigBuilder struct {
	config *models.Config
}

// NewMockConfigBuilder creates a new configuration builder
func NewMockConfigBuilder() *MockConfigBuilder {
	return &MockConfigBuilder{
		config: &models.Config{},
	}
}

// WithRepository adds a repository to the configuration
func (b *MockConfigBuilder) WithRepository(name, gitURL, branch, database, schema string) *MockConfigBuilder {
	b.config.Repositories = append(b.config.Repositories, models.Repository{
		Name:     name,
		GitURL:   gitURL,
		Branch:   branch,
		Database: database,
		Schema:   schema,
	})
	return b
}

// WithSnowflake sets Snowflake configuration
func (b *MockConfigBuilder) WithSnowflake(account, username, password, warehouse, role string) *MockConfigBuilder {
	b.config.Snowflake = models.Snowflake{
		Account:   account,
		Username:  username,
		Password:  password,
		Warehouse: warehouse,
		Role:      role,
	}
	return b
}

// WithLicense sets license configuration
func (b *MockConfigBuilder) WithLicense(key, expiresAt string) *MockConfigBuilder {
	b.config.License = models.License{
		Key:       key,
		ExpiresAt: expiresAt,
		LastCheck: time.Now().Format("2006-01-02"),
	}
	return b
}

// WithDeployment sets deployment configuration
func (b *MockConfigBuilder) WithDeployment(autoCommit, dryRun bool, parallelism int) *MockConfigBuilder {
	b.config.Deployment = models.Deployment{
		DryRun:      dryRun,
		Parallel:    parallelism > 1,
		BatchSize:   parallelism,
		Timeout:     "30m",
		MaxRetries:  3,
		Environment: "test",
	}
	return b
}

// Build returns the constructed configuration
func (b *MockConfigBuilder) Build() *models.Config {
	return b.config
}

// ConfigScenarios provides pre-built configuration scenarios for testing
var ConfigScenarios = struct {
	Basic           func() *models.Config
	MultiRepo       func() *models.Config
	Production      func() *models.Config
	Invalid         func() *models.Config
	MinimalValid    func() *models.Config
}{
	Basic: func() *models.Config {
		return NewMockConfigBuilder().
			WithRepository("main", "https://github.com/company/sql-repo.git", "main", "DEV", "PUBLIC").
			WithSnowflake("test123.us-east-1", "testuser", "testpass", "TEST_WH", "SYSADMIN").
			WithLicense("BASIC-LICENSE", "2025-12-31").
			Build()
	},
	
	MultiRepo: func() *models.Config {
		return NewMockConfigBuilder().
			WithRepository("core", "https://github.com/company/core-sql.git", "main", "CORE_DB", "PUBLIC").
			WithRepository("analytics", "https://github.com/company/analytics-sql.git", "main", "ANALYTICS_DB", "PUBLIC").
			WithRepository("reporting", "https://github.com/company/reporting-sql.git", "main", "REPORTING_DB", "PUBLIC").
			WithSnowflake("prod123.us-east-1", "produser", "prodpass", "PROD_WH", "SYSADMIN").
			WithLicense("ENTERPRISE-LICENSE", "2026-12-31").
			WithDeployment(false, false, 5).
			Build()
	},
	
	Production: func() *models.Config {
		return NewMockConfigBuilder().
			WithRepository("production", "git@github.com:company/production-sql.git", "release/stable", "PROD_DB", "PUBLIC").
			WithSnowflake("prod-account.snowflakecomputing.com", "deploy_user", "secure_pass", "DEPLOY_WH", "DEPLOYMENT_ROLE").
			WithLicense("PROD-LICENSE-KEY", "2030-01-01").
			WithDeployment(false, false, 10).
			Build()
	},
	
	Invalid: func() *models.Config {
		return &models.Config{
			// Missing required fields
			Repositories: []models.Repository{
				{
					Name: "invalid",
					// Missing GitURL, Database
				},
			},
			// Missing Snowflake configuration
		}
	},
	
	MinimalValid: func() *models.Config {
		return NewMockConfigBuilder().
			WithRepository("minimal", "https://github.com/test/minimal.git", "main", "TEST", "PUBLIC").
			WithSnowflake("min.us-east-1", "user", "pass", "WH", "ROLE").
			Build()
	},
}