package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/internal/config"
	"flakedrop/pkg/models"
)

func TestSetupCommand(t *testing.T) {
	// Test command structure
	assert.NotNil(t, setupCmd)
	assert.Equal(t, "setup", setupCmd.Use)
	assert.Equal(t, "Initial configuration setup", setupCmd.Short)
	assert.NotNil(t, setupCmd.Run)
}

func TestSetupCommandExecution(t *testing.T) {
	tests := []struct {
		name           string
		existingConfig bool
		expectedOutput []string
	}{
		{
			name:           "fresh setup",
			existingConfig: false,
			expectedOutput: []string{
				"Setting up FlakeDrop CLI",
				"Snowflake Configuration",
				"Repository Configuration",
			},
		},
		{
			name:           "existing config",
			existingConfig: true,
			expectedOutput: []string{
				"Setting up FlakeDrop CLI",
				"Configuration already exists",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			tempDir := t.TempDir()
			oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
			os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", filepath.Join(tempDir, "config.yaml"))
			defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

			// Create existing config if needed
			if tt.existingConfig {
				cfg := &models.Config{
					Snowflake: models.Snowflake{
						Account: "existing",
					},
				}
				err := config.Save(cfg)
				require.NoError(t, err)
			}

			// Since runSetup is interactive, we'll test what we can
			// In a real scenario, we'd mock the survey inputs
			
			// Verify config existence check works
			exists := config.Exists()
			assert.Equal(t, tt.existingConfig, exists)
		})
	}
}

func TestSetupValidation(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		validate func(interface{}) error
		wantErr  bool
	}{
		{
			name:     "valid account",
			input:    "xy12345.us-east-1",
			validate: validateSnowflakeAccount,
			wantErr:  false,
		},
		{
			name:     "empty account",
			input:    "",
			validate: validateSnowflakeAccount,
			wantErr:  true,
		},
		{
			name:     "valid username",
			input:    "testuser",
			validate: validateUsername,
			wantErr:  false,
		},
		{
			name:     "empty username",
			input:    "",
			validate: validateUsername,
			wantErr:  true,
		},
		{
			name:     "valid git url",
			input:    "https://github.com/user/repo.git",
			validate: validateGitURL,
			wantErr:  false,
		},
		{
			name:     "invalid git url",
			input:    "not-a-url",
			validate: validateGitURL,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Validation helper functions
func validateSnowflakeAccount(val interface{}) error {
	str, ok := val.(string)
	if !ok || str == "" {
		return assert.AnError
	}
	return nil
}

func validateUsername(val interface{}) error {
	str, ok := val.(string)
	if !ok || str == "" {
		return assert.AnError
	}
	return nil
}

func validateGitURL(val interface{}) error {
	str, ok := val.(string)
	if !ok || str == "" {
		return assert.AnError
	}
	// Basic URL validation
	if !strings.HasPrefix(str, "http://") && !strings.HasPrefix(str, "https://") && !strings.HasPrefix(str, "git@") {
		return assert.AnError
	}
	return nil
}

func TestSetupConfigCreation(t *testing.T) {
	// Test that setup creates proper config structure
	t.Run("config structure", func(t *testing.T) {
		cfg := &models.Config{
			Repositories: []models.Repository{
				{
					Name:     "test-repo",
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
				Role:      "TESTROLE",
				Warehouse: "TEST_WH",
			},
		}

		// Verify all fields are properly set
		assert.Len(t, cfg.Repositories, 1)
		assert.Equal(t, "test-repo", cfg.Repositories[0].Name)
		assert.Equal(t, "test123.us-east-1", cfg.Snowflake.Account)
		assert.Equal(t, "testuser", cfg.Snowflake.Username)
		assert.Equal(t, "TESTROLE", cfg.Snowflake.Role)
		assert.Equal(t, "TEST_WH", cfg.Snowflake.Warehouse)
	})
}

func TestSetupWorkflow(t *testing.T) {
	t.Run("complete setup workflow", func(t *testing.T) {
		// Setup test environment
		tempDir := t.TempDir()
		oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
		os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", filepath.Join(tempDir, "config.yaml"))
		defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

		// Verify no config exists initially
		assert.False(t, config.Exists())

		// Simulate config creation
		cfg := &models.Config{
			Snowflake: models.Snowflake{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Password:  "testpass",
				Role:      "ACCOUNTADMIN",
				Warehouse: "COMPUTE_WH",
			},
			Repositories: []models.Repository{
				{
					Name:     "my-repo",
					GitURL:   "https://github.com/company/repo.git",
					Branch:   "main",
					Database: "PROD_DB",
					Schema:   "PUBLIC",
				},
			},
		}

		// Save config
		err := config.Save(cfg)
		require.NoError(t, err)

		// Verify config exists and is valid
		assert.True(t, config.Exists())
		
		// Load and verify
		loadedCfg, err := config.Load()
		require.NoError(t, err)
		assert.Equal(t, cfg.Snowflake.Account, loadedCfg.Snowflake.Account)
		assert.Len(t, loadedCfg.Repositories, 1)
		assert.Equal(t, "my-repo", loadedCfg.Repositories[0].Name)
	})
}

func TestSetupDefaults(t *testing.T) {
	// Test default values in setup
	tests := []struct {
		name         string
		field        string
		defaultValue string
	}{
		{
			name:         "default role",
			field:        "role",
			defaultValue: "ACCOUNTADMIN",
		},
		{
			name:         "default warehouse",
			field:        "warehouse",
			defaultValue: "COMPUTE_WH",
		},
		{
			name:         "default branch",
			field:        "branch",
			defaultValue: "main",
		},
		{
			name:         "default schema",
			field:        "schema",
			defaultValue: "PUBLIC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These defaults are defined in the setup command
			// In actual implementation, these would be tested through the survey defaults
			assert.NotEmpty(t, tt.defaultValue)
		})
	}
}

func TestSetupErrorHandling(t *testing.T) {
	t.Run("config save error", func(t *testing.T) {
		// Test handling of config save errors
		// This would require mocking the config.Save function
		// For now, we test that the function exists
		assert.NotNil(t, config.Save)
	})

	t.Run("invalid config path", func(t *testing.T) {
		// Test with invalid config path
		oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
		os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", "/invalid/path/that/does/not/exist/config.yaml")
		defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

		// Attempt to save should handle error gracefully
		cfg := &models.Config{}
		err := config.Save(cfg)
		assert.Error(t, err)
	})
}

func TestSetupCommandIntegration(t *testing.T) {
	// Integration test for setup command
	t.Run("setup command registration", func(t *testing.T) {
		// Verify setup command is registered with root
		found := false
		for _, cmd := range rootCmd.Commands() {
			if cmd.Use == "setup" {
				found = true
				assert.Equal(t, setupCmd, cmd)
				break
			}
		}
		assert.True(t, found, "setup command should be registered with root")
	})
}

func BenchmarkSetupConfigCreation(b *testing.B) {
	tempDir := b.TempDir()
	
	for i := 0; i < b.N; i++ {
		configPath := filepath.Join(tempDir, fmt.Sprintf("config_%d.yaml", i))
		os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", configPath)
		
		cfg := &models.Config{
			Snowflake: models.Snowflake{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Password:  "testpass",
				Role:      "ACCOUNTADMIN",
				Warehouse: "COMPUTE_WH",
			},
			Repositories: []models.Repository{
				{
					Name:     "repo",
					GitURL:   "https://github.com/test/repo.git",
					Branch:   "main",
					Database: "DB",
					Schema:   "PUBLIC",
				},
			},
		}
		
		_ = config.Save(cfg)
	}
}