package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/internal/config"
	"flakedrop/internal/testutil"
	"flakedrop/pkg/models"
)

func TestRepoCommand(t *testing.T) {
	// Test main repo command
	assert.NotNil(t, repoCmd)
	assert.Equal(t, "repo", repoCmd.Use)
	assert.Equal(t, "Manage repository configurations", repoCmd.Short)
	
	// Test subcommands are registered
	subcommands := []string{"list", "add", "remove"}
	for _, subcmd := range subcommands {
		found := false
		for _, cmd := range repoCmd.Commands() {
			if cmd.Use == subcmd || cmd.Use == subcmd+" [name]" {
				found = true
				break
			}
		}
		assert.True(t, found, "Subcommand %s should be registered", subcmd)
	}
}

func TestRepoListCommand(t *testing.T) {
	tests := []struct {
		name           string
		config         *models.Config
		configError    error
		expectedOutput []string
		notExpected    []string
	}{
		{
			name: "list repositories successfully",
			config: &models.Config{
				Repositories: []models.Repository{
					{
						Name:     "prod-repo",
						GitURL:   "https://github.com/company/prod.git",
						Branch:   "main",
						Database: "PROD_DB",
						Schema:   "PUBLIC",
					},
					{
						Name:     "dev-repo",
						GitURL:   "https://github.com/company/dev.git",
						Branch:   "develop",
						Database: "DEV_DB",
						Schema:   "TEST",
					},
				},
			},
			expectedOutput: []string{
				"Configured Repositories:",
				"NAME",
				"GIT URL",
				"BRANCH",
				"DATABASE",
				"SCHEMA",
				"prod-repo",
				"https://github.com/company/prod.git",
				"main",
				"PROD_DB",
				"PUBLIC",
				"dev-repo",
				"https://github.com/company/dev.git",
				"develop",
				"DEV_DB",
				"TEST",
			},
		},
		{
			name:   "no repositories configured",
			config: &models.Config{Repositories: []models.Repository{}},
			expectedOutput: []string{
				"No repositories configured.",
				"Use 'flakedrop repo add' to add a repository",
			},
			notExpected: []string{
				"NAME",
				"GIT URL",
			},
		},
		{
			name:        "config load error",
			configError: assert.AnError,
			expectedOutput: []string{
				"Error loading configuration:",
				"Run 'flakedrop setup' to create initial configuration",
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

			// Save test config if provided
			if tt.config != nil && tt.configError == nil {
				err := config.Save(tt.config)
				require.NoError(t, err)
			} else if tt.configError != nil {
				// Create a malformed config file to trigger an error
				configFile := filepath.Join(tempDir, "config.yaml")
				err := os.WriteFile(configFile, []byte("invalid yaml:\n  - this is\n  - malformed:\n    missing value"), 0600)
				require.NoError(t, err)
			}

			// Capture output
			buf := new(bytes.Buffer)
			cmd := &cobra.Command{}
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			
			// Use the actual runRepoList function
			runRepoList(cmd, []string{})
			
			output := buf.String()
			
			// Check expected output
			for _, expected := range tt.expectedOutput {
				assert.Contains(t, output, expected)
			}
			
			// Check not expected output
			for _, notExpected := range tt.notExpected {
				assert.NotContains(t, output, notExpected)
			}
		})
	}
}

func TestRepoRemoveCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		config         *models.Config
		repoToRemove   string
		expectError    bool
		expectedOutput []string
	}{
		{
			name: "remove existing repository",
			args: []string{"dev-repo"},
			config: &models.Config{
				Repositories: []models.Repository{
					{Name: "prod-repo", GitURL: "https://github.com/prod.git"},
					{Name: "dev-repo", GitURL: "https://github.com/dev.git"},
				},
			},
			repoToRemove: "dev-repo",
			expectedOutput: []string{
				"Removal cancelled",
			},
		},
		{
			name: "remove non-existent repository",
			args: []string{"missing-repo"},
			config: &models.Config{
				Repositories: []models.Repository{
					{Name: "prod-repo", GitURL: "https://github.com/prod.git"},
				},
			},
			repoToRemove: "missing-repo",
			expectedOutput: []string{
				"Repository 'missing-repo' not found",
			},
		},
		{
			name:        "no arguments provided",
			args:        []string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			tempDir := t.TempDir()
			oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
			os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", filepath.Join(tempDir, "config.yaml"))
			defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

			// Save test config if provided
			if tt.config != nil {
				err := config.Save(tt.config)
				require.NoError(t, err)
			}

			// Create command for testing
			cmd := &cobra.Command{
				Use:   "remove [name]",
				Short: "Remove a repository",
				Args:  cobra.ExactArgs(1),
				Run:   runRepoRemove,
			}

			// Capture output
			buf := new(bytes.Buffer)
			cmd.SetOut(buf)
			cmd.SetErr(buf)
			cmd.SetArgs(tt.args)

			// Execute command
			err := cmd.Execute()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				// Check output
				output := buf.String()
				for _, expected := range tt.expectedOutput {
					assert.Contains(t, output, expected)
				}
			}
		})
	}
}

func TestRepoAddValidation(t *testing.T) {
	tests := []struct {
		name          string
		existingRepos []models.Repository
		newRepoName   string
		shouldError   bool
		errorMsg      string
	}{
		{
			name:          "valid new repository",
			existingRepos: []models.Repository{{Name: "existing"}},
			newRepoName:   "new-repo",
			shouldError:   false,
		},
		{
			name: "duplicate repository name",
			existingRepos: []models.Repository{
				{Name: "existing"},
			},
			newRepoName: "existing",
			shouldError: true,
			errorMsg:    "repository 'existing' already exists",
		},
		{
			name:          "empty repository name",
			existingRepos: []models.Repository{},
			newRepoName:   "",
			shouldError:   true,
			errorMsg:      "name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &models.Config{Repositories: tt.existingRepos}
			
			// Test the validation logic that would be in the survey validation
			err := validateRepoName(tt.newRepoName, cfg)
			
			if tt.shouldError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to test repo name validation
func validateRepoName(name string, cfg *models.Config) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	for _, r := range cfg.Repositories {
		if r.Name == name {
			return fmt.Errorf("repository '%s' already exists", name)
		}
	}
	return nil
}

func TestRepoConfiguration(t *testing.T) {
	t.Run("repository model structure", func(t *testing.T) {
		repo := models.Repository{
			Name:     "test-repo",
			GitURL:   "https://github.com/test/repo.git",
			Branch:   "main",
			Database: "TEST_DB",
			Schema:   "PUBLIC",
		}
		
		assert.Equal(t, "test-repo", repo.Name)
		assert.Equal(t, "https://github.com/test/repo.git", repo.GitURL)
		assert.Equal(t, "main", repo.Branch)
		assert.Equal(t, "TEST_DB", repo.Database)
		assert.Equal(t, "PUBLIC", repo.Schema)
	})
	
	t.Run("repository in config", func(t *testing.T) {
		cfg := testutil.TestConfig()
		assert.Len(t, cfg.Repositories, 1)
		assert.Equal(t, "test-repo", cfg.Repositories[0].Name)
	})
}

func TestRepoCommandIntegration(t *testing.T) {
	t.Run("full repository management workflow", func(t *testing.T) {
		// Setup test environment
		tempDir := t.TempDir()
		oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
		os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", filepath.Join(tempDir, "config.yaml"))
		defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

		// Create initial config
		initialConfig := &models.Config{
			Repositories: []models.Repository{},
			Snowflake:    models.Snowflake{Account: "test"},
		}
		err := config.Save(initialConfig)
		require.NoError(t, err)

		// Test listing empty repositories
		buf := new(bytes.Buffer)
		cmd := &cobra.Command{}
		cmd.SetOut(buf)
		runRepoList(cmd, []string{})
		assert.Contains(t, buf.String(), "No repositories configured")

		// Add a repository (would normally be interactive)
		cfg, err := config.Load()
		require.NoError(t, err)
		cfg.Repositories = append(cfg.Repositories, models.Repository{
			Name:     "test-repo",
			GitURL:   "https://github.com/test/repo.git",
			Branch:   "main",
			Database: "TEST_DB",
			Schema:   "PUBLIC",
		})
		err = config.Save(cfg)
		require.NoError(t, err)

		// Test listing with repository
		buf.Reset()
		runRepoList(cmd, []string{})
		output := buf.String()
		assert.Contains(t, output, "test-repo")
		assert.Contains(t, output, "https://github.com/test/repo.git")
		assert.Contains(t, output, "TEST_DB")
	})
}

func BenchmarkRepoList(b *testing.B) {
	// Setup test environment
	tempDir := b.TempDir()
	oldConfigPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG")
	os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", filepath.Join(tempDir, "config.yaml"))
	defer os.Setenv("SNOWFLAKE_DEPLOY_CONFIG", oldConfigPath)

	// Create config with multiple repositories
	cfg := &models.Config{
		Repositories: make([]models.Repository, 100),
	}
	for i := 0; i < 100; i++ {
		cfg.Repositories[i] = models.Repository{
			Name:     fmt.Sprintf("repo-%d", i),
			GitURL:   fmt.Sprintf("https://github.com/test/repo%d.git", i),
			Branch:   "main",
			Database: fmt.Sprintf("DB_%d", i),
			Schema:   "PUBLIC",
		}
	}
	_ = config.Save(cfg)

	cmd := &cobra.Command{}
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		runRepoList(cmd, []string{})
	}
}