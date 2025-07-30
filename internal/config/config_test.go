package config

import (
    "os"
    "path/filepath"
    "testing"
    "flakedrop/pkg/models"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "gopkg.in/yaml.v3"
)

func TestGetConfigPath(t *testing.T) {
    home, _ := os.UserHomeDir()
    expected := filepath.Join(home, ".flakedrop")
    assert.Equal(t, expected, GetConfigPath())
}

func TestGetConfigFile(t *testing.T) {
    home, _ := os.UserHomeDir()
    expected := filepath.Join(home, ".flakedrop", "config.yaml")
    assert.Equal(t, expected, GetConfigFile())
}

func TestSaveAndLoad(t *testing.T) {
    // Create temporary directory for testing
    tempDir, err := os.MkdirTemp("", "snowflake-deploy-test")
    require.NoError(t, err)
    defer os.RemoveAll(tempDir)

    // Override home directory for testing
    originalHome := os.Getenv("HOME")
    os.Setenv("HOME", tempDir)
    defer os.Setenv("HOME", originalHome)

    // Create test configuration
    testConfig := &models.Config{
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
        License: models.License{
            Key:       "TEST-LICENSE-KEY",
            ExpiresAt: "2025-12-31",
            LastCheck: "2025-01-01",
        },
    }

    // Test Save
    err = Save(testConfig)
    assert.NoError(t, err)

    // Verify file was created
    assert.True(t, Exists())

    // Test Load
    // Read the saved file directly to set up viper
    configFile := GetConfigFile()
    data, err := os.ReadFile(configFile)
    require.NoError(t, err)

    var loadedConfig models.Config
    err = yaml.Unmarshal(data, &loadedConfig)
    require.NoError(t, err)

    // Compare configurations
    assert.Equal(t, testConfig.Repositories[0].Name, loadedConfig.Repositories[0].Name)
    assert.Equal(t, testConfig.Repositories[0].GitURL, loadedConfig.Repositories[0].GitURL)
    assert.Equal(t, testConfig.Snowflake.Account, loadedConfig.Snowflake.Account)
    assert.Equal(t, testConfig.License.Key, loadedConfig.License.Key)
}

func TestExists(t *testing.T) {
    // Create temporary directory for testing
    tempDir, err := os.MkdirTemp("", "snowflake-deploy-test")
    require.NoError(t, err)
    defer os.RemoveAll(tempDir)

    // Override home directory for testing
    originalHome := os.Getenv("HOME")
    os.Setenv("HOME", tempDir)
    defer os.Setenv("HOME", originalHome)

    // Test when config doesn't exist
    assert.False(t, Exists())

    // Create empty config file
    _ = os.MkdirAll(GetConfigPath(), 0700)
    file, err := os.Create(GetConfigFile())
    require.NoError(t, err)
    file.Close()

    // Test when config exists
    assert.True(t, Exists())
}

func TestSaveWithInvalidPath(t *testing.T) {
    // Override home directory to an invalid path
    originalHome := os.Getenv("HOME")
    os.Setenv("HOME", "/invalid/path/that/does/not/exist")
    defer os.Setenv("HOME", originalHome)

    testConfig := &models.Config{}
    err := Save(testConfig)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "failed to create config directory")
}