// +build integration

package test

import (
    "os"
    "os/exec"
    "path/filepath"
    "testing"
    "time"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestIntegrationCLIWorkflow(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Create temporary directory for test
    tempDir, err := os.MkdirTemp("", "snowflake-deploy-integration")
    require.NoError(t, err)
    defer os.RemoveAll(tempDir)

    // Set HOME to temp directory for isolated testing
    originalHome := os.Getenv("HOME")
    os.Setenv("HOME", tempDir)
    defer os.Setenv("HOME", originalHome)

    // Build the CLI
    buildCmd := exec.Command("go", "build", "-o", filepath.Join(tempDir, "flakedrop"), ".")
    buildCmd.Dir = ".."
    output, err := buildCmd.CombinedOutput()
    require.NoError(t, err, "Failed to build CLI: %s", string(output))

    cliPath := filepath.Join(tempDir, "flakedrop")

    t.Run("ShowHelp", func(t *testing.T) {
        cmd := exec.Command(cliPath, "--help")
        output, err := cmd.CombinedOutput()
        assert.NoError(t, err)
        assert.Contains(t, string(output), "flakedrop")
        assert.Contains(t, string(output), "setup")
        assert.Contains(t, string(output), "deploy")
        assert.Contains(t, string(output), "repo")
    })

    t.Run("RunSetupWithoutConfig", func(t *testing.T) {
        // This would normally be interactive, but will fail without input
        cmd := exec.Command(cliPath, "setup")
        cmd.Env = append(os.Environ(), "TERM=dumb") // Disable interactive mode
        output, err := cmd.CombinedOutput()
        // Expected to fail without interactive input
        assert.Error(t, err)
    })

    t.Run("RepoListWithoutConfig", func(t *testing.T) {
        cmd := exec.Command(cliPath, "repo", "list")
        output, err := cmd.CombinedOutput()
        assert.NoError(t, err)
        assert.Contains(t, string(output), "No repositories configured")
    })
}

func TestIntegrationConfigOperations(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Create temporary directory
    tempDir, err := os.MkdirTemp("", "snowflake-deploy-config-test")
    require.NoError(t, err)
    defer os.RemoveAll(tempDir)

    // Create a test config file
    configDir := filepath.Join(tempDir, ".flakedrop")
    err = os.MkdirAll(configDir, 0700)
    require.NoError(t, err)

    configContent := `repositories:
  - name: "test-repo"
    git_url: "https://github.com/test/repo.git"
    branch: "main"
    database: "TEST_DB"
    schema: "PUBLIC"
    
snowflake:
  account: "test123.us-east-1"
  username: "testuser"
  password: "testpass"
  role: "TESTROLE"
  warehouse: "TEST_WH"
  
license:
  key: "TEST-LICENSE-KEY"
  expires_at: "2025-12-31"
  last_check: "2025-01-01"
`

    configFile := filepath.Join(configDir, "config.yaml")
    err = os.WriteFile(configFile, []byte(configContent), 0600)
    require.NoError(t, err)

    // Set HOME to temp directory
    originalHome := os.Getenv("HOME")
    os.Setenv("HOME", tempDir)
    defer os.Setenv("HOME", originalHome)

    // Build CLI
    buildCmd := exec.Command("go", "build", "-o", filepath.Join(tempDir, "flakedrop"), ".")
    buildCmd.Dir = ".."
    output, err := buildCmd.CombinedOutput()
    require.NoError(t, err, "Failed to build CLI: %s", string(output))

    cliPath := filepath.Join(tempDir, "flakedrop")

    t.Run("RepoListWithConfig", func(t *testing.T) {
        cmd := exec.Command(cliPath, "repo", "list")
        output, err := cmd.CombinedOutput()
        assert.NoError(t, err)
        assert.Contains(t, string(output), "test-repo")
        assert.Contains(t, string(output), "https://github.com/test/repo.git")
        assert.Contains(t, string(output), "TEST_DB")
    })

    t.Run("DeployWithInvalidRepo", func(t *testing.T) {
        cmd := exec.Command(cliPath, "deploy", "non-existent-repo")
        output, err := cmd.CombinedOutput()
        assert.NoError(t, err)
        assert.Contains(t, string(output), "Repository 'non-existent-repo' not found")
    })
}

func TestIntegrationBuildAllPlatforms(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Create temporary directory for build outputs
    tempDir, err := os.MkdirTemp("", "snowflake-deploy-build-test")
    require.NoError(t, err)
    defer os.RemoveAll(tempDir)

    // Change to project directory
    cmd := exec.Command("make", "build-all")
    cmd.Dir = ".."
    
    // Run build
    start := time.Now()
    output, err := cmd.CombinedOutput()
    duration := time.Since(start)
    
    assert.NoError(t, err, "Build failed: %s", string(output))
    assert.Less(t, duration.Seconds(), 60.0, "Build took too long")

    // Check that binaries were created
    distDir := filepath.Join("..", "dist")
    expectedBinaries := []string{
        "snowflake-deploy-darwin-amd64",
        "snowflake-deploy-darwin-arm64",
        "snowflake-deploy-linux-amd64",
        "snowflake-deploy-windows-amd64.exe",
    }

    for _, binary := range expectedBinaries {
        binaryPath := filepath.Join(distDir, binary)
        _, err := os.Stat(binaryPath)
        assert.NoError(t, err, "Binary not found: %s", binary)
    }
}