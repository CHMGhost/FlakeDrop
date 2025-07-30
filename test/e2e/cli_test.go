package e2e

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flakedrop/internal/testutil"
)

// TestCLICommands tests the CLI commands end-to-end
func TestCLICommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	
	// Build the CLI binary
	binPath := buildCLI(t)
	
	// Create test environment
	tempDir, cleanup := helper.TempDir()
	defer cleanup()
	
	// Create config file
	configPath := filepath.Join(tempDir, "config.yaml")
	configContent := `
repositories:
  - name: test-repo
    git_url: https://github.com/test/repo.git
    branch: main
    database: TEST_DB
    schema: PUBLIC

snowflake:
  account: test123.us-east-1
  username: testuser
  password: testpass
  role: SYSADMIN
  warehouse: TEST_WH

license:
  key: TEST-LICENSE-KEY
  expires_at: 2025-12-31
`
	helper.WriteFile(tempDir, "config.yaml", configContent)
	
	// Test various commands
	tests := []struct {
		name           string
		args           []string
		expectedOutput []string
		expectedError  bool
	}{
		{
			name: "help",
			args: []string{"--help"},
			expectedOutput: []string{
				"FlakeDrop CLI",
				"Available Commands:",
				"deploy",
				"setup",
				"repo",
			},
		},
		{
			name: "version",
			args: []string{"--version"},
			expectedOutput: []string{
				"flakedrop version",
			},
		},
		{
			name: "setup validate",
			args: []string{"setup", "validate", "--config", configPath},
			expectedOutput: []string{
				"Configuration is valid",
			},
		},
		{
			name: "repo list",
			args: []string{"repo", "list", "--config", configPath},
			expectedOutput: []string{
				"test-repo",
				"TEST_DB.PUBLIC",
			},
		},
		{
			name: "deploy dry-run",
			args: []string{"deploy", "--dry-run", "--config", configPath},
			expectedOutput: []string{
				"DRY RUN",
				"Would deploy",
			},
		},
		{
			name:          "invalid command",
			args:          []string{"invalid"},
			expectedError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(binPath, tt.args...)
			cmd.Dir = tempDir
			
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			
			err := cmd.Run()
			
			if tt.expectedError && err == nil {
				t.Error("Expected error but command succeeded")
			}
			
			if !tt.expectedError && err != nil {
				t.Errorf("Command failed: %v\nStderr: %s", err, stderr.String())
			}
			
			output := stdout.String()
			for _, expected := range tt.expectedOutput {
				if !strings.Contains(output, expected) {
					t.Errorf("Expected output to contain '%s', got:\n%s", expected, output)
				}
			}
		})
	}
}

// TestInteractiveSetup tests the interactive setup flow
func TestInteractiveSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	binPath := buildCLI(t)
	
	tempDir, cleanup := helper.TempDir()
	defer cleanup()
	
	// Prepare input for interactive prompts
	input := strings.Join([]string{
		"test123.us-east-1", // Account
		"testuser",          // Username
		"testpass",          // Password
		"TEST_DB",           // Database
		"TEST_WH",           // Warehouse
		"SYSADMIN",          // Role
		tempDir,             // Repository path
		"main",              // Branch
		"sql",               // SQL path
		"*.sql",             // File pattern
		"y",                 // Auto-commit
		"n",                 // Dry-run
		"n",                 // Advanced settings
		"y",                 // Save configuration
	}, "\n")
	
	cmd := exec.Command(binPath, "setup", "init")
	cmd.Dir = tempDir
	cmd.Stdin = strings.NewReader(input)
	
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	
	// Set timeout for interactive command
	done := make(chan error)
	go func() {
		done <- cmd.Run()
	}()
	
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Setup failed: %v\nStderr: %s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("Setup command timed out")
	}
	
	// Verify configuration was created
	configPath := filepath.Join(tempDir, "config.yaml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Configuration file was not created")
	}
}

// TestDeploymentScenarios tests various deployment scenarios
func TestDeploymentScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	binPath := buildCLI(t)
	
	scenarios := []struct {
		name        string
		setup       func(dir string)
		args        []string
		validation  func(t *testing.T, stdout, stderr string)
	}{
		{
			name: "deploy specific commit",
			setup: func(dir string) {
				helper.CreateTestRepository(dir)
				// Initialize git repo
				exec.Command("git", "init").Run()
				exec.Command("git", "add", ".").Run()
				exec.Command("git", "commit", "-m", "Initial commit").Run()
			},
			args: []string{"deploy", "--commit", "HEAD", "--dry-run"},
			validation: func(t *testing.T, stdout, stderr string) {
				if !strings.Contains(stdout, "Deploying commit") {
					t.Error("Expected commit deployment message")
				}
			},
		},
		{
			name: "deploy with filter",
			setup: func(dir string) {
				helper.CreateTestRepository(dir)
			},
			args: []string{"deploy", "--filter", "tables/*.sql", "--dry-run"},
			validation: func(t *testing.T, stdout, stderr string) {
				if !strings.Contains(stdout, "tables/") {
					t.Error("Expected filtered files to be shown")
				}
				if strings.Contains(stdout, "views/") {
					t.Error("Views should be filtered out")
				}
			},
		},
		{
			name: "deploy specific repository",
			setup: func(dir string) {
				// Create multi-repo config
				config := `
repositories:
  - name: repo1
    git_url: https://github.com/test/repo1.git
    branch: main
    database: DB1
    schema: PUBLIC
  - name: repo2
    git_url: https://github.com/test/repo2.git
    branch: main
    database: DB2
    schema: PUBLIC

snowflake:
  account: test123.us-east-1
  username: testuser
  password: testpass
`
				helper.WriteFile(dir, "config.yaml", config)
			},
			args: []string{"deploy", "--repo", "repo2", "--dry-run"},
			validation: func(t *testing.T, stdout, stderr string) {
				if !strings.Contains(stdout, "repo2") {
					t.Error("Expected repo2 to be deployed")
				}
				if strings.Contains(stdout, "repo1") {
					t.Error("repo1 should not be deployed")
				}
			},
		},
	}
	
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			tempDir, cleanup := helper.TempDir()
			defer cleanup()
			
			// Run setup
			scenario.setup(tempDir)
			
			// Execute command
			cmd := exec.Command(binPath, scenario.args...)
			cmd.Dir = tempDir
			
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			
			err := cmd.Run()
			if err != nil {
				t.Errorf("Command failed: %v", err)
			}
			
			// Run validation
			scenario.validation(t, stdout.String(), stderr.String())
		})
	}
}

// TestErrorScenarios tests error handling in the CLI
func TestErrorScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	binPath := buildCLI(t)
	
	scenarios := []struct {
		name          string
		setup         func(dir string)
		args          []string
		expectedError string
	}{
		{
			name: "missing config",
			args: []string{"deploy", "--config", "nonexistent.yaml"},
			expectedError: "config file not found",
		},
		{
			name: "invalid config",
			setup: func(dir string) {
				helper.WriteFile(dir, "config.yaml", "invalid: yaml: content:")
			},
			args:          []string{"deploy"},
			expectedError: "failed to parse config",
		},
		{
			name: "missing license",
			setup: func(dir string) {
				config := `
repositories:
  - name: test
    git_url: https://github.com/test/repo.git

snowflake:
  account: test123
  username: user
`
				helper.WriteFile(dir, "config.yaml", config)
			},
			args:          []string{"deploy"},
			expectedError: "license",
		},
	}
	
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			tempDir, cleanup := helper.TempDir()
			defer cleanup()
			
			if scenario.setup != nil {
				scenario.setup(tempDir)
			}
			
			cmd := exec.Command(binPath, scenario.args...)
			cmd.Dir = tempDir
			
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			
			err := cmd.Run()
			if err == nil {
				t.Error("Expected command to fail")
			}
			
			errOutput := stderr.String()
			if !strings.Contains(strings.ToLower(errOutput), strings.ToLower(scenario.expectedError)) {
				t.Errorf("Expected error containing '%s', got: %s", scenario.expectedError, errOutput)
			}
		})
	}
}

// TestUICommands tests UI-specific commands
func TestUICommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	binPath := buildCLI(t)
	
	tempDir, cleanup := helper.TempDir()
	defer cleanup()
	
	// Create a simple config
	config := `
repositories:
  - name: test-repo
    git_url: https://github.com/test/repo.git
    branch: main
    database: TEST_DB

snowflake:
  account: test123.us-east-1
  username: testuser
  password: testpass
`
	helper.WriteFile(tempDir, "config.yaml", config)
	
	// Test UI demo command
	t.Run("ui-demo", func(t *testing.T) {
		cmd := exec.Command(binPath, "ui-demo")
		cmd.Dir = tempDir
		
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		
		// Run with timeout since it might have animations
		done := make(chan error)
		go func() {
			done <- cmd.Run()
		}()
		
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("UI demo failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			// This is expected for interactive demos
		}
		
		output := stdout.String()
		if len(output) == 0 {
			t.Error("Expected UI demo to produce output")
		}
	})
}

// Helper function to build the CLI binary
func buildCLI(t *testing.T) string {
	helper := testutil.NewTestHelper(t)
	
	tempDir, cleanup := helper.TempDir()
	t.Cleanup(cleanup)
	
	binPath := filepath.Join(tempDir, "flakedrop")
	if os.Getenv("GOOS") == "windows" {
		binPath += ".exe"
	}
	
	// Build the binary
	cmd := exec.Command("go", "build", "-o", binPath, "../../main.go")
	
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build CLI: %v\nStderr: %s", err, stderr.String())
	}
	
	return binPath
}

// TestPerformance tests CLI performance with large datasets
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	binPath := buildCLI(t)
	
	tempDir, cleanup := helper.TempDir()
	defer cleanup()
	
	// Create a large repository with many SQL files
	generator := testutil.NewTestDataGenerator()
	
	for i := 0; i < 100; i++ {
		dir := fmt.Sprintf("sql/batch%d", i/10)
		filename := fmt.Sprintf("table_%03d.sql", i)
		content := generator.GenerateSQL(i)
		helper.WriteFile(tempDir, filepath.Join(dir, filename), content)
	}
	
	// Create config
	config := `
repositories:
  - name: perf-test
    git_url: https://github.com/test/perf.git
    branch: main
    database: PERF_DB

snowflake:
  account: test123.us-east-1
  username: testuser
  password: testpass
`
	helper.WriteFile(tempDir, "config.yaml", config)
	
	// Measure deployment time
	start := time.Now()
	
	cmd := exec.Command(binPath, "deploy", "--dry-run")
	cmd.Dir = tempDir
	
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	
	err := cmd.Run()
	elapsed := time.Since(start)
	
	if err != nil {
		t.Errorf("Performance test failed: %v", err)
	}
	
	// Check performance threshold
	if elapsed > 10*time.Second {
		t.Errorf("Deployment took too long: %v", elapsed)
	}
	
	t.Logf("Deployed 100 files in %v", elapsed)
	
	// Verify all files were processed
	output := stdout.String()
	processedCount := strings.Count(output, ".sql")
	if processedCount < 100 {
		t.Errorf("Expected 100 files to be processed, got %d", processedCount)
	}
}