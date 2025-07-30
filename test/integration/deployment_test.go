package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"flakedrop/internal/testutil"
	"flakedrop/pkg/models"
)

// TestDeploymentWorkflow tests the complete deployment workflow
func TestDeploymentWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	
	// Setup test environment
	tempDir, cleanup := helper.TempDir()
	defer cleanup()
	
	// Create test repository
	helper.CreateTestRepository(tempDir)
	
	// Setup mocks
	gitService := testutil.NewMockGitService()
	gitService.WorkingDir = tempDir
	
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.Account = "test123.us-east-1"
	snowflakeService.Username = "testuser"
	
	// Test configuration
	config := &models.Config{
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
			Account:   snowflakeService.Account,
			Username:  snowflakeService.Username,
			Password:  "testpass",
			Role:      "SYSADMIN",
			Warehouse: "TEST_WH",
		},
	}
	
	// Test workflow steps
	t.Run("TestConnection", func(t *testing.T) {
		err := snowflakeService.TestConnection()
		helper.AssertNoError(err)
	})
	
	t.Run("CloneRepository", func(t *testing.T) {
		err := gitService.Clone(config.Repositories[0].GitURL, tempDir)
		helper.AssertNoError(err)
		helper.AssertEqual("https://github.com/test/repo.git", gitService.Remotes["origin"])
	})
	
	t.Run("GetCommitHistory", func(t *testing.T) {
		commits, err := gitService.GetCommits(10)
		helper.AssertNoError(err)
		helper.AssertEqual(3, len(commits)) // Default commits
	})
	
	t.Run("DeployCommit", func(t *testing.T) {
		// Get latest commit
		commits, _ := gitService.GetCommits(1)
		latestCommit := commits[0]
		
		// Deploy files from commit
		files, err := gitService.GetCommitFiles(latestCommit.Hash)
		helper.AssertNoError(err)
		
		for _, file := range files {
			if filepath.Ext(file) == ".sql" {
				content, err := gitService.GetFileContent(latestCommit.Hash, file)
				helper.AssertNoError(err)
				
				err = snowflakeService.DB.Execute(
					content,
					config.Repositories[0].Database,
					config.Repositories[0].Schema,
				)
				helper.AssertNoError(err)
			}
		}
		
		// Verify executions
		executed := snowflakeService.DB.GetExecutedQueries()
		if len(executed) == 0 {
			t.Error("Expected SQL queries to be executed")
		}
	})
	
	t.Run("DryRunMode", func(t *testing.T) {
		// Reset execution history
		snowflakeService.DB.Reset()
		
		// Simulate dry run (no actual execution)
		commits, _ := gitService.GetCommits(1)
		files, _ := gitService.GetCommitFiles(commits[0].Hash)
		
		dryRunCount := 0
		for _, file := range files {
			if filepath.Ext(file) == ".sql" {
				dryRunCount++
				// In dry run, we would log but not execute
			}
		}
		
		// Verify no executions in dry run
		executed := snowflakeService.DB.GetExecutedQueries()
		helper.AssertEqual(0, len(executed))
		
		if dryRunCount == 0 {
			t.Error("Expected SQL files to be processed in dry run")
		}
	})
}

// TestMultiRepositoryDeployment tests deploying from multiple repositories
func TestMultiRepositoryDeployment(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	
	// Create multiple test repositories
	repos := []struct {
		name     string
		database string
		schema   string
	}{
		{"repo1", "DEV", "PUBLIC"},
		{"repo2", "TEST", "ANALYTICS"},
		{"repo3", "PROD", "REPORTING"},
	}
	
	snowflakeService := testutil.NewMockSnowflakeService()
	gitServices := make(map[string]*testutil.MockGitService)
	
	for _, repo := range repos {
		tempDir, cleanup := helper.TempDir()
		defer cleanup()
		
		helper.CreateTestRepository(tempDir)
		
		gitService := testutil.NewMockGitService()
		gitService.WorkingDir = tempDir
		gitServices[repo.name] = gitService
		
		// Add repo-specific commits
		gitService.AddCommit(testutil.MockCommit{
			Message: fmt.Sprintf("Changes for %s", repo.name),
			Author:  "Test User",
			Files:   []string{fmt.Sprintf("sql/%s_specific.sql", repo.name)},
		})
	}
	
	// Deploy from each repository
	for _, repo := range repos {
		t.Run(repo.name, func(t *testing.T) {
			gitService := gitServices[repo.name]
			
			commits, err := gitService.GetCommits(1)
			helper.AssertNoError(err)
			
			files, err := gitService.GetCommitFiles(commits[0].Hash)
			helper.AssertNoError(err)
			
			for _, file := range files {
				if filepath.Ext(file) == ".sql" {
					content, _ := gitService.GetFileContent(commits[0].Hash, file)
					err = snowflakeService.DB.Execute(content, repo.database, repo.schema)
					helper.AssertNoError(err)
				}
			}
		})
	}
	
	// Verify all deployments
	executed := snowflakeService.DB.GetExecutedQueries()
	
	// Check that each repository deployed to its correct database/schema
	dbCount := make(map[string]int)
	for _, query := range executed {
		key := fmt.Sprintf("%s.%s", query.Database, query.Schema)
		dbCount[key]++
	}
	
	for _, repo := range repos {
		key := fmt.Sprintf("%s.%s", repo.database, repo.schema)
		if dbCount[key] == 0 {
			t.Errorf("No queries executed for %s", key)
		}
	}
}

// TestErrorHandling tests error scenarios
func TestErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	
	t.Run("ConnectionFailure", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.DB.ConnectError = fmt.Errorf("connection refused")
		
		err := snowflakeService.TestConnection()
		helper.AssertError(err)
		helper.AssertContains(err.Error(), "connection refused")
	})
	
	t.Run("GitCloneFailure", func(t *testing.T) {
		gitService := testutil.NewMockGitService()
		gitService.CloneError = fmt.Errorf("repository not found")
		
		err := gitService.Clone("https://github.com/test/nonexistent.git", "/tmp/test")
		helper.AssertError(err)
		helper.AssertContains(err.Error(), "repository not found")
	})
	
	t.Run("SQLExecutionFailure", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()
		
		// Set error for specific query
		badQuery := "DROP TABLE important_table;"
		snowflakeService.DB.SetQueryError(badQuery, fmt.Errorf("permission denied"))
		
		err := snowflakeService.DB.Execute(badQuery, "TEST", "PUBLIC")
		helper.AssertError(err)
		helper.AssertContains(err.Error(), "permission denied")
		
		// Verify other queries still work
		err = snowflakeService.DB.Execute("SELECT 1;", "TEST", "PUBLIC")
		helper.AssertNoError(err)
	})
	
	t.Run("TransactionRollback", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()
		
		// Start transaction
		err := snowflakeService.DB.BeginTransaction()
		helper.AssertNoError(err)
		
		// Execute queries
		snowflakeService.DB.Execute("INSERT INTO test VALUES (1);", "TEST", "PUBLIC")
		snowflakeService.DB.Execute("INSERT INTO test VALUES (2);", "TEST", "PUBLIC")
		
		// Simulate error and rollback
		err = snowflakeService.DB.Rollback()
		helper.AssertNoError(err)
		
		// Verify rollback state
		if snowflakeService.DB.TxRolledBack != true {
			t.Error("Expected transaction to be rolled back")
		}
	})
}

// TestConcurrentDeployments tests concurrent deployment scenarios
func TestConcurrentDeployments(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()
	
	// Simulate concurrent deployments
	deploymentCount := 5
	done := make(chan bool, deploymentCount)
	errors := make(chan error, deploymentCount)
	
	for i := 0; i < deploymentCount; i++ {
		go func(index int) {
			query := fmt.Sprintf("CREATE TABLE concurrent_test_%d (id INT);", index)
			err := snowflakeService.DB.Execute(query, "TEST", "PUBLIC")
			
			if err != nil {
				errors <- err
			}
			done <- true
		}(i)
	}
	
	// Wait for all deployments
	for i := 0; i < deploymentCount; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Errorf("Concurrent deployment failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent deployments")
		}
	}
	
	// Verify all queries were executed
	executed := snowflakeService.DB.GetExecutedQueries()
	helper.AssertEqual(deploymentCount, len(executed))
}

// TestDeploymentRollback tests deployment rollback scenarios
func TestDeploymentRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)
	
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()
	
	gitService := testutil.NewMockGitService()
	
	// Add commits simulating a deployment that needs rollback
	gitService.AddCommit(testutil.MockCommit{
		Hash:    "good123",
		Message: "Good deployment",
		Files:   []string{"sql/good_change.sql"},
	})
	
	gitService.AddCommit(testutil.MockCommit{
		Hash:    "bad456",
		Message: "Bad deployment",
		Files:   []string{"sql/bad_change.sql"},
	})
	
	// Deploy bad commit
	snowflakeService.DB.SetQueryError(
		"-- Mock content for sql/bad_change.sql at commit",
		fmt.Errorf("syntax error"),
	)
	
	content, _ := gitService.GetFileContent("bad456", "sql/bad_change.sql")
	err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
	
	if err == nil {
		t.Error("Expected deployment to fail")
	}
	
	// Rollback to good commit
	err = gitService.Checkout("good123")
	helper.AssertNoError(err)
	
	// Verify we're on the good commit
	if gitService.HEAD != "good123" {
		t.Errorf("Expected HEAD to be good123, got %s", gitService.HEAD)
	}
}