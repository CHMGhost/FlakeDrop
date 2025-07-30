package integration

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"flakedrop/internal/testutil"
)

// TestAdvancedDeploymentScenarios tests complex deployment scenarios
func TestAdvancedDeploymentScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	helper := testutil.NewTestHelper(t)

	t.Run("ParallelRepositoryDeployment", func(t *testing.T) {
		// Setup multiple repositories
		repos := make(map[string]*testutil.MockGitService)
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		// Create 5 repositories
		for i := 0; i < 5; i++ {
			tempDir, cleanup := helper.TempDir()
			defer cleanup()

			helper.CreateTestRepository(tempDir)
			
			gitService := testutil.NewMockGitService()
			gitService.WorkingDir = tempDir
			repos[fmt.Sprintf("repo-%d", i)] = gitService

			// Add unique commits for each repo
			for j := 0; j < 3; j++ {
				gitService.AddCommit(testutil.MockCommit{
					Message: fmt.Sprintf("Repo %d - Commit %d", i, j),
					Author:  "Test User",
					Files:   []string{fmt.Sprintf("sql/repo%d_file%d.sql", i, j)},
				})
			}
		}

		// Deploy all repositories in parallel
		var wg sync.WaitGroup
		errors := make(chan error, len(repos))

		for name, gitService := range repos {
			wg.Add(1)
			go func(repoName string, git *testutil.MockGitService) {
				defer wg.Done()

				commits, err := git.GetCommits(1)
				if err != nil {
					errors <- fmt.Errorf("%s: %v", repoName, err)
					return
				}

				files, err := git.GetCommitFiles(commits[0].Hash)
				if err != nil {
					errors <- fmt.Errorf("%s: %v", repoName, err)
					return
				}

				for _, file := range files {
					content, _ := git.GetFileContent(commits[0].Hash, file)
					err = snowflakeService.DB.Execute(content, "TEST", repoName)
					if err != nil {
						errors <- fmt.Errorf("%s: %v", repoName, err)
						return
					}
				}
			}(name, gitService)
		}

		// Wait for all deployments
		wg.Wait()
		close(errors)

		// Check for errors
		var deploymentErrors []error
		for err := range errors {
			deploymentErrors = append(deploymentErrors, err)
		}

		if len(deploymentErrors) > 0 {
			t.Errorf("Parallel deployment failed with %d errors", len(deploymentErrors))
			for _, err := range deploymentErrors {
				t.Logf("Error: %v", err)
			}
		}

		// Verify all deployments succeeded
		executed := snowflakeService.DB.GetExecutedQueries()
		if len(executed) < len(repos) {
			t.Errorf("Expected at least %d queries, got %d", len(repos), len(executed))
		}
	})

	t.Run("TransactionalDeployment", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		gitService := testutil.NewMockGitService()
		
		// Add commits with interdependent SQL
		gitService.AddCommit(testutil.MockCommit{
			Message: "Create base tables",
			Files: []string{
				"sql/1_create_tables.sql",
				"sql/2_create_indexes.sql",
				"sql/3_create_constraints.sql",
			},
		})

		// Test successful transaction
		t.Run("Success", func(t *testing.T) {
			err := snowflakeService.DB.BeginTransaction()
			helper.AssertNoError(err)

			commits, _ := gitService.GetCommits(1)
			files, _ := gitService.GetCommitFiles(commits[0].Hash)

			allSuccessful := true
			for _, file := range files {
				content, _ := gitService.GetFileContent(commits[0].Hash, file)
				if err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC"); err != nil {
					allSuccessful = false
					break
				}
			}

			if allSuccessful {
				err = snowflakeService.DB.Commit()
				helper.AssertNoError(err)
				helper.AssertEqual(true, snowflakeService.DB.TxCommitted)
			}
		})

		// Test rollback on failure
		t.Run("Rollback", func(t *testing.T) {
			snowflakeService.DB.Reset()
			snowflakeService.TestConnection()

			// Set one query to fail
			snowflakeService.DB.SetQueryError(
				"-- Mock content for sql/3_create_constraints.sql",
				fmt.Errorf("constraint violation"),
			)

			err := snowflakeService.DB.BeginTransaction()
			helper.AssertNoError(err)

			commits, _ := gitService.GetCommits(1)
			files, _ := gitService.GetCommitFiles(commits[0].Hash)

			failureOccurred := false
			for _, file := range files {
				content, _ := gitService.GetFileContent(commits[0].Hash, file)
				if err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC"); err != nil {
					failureOccurred = true
					break
				}
			}

			if failureOccurred {
				err = snowflakeService.DB.Rollback()
				helper.AssertNoError(err)
				helper.AssertEqual(true, snowflakeService.DB.TxRolledBack)
			}
		})
	})

	t.Run("IncrementalDeployment", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		gitService := testutil.NewMockGitService()
		
		// Simulate deployment history
		deploymentHistory := make(map[string]time.Time)

		// Add initial commits
		baseCommits := []testutil.MockCommit{
			{
				Hash:    "initial",
				Message: "Initial schema",
				Date:    time.Now().Add(-72 * time.Hour),
				Files:   []string{"sql/schema.sql"},
			},
			{
				Hash:    "tables01",
				Message: "Add base tables",
				Date:    time.Now().Add(-48 * time.Hour),
				Files:   []string{"sql/tables/users.sql", "sql/tables/products.sql"},
			},
		}

		for _, commit := range baseCommits {
			gitService.AddCommit(commit)
			deploymentHistory[commit.Hash] = commit.Date
		}

		// Simulate new commits since last deployment
		newCommits := []testutil.MockCommit{
			{
				Hash:    "views001",
				Message: "Add analytics views",
				Date:    time.Now().Add(-24 * time.Hour),
				Files:   []string{"sql/views/user_stats.sql"},
			},
			{
				Hash:    "procedures",
				Message: "Add stored procedures",
				Date:    time.Now().Add(-1 * time.Hour),
				Files:   []string{"sql/procedures/update_stats.sql"},
			},
		}

		for _, commit := range newCommits {
			gitService.AddCommit(commit)
		}

		// Deploy only new commits
		lastDeployedTime := time.Now().Add(-36 * time.Hour)
		deploymentsExecuted := 0

		commits, _ := gitService.GetCommits(10)
		for _, commit := range commits {
			if commit.Date.After(lastDeployedTime) {
				files, _ := gitService.GetCommitFiles(commit.Hash)
				for _, file := range files {
					content, _ := gitService.GetFileContent(commit.Hash, file)
					err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
					helper.AssertNoError(err)
					deploymentsExecuted++
				}
				deploymentHistory[commit.Hash] = time.Now()
			}
		}

		// Verify only new commits were deployed
		if deploymentsExecuted != 2 { // views + procedures
			t.Errorf("Expected 2 deployments, got %d", deploymentsExecuted)
		}
	})

	t.Run("ConditionalDeployment", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		// Test deployment based on file patterns
		gitService := testutil.NewMockGitService()
		gitService.AddCommit(testutil.MockCommit{
			Message: "Mixed changes",
			Files: []string{
				"sql/tables/new_table.sql",
				"sql/views/new_view.sql",
				"sql/migrations/001_add_column.sql",
				"docs/README.md",
				"scripts/backup.sh",
			},
		})

		// Define deployment rules
		deploymentRules := map[string]struct {
			pattern  string
			database string
			schema   string
			deploy   bool
		}{
			"tables": {
				pattern:  "sql/tables/*.sql",
				database: "PROD",
				schema:   "PUBLIC",
				deploy:   true,
			},
			"views": {
				pattern:  "sql/views/*.sql",
				database: "PROD",
				schema:   "ANALYTICS",
				deploy:   true,
			},
			"migrations": {
				pattern:  "sql/migrations/*.sql",
				database: "PROD",
				schema:   "MIGRATIONS",
				deploy:   false, // Skip migrations in this test
			},
			"docs": {
				pattern:  "docs/*",
				deploy:   false,
			},
		}

		commits, _ := gitService.GetCommits(1)
		files, _ := gitService.GetCommitFiles(commits[0].Hash)

		deploymentCount := 0
		for _, file := range files {
			for ruleName, rule := range deploymentRules {
				if matched, _ := filepath.Match(rule.pattern, file); matched && rule.deploy {
					content, _ := gitService.GetFileContent(commits[0].Hash, file)
					err := snowflakeService.DB.Execute(content, rule.database, rule.schema)
					helper.AssertNoError(err)
					deploymentCount++
					t.Logf("Deployed %s using rule %s to %s.%s", file, ruleName, rule.database, rule.schema)
				}
			}
		}

		// Verify correct number of deployments
		if deploymentCount != 2 { // Only tables and views
			t.Errorf("Expected 2 deployments, got %d", deploymentCount)
		}
	})

	t.Run("FailureRecovery", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		gitService := testutil.NewMockGitService()
		
		// Add commits that will partially fail
		gitService.AddCommit(testutil.MockCommit{
			Message: "Batch deployment",
			Files: []string{
				"sql/1_good.sql",
				"sql/2_good.sql",
				"sql/3_bad.sql",
				"sql/4_good.sql",
				"sql/5_good.sql",
			},
		})

		// Set the third file to fail
		snowflakeService.DB.SetQueryError(
			"-- Mock content for sql/3_bad.sql",
			fmt.Errorf("syntax error"),
		)

		// Implement retry logic with recovery
		commits, _ := gitService.GetCommits(1)
		files, _ := gitService.GetCommitFiles(commits[0].Hash)

		successfulDeployments := make(map[string]bool)
		failedDeployments := make(map[string]error)

		// First pass
		for _, file := range files {
			content, _ := gitService.GetFileContent(commits[0].Hash, file)
			err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
			if err != nil {
				failedDeployments[file] = err
			} else {
				successfulDeployments[file] = true
			}
		}

		// Retry failed deployments after fixing
		if len(failedDeployments) > 0 {
			t.Logf("First pass: %d succeeded, %d failed", len(successfulDeployments), len(failedDeployments))

			// "Fix" the bad query
			snowflakeService.DB.SetQueryError("-- Mock content for sql/3_bad.sql", nil)

			// Retry only failed files
			retriedCount := 0
			for file := range failedDeployments {
				content, _ := gitService.GetFileContent(commits[0].Hash, file)
				err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
				if err == nil {
					successfulDeployments[file] = true
					delete(failedDeployments, file)
					retriedCount++
				}
			}

			t.Logf("Retry pass: %d succeeded", retriedCount)
		}

		// Verify all eventually succeeded
		if len(failedDeployments) > 0 {
			t.Errorf("Still have %d failed deployments after retry", len(failedDeployments))
		}

		if len(successfulDeployments) != 5 {
			t.Errorf("Expected 5 successful deployments, got %d", len(successfulDeployments))
		}
	})

	t.Run("DatabaseMigration", func(t *testing.T) {
		snowflakeService := testutil.NewMockSnowflakeService()
		snowflakeService.TestConnection()

		// Simulate migration tracking
		type Migration struct {
			Version   string
			Filename  string
			AppliedAt time.Time
		}

		appliedMigrations := make(map[string]Migration)

		// Add migration files
		gitService := testutil.NewMockGitService()
		gitService.AddCommit(testutil.MockCommit{
			Message: "Database migrations",
			Files: []string{
				"migrations/001_create_users_table.sql",
				"migrations/002_add_email_column.sql",
				"migrations/003_create_orders_table.sql",
				"migrations/004_add_indexes.sql",
			},
		})

		// Apply migrations in order
		commits, _ := gitService.GetCommits(1)
		files, _ := gitService.GetCommitFiles(commits[0].Hash)

		// Sort files to ensure migration order
		for _, file := range files {
			if filepath.Ext(file) == ".sql" {
				version := filepath.Base(file)[:3] // Extract version number

				// Check if already applied
				if _, applied := appliedMigrations[version]; !applied {
					content, _ := gitService.GetFileContent(commits[0].Hash, file)
					
					// Begin transaction for each migration
					snowflakeService.DB.BeginTransaction()
					
					err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
					if err == nil {
						// Record migration
						appliedMigrations[version] = Migration{
							Version:   version,
							Filename:  file,
							AppliedAt: time.Now(),
						}
						snowflakeService.DB.Commit()
						t.Logf("Applied migration: %s", file)
					} else {
						snowflakeService.DB.Rollback()
						t.Errorf("Failed to apply migration %s: %v", file, err)
						break // Stop on first failure
					}
				}
			}
		}

		// Verify all migrations applied
		if len(appliedMigrations) != 4 {
			t.Errorf("Expected 4 migrations, applied %d", len(appliedMigrations))
		}
	})
}

// TestDeploymentPerformance tests deployment performance with large datasets
func TestDeploymentPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()

	// Generate large number of SQL files
	gitService := testutil.NewMockGitService()

	fileCount := 100
	var files []string
	for i := 0; i < fileCount; i++ {
		files = append(files, fmt.Sprintf("sql/generated/table_%03d.sql", i))
	}

	gitService.AddCommit(testutil.MockCommit{
		Message: "Large batch deployment",
		Files:   files,
	})

	// Measure deployment time
	start := time.Now()

	commits, _ := gitService.GetCommits(1)
	commitFiles, _ := gitService.GetCommitFiles(commits[0].Hash)

	// Use goroutines for parallel execution
	const workers = 10
	fileChan := make(chan string, len(commitFiles))
	errorChan := make(chan error, len(commitFiles))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range fileChan {
				content, _ := gitService.GetFileContent(commits[0].Hash, file)
				if err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC"); err != nil {
					errorChan <- err
				}
			}
		}()
	}

	// Send files to workers
	for _, file := range commitFiles {
		fileChan <- file
	}
	close(fileChan)

	// Wait for completion
	wg.Wait()
	close(errorChan)

	elapsed := time.Since(start)

	// Check for errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Errorf("Performance test had %d errors", len(errors))
	}

	// Performance assertions
	avgTimePerFile := elapsed / time.Duration(fileCount)
	t.Logf("Deployed %d files in %v (avg %v per file)", fileCount, elapsed, avgTimePerFile)

	// Ensure reasonable performance
	if avgTimePerFile > 100*time.Millisecond {
		t.Errorf("Deployment too slow: %v per file", avgTimePerFile)
	}
}

// TestDeploymentMonitoring tests deployment monitoring and alerting
func TestDeploymentMonitoring(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping monitoring test in short mode")
	}

	helper := testutil.NewTestHelper(t)

	// Deployment metrics
	type DeploymentMetrics struct {
		StartTime      time.Time
		EndTime        time.Time
		FilesProcessed int
		FilesSucceeded int
		FilesFailed    int
		TotalQueries   int
		Duration       time.Duration
	}

	// Alert thresholds
	type AlertThresholds struct {
		MaxDuration      time.Duration
		MaxFailureRate   float64
		MinSuccessRate   float64
		MaxConcurrent    int
	}

	thresholds := AlertThresholds{
		MaxDuration:    5 * time.Minute,
		MaxFailureRate: 0.05, // 5%
		MinSuccessRate: 0.95, // 95%
		MaxConcurrent:  10,
	}

	// Run deployment with monitoring
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()

	gitService := testutil.NewMockGitService()
	
	// Add test data
	var files []string
	for i := 0; i < 50; i++ {
		files = append(files, fmt.Sprintf("sql/monitored/file_%02d.sql", i))
	}

	gitService.AddCommit(testutil.MockCommit{
		Message: "Monitored deployment",
		Files:   files,
	})

	// Simulate some failures
	for i := 0; i < 3; i++ {
		failQuery := fmt.Sprintf("-- Mock content for sql/monitored/file_%02d.sql", i*10)
		snowflakeService.DB.SetQueryError(failQuery, fmt.Errorf("simulated failure"))
	}

	// Start monitoring
	metrics := DeploymentMetrics{
		StartTime: time.Now(),
	}

	// Deploy with monitoring
	commits, _ := gitService.GetCommits(1)
	deployFiles, _ := gitService.GetCommitFiles(commits[0].Hash)

	for _, file := range deployFiles {
		metrics.FilesProcessed++
		
		content, _ := gitService.GetFileContent(commits[0].Hash, file)
		err := snowflakeService.DB.Execute(content, "TEST", "PUBLIC")
		
		if err != nil {
			metrics.FilesFailed++
			t.Logf("Deployment failed for %s: %v", file, err)
		} else {
			metrics.FilesSucceeded++
		}
		
		metrics.TotalQueries++
	}

	metrics.EndTime = time.Now()
	metrics.Duration = metrics.EndTime.Sub(metrics.StartTime)

	// Check alerts
	alerts := []string{}

	// Duration check
	if metrics.Duration > thresholds.MaxDuration {
		alerts = append(alerts, fmt.Sprintf("Deployment took too long: %v", metrics.Duration))
	}

	// Failure rate check
	failureRate := float64(metrics.FilesFailed) / float64(metrics.FilesProcessed)
	if failureRate > thresholds.MaxFailureRate {
		alerts = append(alerts, fmt.Sprintf("High failure rate: %.2f%%", failureRate*100))
	}

	// Success rate check
	successRate := float64(metrics.FilesSucceeded) / float64(metrics.FilesProcessed)
	if successRate < thresholds.MinSuccessRate {
		alerts = append(alerts, fmt.Sprintf("Low success rate: %.2f%%", successRate*100))
	}

	// Log metrics
	t.Logf("Deployment Metrics:")
	t.Logf("  Duration: %v", metrics.Duration)
	t.Logf("  Files Processed: %d", metrics.FilesProcessed)
	t.Logf("  Succeeded: %d (%.2f%%)", metrics.FilesSucceeded, successRate*100)
	t.Logf("  Failed: %d (%.2f%%)", metrics.FilesFailed, failureRate*100)

	// Check if any alerts were triggered
	if len(alerts) > 0 {
		t.Logf("Alerts triggered:")
		for _, alert := range alerts {
			t.Logf("  - %s", alert)
		}
	}

	// Verify metrics are within acceptable ranges
	helper.AssertEqual(50, metrics.FilesProcessed)
	helper.AssertEqual(3, metrics.FilesFailed)
	helper.AssertEqual(47, metrics.FilesSucceeded)
}