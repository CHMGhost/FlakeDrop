package git

import (
	"context"
	"fmt"
	"log"
	"time"

	"flakedrop/pkg/models"
)

// ExampleGitIntegration demonstrates how to use the enhanced git package
func ExampleGitIntegration() {
	// Create enhanced git service
	service := NewEnhancedService()
	ctx := context.Background()

	// Example repository configuration
	repo := models.Repository{
		Name:   "my-sql-repo",
		GitURL: "https://github.com/myorg/sql-scripts.git",
		Branch: "main",
	}

	// 1. Enhanced repository sync with retry and error recovery
	fmt.Println("=== Enhanced Repository Sync ===")
	err := service.SyncRepositoryEnhanced(ctx, repo)
	if err != nil {
		log.Printf("Failed to sync repository: %v", err)
		// Error will contain helpful suggestions for recovery
		return
	}
	fmt.Println("✅ Repository synced successfully with enhanced error handling")

	// 2. Configure authentication
	fmt.Println("\n=== Authentication Management ===")
	// Store a personal access token
	err = service.authManager.StoreToken("github.com", "ghp_example_token")
	if err != nil {
		log.Printf("Failed to store token: %v", err)
	} else {
		fmt.Println("✅ Token stored securely")
	}

	// 3. Advanced branch management
	fmt.Println("\n=== Branch Management ===")
	branchInfo, err := service.branchManager.ListBranches(service.getLocalPath(repo.Name), true)
	if err != nil {
		log.Printf("Failed to list branches: %v", err)
	} else {
		fmt.Printf("Found %d branches:\n", len(branchInfo))
		for _, branch := range branchInfo {
			status := ""
			if branch.IsCurrent {
				status = " (current)"
			}
			fmt.Printf("  - %s%s\n", branch.Name, status)
		}
	}

	// Create a new feature branch
	err = service.branchManager.CreateBranch(
		service.getLocalPath(repo.Name),
		"feature/enhanced-deployment",
		"main",
		false, // don't push immediately
	)
	if err != nil {
		log.Printf("Failed to create branch: %v", err)
	} else {
		fmt.Println("✅ Created feature branch")
	}

	// 4. Get commits with advanced filtering
	fmt.Println("\n=== Advanced Commit Retrieval ===")
	commitOptions := CommitOptions{
		Limit:      10,
		Since:      time.Now().AddDate(0, -1, 0), // Last month
		Branch:     "main",
		PathFilter: "migrations/",
	}
	
	commits, err := service.GetCommitsWithContext(ctx, repo.Name, commitOptions)
	if err != nil {
		log.Printf("Failed to get commits: %v", err)
	} else {
		fmt.Printf("Found %d commits in migrations/ from the last month:\n", len(commits))
		for _, commit := range commits {
			fmt.Printf("  %s - %s (%s)\n", 
				commit.Hash[:8], 
				commit.Message,
				commit.Date.Format("2006-01-02"))
		}
	}

	// 5. Repository validation
	fmt.Println("\n=== Repository Validation ===")
	validation, err := service.ValidateRepository(ctx, repo.Name)
	if err != nil {
		log.Printf("Failed to validate repository: %v", err)
	} else {
		if validation.Valid {
			fmt.Println("✅ Repository is valid and healthy")
		} else {
			fmt.Println("⚠️  Repository has issues:")
			for _, issue := range validation.Issues {
				fmt.Printf("  - [%s] %s\n", issue.Severity, issue.Message)
				if issue.Solution != "" {
					fmt.Printf("    Solution: %s\n", issue.Solution)
				}
			}
		}
	}

	// 6. Conflict resolution demonstration
	fmt.Println("\n=== Conflict Resolution ===")
	// This would typically happen during a merge
	_ = NewConflictHandler()
	
	// Simulate checking for conflicts (would be real in practice)
	fmt.Println("Conflict detection and resolution capabilities:")
	fmt.Println("  - Automatic resolution for simple conflicts")
	fmt.Println("  - Strategy-based resolution (ours/theirs)")
	fmt.Println("  - Detailed conflict reporting")
	fmt.Println("  - File type-aware resolution hints")

	// 7. Progress tracking demonstration
	fmt.Println("\n=== Progress Tracking ===")
	tracker := NewGitOperationTracker("Repository Sync", []string{
		"Checking connectivity",
		"Fetching updates",
		"Updating branches",
		"Cleaning up",
	})
	
	// Simulate progress
	for i := 0; i < 4; i++ {
		tracker.StartStep(i)
		time.Sleep(500 * time.Millisecond) // Simulate work
		tracker.CompleteStep(i, nil)
	}
	tracker.Complete()

	// 8. Circuit breaker demonstration
	fmt.Println("\n=== Circuit Breaker Protection ===")
	fmt.Println("The enhanced service includes circuit breakers for:")
	fmt.Println("  - Repository operations (clone/fetch)")
	fmt.Println("  - Read operations (commits/files)")
	fmt.Println("  - Merge operations")
	fmt.Println("This prevents cascade failures and provides graceful degradation")

	// 9. Error recovery demonstration
	fmt.Println("\n=== Error Recovery ===")
	invalidRepo := models.Repository{
		Name:   "test-recovery",
		GitURL: "https://invalid-host-12345.com/repo.git",
		Branch: "main",
	}
	
	err = service.SyncRepositoryEnhanced(ctx, invalidRepo)
	if err != nil {
		fmt.Printf("Expected error with recovery suggestions:\n%v\n", err)
		// The error will include:
		// - Specific error code
		// - Contextual information
		// - Recovery suggestions
		// - Whether it's recoverable
	}
}

// ExampleAdvancedGitWorkflow demonstrates a complete workflow
func ExampleAdvancedGitWorkflow() {
	service := NewEnhancedService()
	ctx := context.Background()
	
	fmt.Println("=== Advanced Git Workflow Example ===")
	
	// 1. Setup repository
	repo := models.Repository{
		Name:   "deployment-scripts",
		GitURL: "https://github.com/company/deployment-scripts.git",
		Branch: "develop",
	}
	
	// 2. Sync with progress tracking
	fmt.Println("\n1. Syncing repository with progress tracking...")
	if err := service.SyncRepositoryEnhanced(ctx, repo); err != nil {
		log.Fatal(err)
	}
	
	// 3. Create feature branch
	fmt.Println("\n2. Creating feature branch...")
	branchName := fmt.Sprintf("feature/auto-deploy-%s", time.Now().Format("20060102"))
	if err := service.branchManager.CreateBranch(
		service.getLocalPath(repo.Name),
		branchName,
		"develop",
		true, // push to remote
	); err != nil {
		log.Fatal(err)
	}
	
	// 4. Make changes (simulated)
	fmt.Println("\n3. Making changes...")
	// In real scenario, you'd modify files here
	
	// 5. Check for conflicts before merge
	fmt.Println("\n4. Checking for potential conflicts...")
	// This would check if the branch can be cleanly merged
	
	// 6. Merge back to develop
	fmt.Println("\n5. Merging changes...")
	if err := service.MergeBranches(
		ctx,
		repo.Name,
		branchName,
		"develop",
		MergeStrategyManual, // Require manual conflict resolution
	); err != nil {
		// Handle merge conflicts
		fmt.Printf("Merge requires manual intervention: %v\n", err)
	}
	
	// 7. Cleanup
	fmt.Println("\n6. Cleaning up...")
	if err := service.branchManager.DeleteBranch(
		service.getLocalPath(repo.Name),
		branchName,
		false, // don't force
		true,  // delete remote
	); err != nil {
		log.Printf("Failed to cleanup branch: %v", err)
	}
	
	fmt.Println("\n✅ Workflow completed successfully!")
}

// ExampleLegacyCompatibility shows backward compatibility with the original service
func ExampleLegacyCompatibility() {
	// The enhanced service is backward compatible
	// You can still use the original service methods
	
	service := NewService() // Original service
	enhancedService := NewEnhancedService() // Enhanced service
	
	repo := models.Repository{
		Name:     "legacy-repo",
		GitURL:   "https://github.com/example/sql-scripts.git",
		Branch:   "main",
		Database: "MY_DB",
		Schema:   "PUBLIC",
	}
	
	// Original sync method still works
	if err := service.SyncRepository(repo); err != nil {
		fmt.Printf("Legacy sync error: %v\n", err)
	}
	
	// Enhanced sync provides better error handling and recovery
	ctx := context.Background()
	if err := enhancedService.SyncRepositoryEnhanced(ctx, repo); err != nil {
		fmt.Printf("Enhanced sync error with recovery suggestions: %v\n", err)
	}
	
	// All original methods are available on the enhanced service
	commits, _ := enhancedService.GetRecentCommitsForRepo(repo.Name, 5)
	fmt.Printf("Found %d commits\n", len(commits))
}