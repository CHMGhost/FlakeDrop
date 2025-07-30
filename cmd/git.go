package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	gogit "github.com/go-git/go-git/v5"
	"github.com/spf13/cobra"
	"flakedrop/internal/config"
	"flakedrop/internal/git"
	"flakedrop/pkg/models"
)

var gitCmd = &cobra.Command{
	Use:   "git",
	Short: "Enhanced Git integration commands",
	Long:  `Manage Git repositories with enhanced authentication, branch management, and error recovery`,
}

var gitSyncCmd = &cobra.Command{
	Use:   "sync [repo-name]",
	Short: "Sync a repository with enhanced error recovery",
	Long:  `Synchronize a Git repository with improved authentication handling and retry logic`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Get repository name
		repoName := ""
		if len(args) > 0 {
			repoName = args[0]
		}

		// Create enhanced service
		svc := git.NewEnhancedService()
		ctx := context.Background()

		// Sync specific repository or all repositories
		if repoName != "" {
			// Find specific repository
			var repo *models.Repository
			for _, r := range cfg.Repositories {
				if r.Name == repoName {
					repo = &r
					break
				}
			}
			
			if repo == nil {
				return fmt.Errorf("repository '%s' not found in configuration", repoName)
			}

			fmt.Printf("Syncing repository: %s\n", repo.Name)
			if err := svc.SyncRepositoryEnhanced(ctx, *repo); err != nil {
				return fmt.Errorf("failed to sync repository: %w", err)
			}
			
			fmt.Printf("✅ Successfully synced repository: %s\n", repo.Name)
		} else {
			// Sync all repositories
			fmt.Printf("Syncing %d repositories...\n", len(cfg.Repositories))
			
			errors := []string{}
			for _, repo := range cfg.Repositories {
				fmt.Printf("\nSyncing: %s\n", repo.Name)
				if err := svc.SyncRepositoryEnhanced(ctx, repo); err != nil {
					errors = append(errors, fmt.Sprintf("%s: %v", repo.Name, err))
					fmt.Printf("❌ Failed: %v\n", err)
				} else {
					fmt.Printf("✅ Success\n")
				}
			}
			
			if len(errors) > 0 {
				return fmt.Errorf("failed to sync some repositories:\n%s", strings.Join(errors, "\n"))
			}
			
			fmt.Printf("\n✅ Successfully synced all repositories\n")
		}

		return nil
	},
}

var gitAuthCmd = &cobra.Command{
	Use:   "auth",
	Short: "Manage Git authentication",
	Long:  `Configure and manage Git authentication for different hosts`,
}

var gitAuthAddCmd = &cobra.Command{
	Use:   "add [host]",
	Short: "Add authentication for a host",
	Long:  `Add authentication credentials for a specific Git host`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		host := args[0]
		authType, _ := cmd.Flags().GetString("type")
		
		svc := git.NewEnhancedService()
		authManager := svc.GetAuthManager()

		switch authType {
		case "token":
			token, _ := cmd.Flags().GetString("token")
			if token == "" {
				// Prompt for token
				fmt.Print("Enter personal access token: ")
				_, _ = fmt.Scanln(&token)
			}
			
			if err := authManager.StoreToken(host, token); err != nil {
				return fmt.Errorf("failed to store token: %w", err)
			}
			
			fmt.Printf("✅ Successfully stored token for %s\n", host)
			
		case "basic":
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")
			
			if username == "" {
				fmt.Print("Enter username: ")
				_, _ = fmt.Scanln(&username)
			}
			
			if password == "" {
				fmt.Print("Enter password: ")
				_, _ = fmt.Scanln(&password)
			}
			
			if err := authManager.StoreCredentials(host, username, password); err != nil {
				return fmt.Errorf("failed to store credentials: %w", err)
			}
			
			fmt.Printf("✅ Successfully stored credentials for %s\n", host)
			
		case "ssh":
			keyPath, _ := cmd.Flags().GetString("key")
			if keyPath == "" {
				return fmt.Errorf("SSH key path required for SSH authentication")
			}
			
			passphrase, _ := cmd.Flags().GetString("passphrase")
			
			if err := authManager.GetSSHKeyManager().RegisterKey(host, keyPath, passphrase); err != nil {
				return fmt.Errorf("failed to register SSH key: %w", err)
			}
			
			fmt.Printf("✅ Successfully registered SSH key for %s\n", host)
			
		default:
			return fmt.Errorf("unsupported authentication type: %s", authType)
		}

		return nil
	},
}

var gitBranchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Enhanced branch management",
	Long:  `Manage Git branches with advanced features`,
}

var gitBranchListCmd = &cobra.Command{
	Use:   "list [repo-name]",
	Short: "List branches with enhanced information",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		includeRemote, _ := cmd.Flags().GetBool("remote")
		
		// Get repository path
		repoPath := "."
		if len(args) > 0 {
			svc := git.NewEnhancedService()
			repoPath = svc.GetLocalPath(args[0])
		}
		
		branchMgr := git.NewBranchManager()
		branches, err := branchMgr.ListBranches(repoPath, includeRemote)
		if err != nil {
			return fmt.Errorf("failed to list branches: %w", err)
		}

		// Display branches in table format
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "CURRENT\tNAME\tLAST COMMIT\tDATE\tTRACKING")
		
		for _, branch := range branches {
			current := ""
			if branch.IsCurrent {
				current = "*"
			}
			
			tracking := branch.Tracking
			if tracking == "" {
				tracking = "-"
			}
			
			date := ""
			if !branch.LastCommitDate.IsZero() {
				date = branch.LastCommitDate.Format("2006-01-02")
			}
			
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				current, branch.Name, branch.LastCommit, date, tracking)
		}
		
		_ = w.Flush()
		return nil
	},
}

var gitBranchCreateCmd = &cobra.Command{
	Use:   "create [branch-name]",
	Short: "Create a new branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		branchName := args[0]
		baseBranch, _ := cmd.Flags().GetString("base")
		push, _ := cmd.Flags().GetBool("push")
		repoPath, _ := cmd.Flags().GetString("repo")
		
		if repoPath == "" {
			repoPath = "."
		} else {
			svc := git.NewEnhancedService()
			repoPath = svc.GetLocalPath(repoPath)
		}
		
		branchMgr := git.NewBranchManager()
		if err := branchMgr.CreateBranch(repoPath, branchName, baseBranch, push); err != nil {
			return fmt.Errorf("failed to create branch: %w", err)
		}
		
		fmt.Printf("✅ Successfully created branch '%s'\n", branchName)
		if push {
			fmt.Printf("✅ Pushed to remote\n")
		}
		
		return nil
	},
}

var gitBranchDeleteCmd = &cobra.Command{
	Use:   "delete [branch-name]",
	Short: "Delete a branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		branchName := args[0]
		force, _ := cmd.Flags().GetBool("force")
		deleteRemote, _ := cmd.Flags().GetBool("remote")
		repoPath, _ := cmd.Flags().GetString("repo")
		
		if repoPath == "" {
			repoPath = "."
		} else {
			svc := git.NewEnhancedService()
			repoPath = svc.GetLocalPath(repoPath)
		}
		
		// Confirm deletion
		if !force {
			fmt.Printf("Are you sure you want to delete branch '%s'? (y/N): ", branchName)
			var response string
			_, _ = fmt.Scanln(&response)
			if strings.ToLower(response) != "y" {
				fmt.Println("Deletion cancelled")
				return nil
			}
		}
		
		branchMgr := git.NewBranchManager()
		if err := branchMgr.DeleteBranch(repoPath, branchName, force, deleteRemote); err != nil {
			return fmt.Errorf("failed to delete branch: %w", err)
		}
		
		fmt.Printf("✅ Successfully deleted branch '%s'\n", branchName)
		if deleteRemote {
			fmt.Printf("✅ Also deleted from remote\n")
		}
		
		return nil
	},
}

var gitValidateCmd = &cobra.Command{
	Use:   "validate [repo-name]",
	Short: "Validate repository state",
	Long:  `Perform comprehensive validation of repository state and connectivity`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repoName := ""
		if len(args) > 0 {
			repoName = args[0]
		}
		
		// Load configuration
		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}
		
		svc := git.NewEnhancedService()
		ctx := context.Background()
		
		// Validate specific repository or all
		repos := []string{}
		if repoName != "" {
			repos = append(repos, repoName)
		} else {
			for _, repo := range cfg.Repositories {
				repos = append(repos, repo.Name)
			}
		}
		
		allValid := true
		for _, repo := range repos {
			fmt.Printf("\nValidating repository: %s\n", repo)
			fmt.Println(strings.Repeat("-", 50))
			
			validation, err := svc.ValidateRepository(ctx, repo)
			if err != nil {
				fmt.Printf("❌ Validation failed: %v\n", err)
				allValid = false
				continue
			}
			
			// Display validation results
			if validation.Valid {
				fmt.Printf("✅ Repository is valid\n")
			} else {
				fmt.Printf("❌ Repository has issues\n")
				allValid = false
			}
			
			if validation.CurrentBranch != "" {
				fmt.Printf("📍 Current branch: %s\n", validation.CurrentBranch)
			}
			
			if validation.HasUncommittedChanges {
				fmt.Printf("⚠️  Has uncommitted changes\n")
			}
			
			// Display issues
			if len(validation.Issues) > 0 {
				fmt.Printf("\nIssues found:\n")
				for _, issue := range validation.Issues {
					icon := "ℹ️ "
					switch issue.Severity {
					case "ERROR":
						icon = "❌"
					case "WARNING":
						icon = "⚠️ "
					}
					
					fmt.Printf("  %s %s\n", icon, issue.Message)
					if issue.Solution != "" {
						fmt.Printf("     → %s\n", issue.Solution)
					}
				}
			}
		}
		
		if allValid {
			fmt.Printf("\n✅ All repositories are valid\n")
		} else {
			fmt.Printf("\n❌ Some repositories have issues\n")
		}
		
		return nil
	},
}

var gitConflictCmd = &cobra.Command{
	Use:   "conflict",
	Short: "Manage merge conflicts",
	Long:  `Tools for handling and resolving merge conflicts`,
}

var gitConflictReportCmd = &cobra.Command{
	Use:   "report",
	Short: "Generate a conflict report",
	Long:  `Generate a detailed report of current merge conflicts`,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoPath, _ := cmd.Flags().GetString("repo")
		if repoPath == "" {
			repoPath = "."
		}
		
		// Open repository
		repo, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return fmt.Errorf("failed to open repository: %w", err)
		}
		
		worktree, err := repo.Worktree()
		if err != nil {
			return fmt.Errorf("failed to get worktree: %w", err)
		}
		
		// Generate conflict report
		conflictHandler := git.NewConflictHandler()
		report, err := conflictHandler.GenerateConflictReport(worktree)
		if err != nil {
			return fmt.Errorf("failed to generate report: %w", err)
		}
		
		// Display report
		fmt.Println("Merge Conflict Report")
		fmt.Println(strings.Repeat("=", 50))
		fmt.Printf("Total Files with Conflicts: %d\n", report.TotalFiles)
		fmt.Printf("Total Conflict Sections: %d\n", report.TotalConflicts)
		
		if len(report.ByFileType) > 0 {
			fmt.Println("\nConflicts by File Type:")
			for fileType, count := range report.ByFileType {
				fmt.Printf("  %s: %d\n", fileType, count)
			}
		}
		
		if len(report.SeverityLevels) > 0 {
			fmt.Println("\nSeverity Levels:")
			for severity, count := range report.SeverityLevels {
				fmt.Printf("  %s: %d\n", severity, count)
			}
		}
		
		if len(report.Recommendations) > 0 {
			fmt.Println("\nRecommendations:")
			for _, rec := range report.Recommendations {
				fmt.Printf("  • %s\n", rec)
			}
		}
		
		return nil
	},
}

func init() {
	// Git command tree
	rootCmd.AddCommand(gitCmd)
	
	// Sync command
	gitCmd.AddCommand(gitSyncCmd)
	
	// Auth commands
	gitCmd.AddCommand(gitAuthCmd)
	gitAuthCmd.AddCommand(gitAuthAddCmd)
	gitAuthAddCmd.Flags().String("type", "token", "Authentication type (token, basic, ssh)")
	gitAuthAddCmd.Flags().String("token", "", "Personal access token")
	gitAuthAddCmd.Flags().String("username", "", "Username for basic auth")
	gitAuthAddCmd.Flags().String("password", "", "Password for basic auth")
	gitAuthAddCmd.Flags().String("key", "", "Path to SSH key")
	gitAuthAddCmd.Flags().String("passphrase", "", "SSH key passphrase")
	
	// Branch commands
	gitCmd.AddCommand(gitBranchCmd)
	gitBranchCmd.AddCommand(gitBranchListCmd)
	gitBranchListCmd.Flags().Bool("remote", false, "Include remote branches")
	
	gitBranchCmd.AddCommand(gitBranchCreateCmd)
	gitBranchCreateCmd.Flags().String("base", "", "Base branch (defaults to current)")
	gitBranchCreateCmd.Flags().Bool("push", false, "Push to remote after creation")
	gitBranchCreateCmd.Flags().String("repo", "", "Repository name")
	
	gitBranchCmd.AddCommand(gitBranchDeleteCmd)
	gitBranchDeleteCmd.Flags().Bool("force", false, "Force deletion")
	gitBranchDeleteCmd.Flags().Bool("remote", false, "Also delete from remote")
	gitBranchDeleteCmd.Flags().String("repo", "", "Repository name")
	
	// Validate command
	gitCmd.AddCommand(gitValidateCmd)
	
	// Conflict commands
	gitCmd.AddCommand(gitConflictCmd)
	gitConflictCmd.AddCommand(gitConflictReportCmd)
	gitConflictReportCmd.Flags().String("repo", "", "Repository path")
}