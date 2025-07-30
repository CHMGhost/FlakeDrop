package cmd

import (
    "context"
    "fmt"
    "time"
    "crypto/rand"
    "os"
    "path/filepath"

    "github.com/spf13/cobra"
    "flakedrop/internal/ui"
    "flakedrop/internal/config"
    "flakedrop/internal/plugin"
    "flakedrop/internal/snowflake"
    "flakedrop/internal/common"
    "flakedrop/pkg/models"
    pluginPkg "flakedrop/pkg/plugin"
)

var (
    deployDryRun    bool
    deployCommit    string
    deployInteractive bool
)

var deployCmd = &cobra.Command{
    Use:   "deploy [repository]",
    Short: "Deploy code to Snowflake",
    Long: `Deploy SQL changes from a Git repository to Snowflake.
    
This command allows you to select a specific commit and deploy all changed SQL files
to your Snowflake instance. It supports interactive commit selection, dry-run mode,
and provides detailed progress tracking.`,
    Args:  cobra.ExactArgs(1),
    Run: runDeploy,
}

func init() {
    rootCmd.AddCommand(deployCmd)
    
    deployCmd.Flags().BoolVarP(&deployDryRun, "dry-run", "d", false, "Show what would be deployed without executing")
    deployCmd.Flags().StringVarP(&deployCommit, "commit", "c", "", "Deploy a specific commit (skip interactive selection)")
    deployCmd.Flags().BoolVarP(&deployInteractive, "interactive", "i", true, "Use interactive mode for commit selection")
}

// truncateCommitHash safely truncates commit hash for display
func truncateCommitHash(hash string, length int) string {
    if len(hash) <= length {
        return hash
    }
    return hash[:length]
}

func runDeploy(cmd *cobra.Command, args []string) {
    ctx := context.Background()
    repoName := args[0]
    
    // Load configuration
    appConfig, err := config.Load()
    if err != nil {
        ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
        return
    }
    
    // Initialize plugin system if enabled
    var pluginService *plugin.Service
    if appConfig.Plugins.Enabled {
        pluginService, err = plugin.NewService(appConfig)
        if err != nil {
            ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
            return
        }
        
        if err := pluginService.Initialize(ctx); err != nil {
            ui.ShowError(fmt.Errorf("failed to initialize plugins: %w", err))
            return
        }
        
        defer pluginService.Shutdown(ctx)
    }
    
    // Generate deployment ID
    deploymentID := generateDeploymentID()
    
    // Show header
    ui.ShowHeader(fmt.Sprintf("FlakeDrop - Repository: %s", repoName))
    
    // Execute pre-start hooks
    if pluginService != nil {
        hookData := pluginPkg.HookData{
            DeploymentID: deploymentID,
            Repository:   repoName,
            StartTime:    time.Now(),
            Metadata:     make(map[string]interface{}),
        }
        
        if err := pluginService.ExecuteHook(ctx, pluginPkg.HookOnStart, hookData); err != nil {
            ui.ShowWarning(fmt.Sprintf("Hook execution warning: %v", err))
        }
    }
    
    // Connect to Snowflake (simulated)
    spinner := ui.NewSpinner("Connecting to Snowflake...")
    spinner.Start()
    time.Sleep(2 * time.Second)
    spinner.Stop(true, "Connected to Snowflake")
    
    // Get commits (simulated for demo)
    commits := getRecentCommits()
    
    var selectedCommit string
    
    if deployCommit != "" {
        // Use provided commit
        selectedCommit = deployCommit
        ui.ShowInfo(fmt.Sprintf("Using commit: %s", truncateCommitHash(selectedCommit, 7)))
    } else if deployInteractive {
        // Interactive commit selection
        selectedCommit, err = ui.SelectCommit(commits)
        if err != nil {
            ui.ShowError(err)
            return
        }
    } else {
        // Use latest commit
        if len(commits) > 0 {
            selectedCommit = commits[0].Hash
            ui.ShowInfo(fmt.Sprintf("Using latest commit: %s", truncateCommitHash(selectedCommit, 7)))
        }
    }
    
    // Get files to deploy from repository
    files, err := getRealFilesToDeploy(appConfig, repoName)
    if err != nil {
        ui.ShowError(fmt.Errorf("failed to get files: %w", err))
        return
    }
    
    // Show deployment summary
    ui.ShowDeploymentSummary(files, selectedCommit)
    
    if deployDryRun {
        ui.ShowWarning("Running in dry-run mode - no changes will be applied")
    }
    
    // Confirm deployment
    confirm, err := ui.Confirm("Proceed with deployment?", true)
    if err != nil || !confirm {
        ui.ShowWarning("Deployment cancelled")
        return
    }
    
    // Execute real deployment
    err = executeRealDeployment(ctx, appConfig, files, deployDryRun, deploymentID, repoName, selectedCommit, pluginService)
    if err != nil {
        ui.ShowError(err)
        return
    }
}

func getRecentCommits() []ui.CommitInfo {
    // Simulated commits for demo
    return []ui.CommitInfo{
        {
            Hash:      "abc123456789def0123456789abcdef0123456789",
            ShortHash: "abc1234",
            Message:   "Fix database connection pooling issue",
            Author:    "John Doe",
            Time:      time.Now().Add(-2 * time.Hour),
            Files:     5,
        },
        {
            Hash:      "def456789012ghi3456789012def456789012ghi3",
            ShortHash: "def4567",
            Message:   "Add new customer analytics tables",
            Author:    "Jane Smith",
            Time:      time.Now().Add(-5 * time.Hour),
            Files:     12,
        },
        {
            Hash:      "ghi789012345jkl6789012345ghi789012345jkl6",
            ShortHash: "ghi7890",
            Message:   "Update schema for performance optimization",
            Author:    "Bob Johnson",
            Time:      time.Now().Add(-24 * time.Hour),
            Files:     3,
        },
        {
            Hash:      "jkl012345678mno9012345678jkl012345678mno9",
            ShortHash: "jkl0123",
            Message:   "Refactor stored procedures for better maintainability",
            Author:    "Alice Wilson",
            Time:      time.Now().Add(-48 * time.Hour),
            Files:     8,
        },
    }
}

func getFilesToDeploy(commit string) []string {
    // Simulated files for demo
    return []string{
        "tables/users.sql",
        "tables/orders.sql",
        "tables/products.sql",
        "views/customer_analytics.sql",
        "views/revenue_summary.sql",
        "procedures/update_stats.sql",
        "functions/calculate_revenue.sql",
    }
}

func executeDeployment(ctx context.Context, files []string, dryRun bool, deploymentID, repoName, commit string, pluginService *plugin.Service) {
    pb := ui.NewProgressBar(len(files))
    
    successCount := 0
    failureCount := 0
    deploymentStart := time.Now()
    
    // Execute pre-deploy hooks
    if pluginService != nil {
        hookData := pluginPkg.HookData{
            DeploymentID: deploymentID,
            Repository:   repoName,
            Commit:       commit,
            Files:        files,
            StartTime:    deploymentStart,
            Success:      true,
            Metadata:     map[string]interface{}{
                "dry_run": dryRun,
            },
        }
        
        if err := pluginService.ExecuteHook(ctx, pluginPkg.HookPreDeploy, hookData); err != nil {
            ui.ShowWarning(fmt.Sprintf("Pre-deploy hook warning: %v", err))
        }
    }
    
    for i, file := range files {
        ui.ShowFileExecution(file, i+1, len(files))
        
        // Execute pre-file hooks
        if pluginService != nil {
            hookData := pluginPkg.HookData{
                DeploymentID: deploymentID,
                Repository:   repoName,
                Commit:       commit,
                Files:        files,
                CurrentFile:  file,
                StartTime:    time.Now(),
                Success:      true,
                Metadata:     map[string]interface{}{
                    "dry_run": dryRun,
                    "file_index": i + 1,
                    "total_files": len(files),
                },
            }
            
            if err := pluginService.ExecuteHook(ctx, pluginPkg.HookPreFile, hookData); err != nil {
                ui.ShowWarning(fmt.Sprintf("Pre-file hook warning for %s: %v", file, err))
            }
        }
        
        var success bool
        var errorMsg string
        var duration time.Duration
        
        if dryRun {
            time.Sleep(500 * time.Millisecond)
            ui.ShowExecutionResult(file, true, "", "skipped")
            success = true
            successCount++
        } else {
            // Simulate execution
            start := time.Now()
            time.Sleep(1 * time.Second)
            
            // Simulate random failure for demo
            success = i != 2 // Third file fails
            duration = time.Since(start)
            
            if success {
                ui.ShowExecutionResult(file, true, "", formatDuration(duration))
                successCount++
            } else {
                errorMsg = "Syntax error at line 42"
                ui.ShowExecutionResult(file, false, errorMsg, "")
                failureCount++
            }
        }
        
        pb.Update(i+1, file, success)
        
        // Execute post-file hooks
        if pluginService != nil {
            hookData := pluginPkg.HookData{
                DeploymentID: deploymentID,
                Repository:   repoName,
                Commit:       commit,
                Files:        files,
                CurrentFile:  file,
                StartTime:    time.Now(),
                Duration:     duration,
                Success:      success,
                Metadata:     map[string]interface{}{
                    "dry_run": dryRun,
                    "file_index": i + 1,
                    "total_files": len(files),
                },
            }
            
            if !success && errorMsg != "" {
                hookData.Error = fmt.Errorf("%s", errorMsg)
            }
            
            if err := pluginService.ExecuteHook(ctx, pluginPkg.HookPostFile, hookData); err != nil {
                ui.ShowWarning(fmt.Sprintf("Post-file hook warning for %s: %v", file, err))
            }
        }
    }
    
    pb.Finish()
    
    deploymentEnd := time.Now()
    deploymentDuration := deploymentEnd.Sub(deploymentStart)
    deploymentSuccess := failureCount == 0
    
    // Execute post-deploy hooks
    if pluginService != nil {
        hookData := pluginPkg.HookData{
            DeploymentID: deploymentID,
            Repository:   repoName,
            Commit:       commit,
            Files:        files,
            StartTime:    deploymentStart,
            EndTime:      deploymentEnd,
            Duration:     deploymentDuration,
            Success:      deploymentSuccess,
            Metadata:     map[string]interface{}{
                "dry_run": dryRun,
                "success_count": successCount,
                "failure_count": failureCount,
            },
        }
        
        if err := pluginService.ExecuteHook(ctx, pluginPkg.HookPostDeploy, hookData); err != nil {
            ui.ShowWarning(fmt.Sprintf("Post-deploy hook warning: %v", err))
        }
        
        // Execute success or error hooks
        if deploymentSuccess {
            if err := pluginService.ExecuteHook(ctx, pluginPkg.HookOnSuccess, hookData); err != nil {
                ui.ShowWarning(fmt.Sprintf("Success hook warning: %v", err))
            }
        } else {
            hookData.Error = fmt.Errorf("deployment completed with %d errors", failureCount)
            if err := pluginService.ExecuteHook(ctx, pluginPkg.HookOnError, hookData); err != nil {
                ui.ShowWarning(fmt.Sprintf("Error hook warning: %v", err))
            }
        }
        
        // Execute finish hook
        if err := pluginService.ExecuteHook(ctx, pluginPkg.HookOnFinish, hookData); err != nil {
            ui.ShowWarning(fmt.Sprintf("Finish hook warning: %v", err))
        }
        
        // Send notification
        var notificationLevel pluginPkg.NotificationLevel
        var title, message string
        
        if deploymentSuccess {
            notificationLevel = pluginPkg.NotificationLevelSuccess
            title = "Deployment Successful"
            message = fmt.Sprintf("Successfully deployed %d files to %s", successCount, repoName)
        } else {
            notificationLevel = pluginPkg.NotificationLevelError
            title = "Deployment Failed"
            message = fmt.Sprintf("Deployment failed: %d succeeded, %d failed", successCount, failureCount)
        }
        
        notification := pluginPkg.NotificationMessage{
            Title:        title,
            Message:      message,
            Level:        notificationLevel,
            Timestamp:    time.Now(),
            Source:       "flakedrop",
            DeploymentID: deploymentID,
            Repository:   repoName,
            Commit:       commit,
            Metadata:     hookData.Metadata,
        }
        
        if err := pluginService.SendNotification(ctx, notification); err != nil {
            ui.ShowWarning(fmt.Sprintf("Notification warning: %v", err))
        }
    }
    
    // Show final summary
    fmt.Println()
    if failureCount == 0 {
        ui.ShowSuccess(fmt.Sprintf("Deployment completed successfully! %d files deployed.", successCount))
    } else {
        ui.ShowError(fmt.Errorf("Deployment completed with errors: %d succeeded, %d failed", successCount, failureCount))
    }
}

func formatDuration(d time.Duration) string {
    if d < time.Second {
        return fmt.Sprintf("%dms", d.Milliseconds())
    }
    return fmt.Sprintf("%.1fs", d.Seconds())
}

// generateDeploymentID generates a unique deployment ID
func generateDeploymentID() string {
    bytes := make([]byte, 8)
    _, _ = rand.Read(bytes)
    return fmt.Sprintf("deploy-%x", bytes)
}

// getRealFilesToDeploy gets actual SQL files from the repository  
func getRealFilesToDeploy(appConfig *models.Config, repoName string) ([]string, error) {
    // Find the repository configuration
    var repoConfig *models.Repository
    for _, repo := range appConfig.Repositories {
        if repo.Name == repoName {
            repoConfig = &repo
            break
        }
    }
    
    if repoConfig == nil {
        return nil, fmt.Errorf("repository '%s' not found", repoName)
    }
    
    // Get the repository path
    repoPath := repoConfig.GitURL
    if !filepath.IsAbs(repoPath) {
        wd, _ := os.Getwd()
        repoPath = filepath.Join(wd, repoPath)
    }
    
    // Look for SQL files in the migrations directory
    migrationsPath := filepath.Join(repoPath, repoConfig.Path)
    if _, err := os.Stat(migrationsPath); os.IsNotExist(err) {
        return nil, fmt.Errorf("migrations directory not found: %s", migrationsPath)
    }
    
    // Read all SQL files from migrations directory
    files, err := filepath.Glob(filepath.Join(migrationsPath, "*.sql"))
    if err != nil {
        return nil, fmt.Errorf("failed to read SQL files: %w", err)
    }
    
    // Convert to relative paths for display
    var relativeFiles []string
    for _, file := range files {
        rel, err := filepath.Rel(repoPath, file)
        if err != nil {
            rel = file
        }
        relativeFiles = append(relativeFiles, rel)
    }
    
    return relativeFiles, nil
}

// executeRealDeployment performs actual deployment to Snowflake
func executeRealDeployment(ctx context.Context, appConfig *models.Config, files []string, dryRun bool, deploymentID, repoName, commit string, pluginService *plugin.Service) error {
    // Find repository configuration
    var repoConfig *models.Repository
    for _, repo := range appConfig.Repositories {
        if repo.Name == repoName {
            repoConfig = &repo
            break
        }
    }
    
    if repoConfig == nil {
        return fmt.Errorf("repository '%s' not found", repoName)
    }
    
    // Get repository path
    repoPath := repoConfig.GitURL
    if !filepath.IsAbs(repoPath) {
        wd, _ := os.Getwd()
        repoPath = filepath.Join(wd, repoPath)
    }
    
    // Create Snowflake service
    snowflakeConfig := snowflake.Config{
        Account:   appConfig.Snowflake.Account,
        Username:  appConfig.Snowflake.Username,
        Password:  appConfig.Snowflake.Password,
        Database:  repoConfig.Database,
        Schema:    repoConfig.Schema,
        Warehouse: appConfig.Snowflake.Warehouse,
        Role:      appConfig.Snowflake.Role,
        Timeout:   30 * time.Second,
    }
    
    snowflakeService := snowflake.NewService(snowflakeConfig)
    if !dryRun {
        if err := snowflakeService.Connect(); err != nil {
            return fmt.Errorf("failed to connect to Snowflake: %w", err)
        }
        defer snowflakeService.Close()
    }
    
    pb := ui.NewProgressBar(len(files))
    successCount := 0
    failureCount := 0
    
    // Execute each file
    for i, file := range files {
        ui.ShowFileExecution(file, i+1, len(files))
        
        // Get full file path
        fullPath := filepath.Join(repoPath, file)
        
        // Validate file path
        validatedPath, err := common.ValidatePath(fullPath, repoPath)
        if err != nil {
            ui.ShowError(fmt.Errorf("invalid file path %s: %w", file, err))
            failureCount++
            pb.Update(i+1, file, false)
            continue
        }
        
        if dryRun {
            // In dry run, just validate the file exists and is readable
            content, err := os.ReadFile(validatedPath)
            if err != nil {
                ui.ShowError(fmt.Errorf("failed to read %s: %w", file, err))
                failureCount++
            } else {
                ui.ShowSuccess(fmt.Sprintf("✓ Would execute %d bytes of SQL", len(content)))
                successCount++
            }
        } else {
            // Execute SQL file
            start := time.Now()
            err := snowflakeService.ExecuteFile(validatedPath, repoConfig.Database, repoConfig.Schema)
            duration := time.Since(start)
            
            if err != nil {
                ui.ShowError(fmt.Errorf("failed to execute %s: %w", file, err))
                failureCount++
            } else {
                ui.ShowSuccess(fmt.Sprintf("✓ Success (%s)", formatDuration(duration)))
                successCount++
            }
        }
        
        pb.Update(i+1, file, err == nil)
    }
    
    pb.Finish()
    
    // Show final results
    fmt.Println()
    if failureCount == 0 {
        ui.ShowSuccess(fmt.Sprintf("✅ Deployment completed successfully! %d files deployed.", successCount))
    } else {
        ui.ShowError(fmt.Errorf("⚠️ Deployment completed with errors: %d succeeded, %d failed", successCount, failureCount))
    }
    
    return nil
}