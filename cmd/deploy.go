package cmd

import (
    "context"
    "fmt"
    "time"
    "crypto/rand"

    "github.com/spf13/cobra"
    "flakedrop/internal/ui"
    "flakedrop/internal/config"
    "flakedrop/internal/plugin"
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
        ui.ShowInfo(fmt.Sprintf("Using commit: %s", selectedCommit[:7]))
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
            ui.ShowInfo(fmt.Sprintf("Using latest commit: %s", selectedCommit[:7]))
        }
    }
    
    // Get files to deploy (simulated)
    files := getFilesToDeploy(selectedCommit)
    
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
    
    // Execute deployment with plugin integration
    executeDeployment(ctx, files, deployDryRun, deploymentID, repoName, selectedCommit, pluginService)
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