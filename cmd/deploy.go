package cmd

import (
    "context"
    "fmt"
    "time"
    "crypto/rand"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"

    "github.com/spf13/cobra"
    "flakedrop/internal/ui"
    "flakedrop/internal/config"
    "flakedrop/internal/plugin"
    "flakedrop/internal/snowflake"
    "flakedrop/internal/rollback"
    "flakedrop/internal/common"
    "flakedrop/internal/analytics"
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
    ctx := cmd.Context()
    if ctx == nil {
        ctx = context.Background()
    }
    repoName := args[0]
    
    // Track deployment start
    deploymentStartTime := time.Now()
    
    // Load configuration
    appConfig, err := config.Load()
    if err != nil {
        ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
        analytics.TrackError(ctx, err, "deploy_config_load")
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
    
    // Note: Actual Snowflake connection happens in executeRealDeployment
    ui.ShowInfo("Preparing deployment...")
    
    // Get real commits from git repository
    commits, err := getRealCommits(appConfig, repoName)
    if err != nil {
        ui.ShowError(fmt.Errorf("failed to get commits: %w", err))
        return
    }
    
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
    
    // Track deployment completion
    deploymentDuration := time.Since(deploymentStartTime)
    deploymentMetadata := map[string]interface{}{
        "repository":   repoName,
        "commit":       selectedCommit,
        "files_count":  len(files),
        "dry_run":      deployDryRun,
        "deployment_id": deploymentID,
    }
    
    if err != nil {
        ui.ShowError(err)
        analytics.TrackError(ctx, err, "deployment_failed")
        analytics.TrackDeployment(ctx, false, deploymentDuration, deploymentMetadata)
        
        // Track REAL deployment failure to telemetry (not dry-run)
        if !deployDryRun {
            manager := analytics.GetManager()
            manager.TrackRealDeployment(false, len(files))
        }
        return
    }
    
    // Track successful deployment
    analytics.TrackDeployment(ctx, true, deploymentDuration, deploymentMetadata)
    
    // Track REAL deployment to telemetry (not dry-run)
    if !deployDryRun {
        manager := analytics.GetManager()
        manager.TrackRealDeployment(true, len(files))
    }
    
    // Track feature usage
    if deployDryRun {
        analytics.TrackFeatureUsage(ctx, "dry_run", nil)
    }
    if !deployInteractive {
        analytics.TrackFeatureUsage(ctx, "non_interactive_deploy", nil)
    }
}

// getRealCommits fetches actual commits from the git repository
func getRealCommits(appConfig *models.Config, repoName string) ([]ui.CommitInfo, error) {
    // Find repository configuration
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
    
    // Get repository path
    repoPath := repoConfig.GitURL
    if !filepath.IsAbs(repoPath) {
        wd, _ := os.Getwd()
        repoPath = filepath.Join(wd, repoPath)
    }
    
    // Change to repository directory
    originalDir, err := os.Getwd()
    if err != nil {
        return nil, fmt.Errorf("failed to get current directory: %w", err)
    }
    
    if err := os.Chdir(repoPath); err != nil {
        return nil, fmt.Errorf("failed to change to repository directory: %w", err)
    }
    defer os.Chdir(originalDir)
    
    // Get recent commits using git log
    cmd := exec.Command("git", "log", "--pretty=format:%H|%h|%s|%an|%at", "-n", "10")
    output, err := cmd.Output()
    if err != nil {
        return nil, fmt.Errorf("failed to get git commits: %w", err)
    }
    
    var commits []ui.CommitInfo
    lines := strings.Split(string(output), "\n")
    
    for _, line := range lines {
        if line == "" {
            continue
        }
        
        parts := strings.Split(line, "|")
        if len(parts) < 5 {
            continue
        }
        
        timestamp, _ := strconv.ParseInt(parts[4], 10, 64)
        
        commits = append(commits, ui.CommitInfo{
            Hash:      parts[0],
            ShortHash: parts[1],
            Message:   parts[2],
            Author:    parts[3],
            Time:      time.Unix(timestamp, 0),
            Files:     0, // We can get this with additional git commands if needed
        })
    }
    
    // If no commits found, add HEAD as fallback
    if len(commits) == 0 {
        commits = append(commits, ui.CommitInfo{
            Hash:      "HEAD",
            ShortHash: "HEAD",
            Message:   "Current HEAD",
            Author:    "Unknown",
            Time:      time.Now(),
            Files:     0,
        })
    }
    
    return commits, nil
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
    
    // Initialize rollback manager if enabled
    var rollbackManager *rollback.Manager
    if appConfig.Deployment.Rollback.Enabled && !dryRun {
        rollbackConfig := &rollback.Config{
            StorageDir:         filepath.Join(os.Getenv("HOME"), ".flakedrop", "rollback"),
            EnableAutoSnapshot: appConfig.Deployment.Rollback.OnFailure,
            SnapshotRetention:  time.Duration(appConfig.Deployment.Rollback.BackupRetention) * 24 * time.Hour,
            BackupRetention:    time.Duration(appConfig.Deployment.Rollback.BackupRetention) * 24 * time.Hour,
            MaxRollbackDepth:   10,
            DryRunByDefault:    false,
        }
        
        var err error
        rollbackManager, err = rollback.NewManager(snowflakeService, rollbackConfig)
        if err != nil {
            ui.ShowWarning(fmt.Sprintf("Failed to initialize rollback manager: %v", err))
            // Continue without rollback support
        }
    }
    
    // Start deployment tracking
    var deploymentRecord *rollback.DeploymentRecord
    if rollbackManager != nil {
        deploymentRecord = &rollback.DeploymentRecord{
            ID:           deploymentID,
            Repository:   repoName,
            Commit:       commit,
            Database:     repoConfig.Database,
            Schema:       repoConfig.Schema,
            StartTime:    time.Now(),
            State:        rollback.StateInProgress,
            Files:        make([]rollback.FileExecution, 0),
            Metadata:     make(map[string]string),
        }
        
        if err := rollbackManager.StartDeployment(ctx, deploymentRecord); err != nil {
            ui.ShowWarning(fmt.Sprintf("Failed to start deployment tracking: %v", err))
        }
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
            content, err := os.ReadFile(filepath.Clean(validatedPath))
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
                // Track file execution failure
                analytics.TrackFeatureUsage(ctx, "file_execution_failed", map[string]interface{}{
                    "file": file,
                    "duration_ms": duration.Milliseconds(),
                })
            } else {
                ui.ShowSuccess(fmt.Sprintf("✓ Success (%s)", formatDuration(duration)))
                successCount++
                // Track successful file execution
                analytics.TrackFeatureUsage(ctx, "file_execution_success", map[string]interface{}{
                    "file": file,
                    "duration_ms": duration.Milliseconds(),
                })
            }
        }
        
        pb.Update(i+1, file, err == nil)
    }
    
    pb.Finish()
    
    // Complete deployment tracking and handle rollback if needed
    if rollbackManager != nil && deploymentRecord != nil {
        success := failureCount == 0
        errorMessage := ""
        if !success {
            errorMessage = fmt.Sprintf("Deployment failed with %d errors", failureCount)
        }
        
        // Complete the deployment
        if err := rollbackManager.CompleteDeployment(ctx, deploymentID, success, errorMessage); err != nil {
            ui.ShowWarning(fmt.Sprintf("Failed to complete deployment tracking: %v", err))
        }
        
        // If deployment failed and auto-rollback is enabled, perform rollback
        if !success && appConfig.Deployment.Rollback.OnFailure {
            ui.ShowWarning("Deployment failed. Initiating automatic rollback...")
            
            // Prepare rollback options
            rollbackOptions := &rollback.RollbackOptions{
                Strategy:    rollback.RollbackStrategy(appConfig.Deployment.Rollback.Strategy),
                DryRun:      false,
                StopOnError: false,
            }
            
            // Execute rollback
            result, err := rollbackManager.RollbackDeployment(ctx, deploymentID, rollbackOptions)
            if err != nil {
                ui.ShowError(fmt.Errorf("rollback failed: %w", err))
                analytics.TrackError(ctx, err, "auto_rollback_failed")
            } else {
                ui.ShowSuccess(fmt.Sprintf("Rollback completed successfully. %d operations executed.", len(result.Operations)))
                analytics.TrackFeatureUsage(ctx, "auto_rollback_success", map[string]interface{}{
                    "operations_count": len(result.Operations),
                    "deployment_id": deploymentID,
                })
            }
        }
    }
    
    // Show final results
    fmt.Println()
    if failureCount == 0 {
        ui.ShowSuccess(fmt.Sprintf("✅ Deployment completed successfully! %d files deployed.", successCount))
    } else {
        ui.ShowError(fmt.Errorf("⚠️ Deployment completed with errors: %d succeeded, %d failed", successCount, failureCount))
    }
    
    return nil
}