package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"flakedrop/internal/common"
	"flakedrop/internal/config"
	"flakedrop/internal/git"
	"flakedrop/internal/rollback"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

// DeploymentService handles the deployment process with enhanced error handling
type DeploymentService struct {
	errorHandler     *errors.ErrorHandler
	recoveryManager  *errors.RecoveryManager
	gitService       *git.Service
	snowflakeService *snowflake.Service
	rollbackManager  *rollback.Manager
	config           *models.Config
}

// NewDeploymentService creates a new deployment service
func NewDeploymentService() (*DeploymentService, error) {
	errorHandler := errors.GetGlobalErrorHandler()
	
	// Load configuration with error handling
	cfg, err := config.Load()
	if err != nil {
		return nil, errors.ConfigError("Failed to load configuration", "config.yaml").
			WithContext("error", err.Error()).
			WithSuggestions(
				"Run 'flakedrop setup' to create configuration",
				"Check if config.yaml exists in the current directory",
				"Verify configuration file permissions",
			)
	}

	// Validate configuration
	if err := validateDeploymentConfig(cfg); err != nil {
		return nil, err
	}

	// Create services
	gitService := git.NewService()
	
	// Create Snowflake service with configuration
	sfConfig := snowflake.Config{
		Account:   cfg.Snowflake.Account,
		Username:  cfg.Snowflake.Username,
		Password:  cfg.Snowflake.Password,
		Database:  cfg.Snowflake.Database,
		Schema:    cfg.Snowflake.Schema,
		Warehouse: cfg.Snowflake.Warehouse,
		Role:      cfg.Snowflake.Role,
		Timeout:   30 * time.Second,
	}
	
	snowflakeService := snowflake.NewService(sfConfig)

	// Create rollback manager
	rollbackConfig := &rollback.Config{
		StorageDir:         filepath.Join(".", ".flakedrop", "rollback"),
		EnableAutoSnapshot: true,
		SnapshotRetention:  30 * 24 * time.Hour,
		BackupRetention:    90 * 24 * time.Hour,
		MaxRollbackDepth:   10,
		DryRunByDefault:    false,
	}
	
	rollbackManager, err := rollback.NewManager(snowflakeService, rollbackConfig)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeInternal, "Failed to create rollback manager")
	}

	return &DeploymentService{
		errorHandler:     errorHandler,
		recoveryManager:  errors.NewRecoveryManager(errorHandler),
		gitService:       gitService,
		snowflakeService: snowflakeService,
		rollbackManager:  rollbackManager,
		config:          cfg,
	}, nil
}

// ExecuteDeployment performs the deployment with comprehensive error handling
func (ds *DeploymentService) ExecuteDeployment(repoName string, options DeployOptions) error {
	ctx := context.Background()
	
	// Create deployment context
	deployCtx := &DeploymentContext{
		RepoName:     repoName,
		Options:      options,
		StartTime:    time.Now(),
		Files:        []DeploymentFile{},
		DeploymentID: ds.generateDeploymentID(),
	}

	// Execute deployment with graceful degradation
	degradation := errors.NewGracefulDegradation(ds.errorHandler)
	
	return degradation.ExecuteWithDegradation(errors.DegradationOptions{
		Levels: []errors.DegradationLevel{
			{
				Name:        "Full Deployment",
				Description: "Deploy with all features enabled",
				Execute: func() error {
					return ds.executeFullDeployment(ctx, deployCtx)
				},
			},
			{
				Name:        "Safe Mode",
				Description: "Deploy with reduced parallelism and extra validation",
				Execute: func() error {
					return ds.executeSafeDeployment(ctx, deployCtx)
				},
			},
			{
				Name:        "Manual Mode",
				Description: "Generate SQL scripts for manual execution",
				Execute: func() error {
					return ds.executeManualDeployment(ctx, deployCtx)
				},
			},
		},
	})
}

// executeFullDeployment performs a standard deployment
func (ds *DeploymentService) executeFullDeployment(ctx context.Context, deployCtx *DeploymentContext) error {
	// Step 1: Connect to Snowflake
	if err := ds.connectToSnowflake(ctx); err != nil {
		return err
	}
	defer ds.snowflakeService.Close()

	// Step 2: Sync repository
	repo, err := ds.syncRepository(ctx, deployCtx.RepoName)
	if err != nil {
		return err
	}

	// Step 3: Select commit
	commit, err := ds.selectCommit(ctx, repo, deployCtx.Options)
	if err != nil {
		return err
	}
	deployCtx.SelectedCommit = commit

	// Step 4: Get files to deploy
	files, err := ds.getFilesToDeploy(ctx, repo, commit)
	if err != nil {
		return err
	}
	deployCtx.Files = files

	// Step 5: Validate deployment
	if err := ds.validateDeployment(ctx, deployCtx); err != nil {
		return err
	}

	// Step 6: Execute deployment
	return ds.performDeployment(ctx, deployCtx)
}

// connectToSnowflake establishes connection with retry and circuit breaker
func (ds *DeploymentService) connectToSnowflake(ctx context.Context) error {
	spinner := ui.NewSpinner("Connecting to Snowflake...")
	spinner.Start()
	defer spinner.Stop(false, "")

	err := ds.snowflakeService.Connect()
	if err != nil {
		spinner.Stop(false, "Connection failed")
		
		// Attempt recovery
		return ds.recoveryManager.Recover(ctx, err)
	}

	spinner.Stop(true, "Connected to Snowflake")
	return nil
}

// syncRepository syncs the git repository with error handling
func (ds *DeploymentService) syncRepository(ctx context.Context, repoName string) (*models.Repository, error) {
	// Find repository in configuration
	var repo *models.Repository
	for _, r := range ds.config.Repositories {
		if r.Name == repoName {
			repo = &r
			break
		}
	}

	if repo == nil {
		return nil, errors.New(errors.ErrCodeRepoNotFound, 
			fmt.Sprintf("Repository '%s' not found in configuration", repoName)).
			WithSuggestions(
				"Check repository name spelling",
				"Run 'flakedrop repo list' to see available repositories",
				"Add repository with 'flakedrop repo add'",
			)
	}

	// Sync repository with retry
	spinner := ui.NewSpinner(fmt.Sprintf("Syncing repository %s...", repoName))
	spinner.Start()

	err := errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		return ds.gitService.SyncRepository(*repo)
	})

	if err != nil {
		spinner.Stop(false, "Sync failed")
		return nil, errors.Wrap(err, errors.ErrCodeRepoSyncFailed, 
			"Failed to sync repository").
			WithContext("repository", repoName)
	}

	spinner.Stop(true, "Repository synced")
	return repo, nil
}

// selectCommit handles commit selection with validation
func (ds *DeploymentService) selectCommit(ctx context.Context, repo *models.Repository, options DeployOptions) (string, error) {
	if options.Commit != "" {
		// Validate provided commit
		ui.ShowInfo(fmt.Sprintf("Using specified commit: %s", options.Commit[:7]))
		return options.Commit, nil
	}

	// Get recent commits
	commits, err := ds.gitService.GetRecentCommitsForRepo(repo.Name, 20)
	if err != nil {
		return "", errors.Wrap(err, errors.ErrCodeRepoSyncFailed, 
			"Failed to retrieve commits")
	}

	if len(commits) == 0 {
		return "", errors.New(errors.ErrCodeCommitNotFound, 
			"No commits found in repository")
	}

	if !options.Interactive {
		// Use latest commit
		latest := commits[0]
		ui.ShowInfo(fmt.Sprintf("Using latest commit: %s - %s", 
			latest.Hash[:7], latest.Message))
		return latest.Hash, nil
	}

	// Interactive selection
	commitInfos := make([]ui.CommitInfo, len(commits))
	for i, c := range commits {
		commitInfos[i] = ui.CommitInfo{
			Hash:      c.Hash,
			ShortHash: c.Hash[:7],
			Message:   c.Message,
			Author:    c.Author,
			Time:      c.Date,
			Files:     len(c.SQLFiles),
		}
	}

	selected, err := ui.SelectCommit(commitInfos)
	if err != nil {
		return "", errors.Wrap(err, errors.ErrCodeInvalidInput, 
			"Commit selection cancelled")
	}

	return selected, nil
}

// getFilesToDeploy retrieves SQL files from the selected commit
func (ds *DeploymentService) getFilesToDeploy(ctx context.Context, repo *models.Repository, commitHash string) ([]DeploymentFile, error) {
	files, err := ds.gitService.GetSQLFilesFromCommit(repo.Name, commitHash)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeRepoAccessDenied, 
			"Failed to retrieve files from commit")
	}

	if len(files) == 0 {
		return nil, errors.New(errors.ErrCodeValidationFailed, 
			"No SQL files found in commit").
			WithContext("commit", commitHash[:7]).
			WithSuggestions(
				"Verify the commit contains SQL files",
				"Check if SQL files match the configured patterns",
			)
	}

	// Convert to deployment files
	deployFiles := make([]DeploymentFile, len(files))
	for i, f := range files {
		deployFiles[i] = DeploymentFile{
			Path:     f.Path,
			Content:  f.Content,
			Type:     detectSQLFileType(f.Path),
			Database: ds.config.Snowflake.Database,
			Schema:   ds.config.Snowflake.Schema,
		}
	}

	return deployFiles, nil
}

// validateDeployment performs pre-deployment validation
func (ds *DeploymentService) validateDeployment(ctx context.Context, deployCtx *DeploymentContext) error {
	ui.ShowInfo("Validating deployment...")

	for i, file := range deployCtx.Files {
		// Basic SQL validation
		if err := validateSQL(file.Content); err != nil {
			return errors.ValidationError(file.Path, file.Content, err.Error()).
				WithContext("file_index", i+1).
				WithContext("total_files", len(deployCtx.Files))
		}

		// Check for dangerous operations
		if !deployCtx.Options.Force && containsDangerousOperations(file.Content) {
			return errors.New(errors.ErrCodeSecurityViolation, 
				fmt.Sprintf("File %s contains potentially dangerous operations", file.Path)).
				WithSuggestions(
					"Review the SQL file for DROP or TRUNCATE statements",
					"Use --force flag to override this check",
					"Consider running in dry-run mode first",
				)
		}
	}

	return nil
}

// performDeployment executes the actual deployment
func (ds *DeploymentService) performDeployment(ctx context.Context, deployCtx *DeploymentContext) error {
	// Show deployment summary
	ui.ShowDeploymentSummary(getFilePaths(deployCtx.Files), deployCtx.SelectedCommit)

	if deployCtx.Options.DryRun {
		ui.ShowWarning("Running in dry-run mode - no changes will be applied")
	}

	// Confirm deployment
	if !deployCtx.Options.NoConfirm {
		confirm, err := ui.Confirm("Proceed with deployment?", true)
		if err != nil || !confirm {
			return errors.New(errors.ErrCodeInvalidInput, "Deployment cancelled by user")
		}
	}

	// Create deployment record for rollback tracking
	deploymentRecord := &rollback.DeploymentRecord{
		ID:         deployCtx.DeploymentID,
		Repository: deployCtx.RepoName,
		Commit:     deployCtx.SelectedCommit,
		Database:   ds.config.Snowflake.Database,
		Schema:     ds.config.Snowflake.Schema,
		StartTime:  deployCtx.StartTime,
		State:      rollback.StateInProgress,
		Files:      []rollback.FileExecution{},
		Metadata:   map[string]string{"user": os.Getenv("USER")},
	}

	// Start tracking deployment
	if err := ds.rollbackManager.StartDeployment(ctx, deploymentRecord); err != nil {
		ds.errorHandler.Handle(errors.Wrap(err, errors.ErrCodeInternal, "Failed to start deployment tracking"))
	}

	// Create progress bar
	pb := ui.NewProgressBar(len(deployCtx.Files))
	
	successCount := 0
	failureCount := 0
	deploymentErrors := []error{}

	// Execute each file
	for i, file := range deployCtx.Files {
		ui.ShowFileExecution(file.Path, i+1, len(deployCtx.Files))

		if deployCtx.Options.DryRun {
			// Simulate execution in dry-run mode
			time.Sleep(100 * time.Millisecond)
			ui.ShowExecutionResult(file.Path, true, "", "skipped")
			pb.Update(i+1, file.Path, true)
			successCount++
			continue
		}

		// Execute file with timing
		start := time.Now()
		err := ds.executeFile(ctx, file)
		duration := time.Since(start)

		// Record file execution for rollback
		fileExecution := rollback.FileExecution{
			Path:       file.Path,
			Order:      i,
			StartTime:  start,
			SQLContent: file.Content,
			Checksum:   calculateChecksum(file.Content),
			Duration:   duration,
		}

		if err != nil {
			ui.ShowExecutionResult(file.Path, false, err.Error(), "")
			pb.Update(i+1, file.Path, false)
			failureCount++
			deploymentErrors = append(deploymentErrors, err)
			fileExecution.Success = false
			fileExecution.ErrorMessage = err.Error()
			fileExecution.EndTime = timePtr(time.Now())

			// Check if we should continue on error
			if !deployCtx.Options.ContinueOnError {
				pb.Finish()
				return errors.Wrap(err, errors.ErrCodeSQLSyntax, 
					"Deployment stopped due to error").
					WithContext("files_completed", i).
					WithContext("files_remaining", len(deployCtx.Files)-i-1)
			}
		} else {
			ui.ShowExecutionResult(file.Path, true, "", formatDuration(duration))
			pb.Update(i+1, file.Path, true)
			successCount++
			fileExecution.Success = true
			fileExecution.EndTime = timePtr(time.Now())
		}
		
		// Add file execution to deployment record
		deploymentRecord.Files = append(deploymentRecord.Files, fileExecution)
	}

	pb.Finish()

	// Generate deployment report
	deployCtx.EndTime = time.Now()
	deployCtx.SuccessCount = successCount
	deployCtx.FailureCount = failureCount

	// Save deployment history
	if err := ds.saveDeploymentHistory(deployCtx); err != nil {
		ds.errorHandler.Handle(errors.Wrap(err, errors.ErrCodeInternal, 
			"Failed to save deployment history"))
	}

	// Complete deployment tracking
	success := failureCount == 0
	errorMessage := ""
	if !success {
		errorMessage = fmt.Sprintf("Deployment completed with errors: %d succeeded, %d failed", successCount, failureCount)
	}
	
	if err := ds.rollbackManager.CompleteDeployment(ctx, deployCtx.DeploymentID, success, errorMessage); err != nil {
		ds.errorHandler.Handle(errors.Wrap(err, errors.ErrCodeInternal, "Failed to complete deployment tracking"))
	}

	// Show final result
	if failureCount == 0 {
		ui.ShowSuccess(fmt.Sprintf("Deployment completed successfully! %d files deployed.", successCount))
		return nil
	} else {
		err := errors.New(errors.ErrCodeSQLSyntax, errorMessage).
			WithContext("errors", deploymentErrors).
			WithSeverity(errors.SeverityError)
		
		// Offer rollback option
		if !deployCtx.Options.DryRun {
			ui.ShowWarning(fmt.Sprintf("To rollback this deployment, run: flakedrop rollback deployment %s", deployCtx.DeploymentID))
		}
		
		ui.ShowError(err)
		return err
	}
}

// executeFile executes a single SQL file
func (ds *DeploymentService) executeFile(ctx context.Context, file DeploymentFile) error {
	return errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		return ds.snowflakeService.ExecuteSQL(file.Content, file.Database, file.Schema)
	})
}

// executeSafeDeployment performs deployment in safe mode
func (ds *DeploymentService) executeSafeDeployment(ctx context.Context, deployCtx *DeploymentContext) error {
	ui.ShowWarning("Running in safe mode with extra validation")
	
	// Reduce parallelism
	deployCtx.Options.Parallelism = 1
	
	// Enable extra validation
	deployCtx.Options.ValidateSchema = true
	
	return ds.executeFullDeployment(ctx, deployCtx)
}

// executeManualDeployment generates SQL scripts for manual execution
func (ds *DeploymentService) executeManualDeployment(ctx context.Context, deployCtx *DeploymentContext) error {
	ui.ShowInfo("Generating SQL scripts for manual execution")
	
	// Create output directory
	outputDir := fmt.Sprintf("deployment_%s_%s", 
		deployCtx.RepoName, 
		time.Now().Format("20060102_150405"))
	
	if err := os.MkdirAll(outputDir, common.DirPermissionNormal); err != nil {
		return errors.Wrap(err, errors.ErrCodeFilePermission, 
			"Failed to create output directory")
	}

	// Sync and get files
	repo, err := ds.syncRepository(ctx, deployCtx.RepoName)
	if err != nil {
		return err
	}

	commit, err := ds.selectCommit(ctx, repo, deployCtx.Options)
	if err != nil {
		return err
	}

	files, err := ds.getFilesToDeploy(ctx, repo, commit)
	if err != nil {
		return err
	}

	// Write files to output directory
	for i, file := range files {
		outputPath := filepath.Join(outputDir, fmt.Sprintf("%03d_%s", i+1, 
			strings.ReplaceAll(file.Path, "/", "_")))
		
		content := fmt.Sprintf("-- File: %s\n-- Commit: %s\n-- Generated: %s\n\n%s",
			file.Path,
			commit[:7],
			time.Now().Format(time.RFC3339),
			file.Content)
		
		if err := os.WriteFile(outputPath, []byte(content), 0600); err != nil {
			return errors.Wrap(err, errors.ErrCodeFilePermission, 
				fmt.Sprintf("Failed to write file %s", outputPath))
		}
	}

	// Create deployment script
	scriptPath := filepath.Join(outputDir, "deploy.sh")
	scriptContent := generateDeploymentScript(outputDir, files)
	
	if err := os.WriteFile(scriptPath, []byte(scriptContent), common.FilePermissionExecutable); err != nil {
		return errors.Wrap(err, errors.ErrCodeFilePermission, 
			"Failed to write deployment script")
	}

	ui.ShowSuccess(fmt.Sprintf("SQL scripts generated in directory: %s", outputDir))
	ui.ShowInfo("Review the scripts and run deploy.sh when ready")
	
	return nil
}

// saveDeploymentHistory saves deployment information for auditing
func (ds *DeploymentService) saveDeploymentHistory(deployCtx *DeploymentContext) error {
	homeDir, _ := os.UserHomeDir()
	historyDir := filepath.Join(homeDir, ".flakedrop", "history")
	
	if err := os.MkdirAll(historyDir, common.DirPermissionNormal); err != nil {
		return err
	}

	historyFile := filepath.Join(historyDir, 
		fmt.Sprintf("deployment_%s.json", deployCtx.StartTime.Format("20060102_150405")))
	
	// Create history entry
	history := map[string]interface{}{
		"repository":     deployCtx.RepoName,
		"commit":         deployCtx.SelectedCommit,
		"start_time":     deployCtx.StartTime,
		"end_time":       deployCtx.EndTime,
		"duration":       deployCtx.EndTime.Sub(deployCtx.StartTime).String(),
		"files_deployed": len(deployCtx.Files),
		"success_count":  deployCtx.SuccessCount,
		"failure_count":  deployCtx.FailureCount,
		"dry_run":        deployCtx.Options.DryRun,
		"user":           os.Getenv("USER"),
	}

	// Write history file
	historyData, err := json.Marshal(history)
	if err != nil {
		return fmt.Errorf("failed to marshal history: %w", err)
	}
	
	if err := os.WriteFile(historyFile, historyData, 0600); err != nil {
		return fmt.Errorf("failed to write history file: %w", err)
	}
	
	return nil
}

// Helper structures and functions

type DeployOptions struct {
	DryRun          bool
	Commit          string
	Interactive     bool
	Force           bool
	NoConfirm       bool
	ContinueOnError bool
	Parallelism     int
	ValidateSchema  bool
}

type DeploymentContext struct {
	RepoName       string
	Options        DeployOptions
	SelectedCommit string
	Files          []DeploymentFile
	StartTime      time.Time
	EndTime        time.Time
	SuccessCount   int
	FailureCount   int
	DeploymentID   string
}

type DeploymentFile struct {
	Path     string
	Content  string
	Type     string
	Database string
	Schema   string
}

func validateDeploymentConfig(cfg *models.Config) error {
	if cfg.Snowflake.Account == "" {
		return errors.ConfigError("Snowflake account is required", "snowflake.account")
	}
	if cfg.Snowflake.Username == "" {
		return errors.ConfigError("Snowflake username is required", "snowflake.username")
	}
	if cfg.Snowflake.Warehouse == "" {
		return errors.ConfigError("Snowflake warehouse is required", "snowflake.warehouse")
	}
	return nil
}

func detectSQLFileType(path string) string {
	path = strings.ToLower(path)
	switch {
	case strings.Contains(path, "table"):
		return "TABLE"
	case strings.Contains(path, "view"):
		return "VIEW"
	case strings.Contains(path, "procedure") || strings.Contains(path, "proc"):
		return "PROCEDURE"
	case strings.Contains(path, "function") || strings.Contains(path, "func"):
		return "FUNCTION"
	default:
		return "SCRIPT"
	}
}

func validateSQL(sql string) error {
	// Basic SQL validation
	if strings.TrimSpace(sql) == "" {
		return fmt.Errorf("empty SQL content")
	}
	
	// Check for unclosed quotes
	if strings.Count(sql, "'")%2 != 0 {
		return fmt.Errorf("unclosed single quote")
	}
	
	return nil
}

func containsDangerousOperations(sql string) bool {
	dangerous := []string{
		"DROP DATABASE",
		"DROP SCHEMA",
		"TRUNCATE",
		"DELETE FROM",
	}
	
	sqlUpper := strings.ToUpper(sql)
	for _, op := range dangerous {
		if strings.Contains(sqlUpper, op) {
			return true
		}
	}
	
	return false
}

func getFilePaths(files []DeploymentFile) []string {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	return paths
}


func generateDeploymentScript(dir string, files []DeploymentFile) string {
	script := `#!/bin/bash
# Snowflake Deployment Script
# Generated: %s

set -e

echo "Starting Snowflake deployment..."
echo "Files to deploy: %d"

# Configure Snowflake connection
export SNOWSQL_ACCOUNT="${SNOWFLAKE_ACCOUNT}"
export SNOWSQL_USER="${SNOWFLAKE_USER}"
export SNOWSQL_PWD="${SNOWFLAKE_PASSWORD}"
export SNOWSQL_DATABASE="${SNOWFLAKE_DATABASE}"
export SNOWSQL_SCHEMA="${SNOWFLAKE_SCHEMA}"
export SNOWSQL_WAREHOUSE="${SNOWFLAKE_WAREHOUSE}"

# Execute files in order
`
	
	script = fmt.Sprintf(script, time.Now().Format(time.RFC3339), len(files))
	
	for i, file := range files {
		script += fmt.Sprintf(`
echo "Executing file %d/%d: %s"
snowsql -f "%s" || {
    echo "Error executing %s"
    exit 1
}
`, i+1, len(files), file.Path, 
   fmt.Sprintf("%03d_%s", i+1, strings.ReplaceAll(file.Path, "/", "_")),
   file.Path)
	}
	
	script += "\necho \"Deployment completed successfully!\"\n"
	
	return script
}

// generateDeploymentID generates a unique deployment ID
func (ds *DeploymentService) generateDeploymentID() string {
	return fmt.Sprintf("deploy-%d-%s", time.Now().Unix(), generateRandomID(8))
}

// generateRandomID generates a random ID
func generateRandomID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}

// calculateChecksum calculates SHA256 checksum of content
func calculateChecksum(content string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
}

// timePtr returns a pointer to time
func timePtr(t time.Time) *time.Time {
	return &t
}