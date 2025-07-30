package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"flakedrop/internal/config"
	"flakedrop/internal/rollback"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

var (
	rollbackDryRun      bool
	rollbackDeployment  string
	rollbackToTime      string
	rollbackStrategy    string
	rollbackInteractive bool
	rollbackForce       bool
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "Rollback deployments and manage disaster recovery",
	Long: `Manage rollback operations and disaster recovery for FlakeDrops.
	
This command provides comprehensive rollback capabilities including:
- Automated rollback of failed deployments
- Point-in-time recovery
- Schema snapshot management
- Backup and restore functionality`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

var rollbackDeploymentCmd = &cobra.Command{
	Use:   "deployment [deployment-id]",
	Short: "Rollback a specific deployment",
	Long: `Rollback a specific deployment to its previous state.
	
This command will restore the schema to the state before the specified deployment.
If a pre-deployment snapshot exists, it will be used for fast recovery.`,
	Args: cobra.ExactArgs(1),
	Run:  runRollbackDeployment,
}

var rollbackListCmd = &cobra.Command{
	Use:   "list [database] [schema]",
	Short: "List deployment history and recovery points",
	Long:  `List deployment history and available recovery points for a database/schema.`,
	Args:  cobra.RangeArgs(0, 2),
	Run:   runRollbackList,
}

var rollbackRecoverCmd = &cobra.Command{
	Use:   "recover [database] [schema]",
	Short: "Perform point-in-time recovery",
	Long: `Perform point-in-time recovery to restore a schema to a specific point in time.
	
This command will find the closest recovery point and restore the schema state.`,
	Args: cobra.ExactArgs(2),
	Run:  runRollbackRecover,
}

var rollbackBackupCmd = &cobra.Command{
	Use:   "backup [database] [schema]",
	Short: "Create a backup of a schema",
	Long:  `Create a backup of a database schema including structure and optionally data.`,
	Args:  cobra.ExactArgs(2),
	Run:   runRollbackBackup,
}

var rollbackRestoreCmd = &cobra.Command{
	Use:   "restore [backup-id]",
	Short: "Restore from a backup",
	Long:  `Restore a database schema from a previously created backup.`,
	Args:  cobra.ExactArgs(1),
	Run:   runRollbackRestore,
}

var rollbackValidateCmd = &cobra.Command{
	Use:   "validate [deployment-id]",
	Short: "Validate if a rollback is possible",
	Long:  `Validate if a deployment can be rolled back and check for potential issues.`,
	Args:  cobra.ExactArgs(1),
	Run:   runRollbackValidate,
}

func init() {
	rootCmd.AddCommand(rollbackCmd)
	
	// Add subcommands
	rollbackCmd.AddCommand(rollbackDeploymentCmd)
	rollbackCmd.AddCommand(rollbackListCmd)
	rollbackCmd.AddCommand(rollbackRecoverCmd)
	rollbackCmd.AddCommand(rollbackBackupCmd)
	rollbackCmd.AddCommand(rollbackRestoreCmd)
	rollbackCmd.AddCommand(rollbackValidateCmd)
	
	// Global flags for rollback
	rollbackCmd.PersistentFlags().BoolVarP(&rollbackDryRun, "dry-run", "d", false, "Show what would be done without executing")
	rollbackCmd.PersistentFlags().BoolVarP(&rollbackForce, "force", "f", false, "Force rollback without confirmation")
	
	// Deployment rollback flags
	rollbackDeploymentCmd.Flags().StringVarP(&rollbackStrategy, "strategy", "s", "", "Rollback strategy: snapshot, incremental, transaction")
	
	// Recovery flags
	rollbackRecoverCmd.Flags().StringVarP(&rollbackToTime, "time", "t", "", "Target recovery time (RFC3339 format)")
	
	// List flags
	rollbackListCmd.Flags().IntP("limit", "l", 10, "Limit number of results")
}

func runRollbackDeployment(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	deploymentID := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	// Show header
	ui.ShowHeader(fmt.Sprintf("Rollback Deployment: %s", deploymentID))
	
	// Validate rollback
	ui.ShowInfo("Validating rollback...")
	validation, err := rollbackManager.ValidateRollback(ctx, deploymentID)
	if err != nil {
		ui.ShowError(fmt.Errorf("validation failed: %w", err))
		return
	}
	
	if !validation.CanRollback {
		ui.ShowError(fmt.Errorf("cannot rollback deployment: %v", validation.Errors))
		return
	}
	
	// Show warnings
	for _, warning := range validation.Warnings {
		ui.ShowWarning(warning)
	}
	
	// Prepare rollback options
	options := &rollback.RollbackOptions{
		DryRun:      rollbackDryRun,
		StopOnError: true,
	}
	
	if rollbackStrategy != "" {
		options.Strategy = rollback.RollbackStrategy(rollbackStrategy)
	}
	
	// Show rollback plan
	if rollbackDryRun {
		ui.ShowInfo("Running in dry-run mode - no changes will be made")
	}
	
	// Confirm rollback
	if !rollbackForce && !rollbackDryRun {
		confirm, err := ui.Confirm(
			fmt.Sprintf("Proceed with rollback of deployment %s?", deploymentID),
			false,
		)
		if err != nil || !confirm {
			ui.ShowWarning("Rollback cancelled")
			return
		}
	}
	
	// Execute rollback
	spinner := ui.NewSpinner("Executing rollback...")
	spinner.Start()
	
	result, err := rollbackManager.RollbackDeployment(ctx, deploymentID, options)
	spinner.Stop(result != nil && result.Success, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("rollback failed: %w", err))
		return
	}
	
	// Show results
	showRollbackResult(result)
}

func runRollbackList(cmd *cobra.Command, args []string) {
	// ctx := context.Background() // commented out as not used
	
	// Parse arguments
	var database, schema string
	if len(args) > 0 {
		database = args[0]
	}
	if len(args) > 1 {
		schema = args[1]
	}
	
	// Get limit flag
	limit, _ := cmd.Flags().GetInt("limit")
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	// If database/schema not provided, get from config
	if database == "" {
		database = appConfig.Snowflake.Database
	}
	if schema == "" {
		schema = appConfig.Snowflake.Schema
	}
	
	ui.ShowHeader(fmt.Sprintf("Deployment History: %s.%s", database, schema))
	
	// Get deployment history
	history, err := rollbackManager.GetDeploymentHistory(database, schema, limit)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to get deployment history: %w", err))
		return
	}
	
	// Display history
	fmt.Println("\nRecent Deployments:")
	fmt.Println("==================")
	for _, deployment := range history {
		showDeploymentSummary(deployment)
	}
	
	// Get recovery points
	fmt.Println("\nAvailable Recovery Points:")
	fmt.Println("=========================")
	points, err := rollbackManager.GetRecoveryPoints(database, schema)
	if err != nil {
		ui.ShowWarning(fmt.Sprintf("Failed to get recovery points: %v", err))
	} else {
		for _, point := range points {
			showRecoveryPoint(point)
		}
	}
}

func runRollbackRecover(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	database := args[0]
	schema := args[1]
	
	// Parse target time
	var targetTime time.Time
	if rollbackToTime != "" {
		var err error
		targetTime, err = time.Parse(time.RFC3339, rollbackToTime)
		if err != nil {
			ui.ShowError(fmt.Errorf("invalid time format: %w", err))
			return
		}
	} else {
		// Interactive time selection
		ui.ShowError(fmt.Errorf("please specify target time with --time flag"))
		return
	}
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	ui.ShowHeader(fmt.Sprintf("Point-in-Time Recovery: %s.%s", database, schema))
	ui.ShowInfo(fmt.Sprintf("Target time: %s", targetTime.Format(time.RFC3339)))
	
	// Prepare recovery options
	options := &rollback.RecoveryOptions{
		DryRun: rollbackDryRun,
	}
	
	if rollbackDryRun {
		ui.ShowInfo("Running in dry-run mode - no changes will be made")
	}
	
	// Confirm recovery
	if !rollbackForce && !rollbackDryRun {
		confirm, err := ui.Confirm(
			fmt.Sprintf("Proceed with recovery to %s?", targetTime.Format("2006-01-02 15:04:05")),
			false,
		)
		if err != nil || !confirm {
			ui.ShowWarning("Recovery cancelled")
			return
		}
	}
	
	// Execute recovery
	spinner := ui.NewSpinner("Performing recovery...")
	spinner.Start()
	
	result, err := rollbackManager.PointInTimeRecovery(ctx, database, schema, targetTime, options)
	spinner.Stop(result != nil && result.Success, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("recovery failed: %w", err))
		return
	}
	
	// Show results
	showRecoveryResult(result)
}

func runRollbackBackup(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	database := args[0]
	schema := args[1]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	ui.ShowHeader(fmt.Sprintf("Create Backup: %s.%s", database, schema))
	
	// Prepare backup options
	options := &rollback.BackupOptions{
		Type:            "full",
		Format:          "sql",
		Compression:     true,
		IncludeData:     true,
		IncludeMetadata: true,
	}
	
	// Create backup
	spinner := ui.NewSpinner("Creating backup...")
	spinner.Start()
	
	backup, err := rollbackManager.CreateBackup(ctx, database, schema, options)
	spinner.Stop(err == nil, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("backup failed: %w", err))
		return
	}
	
	// Show backup details
	ui.ShowSuccess("Backup created successfully!")
	fmt.Printf("\nBackup ID: %s\n", backup.ID)
	fmt.Printf("Location: %s\n", backup.Location)
	fmt.Printf("Size: %.2f MB\n", float64(backup.Size)/(1024*1024))
	fmt.Printf("Type: %s\n", backup.Type)
	fmt.Printf("Format: %s\n", backup.Format)
	fmt.Printf("Compression: %s\n", backup.CompressionType)
	fmt.Printf("Checksum: %s\n", backup.Checksum[:16]+"...")
}

func runRollbackRestore(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	backupID := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	ui.ShowHeader(fmt.Sprintf("Restore Backup: %s", backupID))
	
	// Prepare restore options
	options := &rollback.RestoreOptions{
		DryRun:          rollbackDryRun,
		RestoreData:     true,
		RestoreMetadata: true,
		Parallel:        true,
		Workers:         4,
	}
	
	if rollbackDryRun {
		ui.ShowInfo("Running in dry-run mode - no changes will be made")
	}
	
	// Confirm restore
	if !rollbackForce && !rollbackDryRun {
		confirm, err := ui.Confirm("Proceed with restore?", false)
		if err != nil || !confirm {
			ui.ShowWarning("Restore cancelled")
			return
		}
	}
	
	// Execute restore
	spinner := ui.NewSpinner("Restoring from backup...")
	spinner.Start()
	
	result, err := rollbackManager.RestoreBackup(ctx, backupID, options)
	spinner.Stop(result != nil && result.Success, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("restore failed: %w", err))
		return
	}
	
	// Show results
	showRestoreBackupResult(result)
}

func runRollbackValidate(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	deploymentID := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Initialize services
	rollbackManager, snowflakeService, err := initializeRollbackServices(appConfig)
	if err != nil {
		ui.ShowError(err)
		return
	}
	defer snowflakeService.Close()
	
	ui.ShowHeader(fmt.Sprintf("Validate Rollback: %s", deploymentID))
	
	// Validate rollback
	spinner := ui.NewSpinner("Validating...")
	spinner.Start()
	
	validation, err := rollbackManager.ValidateRollback(ctx, deploymentID)
	spinner.Stop(err == nil, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("validation failed: %w", err))
		return
	}
	
	// Show validation results
	fmt.Println("\nValidation Results:")
	fmt.Println("==================")
	fmt.Printf("Can Rollback: %v\n", validation.CanRollback)
	fmt.Printf("Affected Objects: %d\n", validation.AffectedObjects)
	
	if len(validation.Warnings) > 0 {
		fmt.Println("\nWarnings:")
		for _, warning := range validation.Warnings {
			fmt.Printf("  ⚠️  %s\n", warning)
		}
	}
	
	if len(validation.Errors) > 0 {
		fmt.Println("\nErrors:")
		for _, err := range validation.Errors {
			fmt.Printf("  ❌ %s\n", err)
		}
	}
	
	if validation.CanRollback {
		ui.ShowSuccess("Deployment can be rolled back safely")
	} else {
		ui.ShowError(errors.New(errors.ErrCodeValidationFailed, "Deployment cannot be rolled back"))
	}
}

// Helper functions

func initializeRollbackServices(appConfig *models.Config) (*rollback.Manager, *snowflake.Service, error) {
	// Create Snowflake service
	snowflakeConfig := snowflake.Config{
		Account:   appConfig.Snowflake.Account,
		Username:  appConfig.Snowflake.Username,
		Password:  appConfig.Snowflake.Password,
		Database:  appConfig.Snowflake.Database,
		Schema:    appConfig.Snowflake.Schema,
		Warehouse: appConfig.Snowflake.Warehouse,
		Role:      appConfig.Snowflake.Role,
		Timeout:   30 * time.Second,
	}
	
	snowflakeService := snowflake.NewService(snowflakeConfig)
	if err := snowflakeService.Connect(); err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Snowflake: %w", err)
	}
	
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
		_ = snowflakeService.Close()
		return nil, nil, fmt.Errorf("failed to create rollback manager: %w", err)
	}
	
	return rollbackManager, snowflakeService, nil
}

func showDeploymentSummary(deployment *rollback.DeploymentRecord) {
	fmt.Printf("\n🔄 Deployment %s\n", deployment.ID)
	fmt.Printf("   Repository: %s\n", deployment.Repository)
	fmt.Printf("   Commit: %s\n", deployment.Commit[:7])
	fmt.Printf("   Time: %s\n", deployment.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("   State: %s\n", colorizeState(deployment.State))
	fmt.Printf("   Files: %d\n", len(deployment.Files))
	if deployment.SnapshotID != "" {
		fmt.Printf("   Snapshot: ✓\n")
	}
}

func showRecoveryPoint(point *rollback.RecoveryPoint) {
	fmt.Printf("\n📍 %s\n", point.ID)
	fmt.Printf("   Time: %s\n", point.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("   Type: %s\n", point.Type)
	fmt.Printf("   Description: %s\n", point.Description)
	fmt.Printf("   Valid: %v\n", point.Valid)
}

func showRollbackResult(result *rollback.RollbackResult) {
	fmt.Println("\nRollback Results:")
	fmt.Println("================")
	fmt.Printf("Strategy: %s\n", result.Strategy)
	fmt.Printf("Duration: %s\n", result.EndTime.Sub(result.StartTime))
	fmt.Printf("Success: %v\n", result.Success)
	
	if result.DryRun {
		fmt.Println("\n[DRY RUN - No changes were made]")
	}
	
	fmt.Printf("\nOperations (%d):\n", len(result.Operations))
	for _, op := range result.Operations {
		status := "✓"
		if !op.Success {
			status = "✗"
		}
		fmt.Printf("  %s %s - %s\n", status, op.Operation.Type, op.Operation.Description)
		if op.Error != "" {
			fmt.Printf("    Error: %s\n", op.Error)
		}
	}
	
	if result.Success {
		ui.ShowSuccess("Rollback completed successfully!")
	} else {
		ui.ShowError(errors.New(errors.ErrCodeRollbackFailed, "Rollback completed with errors"))
	}
}

func showRecoveryResult(result *rollback.RecoveryResult) {
	fmt.Println("\nRecovery Results:")
	fmt.Println("================")
	fmt.Printf("Target Time: %s\n", result.TargetTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Recovery Point: %s\n", result.RecoveryPoint.ID)
	fmt.Printf("Duration: %s\n", result.EndTime.Sub(result.StartTime))
	fmt.Printf("Success: %v\n", result.Success)
	
	if result.DryRun {
		fmt.Println("\n[DRY RUN - No changes were made]")
	}
	
	if result.Success {
		ui.ShowSuccess("Recovery completed successfully!")
	} else {
		ui.ShowError(errors.New(errors.ErrCodeRecoveryFailed, "Recovery failed").
			WithContext("error", result.Error))
	}
}

func showRestoreResult(result *rollback.RestoreResult) {
	fmt.Println("\nRestore Results:")
	fmt.Println("===============")
	fmt.Printf("Duration: %s\n", result.EndTime.Sub(result.StartTime))
	fmt.Printf("Success: %v\n", result.Success)
}

func showRestoreBackupResult(result *rollback.RestoreBackupResult) {
	fmt.Println("\nRestore Backup Results:")
	fmt.Println("======================")
	duration := "In progress"
	if result.EndTime != nil {
		duration = result.EndTime.Sub(result.StartTime).String()
	}
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Success: %v\n", result.Success)
	
	if result.DryRun {
		fmt.Println("\n[DRY RUN - No changes were made]")
	}
	
	fmt.Printf("\nOperations (%d):\n", len(result.Operations))
	successCount := 0
	for _, op := range result.Operations {
		if op.Success {
			successCount++
		}
	}
	fmt.Printf("  Successful: %d\n", successCount)
	fmt.Printf("  Failed: %d\n", len(result.Operations)-successCount)
	
	if result.Success {
		ui.ShowSuccess("Restore completed successfully!")
	} else {
		ui.ShowError(errors.New(errors.ErrCodeRestoreFailed, "Restore failed"))
	}
}

func colorizeState(state rollback.DeploymentState) string {
	switch state {
	case rollback.StateCompleted:
		return "✅ completed"
	case rollback.StateFailed:
		return "❌ failed"
	case rollback.StateRolledBack:
		return "↩️  rolled back"
	case rollback.StateInProgress:
		return "🔄 in progress"
	default:
		return string(state)
	}
}