package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"flakedrop/internal/migration"
	"flakedrop/internal/schema"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Data migration operations",
	Long: `Perform comprehensive data migration operations including:
- Plan creation and validation
- Migration execution with streaming and batching capabilities
- Progress monitoring and reporting
- Rollback and recovery operations
- Template management

Supports various migration strategies:
- Batch processing for standard datasets
- Streaming for large datasets
- Parallel processing for performance
- Incremental sync for ongoing replication`,
}

// migratePlanCmd creates a migration plan
var migratePlanCmd = &cobra.Command{
	Use:   "plan",
	Short: "Create and manage migration plans",
	Long:  `Create, validate, and optimize migration plans for data migration operations.`,
}

// migrateCreatePlanCmd creates a new migration plan
var migrateCreatePlanCmd = &cobra.Command{
	Use:   "create [name]",
	Short: "Create a new migration plan",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]

		// Get flags
		description, _ := cmd.Flags().GetString("description")
		sourceDB, _ := cmd.Flags().GetString("source-database")
		sourceSchema, _ := cmd.Flags().GetString("source-schema")
		targetDB, _ := cmd.Flags().GetString("target-database")
		targetSchema, _ := cmd.Flags().GetString("target-schema")
		tables, _ := cmd.Flags().GetStringSlice("tables")
		migrationTypeStr, _ := cmd.Flags().GetString("type")
		strategyStr, _ := cmd.Flags().GetString("strategy")
		templateID, _ := cmd.Flags().GetString("template")
		batchSize, _ := cmd.Flags().GetInt("batch-size")
		parallelism, _ := cmd.Flags().GetInt("parallelism")
		enableValidation, _ := cmd.Flags().GetBool("enable-validation")
		enableRollback, _ := cmd.Flags().GetBool("enable-rollback")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		// Initialize services
		snowflakeService, err := initializeSnowflakeService()
		if err != nil {
			return fmt.Errorf("failed to initialize Snowflake service: %w", err)
		}

		schemaService := schema.NewService(snowflakeService)
		migrationRepo := &InMemoryMigrationRepository{} // Use in-memory for demo
		migrationService := migration.NewService(schemaService, snowflakeService, migrationRepo)

		// Parse migration type and strategy
		migType := migration.MigrationTypeData
		if migrationTypeStr != "" {
			migType = migration.MigrationType(strings.ToUpper(migrationTypeStr))
		}

		strategy := migration.MigrationStrategyBatch
		if strategyStr != "" {
			strategy = migration.MigrationStrategy(strings.ToUpper(strategyStr))
		}

		// Create migration config
		migConfig := migration.DefaultMigrationConfig()
		migConfig.BatchSize = batchSize
		migConfig.Parallelism = parallelism
		migConfig.EnableValidation = enableValidation
		migConfig.EnableRollback = enableRollback
		migConfig.DryRun = dryRun

		// Create plan config
		planConfig := migration.PlanConfig{
			Name:        name,
			Description: description,
			Type:        migType,
			Strategy:    strategy,
			SourceConfig: migration.DataSource{
				Name: sourceDB,
				Properties: map[string]interface{}{
					"database": sourceDB,
					"schema":   sourceSchema,
				},
			},
			TargetConfig: migration.DataSource{
				Name: targetDB,
				Properties: map[string]interface{}{
					"database": targetDB,
					"schema":   targetSchema,
				},
			},
			Tables:          tables,
			MigrationConfig: migConfig,
			TemplateID:      templateID,
			Parameters: map[string]interface{}{
				"created_by": "cli-user",
			},
		}

		ctx := context.Background()

		// Apply template if specified
		if templateID != "" {
			params := map[string]interface{}{
				"name":         name,
				"description":  description,
				"source_table": strings.Join(tables, ","),
				"target_table": strings.Join(tables, ","),
				"batch_size":   batchSize,
			}

			plan, err := migrationService.ApplyTemplate(ctx, templateID, params)
			if err != nil {
				return fmt.Errorf("failed to apply template: %w", err)
			}

			ui.PrintSuccess(fmt.Sprintf("Migration plan '%s' created from template '%s'", name, templateID))
			printPlanSummary(plan)
			return nil
		}

		// Create plan
		plan, err := migrationService.CreatePlan(ctx, planConfig)
		if err != nil {
			return fmt.Errorf("failed to create migration plan: %w", err)
		}

		// Validate plan
		if err := migrationService.ValidatePlan(ctx, plan); err != nil {
			return fmt.Errorf("plan validation failed: %w", err)
		}

		// Save plan
		if err := migrationService.SavePlan(ctx, plan); err != nil {
			return fmt.Errorf("failed to save plan: %w", err)
		}

		ui.PrintSuccess(fmt.Sprintf("Migration plan '%s' created successfully", name))
		printPlanSummary(plan)

		return nil
	},
}

// migrateExecuteCmd executes a migration plan
var migrateExecuteCmd = &cobra.Command{
	Use:   "execute [plan-id]",
	Short: "Execute a migration plan",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		planID := args[0]

		monitor, _ := cmd.Flags().GetBool("monitor")
		interval, _ := cmd.Flags().GetDuration("monitor-interval")

		// Initialize services
		snowflakeService, err := initializeSnowflakeService()
		if err != nil {
			return fmt.Errorf("failed to initialize Snowflake service: %w", err)
		}

		schemaService := schema.NewService(snowflakeService)
		migrationRepo := &InMemoryMigrationRepository{}
		migrationService := migration.NewService(schemaService, snowflakeService, migrationRepo)

		ctx := context.Background()

		// Get plan
		plan, err := migrationService.GetPlan(ctx, planID)
		if err != nil {
			return fmt.Errorf("failed to get migration plan: %w", err)
		}

		// Validate migration readiness
		readiness, err := migrationService.ValidateMigrationReadiness(ctx, plan)
		if err != nil {
			return fmt.Errorf("readiness validation failed: %w", err)
		}

		if !readiness.Ready {
			ui.PrintErrorString("Migration not ready:")
			for _, issue := range readiness.Issues {
				ui.PrintErrorString(fmt.Sprintf("  - %s", issue))
			}
			return fmt.Errorf("migration readiness validation failed")
		}

		if len(readiness.Warnings) > 0 {
			ui.PrintWarning("Migration warnings:")
			for _, warning := range readiness.Warnings {
				ui.PrintWarning(fmt.Sprintf("  - %s", warning))
			}
		}

		// Execute migration
		ui.PrintInfo(fmt.Sprintf("Starting migration execution for plan: %s", plan.Name))
		execution, err := migrationService.Execute(ctx, plan)
		if err != nil {
			return fmt.Errorf("migration execution failed: %w", err)
		}

		ui.PrintSuccess(fmt.Sprintf("Migration started with execution ID: %s", execution.ID))

		// Monitor execution if requested
		if monitor {
			return monitorMigrationExecution(ctx, migrationService, execution.ID, interval)
		}

		return nil
	},
}

// migrateStatusCmd shows migration status
var migrateStatusCmd = &cobra.Command{
	Use:   "status [execution-id]",
	Short: "Show migration execution status",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		executionID := args[0]

		// Initialize services
		snowflakeService, err := initializeSnowflakeService()
		if err != nil {
			return fmt.Errorf("failed to initialize Snowflake service: %w", err)
		}

		schemaService := schema.NewService(snowflakeService)
		migrationRepo := &InMemoryMigrationRepository{}
		migrationService := migration.NewService(schemaService, snowflakeService, migrationRepo)

		ctx := context.Background()

		// Get execution
		execution, err := migrationService.GetExecution(ctx, executionID)
		if err != nil {
			return fmt.Errorf("failed to get execution: %w", err)
		}

		// Get progress
		progress, err := migrationService.GetStatus(ctx, executionID)
		if err != nil {
			return fmt.Errorf("failed to get status: %w", err)
		}

		// Get metrics
		metrics, err := migrationService.GetMetrics(ctx, executionID)
		if err != nil {
			return fmt.Errorf("failed to get metrics: %w", err)
		}

		printExecutionStatus(execution, progress, metrics)

		return nil
	},
}

// migrateRollbackCmd performs migration rollback
var migrateRollbackCmd = &cobra.Command{
	Use:   "rollback [execution-id]",
	Short: "Rollback a migration",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		executionID := args[0]

		force, _ := cmd.Flags().GetBool("force")

		// Initialize services
		snowflakeService, err := initializeSnowflakeService()
		if err != nil {
			return fmt.Errorf("failed to initialize Snowflake service: %w", err)
		}

		schemaService := schema.NewService(snowflakeService)
		migrationRepo := &InMemoryMigrationRepository{}
		migrationService := migration.NewService(schemaService, snowflakeService, migrationRepo)

		ctx := context.Background()

		// Validate rollback possibility
		if !force {
			validation, err := migrationService.ValidateRollback(ctx, executionID)
			if err != nil {
				return fmt.Errorf("rollback validation failed: %w", err)
			}

			if !validation.CanRollback {
				ui.PrintErrorString("Rollback not possible:")
				for _, issue := range validation.Issues {
					ui.PrintErrorString(fmt.Sprintf("  - %s", issue))
				}
				return fmt.Errorf("rollback validation failed")
			}

			if len(validation.Recommendations) > 0 {
				ui.PrintWarning("Rollback recommendations:")
				for _, rec := range validation.Recommendations {
					ui.PrintWarning(fmt.Sprintf("  - %s", rec))
				}
			}
		}

		// Perform rollback
		ui.PrintInfo(fmt.Sprintf("Starting rollback for execution: %s", executionID))
		if err := migrationService.Rollback(ctx, executionID); err != nil {
			return fmt.Errorf("rollback failed: %w", err)
		}

		ui.PrintSuccess("Rollback completed successfully")

		return nil
	},
}

// migrateTemplatesCmd manages migration templates
var migrateTemplatesCmd = &cobra.Command{
	Use:   "templates",
	Short: "Manage migration templates",
	Long:  `Create, list, and manage reusable migration templates.`,
}

// migrateListTemplatesCmd lists available templates
var migrateListTemplatesCmd = &cobra.Command{
	Use:   "list",
	Short: "List available migration templates",
	RunE: func(cmd *cobra.Command, args []string) error {
		category, _ := cmd.Flags().GetString("category")

		// Initialize services
		snowflakeService, err := initializeSnowflakeService()
		if err != nil {
			return fmt.Errorf("failed to initialize Snowflake service: %w", err)
		}

		schemaService := schema.NewService(snowflakeService)
		migrationRepo := &InMemoryMigrationRepository{}
		migrationService := migration.NewService(schemaService, snowflakeService, migrationRepo)

		ctx := context.Background()

		// List templates
		templates, err := migrationService.ListTemplates(ctx)
		if err != nil {
			return fmt.Errorf("failed to list templates: %w", err)
		}

		// Filter by category if specified
		if category != "" {
			filteredTemplates := make([]*migration.MigrationTemplate, 0)
			for _, template := range templates {
				if template.Category == category {
					filteredTemplates = append(filteredTemplates, template)
				}
			}
			templates = filteredTemplates
		}

		printTemplatesList(templates)

		return nil
	},
}

// Helper functions

func printPlanSummary(plan *migration.MigrationPlan) {
	ui.PrintSection("Migration Plan Summary")
	ui.PrintKeyValue("ID", plan.ID)
	ui.PrintKeyValue("Name", plan.Name)
	ui.PrintKeyValue("Description", plan.Description)
	ui.PrintKeyValue("Type", string(plan.Type))
	ui.PrintKeyValue("Strategy", string(plan.Strategy))
	ui.PrintKeyValue("Tables", fmt.Sprintf("%d", len(plan.Tables)))
	ui.PrintKeyValue("Estimated Time", plan.EstimatedTime.String())
	ui.PrintKeyValue("Estimated Size", fmt.Sprintf("%d bytes", plan.EstimatedSize))

	if len(plan.Tables) > 0 {
		ui.PrintSubsection("Tables")
		for _, table := range plan.Tables {
			ui.PrintKeyValue("  "+table.SourceTable, fmt.Sprintf("%s -> %s (%s)", 
				table.SourceTable, table.TargetTable, table.MigrationMode))
		}
	}
}

func printExecutionStatus(execution *migration.MigrationExecution, progress *migration.MigrationProgress, metrics *migration.MigrationMetrics) {
	ui.PrintSection("Migration Execution Status")
	ui.PrintKeyValue("Execution ID", execution.ID)
	ui.PrintKeyValue("Plan ID", execution.PlanID)
	ui.PrintKeyValue("Status", string(execution.Status))
	ui.PrintKeyValue("Started At", execution.StartedAt.Format(time.RFC3339))
	if execution.CompletedAt != nil {
		ui.PrintKeyValue("Completed At", execution.CompletedAt.Format(time.RFC3339))
		ui.PrintKeyValue("Duration", execution.Duration.String())
	}

	ui.PrintSubsection("Progress")
	ui.PrintKeyValue("Current Operation", progress.CurrentOperation)
	ui.PrintKeyValue("Tables Completed", fmt.Sprintf("%d/%d", progress.CompletedTables, progress.TotalTables))
	ui.PrintKeyValue("Rows Processed", fmt.Sprintf("%d/%d", progress.ProcessedRows, progress.TotalRows))
	ui.PrintKeyValue("Percent Complete", fmt.Sprintf("%.2f%%", progress.PercentComplete))
	if !progress.EstimatedETA.IsZero() {
		ui.PrintKeyValue("Estimated ETA", progress.EstimatedETA.Format(time.RFC3339))
	}

	ui.PrintSubsection("Metrics")
	ui.PrintKeyValue("Rows Inserted", fmt.Sprintf("%d", metrics.RowsInserted))
	ui.PrintKeyValue("Rows Updated", fmt.Sprintf("%d", metrics.RowsUpdated))
	ui.PrintKeyValue("Rows Deleted", fmt.Sprintf("%d", metrics.RowsDeleted))
	ui.PrintKeyValue("Rows Errored", fmt.Sprintf("%d", metrics.RowsErrored))
	ui.PrintKeyValue("Bytes Processed", fmt.Sprintf("%d", metrics.BytesProcessed))
	ui.PrintKeyValue("Queries Executed", fmt.Sprintf("%d", metrics.QueriesExecuted))

	if len(execution.Errors) > 0 {
		ui.PrintSubsection("Errors")
		for _, err := range execution.Errors {
			ui.PrintErrorString(fmt.Sprintf("%s: %s", err.Code, err.Message))
		}
	}

	if len(execution.Warnings) > 0 {
		ui.PrintSubsection("Warnings")
		for _, warning := range execution.Warnings {
			ui.PrintWarning(warning.Message)
		}
	}
}

func printTemplatesList(templates []*migration.MigrationTemplate) {
	ui.PrintSection("Available Migration Templates")

	if len(templates) == 0 {
		ui.PrintInfo("No templates available")
		return
	}

	for _, template := range templates {
		ui.PrintSubsection(template.Name)
		ui.PrintKeyValue("  ID", template.ID)
		ui.PrintKeyValue("  Description", template.Description)
		ui.PrintKeyValue("  Category", template.Category)
		ui.PrintKeyValue("  Type", string(template.Type))
		ui.PrintKeyValue("  Strategy", string(template.Strategy))
		ui.PrintKeyValue("  Version", template.Version)
		if len(template.Tags) > 0 {
			ui.PrintKeyValue("  Tags", strings.Join(template.Tags, ", "))
		}
	}
}

func monitorMigrationExecution(ctx context.Context, service *migration.Service, executionID string, interval time.Duration) error {
	ui.PrintInfo(fmt.Sprintf("Monitoring migration execution: %s", executionID))
	ui.PrintInfo(fmt.Sprintf("Update interval: %s", interval))
	ui.PrintInfo("Press Ctrl+C to stop monitoring")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Get current status
			execution, err := service.GetExecution(ctx, executionID)
			if err != nil {
				ui.PrintErrorString(fmt.Sprintf("Failed to get execution status: %v", err))
				continue
			}

			progress, err := service.GetStatus(ctx, executionID)
			if err != nil {
				ui.PrintErrorString(fmt.Sprintf("Failed to get progress: %v", err))
				continue
			}

			metrics, err := service.GetMetrics(ctx, executionID)
			if err != nil {
				ui.PrintErrorString(fmt.Sprintf("Failed to get metrics: %v", err))
				continue
			}

			// Clear screen and print status
			fmt.Print("\033[2J\033[H") // Clear screen
			printExecutionStatus(execution, progress, metrics)

			// Check if completed
			if execution.Status == migration.MigrationStatusCompleted ||
				execution.Status == migration.MigrationStatusFailed ||
				execution.Status == migration.MigrationStatusRolledBack {
				ui.PrintInfo("Migration execution completed")
				return nil
			}
		}
	}
}

func initializeSnowflakeService() (*snowflake.Service, error) {
	// Get connection parameters from viper config
	account := viper.GetString("snowflake.account")
	username := viper.GetString("snowflake.username")
	password := viper.GetString("snowflake.password")
	database := viper.GetString("snowflake.database")
	schema := viper.GetString("snowflake.schema")
	warehouse := viper.GetString("snowflake.warehouse")
	role := viper.GetString("snowflake.role")

	if account == "" || username == "" || password == "" {
		return nil, fmt.Errorf("snowflake connection parameters not configured")
	}

	config := snowflake.Config{
		Account:   account,
		Username:  username,
		Password:  password,
		Database:  database,
		Schema:    schema,
		Warehouse: warehouse,
		Role:      role,
	}
	return snowflake.NewService(config), nil
}

// Simple in-memory repository for demonstration
type InMemoryMigrationRepository struct {
	plans       map[string]*migration.MigrationPlan
	executions  map[string]*migration.MigrationExecution
	templates   map[string]*migration.MigrationTemplate
	checkpoints map[string][]*migration.ExecutionCheckpoint
}

func (r *InMemoryMigrationRepository) SavePlan(ctx context.Context, plan *migration.MigrationPlan) error {
	if r.plans == nil {
		r.plans = make(map[string]*migration.MigrationPlan)
	}
	r.plans[plan.ID] = plan
	return nil
}

func (r *InMemoryMigrationRepository) GetPlan(ctx context.Context, planID string) (*migration.MigrationPlan, error) {
	if r.plans == nil {
		r.plans = make(map[string]*migration.MigrationPlan)
	}
	plan, exists := r.plans[planID]
	if !exists {
		return nil, fmt.Errorf("plan not found")
	}
	return plan, nil
}

func (r *InMemoryMigrationRepository) ListPlans(ctx context.Context, filters map[string]interface{}) ([]*migration.MigrationPlan, error) {
	if r.plans == nil {
		r.plans = make(map[string]*migration.MigrationPlan)
	}
	plans := make([]*migration.MigrationPlan, 0, len(r.plans))
	for _, plan := range r.plans {
		plans = append(plans, plan)
	}
	return plans, nil
}

func (r *InMemoryMigrationRepository) DeletePlan(ctx context.Context, planID string) error {
	if r.plans == nil {
		r.plans = make(map[string]*migration.MigrationPlan)
	}
	delete(r.plans, planID)
	return nil
}

func (r *InMemoryMigrationRepository) SaveExecution(ctx context.Context, execution *migration.MigrationExecution) error {
	if r.executions == nil {
		r.executions = make(map[string]*migration.MigrationExecution)
	}
	r.executions[execution.ID] = execution
	return nil
}

func (r *InMemoryMigrationRepository) GetExecution(ctx context.Context, executionID string) (*migration.MigrationExecution, error) {
	if r.executions == nil {
		r.executions = make(map[string]*migration.MigrationExecution)
	}
	execution, exists := r.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution not found")
	}
	return execution, nil
}

func (r *InMemoryMigrationRepository) ListExecutions(ctx context.Context, filters map[string]interface{}) ([]*migration.MigrationExecution, error) {
	if r.executions == nil {
		r.executions = make(map[string]*migration.MigrationExecution)
	}
	executions := make([]*migration.MigrationExecution, 0, len(r.executions))
	for _, execution := range r.executions {
		executions = append(executions, execution)
	}
	return executions, nil
}

func (r *InMemoryMigrationRepository) SaveCheckpoint(ctx context.Context, checkpoint *migration.ExecutionCheckpoint) error {
	if r.checkpoints == nil {
		r.checkpoints = make(map[string][]*migration.ExecutionCheckpoint)
	}
	// Assuming checkpoint belongs to an execution - this is simplified
	executionID := "default"
	r.checkpoints[executionID] = append(r.checkpoints[executionID], checkpoint)
	return nil
}

func (r *InMemoryMigrationRepository) GetCheckpoints(ctx context.Context, executionID string) ([]*migration.ExecutionCheckpoint, error) {
	if r.checkpoints == nil {
		r.checkpoints = make(map[string][]*migration.ExecutionCheckpoint)
	}
	checkpoints, exists := r.checkpoints[executionID]
	if !exists {
		return []*migration.ExecutionCheckpoint{}, nil
	}
	return checkpoints, nil
}

func (r *InMemoryMigrationRepository) DeleteCheckpoints(ctx context.Context, executionID string) error {
	if r.checkpoints == nil {
		r.checkpoints = make(map[string][]*migration.ExecutionCheckpoint)
	}
	delete(r.checkpoints, executionID)
	return nil
}

func (r *InMemoryMigrationRepository) SaveTemplate(ctx context.Context, template *migration.MigrationTemplate) error {
	if r.templates == nil {
		r.templates = make(map[string]*migration.MigrationTemplate)
	}
	r.templates[template.ID] = template
	return nil
}

func (r *InMemoryMigrationRepository) GetTemplate(ctx context.Context, templateID string) (*migration.MigrationTemplate, error) {
	if r.templates == nil {
		r.templates = make(map[string]*migration.MigrationTemplate)
	}
	template, exists := r.templates[templateID]
	if !exists {
		return nil, fmt.Errorf("template not found")
	}
	return template, nil
}

func (r *InMemoryMigrationRepository) ListTemplates(ctx context.Context) ([]*migration.MigrationTemplate, error) {
	if r.templates == nil {
		r.templates = make(map[string]*migration.MigrationTemplate)
	}
	templates := make([]*migration.MigrationTemplate, 0, len(r.templates))
	for _, template := range r.templates {
		templates = append(templates, template)
	}
	return templates, nil
}

func (r *InMemoryMigrationRepository) DeleteTemplate(ctx context.Context, templateID string) error {
	if r.templates == nil {
		r.templates = make(map[string]*migration.MigrationTemplate)
	}
	delete(r.templates, templateID)
	return nil
}

func init() {
	// Add migrate command
	rootCmd.AddCommand(migrateCmd)

	// Add plan subcommands
	migrateCmd.AddCommand(migratePlanCmd)
	migratePlanCmd.AddCommand(migrateCreatePlanCmd)

	// Add plan creation flags
	migrateCreatePlanCmd.Flags().String("description", "", "Migration plan description")
	migrateCreatePlanCmd.Flags().String("source-database", "", "Source database name")
	migrateCreatePlanCmd.Flags().String("source-schema", "", "Source schema name")
	migrateCreatePlanCmd.Flags().String("target-database", "", "Target database name")
	migrateCreatePlanCmd.Flags().String("target-schema", "", "Target schema name")
	migrateCreatePlanCmd.Flags().StringSlice("tables", []string{}, "Tables to migrate")
	migrateCreatePlanCmd.Flags().String("type", "DATA", "Migration type (DATA, SCHEMA, COMBINED, TRANSFORM)")
	migrateCreatePlanCmd.Flags().String("strategy", "BATCH", "Migration strategy (BATCH, STREAM, PARALLEL, INCREMENTAL, BULK)")
	migrateCreatePlanCmd.Flags().String("template", "", "Template ID to use")
	migrateCreatePlanCmd.Flags().Int("batch-size", 10000, "Batch size for processing")
	migrateCreatePlanCmd.Flags().Int("parallelism", 1, "Number of parallel processes")
	migrateCreatePlanCmd.Flags().Bool("enable-validation", true, "Enable data validation")
	migrateCreatePlanCmd.Flags().Bool("enable-rollback", true, "Enable rollback capability")
	migrateCreatePlanCmd.Flags().Bool("dry-run", false, "Perform dry run without actual migration")

	// Add execution commands
	migrateCmd.AddCommand(migrateExecuteCmd)
	migrateExecuteCmd.Flags().Bool("monitor", false, "Monitor execution progress")
	migrateExecuteCmd.Flags().Duration("monitor-interval", 10*time.Second, "Monitoring update interval")

	// Add status command
	migrateCmd.AddCommand(migrateStatusCmd)

	// Add rollback command
	migrateCmd.AddCommand(migrateRollbackCmd)
	migrateRollbackCmd.Flags().Bool("force", false, "Force rollback without validation")

	// Add templates commands
	migrateCmd.AddCommand(migrateTemplatesCmd)
	migrateTemplatesCmd.AddCommand(migrateListTemplatesCmd)
	migrateListTemplatesCmd.Flags().String("category", "", "Filter templates by category")
}
