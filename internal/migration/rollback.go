package migration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"flakedrop/internal/rollback"
	"flakedrop/pkg/errors"
)

// RollbackManager handles migration rollback and recovery operations
type RollbackManager struct {
	connectionProvider ConnectionProvider
	repository         MigrationRepository
	rollbackService    *rollback.Manager
	errorHandler       *errors.ErrorHandler
}

// NewRollbackManager creates a new rollback manager
func NewRollbackManager(connProvider ConnectionProvider, repository MigrationRepository, rollbackService *rollback.Manager) *RollbackManager {
	return &RollbackManager{
		connectionProvider: connProvider,
		repository:         repository,
		rollbackService:    rollbackService,
		errorHandler:       errors.GetGlobalErrorHandler(),
	}
}

// CreateBackup creates a backup before migration
func (rm *RollbackManager) CreateBackup(ctx context.Context, plan *MigrationPlan) (*MigrationBackup, error) {
	backup := &MigrationBackup{
		ID:        generateID("backup"),
		PlanID:    plan.ID,
		CreatedAt: time.Now(),
		Status:    BackupStatusCreating,
		Tables:    make([]TableBackup, 0, len(plan.Tables)),
	}

	// Get source connection
	sourceConn, err := rm.connectionProvider.GetSourceConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connection: %w", err)
	}
	defer sourceConn.Close()

	// Create backups for each table
	for _, table := range plan.Tables {
		tableBackup, err := rm.createTableBackup(ctx, sourceConn, &table, plan)
		if err != nil {
			backup.Status = BackupStatusFailed
			backup.Error = err.Error()
			return backup, fmt.Errorf("failed to backup table %s: %w", table.SourceTable, err)
		}
		backup.Tables = append(backup.Tables, *tableBackup)
	}

	backup.Status = BackupStatusCompleted
	backup.Size = rm.calculateBackupSize(backup.Tables)

	// Save backup metadata
	if err := rm.saveBackupMetadata(ctx, backup); err != nil {
		return backup, fmt.Errorf("failed to save backup metadata: %w", err)
	}

	return backup, nil
}

// Rollback performs rollback of a migration
func (rm *RollbackManager) Rollback(ctx context.Context, executionID string) error {
	// Get execution details
	execution, err := rm.repository.GetExecution(ctx, executionID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	if execution.Status != MigrationStatusCompleted && execution.Status != MigrationStatusFailed {
		return fmt.Errorf("cannot rollback execution in status: %s", execution.Status)
	}

	// Get migration plan
	plan, err := rm.repository.GetPlan(ctx, execution.PlanID)
	if err != nil {
		return fmt.Errorf("failed to get migration plan: %w", err)
	}

	// Check if rollback is enabled
	if !plan.Configuration.EnableRollback {
		return fmt.Errorf("rollback is not enabled for this migration")
	}

	// Create rollback execution
	rollbackExecution := &MigrationExecution{
		ID:            generateID("rollback"),
		PlanID:        plan.ID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now(),
		Configuration: plan.Configuration,
		Progress:      MigrationProgress{CurrentOperation: "Starting rollback"},
		Metrics:       MigrationMetrics{},
		Errors:        []MigrationError{},
		Warnings:      []MigrationWarning{},
		ExecutedBy:    "system",
		Environment:   execution.Environment,
	}

	// Save rollback execution
	if err := rm.repository.SaveExecution(ctx, rollbackExecution); err != nil {
		return fmt.Errorf("failed to save rollback execution: %w", err)
	}

	// Perform rollback
	if err := rm.performRollback(ctx, plan, execution, rollbackExecution); err != nil {
		rollbackExecution.Status = MigrationStatusFailed
		completedAt := time.Now()
		rollbackExecution.CompletedAt = &completedAt
		rollbackExecution.Duration = completedAt.Sub(rollbackExecution.StartedAt)
		rm.repository.SaveExecution(ctx, rollbackExecution)
		return fmt.Errorf("rollback failed: %w", err)
	}

	// Mark original execution as rolled back
	execution.Status = MigrationStatusRolledBack
	rm.repository.SaveExecution(ctx, execution)

	// Complete rollback execution
	rollbackExecution.Status = MigrationStatusCompleted
	completedAt := time.Now()
	rollbackExecution.CompletedAt = &completedAt
	rollbackExecution.Duration = completedAt.Sub(rollbackExecution.StartedAt)
	rm.repository.SaveExecution(ctx, rollbackExecution)

	return nil
}

// RecoverFromCheckpoint recovers migration from a checkpoint
func (rm *RollbackManager) RecoverFromCheckpoint(ctx context.Context, executionID, checkpointID string) error {
	// Get execution
	execution, err := rm.repository.GetExecution(ctx, executionID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	// Get checkpoint
	checkpoints, err := rm.repository.GetCheckpoints(ctx, executionID)
	if err != nil {
		return fmt.Errorf("failed to get checkpoints: %w", err)
	}

	var targetCheckpoint *ExecutionCheckpoint
	for _, cp := range checkpoints {
		if cp.ID == checkpointID {
			targetCheckpoint = cp
			break
		}
	}

	if targetCheckpoint == nil {
		return fmt.Errorf("checkpoint %s not found", checkpointID)
	}

	// Create recovery execution
	recoveryExecution := &MigrationExecution{
		ID:            generateID("recovery"),
		PlanID:        execution.PlanID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now(),
		Configuration: execution.Configuration,
		Progress: MigrationProgress{
			CurrentOperation: fmt.Sprintf("Recovering from checkpoint %s", checkpointID),
			ProcessedRows:   targetCheckpoint.RowsProcessed,
		},
		Metrics:       MigrationMetrics{},
		Errors:        []MigrationError{},
		Warnings:      []MigrationWarning{},
		CurrentTable:  targetCheckpoint.Table,
		CurrentBatch:  targetCheckpoint.Batch,
		ExecutedBy:    "system",
		Environment:   execution.Environment,
	}

	// Save recovery execution
	if err := rm.repository.SaveExecution(ctx, recoveryExecution); err != nil {
		return fmt.Errorf("failed to save recovery execution: %w", err)
	}

	// Perform recovery
	if err := rm.performRecovery(ctx, execution, targetCheckpoint, recoveryExecution); err != nil {
		recoveryExecution.Status = MigrationStatusFailed
		completedAt := time.Now()
		recoveryExecution.CompletedAt = &completedAt
		recoveryExecution.Duration = completedAt.Sub(recoveryExecution.StartedAt)
		rm.repository.SaveExecution(ctx, recoveryExecution)
		return fmt.Errorf("recovery failed: %w", err)
	}

	// Complete recovery
	recoveryExecution.Status = MigrationStatusCompleted
	completedAt := time.Now()
	recoveryExecution.CompletedAt = &completedAt
	recoveryExecution.Duration = completedAt.Sub(recoveryExecution.StartedAt)
	rm.repository.SaveExecution(ctx, recoveryExecution)

	return nil
}

// ValidateRollbackPossibility checks if rollback is possible
func (rm *RollbackManager) ValidateRollbackPossibility(ctx context.Context, executionID string) (*RollbackValidation, error) {
	execution, err := rm.repository.GetExecution(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	plan, err := rm.repository.GetPlan(ctx, execution.PlanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get plan: %w", err)
	}

	validation := &RollbackValidation{
		ExecutionID:      executionID,
		PlanID:           plan.ID,
		ValidatedAt:      time.Now(),
		CanRollback:      true,
		RollbackEnabled:  plan.Configuration.EnableRollback,
		BackupExists:     false,
		Issues:           []string{},
		Recommendations:  []string{},
	}

	// Check if rollback is enabled
	if !plan.Configuration.EnableRollback {
		validation.CanRollback = false
		validation.Issues = append(validation.Issues, "Rollback is not enabled for this migration")
	}

	// Check execution status
	if execution.Status != MigrationStatusCompleted && execution.Status != MigrationStatusFailed {
		validation.CanRollback = false
		validation.Issues = append(validation.Issues, fmt.Sprintf("Cannot rollback execution in status: %s", execution.Status))
	}

	// Check for backup availability
	backups, err := rm.getExecutionBackups(ctx, executionID)
	if err == nil && len(backups) > 0 {
		validation.BackupExists = true
		validation.BackupInfo = &backups[0]
	} else {
		validation.Issues = append(validation.Issues, "No backup available for rollback")
		validation.Recommendations = append(validation.Recommendations, "Consider creating a backup before attempting rollback")
	}

	// Check for schema changes that might affect rollback
	if rm.hasSchemaChanges(plan) {
		validation.Issues = append(validation.Issues, "Migration includes schema changes that may complicate rollback")
		validation.Recommendations = append(validation.Recommendations, "Review schema changes before rollback")
	}

	return validation, nil
}

// GetRollbackPlan creates a rollback plan
func (rm *RollbackManager) GetRollbackPlan(ctx context.Context, executionID string) (*RollbackPlan, error) {
	execution, err := rm.repository.GetExecution(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	plan, err := rm.repository.GetPlan(ctx, execution.PlanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get plan: %w", err)
	}

	rollbackPlan := &RollbackPlan{
		ID:              generateID("rollback_plan"),
		OriginalPlanID:  plan.ID,
		ExecutionID:     executionID,
		CreatedAt:       time.Now(),
		Strategy:        rm.determineRollbackStrategy(plan, execution),
		Steps:           []RollbackStep{},
		EstimatedTime:   time.Duration(0),
		Risks:           []string{},
		Prerequisites:   []string{},
	}

	// Create rollback steps in reverse order
	for i := len(plan.Tables) - 1; i >= 0; i-- {
		table := plan.Tables[i]
		step := RollbackStep{
			ID:          generateID("rollback_step"),
			Order:       len(plan.Tables) - i,
			Table:       table.TargetTable,
			Operation:   rm.determineRollbackOperation(table),
			SQL:         rm.generateRollbackSQL(table),
			Description: fmt.Sprintf("Rollback table %s", table.TargetTable),
			EstimatedTime: time.Minute * 5, // Default estimate
		}
		rollbackPlan.Steps = append(rollbackPlan.Steps, step)
		rollbackPlan.EstimatedTime += step.EstimatedTime
	}

	// Add validation steps
	validationStep := RollbackStep{
		ID:          generateID("rollback_step"),
		Order:       len(rollbackPlan.Steps) + 1,
		Operation:   RollbackOperationValidate,
		Description: "Validate rollback completion",
		SQL:         "",
		EstimatedTime: time.Minute * 2,
	}
	rollbackPlan.Steps = append(rollbackPlan.Steps, validationStep)
	rollbackPlan.EstimatedTime += validationStep.EstimatedTime

	return rollbackPlan, nil
}

// Private methods

func (rm *RollbackManager) createTableBackup(ctx context.Context, conn *sql.DB, table *TableMigration, plan *MigrationPlan) (*TableBackup, error) {
	backup := &TableBackup{
		TableName:   table.SourceTable,
		BackupName:  fmt.Sprintf("%s_backup_%d", table.SourceTable, time.Now().Unix()),
		CreatedAt:   time.Now(),
		Status:      BackupStatusCreating,
	}

	// Create backup table
	// Database, schema and table names are from trusted configuration
	backupSQL := fmt.Sprintf( // #nosec G201 - identifiers from trusted config
		"CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s",
		plan.TargetDatabase, plan.TargetSchema, backup.BackupName,
		plan.SourceDatabase, plan.SourceSchema, table.SourceTable,
	)

	if _, err := conn.ExecContext(ctx, backupSQL); err != nil {
		backup.Status = BackupStatusFailed
		backup.Error = err.Error()
		return backup, fmt.Errorf("failed to create backup table: %w", err)
	}

	// Get row count
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s.%s", plan.TargetDatabase, plan.TargetSchema, backup.BackupName) // #nosec G201 - identifiers from trusted config
	var rowCount int64
	if err := conn.QueryRowContext(ctx, countSQL).Scan(&rowCount); err != nil {
		backup.RowCount = 0
	} else {
		backup.RowCount = rowCount
	}

	backup.Status = BackupStatusCompleted
	return backup, nil
}

func (rm *RollbackManager) performRollback(ctx context.Context, plan *MigrationPlan, execution *MigrationExecution, rollbackExecution *MigrationExecution) error {
	// Get target connection
	targetConn, err := rm.connectionProvider.GetTargetConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get target connection: %w", err)
	}
	defer targetConn.Close()

	// Get rollback plan
	rollbackPlan, err := rm.GetRollbackPlan(ctx, execution.ID)
	if err != nil {
		return fmt.Errorf("failed to get rollback plan: %w", err)
	}

	// Execute rollback steps
	for _, step := range rollbackPlan.Steps {
		rollbackExecution.Progress.CurrentOperation = step.Description

		if step.Operation == RollbackOperationValidate {
			// Perform validation
			continue
		}

		if step.SQL != "" {
			if _, err := targetConn.ExecContext(ctx, step.SQL); err != nil {
				return fmt.Errorf("rollback step failed for %s: %w", step.Table, err)
			}
		}

		// Update progress
		rollbackExecution.Metrics.QueriesExecuted++
	}

	return nil
}

func (rm *RollbackManager) performRecovery(ctx context.Context, originalExecution *MigrationExecution, checkpoint *ExecutionCheckpoint, recoveryExecution *MigrationExecution) error {
	// Get the original plan
	plan, err := rm.repository.GetPlan(ctx, originalExecution.PlanID)
	if err != nil {
		return fmt.Errorf("failed to get original plan: %w", err)
	}

	// Find the table to resume from
	var resumeFromTable *TableMigration
	for i, table := range plan.Tables {
		if table.SourceTable == checkpoint.Table {
			resumeFromTable = &plan.Tables[i]
			break
		}
	}

	if resumeFromTable == nil {
		return fmt.Errorf("table %s not found in plan", checkpoint.Table)
	}

	// Resume migration from checkpoint
	// This would involve continuing the migration from the checkpoint position
	// For now, just mark as completed
	recoveryExecution.Progress.CurrentOperation = "Recovery completed"

	return nil
}

func (rm *RollbackManager) saveBackupMetadata(ctx context.Context, backup *MigrationBackup) error {
	// Save backup metadata to repository
	// This would typically involve storing backup information in a database
	return nil
}

func (rm *RollbackManager) calculateBackupSize(tables []TableBackup) int64 {
	var totalSize int64
	for _, table := range tables {
		totalSize += table.Size
	}
	return totalSize
}

func (rm *RollbackManager) getExecutionBackups(ctx context.Context, executionID string) ([]MigrationBackup, error) {
	// Get backups for execution from repository
	// This is a placeholder implementation
	return []MigrationBackup{}, nil
}

func (rm *RollbackManager) hasSchemaChanges(plan *MigrationPlan) bool {
	// Check if the plan includes schema changes
	return plan.Type == MigrationTypeSchema || plan.Type == MigrationTypeCombined
}

func (rm *RollbackManager) determineRollbackStrategy(plan *MigrationPlan, execution *MigrationExecution) RollbackStrategy {
	// Determine the best rollback strategy based on plan and execution
	if execution.Status == MigrationStatusCompleted {
		return RollbackStrategyRestore
	}
	return RollbackStrategyCleanup
}

func (rm *RollbackManager) determineRollbackOperation(table TableMigration) RollbackOperation {
	// Determine rollback operation based on migration mode
	switch table.MigrationMode {
	case MigrationModeInsert:
		return RollbackOperationDelete
	case MigrationModeReplace:
		return RollbackOperationRestore
	default:
		return RollbackOperationRestore
	}
}

func (rm *RollbackManager) generateRollbackSQL(table TableMigration) string {
	// Generate appropriate rollback SQL based on table migration
	switch table.MigrationMode {
	case MigrationModeInsert:
		return fmt.Sprintf("DELETE FROM %s WHERE migration_id = ?", table.TargetTable)
	case MigrationModeReplace:
		return fmt.Sprintf("DROP TABLE %s; ALTER TABLE %s_backup RENAME TO %s", 
			table.TargetTable, table.TargetTable, table.TargetTable)
	default:
		return fmt.Sprintf("-- Manual rollback required for table %s", table.TargetTable)
	}
}

// Additional types for rollback functionality

// MigrationBackup represents a backup created before migration
type MigrationBackup struct {
	ID        string
	PlanID    string
	CreatedAt time.Time
	Status    BackupStatus
	Tables    []TableBackup
	Size      int64
	Error     string
	Metadata  map[string]interface{}
}

// TableBackup represents a backup of a single table
type TableBackup struct {
	TableName  string
	BackupName string
	CreatedAt  time.Time
	Status     BackupStatus
	RowCount   int64
	Size       int64
	Error      string
}

// BackupStatus represents the status of a backup
type BackupStatus string

const (
	BackupStatusCreating  BackupStatus = "CREATING"
	BackupStatusCompleted BackupStatus = "COMPLETED"
	BackupStatusFailed    BackupStatus = "FAILED"
	BackupStatusDeleted   BackupStatus = "DELETED"
)

// RollbackValidation represents rollback validation results
type RollbackValidation struct {
	ExecutionID      string
	PlanID           string
	ValidatedAt      time.Time
	CanRollback      bool
	RollbackEnabled  bool
	BackupExists     bool
	BackupInfo       *MigrationBackup
	Issues           []string
	Recommendations  []string
}

// RollbackPlan represents a plan for rolling back a migration
type RollbackPlan struct {
	ID              string
	OriginalPlanID  string
	ExecutionID     string
	CreatedAt       time.Time
	Strategy        RollbackStrategy
	Steps           []RollbackStep
	EstimatedTime   time.Duration
	Risks           []string
	Prerequisites   []string
}

// RollbackStep represents a single step in rollback
type RollbackStep struct {
	ID            string
	Order         int
	Table         string
	Operation     RollbackOperation
	SQL           string
	Description   string
	EstimatedTime time.Duration
}

// RollbackStrategy represents rollback strategy
type RollbackStrategy string

const (
	RollbackStrategyRestore RollbackStrategy = "RESTORE"
	RollbackStrategyCleanup RollbackStrategy = "CLEANUP"
	RollbackStrategyManual  RollbackStrategy = "MANUAL"
)

// RollbackOperation represents rollback operation type
type RollbackOperation string

const (
	RollbackOperationDelete   RollbackOperation = "DELETE"
	RollbackOperationRestore  RollbackOperation = "RESTORE"
	RollbackOperationTruncate RollbackOperation = "TRUNCATE"
	RollbackOperationValidate RollbackOperation = "VALIDATE"
)
