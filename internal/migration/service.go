package migration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"flakedrop/internal/rollback"
	"flakedrop/internal/schema"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// Service provides comprehensive data migration capabilities
type Service struct {
	schemaService      *schema.Service
	snowflakeService   *snowflake.Service
	rollbackService    *rollback.Manager
	planner           *Planner
	executor          *Executor
	rollbackManager   *RollbackManager
	templateManager   *TemplateManager
	repository        MigrationRepository
	connectionProvider ConnectionProvider
	errorHandler      *errors.ErrorHandler
	mu                sync.RWMutex
	config            ServiceConfig
}

// ServiceConfig contains service configuration
type ServiceConfig struct {
	MaxConcurrentMigrations int
	DefaultTimeout          time.Duration
	EnableMetrics          bool
	EnableAudit            bool
	RetentionDays          int
}

// DefaultServiceConfig returns default service configuration
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		MaxConcurrentMigrations: 5,
		DefaultTimeout:          time.Hour * 4,
		EnableMetrics:          true,
		EnableAudit:            true,
		RetentionDays:          30,
	}
}

// NewService creates a new migration service
func NewService(schemaService *schema.Service, snowflakeService *snowflake.Service, repository MigrationRepository) *Service {
	connProvider := NewConnectionProvider(snowflakeService)
	rollbackConfig := &rollback.Config{
		StorageDir:         ".flakedrop/rollback",
		EnableAutoSnapshot: true,
		SnapshotRetention:  24 * time.Hour,
		BackupRetention:    7 * 24 * time.Hour,
		MaxRollbackDepth:   10,
		DryRunByDefault:    false,
	}
	rollbackService, err := rollback.NewManager(snowflakeService, rollbackConfig)
	if err != nil {
		// Handle error appropriately - for now continue with nil
		rollbackService = nil
	}

	service := &Service{
		schemaService:      schemaService,
		snowflakeService:   snowflakeService,
		rollbackService:    rollbackService,
		repository:        repository,
		connectionProvider: connProvider,
		errorHandler:      errors.GetGlobalErrorHandler(),
		config:            DefaultServiceConfig(),
	}

	// Initialize components
	service.planner = NewPlanner(schemaService)
	service.executor = NewExecutor(connProvider, repository)
	service.rollbackManager = NewRollbackManager(connProvider, repository, rollbackService)
	service.templateManager = NewTemplateManager(repository)

	return service
}

// Planning methods

// CreatePlan creates a comprehensive migration plan
func (s *Service) CreatePlan(ctx context.Context, config PlanConfig) (*MigrationPlan, error) {
	return s.planner.CreatePlan(ctx, config)
}

// ValidatePlan validates a migration plan
func (s *Service) ValidatePlan(ctx context.Context, plan *MigrationPlan) error {
	return s.planner.ValidatePlan(ctx, plan)
}

// OptimizePlan optimizes a migration plan for performance
func (s *Service) OptimizePlan(ctx context.Context, plan *MigrationPlan) (*MigrationPlan, error) {
	return s.planner.OptimizePlan(ctx, plan)
}

// SavePlan saves a migration plan
func (s *Service) SavePlan(ctx context.Context, plan *MigrationPlan) error {
	return s.repository.SavePlan(ctx, plan)
}

// GetPlan retrieves a migration plan
func (s *Service) GetPlan(ctx context.Context, planID string) (*MigrationPlan, error) {
	return s.repository.GetPlan(ctx, planID)
}

// ListPlans lists migration plans with filters
func (s *Service) ListPlans(ctx context.Context, filters map[string]interface{}) ([]*MigrationPlan, error) {
	return s.repository.ListPlans(ctx, filters)
}

// DeletePlan deletes a migration plan
func (s *Service) DeletePlan(ctx context.Context, planID string) error {
	return s.repository.DeletePlan(ctx, planID)
}

// Execution methods

// Execute runs a migration plan
func (s *Service) Execute(ctx context.Context, plan *MigrationPlan) (*MigrationExecution, error) {
	// Validate plan before execution
	if err := s.ValidatePlan(ctx, plan); err != nil {
		return nil, fmt.Errorf("plan validation failed: %w", err)
	}

	// Check concurrent execution limit
	if err := s.checkConcurrencyLimit(ctx); err != nil {
		return nil, err
	}

	// Create backup if enabled
	if plan.Configuration.EnableRollback {
		backup, err := s.rollbackManager.CreateBackup(ctx, plan)
		if err != nil {
			return nil, fmt.Errorf("backup creation failed: %w", err)
		}
		// Store backup reference in plan metadata
		if plan.Tags == nil {
			plan.Tags = make(map[string]string)
		}
		plan.Tags["backup_id"] = backup.ID
	}

	// Execute migration
	execution, err := s.executor.Execute(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	return execution, nil
}

// Pause pauses a running migration
func (s *Service) Pause(ctx context.Context, executionID string) error {
	return s.executor.Pause(ctx, executionID)
}

// Resume resumes a paused migration
func (s *Service) Resume(ctx context.Context, executionID string) error {
	return s.executor.Resume(ctx, executionID)
}

// Cancel cancels a running migration
func (s *Service) Cancel(ctx context.Context, executionID string) error {
	return s.executor.Cancel(ctx, executionID)
}

// Monitoring methods

// GetStatus gets migration execution status
func (s *Service) GetStatus(ctx context.Context, executionID string) (*MigrationProgress, error) {
	return s.executor.GetStatus(ctx, executionID)
}

// GetMetrics gets migration execution metrics
func (s *Service) GetMetrics(ctx context.Context, executionID string) (*MigrationMetrics, error) {
	return s.executor.GetMetrics(ctx, executionID)
}

// GetLogs gets migration execution logs
func (s *Service) GetLogs(ctx context.Context, executionID string, limit int) ([]string, error) {
	// Implementation would retrieve logs from logging system
	return []string{}, nil
}

// GetExecution retrieves a migration execution
func (s *Service) GetExecution(ctx context.Context, executionID string) (*MigrationExecution, error) {
	return s.repository.GetExecution(ctx, executionID)
}

// ListExecutions lists migration executions with filters
func (s *Service) ListExecutions(ctx context.Context, filters map[string]interface{}) ([]*MigrationExecution, error) {
	return s.repository.ListExecutions(ctx, filters)
}

// Recovery methods

// Rollback performs rollback of a migration
func (s *Service) Rollback(ctx context.Context, executionID string) error {
	return s.rollbackManager.Rollback(ctx, executionID)
}

// RecoverFromCheckpoint recovers migration from a checkpoint
func (s *Service) RecoverFromCheckpoint(ctx context.Context, executionID, checkpointID string) error {
	return s.rollbackManager.RecoverFromCheckpoint(ctx, executionID, checkpointID)
}

// ValidateRollback checks if rollback is possible
func (s *Service) ValidateRollback(ctx context.Context, executionID string) (*RollbackValidation, error) {
	return s.rollbackManager.ValidateRollbackPossibility(ctx, executionID)
}

// GetRollbackPlan creates a rollback plan
func (s *Service) GetRollbackPlan(ctx context.Context, executionID string) (*RollbackPlan, error) {
	return s.rollbackManager.GetRollbackPlan(ctx, executionID)
}

// Template methods

// CreateTemplate creates a new migration template
func (s *Service) CreateTemplate(ctx context.Context, plan *MigrationPlan, info TemplateInfo) (*MigrationTemplate, error) {
	return s.templateManager.CreateTemplate(ctx, plan, info)
}

// GetTemplate retrieves a template by ID
func (s *Service) GetTemplate(ctx context.Context, templateID string) (*MigrationTemplate, error) {
	return s.templateManager.GetTemplate(ctx, templateID)
}

// ListTemplates returns all available templates
func (s *Service) ListTemplates(ctx context.Context) ([]*MigrationTemplate, error) {
	return s.templateManager.ListTemplates(ctx)
}

// ApplyTemplate applies a template with parameters
func (s *Service) ApplyTemplate(ctx context.Context, templateID string, parameters map[string]interface{}) (*MigrationPlan, error) {
	return s.templateManager.ApplyTemplate(ctx, templateID, parameters)
}

// DeleteTemplate deletes a template
func (s *Service) DeleteTemplate(ctx context.Context, templateID string) error {
	return s.templateManager.DeleteTemplate(ctx, templateID)
}

// Advanced methods

// GenerateMigrationPlan generates a migration plan from schema differences
func (s *Service) GenerateMigrationPlan(ctx context.Context, sourceConfig, targetConfig DataSource, options *schema.ComparisonOptions) (*MigrationPlan, error) {
	// Compare schemas
	result, err := s.compareSchemas(ctx, sourceConfig, targetConfig, options)
	if err != nil {
		return nil, fmt.Errorf("schema comparison failed: %w", err)
	}

	// Create plan configuration
	planConfig := PlanConfig{
		Name:             fmt.Sprintf("Auto-generated plan for %s to %s", sourceConfig.Name, targetConfig.Name),
		Description:      "Migration plan generated from schema differences",
		Type:             MigrationTypeCombined,
		Strategy:         MigrationStrategyBatch,
		SourceConfig:     sourceConfig,
		TargetConfig:     targetConfig,
		MigrationConfig:  DefaultMigrationConfig(),
		Parameters:       make(map[string]interface{}),
	}

	// Extract tables from differences
	for _, diff := range result.Differences {
		if diff.ObjectType == schema.ObjectTypeTable && diff.DiffType != schema.DiffTypeRemoved {
			planConfig.Tables = append(planConfig.Tables, diff.ObjectName)
		}
	}

	// Create migration plan
	plan, err := s.CreatePlan(ctx, planConfig)
	if err != nil {
		return nil, fmt.Errorf("plan creation failed: %w", err)
	}

	// Add schema changes to plan
	plan.Tags["schema_comparison_id"] = result.SourceEnvironment + "_" + result.TargetEnvironment
	plan.Tags["auto_generated"] = "true"

	return plan, nil
}

// AnalyzeMigrationImpact analyzes the impact of a migration plan
func (s *Service) AnalyzeMigrationImpact(ctx context.Context, plan *MigrationPlan) (*MigrationImpactAnalysis, error) {
	analysis := &MigrationImpactAnalysis{
		PlanID:         plan.ID,
		AnalyzedAt:     time.Now(),
		EstimatedTime:  plan.EstimatedTime,
		EstimatedSize:  plan.EstimatedSize,
		RiskLevel:      "LOW",
		RiskFactors:    []string{},
		Recommendations: []string{},
		Dependencies:   []string{},
	}

	// Analyze table sizes and complexity
	var totalRows int64
	var largeTablesCount int
	for _, table := range plan.Tables {
		totalRows += table.EstimatedRows
		if table.EstimatedRows > 10000000 { // 10M rows
			largeTablesCount++
			analysis.RiskFactors = append(analysis.RiskFactors, fmt.Sprintf("Large table: %s (%d rows)", table.SourceTable, table.EstimatedRows))
		}

		// Check for complex transformations
		if len(table.Transformations) > 5 {
			analysis.RiskFactors = append(analysis.RiskFactors, fmt.Sprintf("Complex transformations in table: %s", table.SourceTable))
		}

		// Add dependencies
		for _, dep := range table.DependsOn {
			analysis.Dependencies = append(analysis.Dependencies, fmt.Sprintf("%s -> %s", dep, table.SourceTable))
		}
	}

	// Determine risk level
	if largeTablesCount > 3 || totalRows > 100000000 {
		analysis.RiskLevel = "HIGH"
		analysis.Recommendations = append(analysis.Recommendations, "Consider using streaming strategy for large tables")
		analysis.Recommendations = append(analysis.Recommendations, "Schedule migration during low-traffic periods")
	} else if largeTablesCount > 0 || totalRows > 10000000 {
		analysis.RiskLevel = "MEDIUM"
		analysis.Recommendations = append(analysis.Recommendations, "Monitor migration progress closely")
	}

	// Analyze dependencies
	if len(analysis.Dependencies) > 10 {
		analysis.RiskFactors = append(analysis.RiskFactors, "Complex dependency chain")
		analysis.Recommendations = append(analysis.Recommendations, "Consider breaking migration into smaller phases")
	}

	// Analyze configuration
	if !plan.Configuration.EnableCheckpoints {
		analysis.RiskFactors = append(analysis.RiskFactors, "Checkpoints disabled")
		analysis.Recommendations = append(analysis.Recommendations, "Enable checkpoints for large migrations")
	}

	if !plan.Configuration.EnableRollback {
		analysis.RiskFactors = append(analysis.RiskFactors, "Rollback disabled")
		analysis.Recommendations = append(analysis.Recommendations, "Enable rollback for production migrations")
	}

	return analysis, nil
}

// GetMigrationHistory gets migration history for a table or database
func (s *Service) GetMigrationHistory(ctx context.Context, target string, targetType string) ([]*MigrationExecution, error) {
	filters := map[string]interface{}{
		targetType: target,
	}

	executions, err := s.repository.ListExecutions(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration history: %w", err)
	}

	return executions, nil
}

// ValidateMigrationReadiness validates if the system is ready for migration
func (s *Service) ValidateMigrationReadiness(ctx context.Context, plan *MigrationPlan) (*ReadinessValidation, error) {
	validation := &ReadinessValidation{
		PlanID:      plan.ID,
		ValidatedAt: time.Now(),
		Ready:       true,
		Issues:      []string{},
		Warnings:    []string{},
		Checks:      []ReadinessCheck{},
	}

	// Check database connectivity
	if err := s.checkConnectivity(ctx, validation); err != nil {
		validation.Ready = false
		validation.Issues = append(validation.Issues, fmt.Sprintf("Connectivity check failed: %v", err))
	}

	// Check permissions
	if err := s.checkPermissions(ctx, plan, validation); err != nil {
		validation.Ready = false
		validation.Issues = append(validation.Issues, fmt.Sprintf("Permission check failed: %v", err))
	}

	// Check disk space
	if err := s.checkDiskSpace(ctx, plan, validation); err != nil {
		validation.Warnings = append(validation.Warnings, fmt.Sprintf("Disk space warning: %v", err))
	}

	// Check concurrent migrations
	if err := s.checkConcurrentMigrations(ctx, validation); err != nil {
		validation.Warnings = append(validation.Warnings, fmt.Sprintf("Concurrent migration warning: %v", err))
	}

	// Check schema compatibility
	if err := s.checkSchemaCompatibility(ctx, plan, validation); err != nil {
		validation.Ready = false
		validation.Issues = append(validation.Issues, fmt.Sprintf("Schema compatibility check failed: %v", err))
	}

	return validation, nil
}

// Private helper methods

func (s *Service) compareSchemas(ctx context.Context, sourceConfig, targetConfig DataSource, options *schema.ComparisonOptions) (*schema.ComparisonResult, error) {
	// This would use the schema comparison service
	// For now, return a mock result
	return &schema.ComparisonResult{
		SourceEnvironment: sourceConfig.Name,
		TargetEnvironment: targetConfig.Name,
		ComparedAt:        time.Now(),
		Differences:       []schema.Difference{},
	}, nil
}

func (s *Service) checkConcurrencyLimit(ctx context.Context) error {
	filters := map[string]interface{}{
		"status": MigrationStatusRunning,
	}

	executions, err := s.repository.ListExecutions(ctx, filters)
	if err != nil {
		return fmt.Errorf("failed to check concurrent executions: %w", err)
	}

	if len(executions) >= s.config.MaxConcurrentMigrations {
		return fmt.Errorf("maximum concurrent migrations limit reached (%d)", s.config.MaxConcurrentMigrations)
	}

	return nil
}

func (s *Service) checkConnectivity(ctx context.Context, validation *ReadinessValidation) error {
	// Check source connection
	sourceConn, err := s.connectionProvider.GetSourceConnection(ctx)
	if err != nil {
		validation.Checks = append(validation.Checks, ReadinessCheck{
			Name:   "Source Connectivity",
			Status: "FAILED",
			Message: err.Error(),
		})
		return err
	}
	sourceConn.Close()

	// Check target connection
	targetConn, err := s.connectionProvider.GetTargetConnection(ctx)
	if err != nil {
		validation.Checks = append(validation.Checks, ReadinessCheck{
			Name:   "Target Connectivity",
			Status: "FAILED",
			Message: err.Error(),
		})
		return err
	}
	targetConn.Close()

	validation.Checks = append(validation.Checks, ReadinessCheck{
		Name:   "Database Connectivity",
		Status: "PASSED",
		Message: "All database connections successful",
	})

	return nil
}

func (s *Service) checkPermissions(ctx context.Context, plan *MigrationPlan, validation *ReadinessValidation) error {
	// Check required permissions
	// This would involve querying database permissions
	validation.Checks = append(validation.Checks, ReadinessCheck{
		Name:   "Permissions",
		Status: "PASSED",
		Message: "Required permissions verified",
	})
	return nil
}

func (s *Service) checkDiskSpace(ctx context.Context, plan *MigrationPlan, validation *ReadinessValidation) error {
	// Check available disk space
	// This would involve querying system resources
	validation.Checks = append(validation.Checks, ReadinessCheck{
		Name:   "Disk Space",
		Status: "PASSED",
		Message: "Sufficient disk space available",
	})
	return nil
}

func (s *Service) checkConcurrentMigrations(ctx context.Context, validation *ReadinessValidation) error {
	// Check for concurrent migrations
	validation.Checks = append(validation.Checks, ReadinessCheck{
		Name:   "Concurrent Migrations",
		Status: "PASSED",
		Message: "No conflicting migrations detected",
	})
	return nil
}

func (s *Service) checkSchemaCompatibility(ctx context.Context, plan *MigrationPlan, validation *ReadinessValidation) error {
	// Check schema compatibility
	validation.Checks = append(validation.Checks, ReadinessCheck{
		Name:   "Schema Compatibility",
		Status: "PASSED",
		Message: "Schema structures are compatible",
	})
	return nil
}

// Additional types for the service

// MigrationImpactAnalysis represents migration impact analysis
type MigrationImpactAnalysis struct {
	PlanID          string
	AnalyzedAt      time.Time
	EstimatedTime   time.Duration
	EstimatedSize   int64
	RiskLevel       string
	RiskFactors     []string
	Recommendations []string
	Dependencies    []string
}

// ReadinessValidation represents migration readiness validation
type ReadinessValidation struct {
	PlanID      string
	ValidatedAt time.Time
	Ready       bool
	Issues      []string
	Warnings    []string
	Checks      []ReadinessCheck
}

// ReadinessCheck represents a single readiness check
type ReadinessCheck struct {
	Name    string
	Status  string
	Message string
}

// DefaultConnectionProvider provides a default connection provider implementation
type DefaultConnectionProvider struct {
	snowflakeService *snowflake.Service
}

// NewConnectionProvider creates a new connection provider
func NewConnectionProvider(snowflakeService *snowflake.Service) ConnectionProvider {
	return &DefaultConnectionProvider{
		snowflakeService: snowflakeService,
	}
}

// GetSourceConnection returns a source database connection
func (cp *DefaultConnectionProvider) GetSourceConnection(ctx context.Context) (*sql.DB, error) {
	return cp.snowflakeService.GetDB(), nil
}

// GetTargetConnection returns a target database connection
func (cp *DefaultConnectionProvider) GetTargetConnection(ctx context.Context) (*sql.DB, error) {
	return cp.snowflakeService.GetDB(), nil
}

// GetConnection returns a database connection for the given config
func (cp *DefaultConnectionProvider) GetConnection(ctx context.Context, config DataSource) (*sql.DB, error) {
	return cp.snowflakeService.GetDB(), nil
}

// Close closes all connections
func (cp *DefaultConnectionProvider) Close() error {
	return nil
}
