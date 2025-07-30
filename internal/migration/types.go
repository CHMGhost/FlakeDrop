package migration

import (
	"context"
	"database/sql"
	"time"

	"flakedrop/internal/schema"
)

// MigrationType represents the type of data migration
type MigrationType string

const (
	MigrationTypeData     MigrationType = "DATA"
	MigrationTypeSchema   MigrationType = "SCHEMA"
	MigrationTypeCombined MigrationType = "COMBINED"
	MigrationTypeTransform MigrationType = "TRANSFORM"
)

// MigrationStatus represents the status of a migration
type MigrationStatus string

const (
	MigrationStatusPending    MigrationStatus = "PENDING"
	MigrationStatusPlanning   MigrationStatus = "PLANNING"
	MigrationStatusReady      MigrationStatus = "READY"
	MigrationStatusRunning    MigrationStatus = "RUNNING"
	MigrationStatusCompleted  MigrationStatus = "COMPLETED"
	MigrationStatusFailed     MigrationStatus = "FAILED"
	MigrationStatusRolledBack MigrationStatus = "ROLLED_BACK"
	MigrationStatusPaused     MigrationStatus = "PAUSED"
)

// MigrationStrategy represents the execution strategy
type MigrationStrategy string

const (
	MigrationStrategyBatch      MigrationStrategy = "BATCH"
	MigrationStrategyStream     MigrationStrategy = "STREAM"
	MigrationStrategyIncremental MigrationStrategy = "INCREMENTAL"
	MigrationStrategyBulk       MigrationStrategy = "BULK"
	MigrationStrategyParallel   MigrationStrategy = "PARALLEL"
)

// MigrationPlan represents a complete migration plan
type MigrationPlan struct {
	ID               string
	Name             string
	Description      string
	Type             MigrationType
	Strategy         MigrationStrategy
	SourceDatabase   string
	SourceSchema     string
	TargetDatabase   string
	TargetSchema     string
	Tables           []TableMigration
	Dependencies     []string
	PreConditions    []PreCondition
	PostActions      []PostAction
	Validations      []ValidationRule
	Configuration    MigrationConfig
	EstimatedTime    time.Duration
	EstimatedSize    int64
	CreatedAt        time.Time
	CreatedBy        string
	Version          string
	Tags             map[string]string
}

// TableMigration represents migration of a single table
type TableMigration struct {
	SourceTable      string
	TargetTable      string
	MigrationMode    MigrationMode
	BatchSize        int
	Parallelism      int
	Transformations  []DataTransformation
	Filters          []DataFilter
	Mappings         []ColumnMapping
	Validations      []ValidationRule
	DependsOn        []string
	Priority         int
	EstimatedRows    int64
	EstimatedSize    int64
	Checkpoints      []Checkpoint
}

// MigrationMode represents how data should be migrated
type MigrationMode string

const (
	MigrationModeInsert    MigrationMode = "INSERT"
	MigrationModeUpsert    MigrationMode = "UPSERT"
	MigrationModeReplace   MigrationMode = "REPLACE"
	MigrationModeAppend    MigrationMode = "APPEND"
	MigrationModeMerge     MigrationMode = "MERGE"
	MigrationModeSync      MigrationMode = "SYNC"
)

// DataTransformation represents a data transformation rule
type DataTransformation struct {
	ID          string
	Name        string
	Type        TransformationType
	SourceField string
	TargetField string
	Expression  string
	Parameters  map[string]interface{}
	Order       int
	Condition   string
}

// TransformationType represents the type of transformation
type TransformationType string

const (
	TransformationTypeMap      TransformationType = "MAP"
	TransformationTypeConvert  TransformationType = "CONVERT"
	TransformationTypeCompute  TransformationType = "COMPUTE"
	TransformationTypeFilter   TransformationType = "FILTER"
	TransformationTypeAggregate TransformationType = "AGGREGATE"
	TransformationTypeLookup   TransformationType = "LOOKUP"
	TransformationTypeCustom   TransformationType = "CUSTOM"
)

// DataFilter represents a filter condition
type DataFilter struct {
	ID         string
	Name       string
	Expression string
	Parameters map[string]interface{}
	Enabled    bool
}

// ColumnMapping represents column mappings between source and target
type ColumnMapping struct {
	SourceColumn string
	TargetColumn string
	DataType     string
	Transform    *DataTransformation
	Required     bool
	DefaultValue interface{}
}

// ValidationRule represents a data validation rule
type ValidationRule struct {
	ID          string
	Name        string
	Type        ValidationType
	Expression  string
	Parameters  map[string]interface{}
	Severity    ValidationSeverity
	Enabled     bool
	Description string
}

// ValidationType represents the type of validation
type ValidationType string

const (
	ValidationTypeRowCount    ValidationType = "ROW_COUNT"
	ValidationTypeDataQuality ValidationType = "DATA_QUALITY"
	ValidationTypeIntegrity   ValidationType = "INTEGRITY"
	ValidationTypeConsistency ValidationType = "CONSISTENCY"
	ValidationTypeCompleteness ValidationType = "COMPLETENESS"
	ValidationTypeAccuracy    ValidationType = "ACCURACY"
	ValidationTypeCustom      ValidationType = "CUSTOM"
)

// ValidationSeverity represents the severity of validation issues
type ValidationSeverity string

const (
	ValidationSeverityInfo    ValidationSeverity = "INFO"
	ValidationSeverityWarning ValidationSeverity = "WARNING"
	ValidationSeverityError   ValidationSeverity = "ERROR"
	ValidationSeverityCritical ValidationSeverity = "CRITICAL"
)

// PreCondition represents a condition that must be met before migration
type PreCondition struct {
	ID          string
	Name        string
	Type        PreConditionType
	Expression  string
	Parameters  map[string]interface{}
	Required    bool
	Description string
}

// PreConditionType represents the type of pre-condition
type PreConditionType string

const (
	PreConditionTypeTableExists    PreConditionType = "TABLE_EXISTS"
	PreConditionTypeSchemaExists   PreConditionType = "SCHEMA_EXISTS"
	PreConditionTypeDataExists     PreConditionType = "DATA_EXISTS"
	PreConditionTypePermission     PreConditionType = "PERMISSION"
	PreConditionTypeSpace          PreConditionType = "SPACE"
	PreConditionTypeCustom         PreConditionType = "CUSTOM"
)

// PostAction represents an action to execute after migration
type PostAction struct {
	ID          string
	Name        string
	Type        PostActionType
	SQL         string
	Parameters  map[string]interface{}
	Order       int
	Required    bool
	Description string
}

// PostActionType represents the type of post-action
type PostActionType string

const (
	PostActionTypeAnalyze     PostActionType = "ANALYZE"
	PostActionTypeIndex       PostActionType = "INDEX"
	PostActionTypeConstraint  PostActionType = "CONSTRAINT"
	PostActionTypeGrant       PostActionType = "GRANT"
	PostActionTypeCleanup     PostActionType = "CLEANUP"
	PostActionTypeNotify      PostActionType = "NOTIFY"
	PostActionTypeCustom      PostActionType = "CUSTOM"
)

// MigrationConfig contains configuration for migration execution
type MigrationConfig struct {
	BatchSize           int
	Parallelism         int
	MaxRetries          int
	RetryDelay          time.Duration
	Timeout             time.Duration
	CheckpointInterval  time.Duration
	ValidationInterval  time.Duration
	MemoryLimit         int64
	TempTablePrefix     string
	UseTransactions     bool
	EnableCheckpoints   bool
	EnableValidation    bool
	EnableRollback      bool
	DryRun              bool
	Verbose             bool
	ProgressReporting   bool
	CustomProperties    map[string]interface{}
}

// DefaultMigrationConfig returns sensible defaults
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		BatchSize:           10000,
		Parallelism:         4,
		MaxRetries:          3,
		RetryDelay:          time.Second * 30,
		Timeout:             time.Hour * 2,
		CheckpointInterval:  time.Minute * 10,
		ValidationInterval:  time.Minute * 5,
		MemoryLimit:         1024 * 1024 * 1024, // 1GB
		TempTablePrefix:     "_MIGRATION_TEMP_",
		UseTransactions:     true,
		EnableCheckpoints:   true,
		EnableValidation:    true,
		EnableRollback:      true,
		DryRun:              false,
		Verbose:             false,
		ProgressReporting:   true,
		CustomProperties:    make(map[string]interface{}),
	}
}

// MigrationExecution represents a running migration
type MigrationExecution struct {
	ID                string
	PlanID            string
	Status            MigrationStatus
	StartedAt         time.Time
	CompletedAt       *time.Time
	Duration          time.Duration
	Progress          MigrationProgress
	Metrics           MigrationMetrics
	Errors            []MigrationError
	Warnings          []MigrationWarning
	Checkpoints       []ExecutionCheckpoint
	CurrentTable      string
	CurrentBatch      int64
	TotalBatches      int64
	ExecutedBy        string
	Environment       string
	Configuration     MigrationConfig
	ValidationResults []ValidationResult
}

// MigrationProgress tracks migration progress
type MigrationProgress struct {
	TotalTables      int
	CompletedTables  int
	TotalRows        int64
	ProcessedRows    int64
	TotalSize        int64
	ProcessedSize    int64
	PercentComplete  float64
	EstimatedETA     time.Time
	CurrentOperation string
	ThroughputRows   float64
	ThroughputSize   float64
}

// MigrationMetrics contains detailed metrics
type MigrationMetrics struct {
	RowsInserted     int64
	RowsUpdated      int64
	RowsDeleted      int64
	RowsSkipped      int64
	RowsErrored      int64
	BytesProcessed   int64
	QueriesExecuted  int64
	Transactions     int64
	Checkpoints      int64
	Validations      int64
	Transformations  int64
	Retries          int64
	ExecutionTime    time.Duration
	ValidationTime   time.Duration
	TransformTime    time.Duration
	IOTime           time.Duration
}

// MigrationError represents an error during migration
type MigrationError struct {
	ID          string
	Code        string
	Message     string
	Table       string
	Batch       int64
	SQL         string
	Context     map[string]interface{}
	OccurredAt  time.Time
	RetryCount  int
	Severity    string
	Resolved    bool
}

// MigrationWarning represents a warning during migration
type MigrationWarning struct {
	ID         string
	Message    string
	Table      string
	Context    map[string]interface{}
	OccurredAt time.Time
	Severity   string
}

// ExecutionCheckpoint represents a checkpoint during execution
type ExecutionCheckpoint struct {
	ID           string
	CreatedAt    time.Time
	Table        string
	Batch        int64
	RowsProcessed int64
	State        map[string]interface{}
	Metadata     map[string]interface{}
}

// ValidationResult represents the result of a validation
type ValidationResult struct {
	ID          string
	RuleID      string
	RuleName    string
	Table       string
	Status      ValidationResultStatus
	Message     string
	Details     map[string]interface{}
	ExecutedAt  time.Time
	Duration    time.Duration
	Severity    ValidationSeverity
}

// ValidationResultStatus represents the status of validation result
type ValidationResultStatus string

const (
	ValidationResultStatusPassed ValidationResultStatus = "PASSED"
	ValidationResultStatusFailed ValidationResultStatus = "FAILED"
	ValidationResultStatusSkipped ValidationResultStatus = "SKIPPED"
	ValidationResultStatusError  ValidationResultStatus = "ERROR"
)

// Checkpoint represents a migration checkpoint
type Checkpoint struct {
	ID           string
	Table        string
	Position     int64
	CreatedAt    time.Time
	Metadata     map[string]interface{}
	RecoveryData map[string]interface{}
}

// MigrationTemplate represents a reusable migration template
type MigrationTemplate struct {
	ID              string
	Name            string
	Description     string
	Category        string
	Version         string
	Type            MigrationType
	Strategy        MigrationStrategy
	Parameters      []TemplateParameter
	PlanTemplate    string
	ScriptTemplates map[string]string
	Validations     []ValidationRule
	Tags            []string
	CreatedAt       time.Time
	CreatedBy       string
}

// TemplateParameter represents a template parameter
type TemplateParameter struct {
	Name         string
	Type         string
	Description  string
	Required     bool
	DefaultValue interface{}
	Validation   string
}

// MigrationInterface defines the contract for migration operations
type MigrationInterface interface {
	// Planning
	CreatePlan(ctx context.Context, config PlanConfig) (*MigrationPlan, error)
	ValidatePlan(ctx context.Context, plan *MigrationPlan) error
	OptimizePlan(ctx context.Context, plan *MigrationPlan) (*MigrationPlan, error)
	
	// Execution
	Execute(ctx context.Context, plan *MigrationPlan) (*MigrationExecution, error)
	Pause(ctx context.Context, executionID string) error
	Resume(ctx context.Context, executionID string) error
	Cancel(ctx context.Context, executionID string) error
	
	// Monitoring
	GetStatus(ctx context.Context, executionID string) (*MigrationProgress, error)
	GetMetrics(ctx context.Context, executionID string) (*MigrationMetrics, error)
	GetLogs(ctx context.Context, executionID string, limit int) ([]string, error)
	
	// Recovery
	Rollback(ctx context.Context, executionID string) error
	RecoverFromCheckpoint(ctx context.Context, executionID, checkpointID string) error
	
	// Templates
	CreateTemplate(ctx context.Context, template *MigrationTemplate) error
	GetTemplate(ctx context.Context, templateID string) (*MigrationTemplate, error)
	ListTemplates(ctx context.Context) ([]*MigrationTemplate, error)
}

// DataSource represents a data source configuration
type DataSource struct {
	ID          string
	Name        string
	Type        string
	Connection  map[string]interface{}
	Credentials map[string]interface{}
	Properties  map[string]interface{}
}

// PlanConfig contains configuration for plan creation
type PlanConfig struct {
	Name            string
	Description     string
	Type            MigrationType
	Strategy        MigrationStrategy
	SourceConfig    DataSource
	TargetConfig    DataSource
	Tables          []string
	SchemaConfig    *schema.ComparisonOptions
	MigrationConfig MigrationConfig
	TemplateID      string
	Parameters      map[string]interface{}
}

// MigrationRepository defines data persistence operations
type MigrationRepository interface {
	// Plans
	SavePlan(ctx context.Context, plan *MigrationPlan) error
	GetPlan(ctx context.Context, planID string) (*MigrationPlan, error)
	ListPlans(ctx context.Context, filters map[string]interface{}) ([]*MigrationPlan, error)
	DeletePlan(ctx context.Context, planID string) error
	
	// Executions
	SaveExecution(ctx context.Context, execution *MigrationExecution) error
	GetExecution(ctx context.Context, executionID string) (*MigrationExecution, error)
	ListExecutions(ctx context.Context, filters map[string]interface{}) ([]*MigrationExecution, error)
	
	// Checkpoints
	SaveCheckpoint(ctx context.Context, checkpoint *ExecutionCheckpoint) error
	GetCheckpoints(ctx context.Context, executionID string) ([]*ExecutionCheckpoint, error)
	DeleteCheckpoints(ctx context.Context, executionID string) error
	
	// Templates
	SaveTemplate(ctx context.Context, template *MigrationTemplate) error
	GetTemplate(ctx context.Context, templateID string) (*MigrationTemplate, error)
	ListTemplates(ctx context.Context) ([]*MigrationTemplate, error)
	DeleteTemplate(ctx context.Context, templateID string) error
}

// ConnectionProvider provides database connections
type ConnectionProvider interface {
	GetSourceConnection(ctx context.Context) (*sql.DB, error)
	GetTargetConnection(ctx context.Context) (*sql.DB, error)
	GetConnection(ctx context.Context, config DataSource) (*sql.DB, error)
	Close() error
}
