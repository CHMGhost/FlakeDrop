package rollback

import (
	"time"
)

// DeploymentState represents the state of a deployment
type DeploymentState string

const (
	StateInProgress DeploymentState = "in_progress"
	StateCompleted  DeploymentState = "completed"
	StateFailed     DeploymentState = "failed"
	StateRolledBack DeploymentState = "rolled_back"
)

// DeploymentRecord stores information about a deployment
type DeploymentRecord struct {
	ID            string            `json:"id"`
	Repository    string            `json:"repository"`
	Commit        string            `json:"commit"`
	Database      string            `json:"database"`
	Schema        string            `json:"schema"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       *time.Time        `json:"end_time,omitempty"`
	State         DeploymentState   `json:"state"`
	Files         []FileExecution   `json:"files"`
	Metadata      map[string]string `json:"metadata"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	SnapshotID    string            `json:"snapshot_id,omitempty"`
	PreviousID    string            `json:"previous_id,omitempty"`
}

// FileExecution represents the execution of a single file
type FileExecution struct {
	Path         string        `json:"path"`
	Order        int           `json:"order"`
	StartTime    time.Time     `json:"start_time"`
	EndTime      *time.Time    `json:"end_time,omitempty"`
	Success      bool          `json:"success"`
	ErrorMessage string        `json:"error_message,omitempty"`
	SQLContent   string        `json:"sql_content"`
	Checksum     string        `json:"checksum"`
	Duration     time.Duration `json:"duration,omitempty"`
}

// SchemaSnapshot represents a point-in-time snapshot of a schema
type SchemaSnapshot struct {
	ID           string                 `json:"id"`
	DeploymentID string                 `json:"deployment_id"`
	Database     string                 `json:"database"`
	Schema       string                 `json:"schema"`
	Timestamp    time.Time              `json:"timestamp"`
	Objects      []SchemaObject         `json:"objects"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// SchemaObject represents a database object in a schema
type SchemaObject struct {
	Type       string    `json:"type"` // TABLE, VIEW, PROCEDURE, FUNCTION, etc.
	Name       string    `json:"name"`
	Definition string    `json:"definition"`
	Checksum   string    `json:"checksum"`
	CreatedAt  time.Time `json:"created_at"`
	ModifiedAt time.Time `json:"modified_at"`
}

// RollbackPlan represents a plan for rolling back a deployment
type RollbackPlan struct {
	DeploymentID   string             `json:"deployment_id"`
	TargetID       string             `json:"target_id"`
	Strategy       RollbackStrategy   `json:"strategy"`
	Operations     []RollbackOperation `json:"operations"`
	EstimatedTime  time.Duration      `json:"estimated_time"`
	RiskLevel      string             `json:"risk_level"`
	Prerequisites  []string           `json:"prerequisites"`
}

// RollbackStrategy defines how a rollback should be performed
type RollbackStrategy string

const (
	StrategyPointInTime  RollbackStrategy = "point_in_time"
	StrategyTransaction  RollbackStrategy = "transaction"
	StrategyIncremental  RollbackStrategy = "incremental"
	StrategySnapshot     RollbackStrategy = "snapshot"
)

// RollbackOperation represents a single rollback operation
type RollbackOperation struct {
	Type        string `json:"type"`
	Object      string `json:"object"`
	SQL         string `json:"sql"`
	Order       int    `json:"order"`
	Description string `json:"description"`
}

// RecoveryPoint represents a point from which recovery can be performed
type RecoveryPoint struct {
	ID           string    `json:"id"`
	DeploymentID string    `json:"deployment_id"`
	Timestamp    time.Time `json:"timestamp"`
	Type         string    `json:"type"` // automatic, manual, scheduled
	Description  string    `json:"description"`
	SnapshotID   string    `json:"snapshot_id"`
	Valid        bool      `json:"valid"`
}

// BackupMetadata stores metadata about a backup
type BackupMetadata struct {
	ID            string                 `json:"id"`
	Database      string                 `json:"database"`
	Schema        string                 `json:"schema"`
	Timestamp     time.Time              `json:"timestamp"`
	Size          int64                  `json:"size"`
	Type          string                 `json:"type"` // full, incremental, differential
	Format        string                 `json:"format"` // sql, parquet, json
	Location      string                 `json:"location"`
	Checksum      string                 `json:"checksum"`
	CompressionType string               `json:"compression_type"`
	Encrypted     bool                   `json:"encrypted"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// FailureScenario represents different types of deployment failures
type FailureScenario string

const (
	FailurePartialDeployment FailureScenario = "partial_deployment"
	FailureConnectionLost    FailureScenario = "connection_lost"
	FailureSyntaxError       FailureScenario = "syntax_error"
	FailurePermissionDenied  FailureScenario = "permission_denied"
	FailureConstraintViolation FailureScenario = "constraint_violation"
	FailureTimeout           FailureScenario = "timeout"
	FailureResourceLimit     FailureScenario = "resource_limit"
)

// RecoveryStrategy defines how to recover from a failure
type RecoveryStrategy struct {
	Scenario     FailureScenario `json:"scenario"`
	AutoRecover  bool            `json:"auto_recover"`
	Steps        []RecoveryStep  `json:"steps"`
	MaxRetries   int             `json:"max_retries"`
	RetryDelay   time.Duration   `json:"retry_delay"`
	Notification bool            `json:"notification"`
}

// RecoveryStep represents a single step in a recovery process
type RecoveryStep struct {
	Order       int           `json:"order"`
	Action      string        `json:"action"`
	Description string        `json:"description"`
	Timeout     time.Duration `json:"timeout"`
	OnFailure   string        `json:"on_failure"` // continue, abort, retry
}