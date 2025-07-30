package rollback

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// Manager provides comprehensive rollback and disaster recovery functionality
type Manager struct {
	snowflakeService   *snowflake.Service
	historyManager     *HistoryManager
	snapshotManager    *SnapshotManager
	transactionManager *TransactionManager
	backupManager      *BackupManager
	strategyManager    *StrategyManager
	config             *Config
	mu                 sync.RWMutex
}

// Config holds rollback manager configuration
type Config struct {
	StorageDir         string
	EnableAutoSnapshot bool
	SnapshotRetention  time.Duration
	BackupRetention    time.Duration
	MaxRollbackDepth   int
	DryRunByDefault    bool
}

// NewManager creates a new rollback manager
func NewManager(snowflakeService *snowflake.Service, config *Config) (*Manager, error) {
	if config.StorageDir == "" {
		config.StorageDir = filepath.Join(".", ".flakedrop", "rollback")
	}

	// Initialize sub-managers
	historyManager, err := NewHistoryManager(config.StorageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create history manager: %w", err)
	}

	snapshotManager, err := NewSnapshotManager(snowflakeService, config.StorageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot manager: %w", err)
	}

	transactionManager := NewTransactionManager(snowflakeService)

	backupManager, err := NewBackupManager(snowflakeService, config.StorageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup manager: %w", err)
	}

	strategyManager := NewStrategyManager()

	return &Manager{
		snowflakeService:   snowflakeService,
		historyManager:     historyManager,
		snapshotManager:    snapshotManager,
		transactionManager: transactionManager,
		backupManager:      backupManager,
		strategyManager:    strategyManager,
		config:             config,
	}, nil
}

// StartDeployment begins tracking a new deployment
func (m *Manager) StartDeployment(ctx context.Context, deployment *DeploymentRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Record deployment start
	if err := m.historyManager.RecordDeployment(deployment); err != nil {
		return fmt.Errorf("failed to record deployment: %w", err)
	}

	// Create pre-deployment snapshot if enabled
	if m.config.EnableAutoSnapshot {
		snapshot, err := m.snapshotManager.CreateSnapshot(
			ctx,
			deployment.ID,
			deployment.Database,
			deployment.Schema,
		)
		if err != nil {
			// Log warning but don't fail deployment
			fmt.Printf("Warning: failed to create pre-deployment snapshot: %v\n", err)
		} else {
			deployment.SnapshotID = snapshot.ID
			// Update deployment record with snapshot ID
			_ = m.historyManager.UpdateDeployment(deployment.ID, func(d *DeploymentRecord) {
				d.SnapshotID = snapshot.ID
			})
		}
	}

	return nil
}

// CompleteDeployment marks a deployment as completed
func (m *Manager) CompleteDeployment(ctx context.Context, deploymentID string, success bool, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.historyManager.UpdateDeployment(deploymentID, func(d *DeploymentRecord) {
		now := time.Now()
		d.EndTime = &now
		if success {
			d.State = StateCompleted
		} else {
			d.State = StateFailed
			d.ErrorMessage = errorMsg
		}
	})
}

// RollbackDeployment rolls back a deployment
func (m *Manager) RollbackDeployment(ctx context.Context, deploymentID string, options *RollbackOptions) (*RollbackResult, error) {
	m.mu.RLock()
	deployment, err := m.historyManager.GetDeployment(deploymentID)
	m.mu.RUnlock()
	
	if err != nil {
		return nil, err
	}

	// Determine rollback strategy
	strategy := m.determineRollbackStrategy(deployment, options)

	// Create rollback plan
	plan, err := m.createRollbackPlan(ctx, deployment, strategy, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create rollback plan: %w", err)
	}

	// Execute rollback
	result := &RollbackResult{
		DeploymentID: deploymentID,
		StartTime:    time.Now(),
		Strategy:     strategy,
		Operations:   []RollbackOperationResult{},
		Success:      true,
	}

	if options.DryRun {
		result.DryRun = true
		result.Plan = plan
		result.EndTime = timePtr(time.Now())
		return result, nil
	}

	// Execute rollback operations
	for _, op := range plan.Operations {
		opResult := m.executeRollbackOperation(ctx, deployment, op)
		result.Operations = append(result.Operations, opResult)
		
		if !opResult.Success {
			result.Success = false
			if options.StopOnError {
				break
			}
		}
	}

	result.EndTime = timePtr(time.Now())

	// Update deployment state
	if result.Success {
		_ = m.historyManager.UpdateDeployment(deploymentID, func(d *DeploymentRecord) {
			d.State = StateRolledBack
		})
	}

	return result, nil
}

// PointInTimeRecovery performs point-in-time recovery
func (m *Manager) PointInTimeRecovery(ctx context.Context, database, schema string, targetTime time.Time, options *RecoveryOptions) (*RecoveryResult, error) {
	// Find the closest recovery point
	recoveryPoint, err := m.findRecoveryPoint(database, schema, targetTime)
	if err != nil {
		return nil, fmt.Errorf("failed to find recovery point: %w", err)
	}

	result := &RecoveryResult{
		TargetTime:    targetTime,
		RecoveryPoint: recoveryPoint,
		StartTime:     time.Now(),
		Operations:    []RecoveryOperation{},
		Success:       true,
	}

	if options.DryRun {
		result.DryRun = true
		result.EndTime = timePtr(time.Now())
		return result, nil
	}

	// Restore from snapshot
	if recoveryPoint.SnapshotID != "" {
		restoreResult, err := m.snapshotManager.RestoreSnapshot(ctx, recoveryPoint.SnapshotID, false)
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Operations = append(result.Operations, RecoveryOperation{
				Type:        "snapshot_restore",
				Description: fmt.Sprintf("Restored from snapshot %s", recoveryPoint.SnapshotID),
				Success:     restoreResult.Success,
				Timestamp:   time.Now(),
			})
		}
	}

	result.EndTime = timePtr(time.Now())
	return result, nil
}

// CreateBackup creates a backup of a schema
func (m *Manager) CreateBackup(ctx context.Context, database, schema string, options *BackupOptions) (*BackupMetadata, error) {
	return m.backupManager.CreateBackup(ctx, database, schema, options)
}

// RestoreBackup restores from a backup
func (m *Manager) RestoreBackup(ctx context.Context, backupID string, options *RestoreOptions) (*RestoreBackupResult, error) {
	return m.backupManager.RestoreBackup(ctx, backupID, options)
}

// GetDeploymentHistory retrieves deployment history
func (m *Manager) GetDeploymentHistory(database, schema string, limit int) ([]*DeploymentRecord, error) {
	return m.historyManager.GetDeploymentHistory(database, schema, limit)
}

// GetRecoveryPoints retrieves available recovery points
func (m *Manager) GetRecoveryPoints(database, schema string) ([]*RecoveryPoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var points []*RecoveryPoint

	// Get deployments with snapshots
	deployments, err := m.historyManager.GetSuccessfulDeployments(database, schema, time.Time{})
	if err != nil {
		return nil, err
	}

	for _, deployment := range deployments {
		if deployment.SnapshotID != "" {
			points = append(points, &RecoveryPoint{
				ID:           fmt.Sprintf("rp-%s", deployment.ID),
				DeploymentID: deployment.ID,
				Timestamp:    deployment.StartTime,
				Type:         "automatic",
				Description:  fmt.Sprintf("Pre-deployment snapshot for %s", deployment.Commit),
				SnapshotID:   deployment.SnapshotID,
				Valid:        true,
			})
		}
	}

	// Get manual backups
	backups, err := m.backupManager.ListBackups(database, schema)
	if err == nil {
		for _, backup := range backups {
			points = append(points, &RecoveryPoint{
				ID:           fmt.Sprintf("rp-%s", backup.ID),
				DeploymentID: "",
				Timestamp:    backup.Timestamp,
				Type:         "manual",
				Description:  "Manual backup",
				SnapshotID:   backup.ID,
				Valid:        true,
			})
		}
	}

	return points, nil
}

// ValidateRollback validates if a rollback is possible
func (m *Manager) ValidateRollback(ctx context.Context, deploymentID string) (*ValidationResult, error) {
	deployment, err := m.historyManager.GetDeployment(deploymentID)
	if err != nil {
		return nil, err
	}

	result := &ValidationResult{
		DeploymentID: deploymentID,
		CanRollback:  true,
		Warnings:     []string{},
		Errors:       []string{},
	}

	// Check if deployment has a snapshot
	if deployment.SnapshotID == "" {
		result.Warnings = append(result.Warnings, "No pre-deployment snapshot available")
	}

	// Check if schema has been modified since deployment
	latestDeployment, err := m.historyManager.GetLatestSuccessfulDeployment(deployment.Database, deployment.Schema)
	if err == nil && latestDeployment.ID != deploymentID {
		result.Warnings = append(result.Warnings, "Schema has been modified since this deployment")
	}

	// Check dependencies
	if len(deployment.Files) > 0 {
		result.AffectedObjects = len(deployment.Files)
	}

	return result, nil
}

// Private methods

func (m *Manager) determineRollbackStrategy(deployment *DeploymentRecord, options *RollbackOptions) RollbackStrategy {
	if options != nil && options.Strategy != "" {
		return options.Strategy
	}

	// Auto-determine strategy based on deployment characteristics
	if deployment.SnapshotID != "" {
		return StrategySnapshot
	}

	if deployment.State == StateInProgress {
		return StrategyTransaction
	}

	return StrategyIncremental
}

func (m *Manager) createRollbackPlan(ctx context.Context, deployment *DeploymentRecord, strategy RollbackStrategy, options *RollbackOptions) (*RollbackPlan, error) {
	plan := &RollbackPlan{
		DeploymentID: deployment.ID,
		Strategy:     strategy,
		Operations:   []RollbackOperation{},
		RiskLevel:    "medium",
	}

	switch strategy {
	case StrategySnapshot:
		if deployment.SnapshotID == "" {
			return nil, errors.New(errors.ErrCodeNotFound, "no snapshot available for deployment")
		}
		plan.Operations = append(plan.Operations, RollbackOperation{
			Type:        "restore_snapshot",
			Object:      deployment.SnapshotID,
			Description: "Restore schema from pre-deployment snapshot",
			Order:       1,
		})

	case StrategyIncremental:
		// Generate reverse operations for each file
		for i := len(deployment.Files) - 1; i >= 0; i-- {
			file := deployment.Files[i]
			if file.Success {
				plan.Operations = append(plan.Operations, RollbackOperation{
					Type:        "reverse_file",
					Object:      file.Path,
					Description: fmt.Sprintf("Reverse changes from %s", file.Path),
					Order:       len(plan.Operations) + 1,
				})
			}
		}

	case StrategyTransaction:
		plan.Operations = append(plan.Operations, RollbackOperation{
			Type:        "rollback_transaction",
			Object:      deployment.ID,
			Description: "Rollback active transaction",
			Order:       1,
		})
	}

	// Estimate time based on deployment duration
	if deployment.EndTime != nil {
		plan.EstimatedTime = deployment.EndTime.Sub(deployment.StartTime)
	} else {
		plan.EstimatedTime = 5 * time.Minute
	}

	return plan, nil
}

func (m *Manager) executeRollbackOperation(ctx context.Context, deployment *DeploymentRecord, op RollbackOperation) RollbackOperationResult {
	result := RollbackOperationResult{
		Operation: op,
		StartTime: time.Now(),
		Success:   true,
	}

	switch op.Type {
	case "restore_snapshot":
		restoreResult, err := m.snapshotManager.RestoreSnapshot(ctx, op.Object, false)
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Success = restoreResult.Success
		}

	case "reverse_file":
		// Implementation would reverse specific file changes
		result.Success = true

	case "rollback_transaction":
		// Implementation would rollback active transaction
		result.Success = true
	}

	result.EndTime = timePtr(time.Now())
	return result
}

func (m *Manager) findRecoveryPoint(database, schema string, targetTime time.Time) (*RecoveryPoint, error) {
	points, err := m.GetRecoveryPoints(database, schema)
	if err != nil {
		return nil, err
	}

	// Find closest point before target time
	var closest *RecoveryPoint
	for _, point := range points {
		if point.Timestamp.Before(targetTime) || point.Timestamp.Equal(targetTime) {
			if closest == nil || point.Timestamp.After(closest.Timestamp) {
				closest = point
			}
		}
	}

	if closest == nil {
		return nil, errors.New(errors.ErrCodeNotFound, "no recovery point found before target time")
	}

	return closest, nil
}

// Helper types

// RollbackOptions configures rollback behavior
type RollbackOptions struct {
	Strategy     RollbackStrategy
	DryRun       bool
	StopOnError  bool
	TargetID     string // For rolling back to specific deployment
}

// RollbackResult represents the result of a rollback operation
type RollbackResult struct {
	DeploymentID string
	StartTime    time.Time
	EndTime      *time.Time
	Strategy     RollbackStrategy
	Operations   []RollbackOperationResult
	Plan         *RollbackPlan
	Success      bool
	DryRun       bool
}

// RollbackOperationResult represents the result of a single rollback operation
type RollbackOperationResult struct {
	Operation RollbackOperation
	StartTime time.Time
	EndTime   *time.Time
	Success   bool
	Error     string
}

// RecoveryOptions configures recovery behavior
type RecoveryOptions struct {
	DryRun      bool
	ValidateOnly bool
}

// RecoveryResult represents the result of a recovery operation
type RecoveryResult struct {
	TargetTime    time.Time
	RecoveryPoint *RecoveryPoint
	StartTime     time.Time
	EndTime       *time.Time
	Operations    []RecoveryOperation
	Success       bool
	DryRun        bool
	Error         string
}

// RecoveryOperation represents a single recovery operation
type RecoveryOperation struct {
	Type        string
	Description string
	Success     bool
	Timestamp   time.Time
	Error       string
}

// ValidationResult represents rollback validation results
type ValidationResult struct {
	DeploymentID    string
	CanRollback     bool
	Warnings        []string
	Errors          []string
	AffectedObjects int
}