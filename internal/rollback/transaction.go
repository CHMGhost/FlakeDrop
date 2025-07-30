package rollback

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// TransactionManager handles transactional deployments with rollback support
type TransactionManager struct {
	snowflakeService *snowflake.Service
	mu               sync.Mutex
	activeTransactions map[string]*ManagedTransaction
}

// ManagedTransaction represents a managed database transaction
type ManagedTransaction struct {
	ID           string
	DeploymentID string
	Tx           *sql.Tx
	StartTime    time.Time
	Operations   []TransactionOperation
	State        TransactionState
	Savepoints   map[string]int
}

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	Order       int
	Type        string
	SQL         string
	File        string
	StartTime   time.Time
	EndTime     *time.Time
	Success     bool
	Error       error
	Savepoint   string
}

// TransactionState represents the state of a transaction
type TransactionState string

const (
	TxStateActive     TransactionState = "active"
	TxStateCommitted  TransactionState = "committed"
	TxStateRolledBack TransactionState = "rolled_back"
	TxStateFailed     TransactionState = "failed"
)

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(snowflakeService *snowflake.Service) *TransactionManager {
	return &TransactionManager{
		snowflakeService:   snowflakeService,
		activeTransactions: make(map[string]*ManagedTransaction),
	}
}

// BeginTransaction starts a new managed transaction
func (tm *TransactionManager) BeginTransaction(ctx context.Context, deploymentID string) (*ManagedTransaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Begin database transaction
	tx, err := tm.snowflakeService.BeginTransaction()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSQLTransaction, "failed to begin transaction")
	}

	managedTx := &ManagedTransaction{
		ID:           generateTransactionID(),
		DeploymentID: deploymentID,
		Tx:           tx,
		StartTime:    time.Now(),
		Operations:   []TransactionOperation{},
		State:        TxStateActive,
		Savepoints:   make(map[string]int),
	}

	tm.activeTransactions[managedTx.ID] = managedTx
	return managedTx, nil
}

// ExecuteInTransaction executes SQL within a managed transaction
func (tm *TransactionManager) ExecuteInTransaction(
	ctx context.Context,
	txID string,
	sql string,
	file string,
) error {
	tm.mu.Lock()
	managedTx, exists := tm.activeTransactions[txID]
	tm.mu.Unlock()

	if !exists {
		return errors.New(errors.ErrCodeNotFound, "transaction not found").
			WithContext("transaction_id", txID)
	}

	if managedTx.State != TxStateActive {
		return errors.New(errors.ErrCodeInvalidState, "transaction is not active").
			WithContext("state", string(managedTx.State))
	}

	operation := TransactionOperation{
		Order:     len(managedTx.Operations),
		Type:      "EXECUTE",
		SQL:       sql,
		File:      file,
		StartTime: time.Now(),
	}

	// Execute SQL
	_, err := managedTx.Tx.ExecContext(ctx, sql)
	if err != nil {
		operation.Success = false
		operation.Error = err
		operation.EndTime = timePtr(time.Now())
		
		tm.mu.Lock()
		managedTx.Operations = append(managedTx.Operations, operation)
		tm.mu.Unlock()
		
		return errors.SQLError("failed to execute SQL in transaction", sql, err).
			WithContext("file", file).
			WithContext("operation_order", operation.Order)
	}

	operation.Success = true
	operation.EndTime = timePtr(time.Now())

	tm.mu.Lock()
	managedTx.Operations = append(managedTx.Operations, operation)
	tm.mu.Unlock()

	return nil
}

// CreateSavepoint creates a savepoint within a transaction
func (tm *TransactionManager) CreateSavepoint(ctx context.Context, txID, name string) error {
	tm.mu.Lock()
	managedTx, exists := tm.activeTransactions[txID]
	tm.mu.Unlock()

	if !exists {
		return errors.New(errors.ErrCodeNotFound, "transaction not found")
	}

	if managedTx.State != TxStateActive {
		return errors.New(errors.ErrCodeInvalidState, "transaction is not active")
	}

	// Create savepoint
	savepointSQL := fmt.Sprintf("SAVEPOINT %s", name)
	if _, err := managedTx.Tx.ExecContext(ctx, savepointSQL); err != nil {
		return errors.SQLError("failed to create savepoint", savepointSQL, err)
	}

	// Record savepoint position
	tm.mu.Lock()
	managedTx.Savepoints[name] = len(managedTx.Operations)
	operation := TransactionOperation{
		Order:     len(managedTx.Operations),
		Type:      "SAVEPOINT",
		SQL:       savepointSQL,
		StartTime: time.Now(),
		EndTime:   timePtr(time.Now()),
		Success:   true,
		Savepoint: name,
	}
	managedTx.Operations = append(managedTx.Operations, operation)
	tm.mu.Unlock()

	return nil
}

// RollbackToSavepoint rolls back to a specific savepoint
func (tm *TransactionManager) RollbackToSavepoint(ctx context.Context, txID, name string) error {
	tm.mu.Lock()
	managedTx, exists := tm.activeTransactions[txID]
	tm.mu.Unlock()

	if !exists {
		return errors.New(errors.ErrCodeNotFound, "transaction not found")
	}

	if managedTx.State != TxStateActive {
		return errors.New(errors.ErrCodeInvalidState, "transaction is not active")
	}

	position, exists := managedTx.Savepoints[name]
	if !exists {
		return errors.New(errors.ErrCodeNotFound, "savepoint not found").
			WithContext("savepoint", name)
	}

	// Rollback to savepoint
	rollbackSQL := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
	if _, err := managedTx.Tx.ExecContext(ctx, rollbackSQL); err != nil {
		return errors.SQLError("failed to rollback to savepoint", rollbackSQL, err)
	}

	// Record rollback operation
	tm.mu.Lock()
	operation := TransactionOperation{
		Order:     len(managedTx.Operations),
		Type:      "ROLLBACK_TO_SAVEPOINT",
		SQL:       rollbackSQL,
		StartTime: time.Now(),
		EndTime:   timePtr(time.Now()),
		Success:   true,
		Savepoint: name,
	}
	managedTx.Operations = append(managedTx.Operations, operation)

	// Mark operations after savepoint as rolled back
	for i := position + 1; i < len(managedTx.Operations)-1; i++ {
		if managedTx.Operations[i].Success {
			managedTx.Operations[i].Success = false
			if managedTx.Operations[i].Error == nil {
				managedTx.Operations[i].Error = fmt.Errorf("rolled back to savepoint %s", name)
			}
		}
	}
	tm.mu.Unlock()

	return nil
}

// CommitTransaction commits a managed transaction
func (tm *TransactionManager) CommitTransaction(ctx context.Context, txID string) error {
	tm.mu.Lock()
	managedTx, exists := tm.activeTransactions[txID]
	tm.mu.Unlock()

	if !exists {
		return errors.New(errors.ErrCodeNotFound, "transaction not found")
	}

	if managedTx.State != TxStateActive {
		return errors.New(errors.ErrCodeInvalidState, "transaction is not active")
	}

	// Commit transaction
	if err := managedTx.Tx.Commit(); err != nil {
		tm.mu.Lock()
		managedTx.State = TxStateFailed
		tm.mu.Unlock()
		
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "failed to commit transaction")
	}

	tm.mu.Lock()
	managedTx.State = TxStateCommitted
	delete(tm.activeTransactions, txID)
	tm.mu.Unlock()

	return nil
}

// RollbackTransaction rolls back a managed transaction
func (tm *TransactionManager) RollbackTransaction(ctx context.Context, txID string) error {
	tm.mu.Lock()
	managedTx, exists := tm.activeTransactions[txID]
	tm.mu.Unlock()

	if !exists {
		return errors.New(errors.ErrCodeNotFound, "transaction not found")
	}

	if managedTx.State != TxStateActive {
		return errors.New(errors.ErrCodeInvalidState, "transaction is not active")
	}

	// Rollback transaction
	if err := managedTx.Tx.Rollback(); err != nil {
		// If rollback fails, still mark as rolled back
		tm.mu.Lock()
		managedTx.State = TxStateFailed
		tm.mu.Unlock()
		
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "failed to rollback transaction")
	}

	tm.mu.Lock()
	managedTx.State = TxStateRolledBack
	delete(tm.activeTransactions, txID)
	tm.mu.Unlock()

	return nil
}

// GetTransactionInfo retrieves information about a transaction
func (tm *TransactionManager) GetTransactionInfo(txID string) (*TransactionInfo, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	managedTx, exists := tm.activeTransactions[txID]
	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "transaction not found")
	}

	info := &TransactionInfo{
		ID:             managedTx.ID,
		DeploymentID:   managedTx.DeploymentID,
		StartTime:      managedTx.StartTime,
		State:          managedTx.State,
		OperationCount: len(managedTx.Operations),
		Savepoints:     []string{},
	}

	for name := range managedTx.Savepoints {
		info.Savepoints = append(info.Savepoints, name)
	}

	// Calculate statistics
	for _, op := range managedTx.Operations {
		if op.Success {
			info.SuccessfulOps++
		} else {
			info.FailedOps++
		}
	}

	return info, nil
}

// CleanupStaleTransactions cleans up stale transactions
func (tm *TransactionManager) CleanupStaleTransactions(ctx context.Context, maxAge time.Duration) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	var staleIDs []string

	for id, tx := range tm.activeTransactions {
		if tx.StartTime.Before(cutoff) {
			staleIDs = append(staleIDs, id)
		}
	}

	for _, id := range staleIDs {
		if tx := tm.activeTransactions[id]; tx != nil && tx.State == TxStateActive {
			// Attempt to rollback stale transaction
			_ = tx.Tx.Rollback()
			tx.State = TxStateRolledBack
		}
		delete(tm.activeTransactions, id)
	}

	return nil
}

// TransactionInfo contains information about a transaction
type TransactionInfo struct {
	ID             string
	DeploymentID   string
	StartTime      time.Time
	State          TransactionState
	OperationCount int
	SuccessfulOps  int
	FailedOps      int
	Savepoints     []string
}

// Helper functions

func generateTransactionID() string {
	return fmt.Sprintf("tx-%d-%s", time.Now().Unix(), generateRandomID(8))
}

func timePtr(t time.Time) *time.Time {
	return &t
}