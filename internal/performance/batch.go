package performance

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// BatchProcessor handles batch execution of SQL statements
type BatchProcessor struct {
	pool           *ConnectionPool
	batchSize      int
	flushInterval  time.Duration
	maxRetries     int
	mu             sync.Mutex
	batches        map[string]*Batch
	flushTimer     *time.Timer
	stopChan       chan struct{}
	wg             sync.WaitGroup
	metrics        *BatchMetrics
	errorHandler   *errors.ErrorHandler
}

// Batch represents a collection of SQL statements to be executed together
type Batch struct {
	ID            string
	Database      string
	Schema        string
	Statements    []BatchStatement
	CreatedAt     time.Time
	Size          int64
	Priority      int
}

// BatchStatement represents a single statement in a batch
type BatchStatement struct {
	ID        string
	SQL       string
	Type      StatementType
	FilePath  string
	Size      int64
}

// StatementType defines the type of SQL statement
type StatementType int

const (
	StatementTypeUnknown StatementType = iota
	StatementTypeInsert
	StatementTypeUpdate
	StatementTypeDelete
	StatementTypeCreate
	StatementTypeAlter
	StatementTypeDrop
	StatementTypeMerge
	StatementTypeCopy
)

// String returns the string representation of StatementType
func (st StatementType) String() string {
	switch st {
	case StatementTypeInsert:
		return "INSERT"
	case StatementTypeUpdate:
		return "UPDATE"
	case StatementTypeDelete:
		return "DELETE"
	case StatementTypeCreate:
		return "CREATE"
	case StatementTypeAlter:
		return "ALTER"
	case StatementTypeDrop:
		return "DROP"
	case StatementTypeMerge:
		return "MERGE"
	case StatementTypeCopy:
		return "COPY"
	default:
		return "UNKNOWN"
	}
}

// BatchMetrics tracks batch processing performance
type BatchMetrics struct {
	mu                sync.RWMutex
	TotalBatches      int64
	TotalStatements   int64
	SuccessfulBatches int64
	FailedBatches     int64
	RetryCount        int64
	AvgBatchSize      float64
	AvgExecutionTime  time.Duration
	TotalBytes        int64
	Throughput        float64
}

// BatchConfig contains batch processor configuration
type BatchConfig struct {
	BatchSize      int
	FlushInterval  time.Duration
	MaxRetries     int
	MaxBatchMemory int64
}

// DefaultBatchConfig returns sensible defaults
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:      100,
		FlushInterval:  5 * time.Second,
		MaxRetries:     3,
		MaxBatchMemory: 10 * 1024 * 1024, // 10MB
	}
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(pool *ConnectionPool, config BatchConfig) *BatchProcessor {
	bp := &BatchProcessor{
		pool:          pool,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		maxRetries:    config.MaxRetries,
		batches:       make(map[string]*Batch),
		stopChan:      make(chan struct{}),
		metrics:       &BatchMetrics{},
		errorHandler:  errors.GetGlobalErrorHandler(),
	}

	// Start flush routine
	bp.wg.Add(1)
	go bp.flushRoutine()

	return bp
}

// Add adds a statement to the batch
func (bp *BatchProcessor) Add(database, schema string, statement BatchStatement) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, schema)
	batch, exists := bp.batches[key]
	
	if !exists {
		batch = &Batch{
			ID:         generateBatchID(),
			Database:   database,
			Schema:     schema,
			Statements: make([]BatchStatement, 0, bp.batchSize),
			CreatedAt:  time.Now(),
		}
		bp.batches[key] = batch
	}

	// Determine statement type
	statement.Type = detectStatementType(statement.SQL)
	
	// Add statement to batch
	batch.Statements = append(batch.Statements, statement)
	batch.Size += statement.Size
	
	// Check if batch should be flushed
	if len(batch.Statements) >= bp.batchSize {
		go bp.flushBatch(key, batch)
		delete(bp.batches, key)
	}

	// Reset flush timer
	if bp.flushTimer != nil {
		bp.flushTimer.Stop()
	}
	bp.flushTimer = time.AfterFunc(bp.flushInterval, func() {
		bp.flushAll()
	})

	return nil
}

// FlushAll flushes all pending batches
func (bp *BatchProcessor) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	return bp.flushAll()
}

// flushAll flushes all batches without locking
func (bp *BatchProcessor) flushAll() error {
	var lastErr error
	
	for key, batch := range bp.batches {
		if err := bp.flushBatch(key, batch); err != nil {
			lastErr = err
		}
		delete(bp.batches, key)
	}
	
	return lastErr
}

// Stop gracefully stops the batch processor
func (bp *BatchProcessor) Stop() error {
	close(bp.stopChan)
	
	// Flush remaining batches
	bp.FlushAll()
	
	// Wait for flush routine to complete
	bp.wg.Wait()
	
	return nil
}

// GetMetrics returns batch processing metrics
func (bp *BatchProcessor) GetMetrics() BatchMetrics {
	bp.metrics.mu.RLock()
	defer bp.metrics.mu.RUnlock()
	return *bp.metrics
}

// flushRoutine periodically flushes batches
func (bp *BatchProcessor) flushRoutine() {
	defer bp.wg.Done()
	
	ticker := time.NewTicker(bp.flushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			bp.FlushAll()
		case <-bp.stopChan:
			return
		}
	}
}

// flushBatch executes a batch of statements
func (bp *BatchProcessor) flushBatch(key string, batch *Batch) error {
	if len(batch.Statements) == 0 {
		return nil
	}

	startTime := time.Now()
	bp.metrics.recordBatchStart(batch)

	// Group statements by type for optimal execution
	grouped := bp.groupStatementsByType(batch.Statements)
	
	// Get connection from pool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	
	conn, err := bp.pool.Get(ctx)
	if err != nil {
		bp.metrics.recordBatchFailure(batch, time.Since(startTime))
		return errors.Wrap(err, errors.ErrCodeConnectionFailed, "Failed to get connection for batch")
	}
	defer bp.pool.Put(conn)

	// Execute batch with retry logic
	err = bp.executeWithRetry(ctx, conn, batch, grouped)
	
	if err != nil {
		bp.metrics.recordBatchFailure(batch, time.Since(startTime))
		return err
	}
	
	bp.metrics.recordBatchSuccess(batch, time.Since(startTime))
	return nil
}

// executeWithRetry executes a batch with retry logic
func (bp *BatchProcessor) executeWithRetry(ctx context.Context, conn *PooledConnection, batch *Batch, grouped map[StatementType][]BatchStatement) error {
	var lastErr error
	
	for attempt := 0; attempt <= bp.maxRetries; attempt++ {
		if attempt > 0 {
			bp.metrics.incrementRetryCount()
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		
		err := bp.executeBatch(ctx, conn, batch, grouped)
		if err == nil {
			return nil
		}
		
		lastErr = err
		
		// Check if error is retryable
		if !isRetryableError(err) {
			break
		}
	}
	
	return lastErr
}

// executeBatch executes the grouped statements
func (bp *BatchProcessor) executeBatch(ctx context.Context, conn *PooledConnection, batch *Batch, grouped map[StatementType][]BatchStatement) error {
	// Start transaction
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "Failed to begin batch transaction")
	}

	// Create transaction handler
	txHandler := bp.errorHandler.NewTransactionHandler(tx, tx.Rollback)

	return txHandler.Execute(func() error {
		// Set database and schema context
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", batch.Database)); err != nil {
			return errors.SQLError("Failed to use database", fmt.Sprintf("USE DATABASE %s", batch.Database), err)
		}
		
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s", batch.Schema)); err != nil {
			return errors.SQLError("Failed to use schema", fmt.Sprintf("USE SCHEMA %s", batch.Schema), err)
		}

		// Execute DDL statements first (CREATE, ALTER, DROP)
		for _, stmtType := range []StatementType{StatementTypeCreate, StatementTypeAlter, StatementTypeDrop} {
			if statements, ok := grouped[stmtType]; ok {
				for _, stmt := range statements {
					if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
						return errors.SQLError(
							fmt.Sprintf("Failed to execute %s statement", stmtType),
							stmt.SQL,
							err,
						).WithContext("file", stmt.FilePath)
					}
				}
			}
		}

		// Execute DML statements (INSERT, UPDATE, DELETE, MERGE)
		for _, stmtType := range []StatementType{StatementTypeInsert, StatementTypeUpdate, StatementTypeDelete, StatementTypeMerge} {
			if statements, ok := grouped[stmtType]; ok {
				if err := bp.executeDMLBatch(ctx, &txWrapper{tx}, stmtType, statements); err != nil {
					return err
				}
			}
		}

		// Execute COPY statements separately
		if statements, ok := grouped[StatementTypeCopy]; ok {
			for _, stmt := range statements {
				if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
					return errors.SQLError("Failed to execute COPY statement", stmt.SQL, err).
						WithContext("file", stmt.FilePath)
				}
			}
		}

		// Commit transaction
		return tx.Commit()
	})
}

// executeDMLBatch optimizes DML statement execution
func (bp *BatchProcessor) executeDMLBatch(ctx context.Context, tx TxInterface, stmtType StatementType, statements []BatchStatement) error {
	switch stmtType {
	case StatementTypeInsert:
		// Try to combine INSERT statements for the same table
		return bp.combineInserts(ctx, tx, statements)
	
	case StatementTypeUpdate, StatementTypeDelete:
		// Execute updates and deletes individually but in sequence
		for _, stmt := range statements {
			if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
				return errors.SQLError(
					fmt.Sprintf("Failed to execute %s statement", stmtType),
					stmt.SQL,
					err,
				).WithContext("file", stmt.FilePath)
			}
		}
		return nil
	
	case StatementTypeMerge:
		// MERGE statements must be executed individually
		for _, stmt := range statements {
			if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
				return errors.SQLError("Failed to execute MERGE statement", stmt.SQL, err).
					WithContext("file", stmt.FilePath)
			}
		}
		return nil
	
	default:
		return fmt.Errorf("unsupported DML type: %v", stmtType)
	}
}

// combineInserts combines multiple INSERT statements for the same table
func (bp *BatchProcessor) combineInserts(ctx context.Context, tx TxInterface, statements []BatchStatement) error {
	// Group by table name
	tableInserts := make(map[string][]BatchStatement)
	
	for _, stmt := range statements {
		table := extractTableName(stmt.SQL, "INSERT")
		if table != "" {
			tableInserts[table] = append(tableInserts[table], stmt)
		} else {
			// Execute non-standard INSERT individually
			if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
				return errors.SQLError("Failed to execute INSERT statement", stmt.SQL, err).
					WithContext("file", stmt.FilePath)
			}
		}
	}

	// Execute combined INSERTs
	for table, inserts := range tableInserts {
		if len(inserts) == 1 {
			// Single insert, execute as-is
			if _, err := tx.ExecContext(ctx, inserts[0].SQL); err != nil {
				return errors.SQLError("Failed to execute INSERT statement", inserts[0].SQL, err).
					WithContext("file", inserts[0].FilePath)
			}
		} else {
			// Multiple inserts, try to combine
			combined := combineInsertStatements(table, inserts)
			if combined != "" {
				if _, err := tx.ExecContext(ctx, combined); err != nil {
					// Fallback to individual execution
					for _, stmt := range inserts {
						if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
							return errors.SQLError("Failed to execute INSERT statement", stmt.SQL, err).
								WithContext("file", stmt.FilePath)
						}
					}
				}
			} else {
				// Cannot combine, execute individually
				for _, stmt := range inserts {
					if _, err := tx.ExecContext(ctx, stmt.SQL); err != nil {
						return errors.SQLError("Failed to execute INSERT statement", stmt.SQL, err).
							WithContext("file", stmt.FilePath)
					}
				}
			}
		}
	}

	return nil
}

// groupStatementsByType groups statements by their type
func (bp *BatchProcessor) groupStatementsByType(statements []BatchStatement) map[StatementType][]BatchStatement {
	grouped := make(map[StatementType][]BatchStatement)
	
	for _, stmt := range statements {
		grouped[stmt.Type] = append(grouped[stmt.Type], stmt)
	}
	
	return grouped
}

// Helper functions

func generateBatchID() string {
	return fmt.Sprintf("batch_%d_%d", time.Now().Unix(), time.Now().Nanosecond())
}

func detectStatementType(sql string) StatementType {
	sql = strings.TrimSpace(strings.ToUpper(sql))
	
	switch {
	case strings.HasPrefix(sql, "INSERT"):
		return StatementTypeInsert
	case strings.HasPrefix(sql, "UPDATE"):
		return StatementTypeUpdate
	case strings.HasPrefix(sql, "DELETE"):
		return StatementTypeDelete
	case strings.HasPrefix(sql, "CREATE"):
		return StatementTypeCreate
	case strings.HasPrefix(sql, "ALTER"):
		return StatementTypeAlter
	case strings.HasPrefix(sql, "DROP"):
		return StatementTypeDrop
	case strings.HasPrefix(sql, "MERGE"):
		return StatementTypeMerge
	case strings.HasPrefix(sql, "COPY"):
		return StatementTypeCopy
	default:
		return StatementTypeUnknown
	}
}

func extractTableName(sql, operation string) string {
	// Simple table name extraction - in production use a proper SQL parser
	sql = strings.ToUpper(sql)
	operation = strings.ToUpper(operation)
	
	switch operation {
	case "INSERT":
		if idx := strings.Index(sql, "INTO "); idx >= 0 {
			parts := strings.Fields(sql[idx+5:])
			if len(parts) > 0 {
				return strings.TrimSuffix(parts[0], "(")
			}
		}
	case "UPDATE":
		parts := strings.Fields(sql)
		if len(parts) > 1 {
			return parts[1]
		}
	case "DELETE":
		if idx := strings.Index(sql, "FROM "); idx >= 0 {
			parts := strings.Fields(sql[idx+5:])
			if len(parts) > 0 {
				return parts[0]
			}
		}
	}
	
	return ""
}

func combineInsertStatements(table string, statements []BatchStatement) string {
	// This is a simplified version - in production, use a proper SQL parser
	// to safely combine INSERT statements
	
	// For now, return empty to indicate cannot combine
	// A full implementation would parse the INSERT statements and combine VALUES
	return ""
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	retryablePatterns := []string{
		"connection reset",
		"connection refused",
		"timeout",
		"deadlock",
		"too many connections",
		"resource busy",
	}
	
	for _, pattern := range retryablePatterns {
		if strings.Contains(strings.ToLower(errStr), pattern) {
			return true
		}
	}
	
	return false
}

// TxInterface wraps sql.Tx for easier testing
type TxInterface interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error)
	Commit() error
	Rollback() error
}

// txWrapper wraps *sql.Tx to implement TxInterface
type txWrapper struct {
	tx *sql.Tx
}

func (w *txWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	return w.tx.ExecContext(ctx, query, args...)
}

func (w *txWrapper) Commit() error {
	return w.tx.Commit()
}

func (w *txWrapper) Rollback() error {
	return w.tx.Rollback()
}

// Metrics methods

func (bm *BatchMetrics) recordBatchStart(batch *Batch) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.TotalBatches++
	bm.TotalStatements += int64(len(batch.Statements))
	bm.TotalBytes += batch.Size
}

func (bm *BatchMetrics) recordBatchSuccess(batch *Batch, duration time.Duration) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.SuccessfulBatches++
	bm.updateAverages(batch, duration)
}

func (bm *BatchMetrics) recordBatchFailure(batch *Batch, duration time.Duration) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.FailedBatches++
	bm.updateAverages(batch, duration)
}

func (bm *BatchMetrics) incrementRetryCount() {
	bm.mu.Lock()
	bm.RetryCount++
	bm.mu.Unlock()
}

func (bm *BatchMetrics) updateAverages(batch *Batch, duration time.Duration) {
	// Update average batch size
	totalProcessed := bm.SuccessfulBatches + bm.FailedBatches
	if totalProcessed > 0 {
		bm.AvgBatchSize = float64(bm.TotalStatements) / float64(totalProcessed)
	}
	
	// Update average execution time
	if bm.AvgExecutionTime == 0 {
		bm.AvgExecutionTime = duration
	} else {
		bm.AvgExecutionTime = (bm.AvgExecutionTime + duration) / 2
	}
	
	// Update throughput (statements per second)
	if duration > 0 {
		bm.Throughput = float64(len(batch.Statements)) / duration.Seconds()
	}
}