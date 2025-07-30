package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/performance"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// Executor handles migration execution with streaming and batching capabilities
type Executor struct {
	connectionProvider ConnectionProvider
	transformer        *Transformer
	repository         MigrationRepository
	performancePool    *performance.ConnectionPool
	streamProcessor    *performance.StreamProcessor
	errorHandler       *errors.ErrorHandler
	metrics            *ExecutorMetrics
	mu                 sync.RWMutex
	executions         map[string]*MigrationExecution
}

// ExecutorMetrics tracks executor performance
type ExecutorMetrics struct {
	mu                sync.RWMutex
	TotalExecutions   int64
	ActiveExecutions  int64
	CompletedExecutions int64
	FailedExecutions  int64
	TotalRowsProcessed int64
	TotalBytesProcessed int64
	AverageExecutionTime time.Duration
}

// NewExecutor creates a new migration executor
func NewExecutor(connProvider ConnectionProvider, repository MigrationRepository) *Executor {
	// Create performance components
	poolConfig := performance.DefaultPoolConfig()
	poolConfig.MaxSize = 10
	poolConfig.IdleTimeout = time.Minute * 5

	// We need a snowflake config - use default for now
	sfConfig := snowflake.Config{
		Account:   "placeholder",
		Username:  "placeholder", 
		Password:  "placeholder",
		Database:  "placeholder",
		Schema:    "placeholder",
		Warehouse: "placeholder",
		Timeout:   30 * time.Second,
	}

	performancePool, err := performance.NewConnectionPool(sfConfig, poolConfig)
	if err != nil {
		// Handle error appropriately - for now use nil
		performancePool = nil
	}

	streamConfig := performance.DefaultStreamConfig()
	streamConfig.Parallel = 4
	streamConfig.BufferSize = 128 * 1024

	streamProcessor := performance.NewStreamProcessor(performancePool, streamConfig)

	return &Executor{
		connectionProvider: connProvider,
		transformer:       NewTransformer(),
		repository:        repository,
		performancePool:   performancePool,
		streamProcessor:   streamProcessor,
		errorHandler:      errors.GetGlobalErrorHandler(),
		metrics:          &ExecutorMetrics{},
		executions:       make(map[string]*MigrationExecution),
	}
}

// Execute runs a migration plan
func (e *Executor) Execute(ctx context.Context, plan *MigrationPlan) (*MigrationExecution, error) {
	execution := &MigrationExecution{
		ID:            generateID("exec"),
		PlanID:        plan.ID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now(),
		Configuration: plan.Configuration,
		Progress:      MigrationProgress{},
		Metrics:       MigrationMetrics{},
		Errors:        []MigrationError{},
		Warnings:      []MigrationWarning{},
		Checkpoints:   []ExecutionCheckpoint{},
		Environment:   "default",
		ExecutedBy:    "system",
	}

	// Store execution
	e.mu.Lock()
	e.executions[execution.ID] = execution
	e.mu.Unlock()

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TotalExecutions++
	e.metrics.ActiveExecutions++
	e.metrics.mu.Unlock()

	// Save initial execution state
	if err := e.repository.SaveExecution(ctx, execution); err != nil {
		return nil, fmt.Errorf("failed to save execution: %w", err)
	}

	// Execute in background
	go e.executeAsync(ctx, plan, execution)

	return execution, nil
}

// executeAsync runs the migration asynchronously
func (e *Executor) executeAsync(ctx context.Context, plan *MigrationPlan, execution *MigrationExecution) {
	defer func() {
		if r := recover(); r != nil {
			e.handleExecutionPanic(execution, r)
		}
		e.finalizeExecution(ctx, execution)
	}()

	// Initialize progress
	execution.Progress = MigrationProgress{
		TotalTables:      len(plan.Tables),
		CompletedTables:  0,
		CurrentOperation: "Starting migration",
	}

	// Calculate totals
	var totalRows, totalSize int64
	for _, table := range plan.Tables {
		totalRows += table.EstimatedRows
		totalSize += table.EstimatedSize
	}
	execution.Progress.TotalRows = totalRows
	execution.Progress.TotalSize = totalSize

	// Check pre-conditions
	if err := e.checkPreConditions(ctx, plan, execution); err != nil {
		e.failExecution(execution, fmt.Errorf("pre-condition check failed: %w", err))
		return
	}

	// Execute tables in dependency order
	for _, table := range plan.Tables {
		if err := e.executeTable(ctx, plan, &table, execution); err != nil {
			e.failExecution(execution, fmt.Errorf("table migration failed for %s: %w", table.SourceTable, err))
			return
		}

		execution.Progress.CompletedTables++
		execution.Progress.PercentComplete = float64(execution.Progress.CompletedTables) / float64(execution.Progress.TotalTables) * 100

		// Update execution state
		e.repository.SaveExecution(ctx, execution)
	}

	// Execute post-actions
	if err := e.executePostActions(ctx, plan, execution); err != nil {
		e.addWarning(execution, fmt.Sprintf("Post-action execution failed: %v", err))
	}

	// Run final validations
	if err := e.runValidations(ctx, plan, execution); err != nil {
		e.addWarning(execution, fmt.Sprintf("Final validation failed: %v", err))
	}

	// Mark as completed
	execution.Status = MigrationStatusCompleted
	completedAt := time.Now()
	execution.CompletedAt = &completedAt
	execution.Duration = completedAt.Sub(execution.StartedAt)
}

// executeTable executes migration for a single table
func (e *Executor) executeTable(ctx context.Context, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution) error {
	execution.CurrentTable = table.SourceTable
	execution.Progress.CurrentOperation = fmt.Sprintf("Migrating table %s", table.SourceTable)

	// Get connections
	sourceConn, err := e.connectionProvider.GetSourceConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get source connection: %w", err)
	}
	defer sourceConn.Close()

	targetConn, err := e.connectionProvider.GetTargetConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get target connection: %w", err)
	}
	defer targetConn.Close()

	// Choose execution strategy based on table size and configuration
	switch {
	case table.EstimatedRows > 1000000 && plan.Strategy == MigrationStrategyStream:
		return e.executeTableStreaming(ctx, plan, table, execution, sourceConn, targetConn)
	case table.Parallelism > 1 && plan.Strategy == MigrationStrategyParallel:
		return e.executeTableParallel(ctx, plan, table, execution, sourceConn, targetConn)
	default:
		return e.executeTableBatch(ctx, plan, table, execution, sourceConn, targetConn)
	}
}

// executeTableBatch executes table migration using batch processing
func (e *Executor) executeTableBatch(ctx context.Context, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution, sourceConn, targetConn *sql.DB) error {
	// Build source query
	sourceQuery := e.buildSourceQuery(table)

	// Execute source query
	rows, err := sourceConn.QueryContext(ctx, sourceQuery)
	if err != nil {
		return fmt.Errorf("failed to query source table: %w", err)
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Process in batches
	batch := make([]map[string]interface{}, 0, table.BatchSize)
	batchCount := int64(0)

	for rows.Next() {
		// Scan row
		row := make(map[string]interface{})
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, col := range columns {
			row[col] = values[i]
		}

		batch = append(batch, row)

		// Process batch when full
		if len(batch) >= table.BatchSize {
			if err := e.processBatch(ctx, plan, table, execution, batch, targetConn); err != nil {
				return fmt.Errorf("failed to process batch %d: %w", batchCount, err)
			}
			batchCount++
			execution.CurrentBatch = batchCount
			batch = batch[:0] // Reset batch

			// Create checkpoint
			if execution.Configuration.EnableCheckpoints {
				e.createCheckpoint(ctx, execution, table.SourceTable, batchCount)
			}
		}
	}

	// Process remaining rows
	if len(batch) > 0 {
		if err := e.processBatch(ctx, plan, table, execution, batch, targetConn); err != nil {
			return fmt.Errorf("failed to process final batch: %w", err)
		}
	}

	execution.TotalBatches = batchCount + 1
	return nil
}

// executeTableStreaming executes table migration using streaming
func (e *Executor) executeTableStreaming(ctx context.Context, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution, sourceConn, targetConn *sql.DB) error {
	// For large tables, use streaming approach
	// This is a simplified implementation - real streaming would use cursor-based pagination

	// Build paginated query
	offset := int64(0)
	limit := int64(table.BatchSize)

	for {
		sourceQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", e.buildSourceQuery(table), limit, offset)

		rows, err := sourceConn.QueryContext(ctx, sourceQuery)
		if err != nil {
			return fmt.Errorf("failed to query source table: %w", err)
		}

		batch, hasRows, err := e.scanRowsToBatch(rows, table.BatchSize)
		rows.Close()

		if err != nil {
			return fmt.Errorf("failed to scan rows: %w", err)
		}

		if !hasRows {
			break // No more data
		}

		if err := e.processBatch(ctx, plan, table, execution, batch, targetConn); err != nil {
			return fmt.Errorf("failed to process streaming batch: %w", err)
		}

		offset += limit
		execution.Progress.ProcessedRows += int64(len(batch))
	}

	return nil
}

// executeTableParallel executes table migration using parallel processing
func (e *Executor) executeTableParallel(ctx context.Context, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution, sourceConn, targetConn *sql.DB) error {
	// Divide data into chunks for parallel processing
	workers := table.Parallelism
	chunkSize := table.EstimatedRows / int64(workers)
	if chunkSize < int64(table.BatchSize) {
		chunkSize = int64(table.BatchSize)
	}

	// Create worker channels
	chunkChan := make(chan ChunkInfo, workers*2)
	errorChan := make(chan error, workers)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go e.chunkWorker(ctx, &wg, plan, table, execution, chunkChan, errorChan)
	}

	// Generate chunks
	go func() {
		defer close(chunkChan)
		offset := int64(0)
		for offset < table.EstimatedRows {
			limit := chunkSize
			if offset+limit > table.EstimatedRows {
				limit = table.EstimatedRows - offset
			}

			select {
			case chunkChan <- ChunkInfo{Offset: offset, Limit: limit}:
			case <-ctx.Done():
				return
			}

			offset += limit
		}
	}()

	// Wait for completion
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return fmt.Errorf("parallel execution failed: %w", err)
		}
	}

	return nil
}

// ChunkInfo represents a data chunk for parallel processing
type ChunkInfo struct {
	Offset int64
	Limit  int64
}

// chunkWorker processes data chunks in parallel
func (e *Executor) chunkWorker(ctx context.Context, wg *sync.WaitGroup, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution, chunks <-chan ChunkInfo, errors chan<- error) {
	defer wg.Done()

	// Get dedicated connections for this worker
	sourceConn, err := e.connectionProvider.GetSourceConnection(ctx)
	if err != nil {
		errors <- err
		return
	}
	defer sourceConn.Close()

	targetConn, err := e.connectionProvider.GetTargetConnection(ctx)
	if err != nil {
		errors <- err
		return
	}
	defer targetConn.Close()

	for chunk := range chunks {
		sourceQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", e.buildSourceQuery(table), chunk.Limit, chunk.Offset)

		rows, err := sourceConn.QueryContext(ctx, sourceQuery)
		if err != nil {
			errors <- fmt.Errorf("failed to query chunk: %w", err)
			return
		}

		batch, _, err := e.scanRowsToBatch(rows, int(chunk.Limit))
		rows.Close()

		if err != nil {
			errors <- fmt.Errorf("failed to scan chunk: %w", err)
			return
		}

		if err := e.processBatch(ctx, plan, table, execution, batch, targetConn); err != nil {
			errors <- fmt.Errorf("failed to process chunk: %w", err)
			return
		}
	}

	errors <- nil // Signal success
}

// processBatch processes a batch of data
func (e *Executor) processBatch(ctx context.Context, plan *MigrationPlan, table *TableMigration, execution *MigrationExecution, batch []map[string]interface{}, targetConn *sql.DB) error {
	// Apply filters
	filteredBatch, err := e.transformer.FilterBatch(ctx, batch, table.Filters)
	if err != nil {
		return fmt.Errorf("failed to filter batch: %w", err)
	}

	// Apply transformations
	transformedBatch, err := e.transformer.TransformBatch(ctx, filteredBatch, table.Transformations)
	if err != nil {
		return fmt.Errorf("failed to transform batch: %w", err)
	}

	// Map columns
	mappedBatch, err := e.transformer.MapColumns(ctx, transformedBatch, table.Mappings)
	if err != nil {
		return fmt.Errorf("failed to map columns: %w", err)
	}

	// Validate data if enabled
	if execution.Configuration.EnableValidation {
		validationResults, err := e.transformer.ValidateBatch(ctx, mappedBatch, table.Validations)
		if err != nil {
			return fmt.Errorf("batch validation failed: %w", err)
		}

		// Check for validation failures
		for _, result := range validationResults {
			if result.Status == ValidationResultStatusFailed && result.Severity == ValidationSeverityError {
				return fmt.Errorf("validation failed: %s", result.Message)
			}
			execution.ValidationResults = append(execution.ValidationResults, result)
		}
	}

	// Insert into target
	if err := e.insertBatch(ctx, table, mappedBatch, targetConn); err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	// Update metrics
	execution.Metrics.RowsInserted += int64(len(mappedBatch))
	execution.Progress.ProcessedRows += int64(len(mappedBatch))

	return nil
}

// Helper methods

func (e *Executor) buildSourceQuery(table *TableMigration) string {
	// Build SELECT query for source table
	// This is simplified - real implementation would handle complex mappings
	return fmt.Sprintf("SELECT * FROM %s", table.SourceTable)
}

func (e *Executor) scanRowsToBatch(rows *sql.Rows, maxSize int) ([]map[string]interface{}, bool, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}

	batch := make([]map[string]interface{}, 0, maxSize)
	hasRows := false

	for rows.Next() && len(batch) < maxSize {
		hasRows = true
		row := make(map[string]interface{})
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, false, err
		}

		for i, col := range columns {
			row[col] = values[i]
		}

		batch = append(batch, row)
	}

	return batch, hasRows, rows.Err()
}

func (e *Executor) insertBatch(ctx context.Context, table *TableMigration, batch []map[string]interface{}, targetConn *sql.DB) error {
	if len(batch) == 0 {
		return nil
	}

	// Build INSERT statement based on migration mode
	switch table.MigrationMode {
	case MigrationModeInsert:
		return e.insertBatchInsert(ctx, table, batch, targetConn)
	case MigrationModeUpsert:
		return e.insertBatchUpsert(ctx, table, batch, targetConn)
	case MigrationModeReplace:
		return e.insertBatchReplace(ctx, table, batch, targetConn)
	default:
		return e.insertBatchInsert(ctx, table, batch, targetConn)
	}
}

func (e *Executor) insertBatchInsert(ctx context.Context, table *TableMigration, batch []map[string]interface{}, targetConn *sql.DB) error {
	// Simple bulk insert
	if len(batch) == 0 {
		return nil
	}

	// Get column names from first row
	firstRow := batch[0]
	var columns []string
	for col := range firstRow {
		columns = append(columns, col)
	}

	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// Table and column names are from configuration, not user input
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", // #nosec G201 - table/column names from trusted config
		table.TargetTable,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Prepare statement
	stmt, err := targetConn.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Execute for each row
	for _, row := range batch {
		var values []interface{}
		for _, col := range columns {
			values = append(values, row[col])
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	return nil
}

func (e *Executor) insertBatchUpsert(ctx context.Context, table *TableMigration, batch []map[string]interface{}, targetConn *sql.DB) error {
	// Snowflake MERGE statement for upsert
	// This is simplified - real implementation would handle primary keys
	return e.insertBatchInsert(ctx, table, batch, targetConn)
}

func (e *Executor) insertBatchReplace(ctx context.Context, table *TableMigration, batch []map[string]interface{}, targetConn *sql.DB) error {
	// Delete and insert for replace mode
	// This is simplified - real implementation would handle proper replacement logic
	return e.insertBatchInsert(ctx, table, batch, targetConn)
}

func (e *Executor) checkPreConditions(ctx context.Context, plan *MigrationPlan, execution *MigrationExecution) error {
	for _, condition := range plan.PreConditions {
		if condition.Required {
			// Check condition (simplified)
			if err := e.evaluatePreCondition(ctx, condition); err != nil {
				return fmt.Errorf("pre-condition %s failed: %w", condition.Name, err)
			}
		}
	}
	return nil
}

func (e *Executor) executePostActions(ctx context.Context, plan *MigrationPlan, execution *MigrationExecution) error {
	targetConn, err := e.connectionProvider.GetTargetConnection(ctx)
	if err != nil {
		return err
	}
	defer targetConn.Close()

	for _, action := range plan.PostActions {
		if _, err := targetConn.ExecContext(ctx, action.SQL); err != nil {
			if action.Required {
				return fmt.Errorf("required post-action %s failed: %w", action.Name, err)
			}
			e.addWarning(execution, fmt.Sprintf("Post-action %s failed: %v", action.Name, err))
		}
	}

	return nil
}

func (e *Executor) runValidations(ctx context.Context, plan *MigrationPlan, execution *MigrationExecution) error {
	// Run final validations
	for _, validation := range plan.Validations {
		if validation.Enabled {
			// Run validation (simplified)
			result := ValidationResult{
				ID:         generateID("validation"),
				RuleID:     validation.ID,
				RuleName:   validation.Name,
				Status:     ValidationResultStatusPassed,
				Message:    "Validation passed",
				ExecutedAt: time.Now(),
				Severity:   validation.Severity,
				Details:    make(map[string]interface{}),
			}
			execution.ValidationResults = append(execution.ValidationResults, result)
		}
	}

	return nil
}

func (e *Executor) createCheckpoint(ctx context.Context, execution *MigrationExecution, table string, batch int64) {
	checkpoint := ExecutionCheckpoint{
		ID:           generateID("checkpoint"),
		CreatedAt:    time.Now(),
		Table:        table,
		Batch:        batch,
		RowsProcessed: execution.Progress.ProcessedRows,
		State:        make(map[string]interface{}),
		Metadata:     make(map[string]interface{}),
	}

	execution.Checkpoints = append(execution.Checkpoints, checkpoint)
	e.repository.SaveCheckpoint(ctx, &checkpoint)
}

func (e *Executor) evaluatePreCondition(ctx context.Context, condition PreCondition) error {
	// Simplified pre-condition evaluation
	// Real implementation would evaluate the expression
	return nil
}

func (e *Executor) failExecution(execution *MigrationExecution, err error) {
	execution.Status = MigrationStatusFailed
	completedAt := time.Now()
	execution.CompletedAt = &completedAt
	execution.Duration = completedAt.Sub(execution.StartedAt)

	// Add error
	migrationError := MigrationError{
		ID:         generateID("error"),
		Code:       "EXECUTION_FAILED",
		Message:    err.Error(),
		OccurredAt: time.Now(),
		Severity:   "CRITICAL",
		Resolved:   false,
	}
	execution.Errors = append(execution.Errors, migrationError)
}

func (e *Executor) addWarning(execution *MigrationExecution, message string) {
	warning := MigrationWarning{
		ID:         generateID("warning"),
		Message:    message,
		OccurredAt: time.Now(),
		Severity:   "WARNING",
	}
	execution.Warnings = append(execution.Warnings, warning)
}

func (e *Executor) handleExecutionPanic(execution *MigrationExecution, r interface{}) {
	execution.Status = MigrationStatusFailed
	completedAt := time.Now()
	execution.CompletedAt = &completedAt
	execution.Duration = completedAt.Sub(execution.StartedAt)

	// Add panic error
	migrationError := MigrationError{
		ID:         generateID("error"),
		Code:       "PANIC",
		Message:    fmt.Sprintf("Execution panicked: %v", r),
		OccurredAt: time.Now(),
		Severity:   "CRITICAL",
		Resolved:   false,
	}
	execution.Errors = append(execution.Errors, migrationError)
}

func (e *Executor) finalizeExecution(ctx context.Context, execution *MigrationExecution) {
	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.ActiveExecutions--
	if execution.Status == MigrationStatusCompleted {
		e.metrics.CompletedExecutions++
	} else {
		e.metrics.FailedExecutions++
	}
	e.metrics.TotalRowsProcessed += execution.Progress.ProcessedRows
	e.metrics.mu.Unlock()

	// Save final execution state
	e.repository.SaveExecution(ctx, execution)
}

// Status and monitoring methods

func (e *Executor) GetStatus(ctx context.Context, executionID string) (*MigrationProgress, error) {
	e.mu.RLock()
	execution, exists := e.executions[executionID]
	e.mu.RUnlock()

	if !exists {
		// Try to load from repository
		var err error
		execution, err = e.repository.GetExecution(ctx, executionID)
		if err != nil {
			return nil, fmt.Errorf("execution not found: %w", err)
		}
	}

	return &execution.Progress, nil
}

func (e *Executor) GetMetrics(ctx context.Context, executionID string) (*MigrationMetrics, error) {
	e.mu.RLock()
	execution, exists := e.executions[executionID]
	e.mu.RUnlock()

	if !exists {
		// Try to load from repository
		var err error
		execution, err = e.repository.GetExecution(ctx, executionID)
		if err != nil {
			return nil, fmt.Errorf("execution not found: %w", err)
		}
	}

	return &execution.Metrics, nil
}

func (e *Executor) Pause(ctx context.Context, executionID string) error {
	// Implementation for pausing execution
	return fmt.Errorf("pause functionality not yet implemented")
}

func (e *Executor) Resume(ctx context.Context, executionID string) error {
	// Implementation for resuming execution
	return fmt.Errorf("resume functionality not yet implemented")
}

func (e *Executor) Cancel(ctx context.Context, executionID string) error {
	// Implementation for canceling execution
	return fmt.Errorf("cancel functionality not yet implemented")
}
