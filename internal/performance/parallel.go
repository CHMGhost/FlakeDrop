package performance

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"flakedrop/pkg/errors"
)

// ParallelExecutor handles concurrent execution of deployment tasks
type ParallelExecutor struct {
	pool          *ConnectionPool
	maxWorkers    int
	taskQueue     chan Task
	results       chan TaskResult
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	metrics       *ExecutorMetrics
	errorHandler  *errors.ErrorHandler
	rateLimit     *RateLimiter
	batchSize     int
}

// Task represents a deployment task
type Task struct {
	ID          string
	Type        TaskType
	FilePath    string
	Content     string
	Database    string
	Schema      string
	Priority    int
	RetryCount  int
	CreatedAt   time.Time
}

// TaskType defines the type of deployment task
type TaskType int

const (
	TaskTypeSQL TaskType = iota
	TaskTypeDDL
	TaskTypeDML
	TaskTypeFunction
	TaskTypeProcedure
	TaskTypeView
)

// TaskResult contains the result of a task execution
type TaskResult struct {
	TaskID      string
	Success     bool
	Error       error
	Duration    time.Duration
	RetryCount  int
	StartedAt   time.Time
	CompletedAt time.Time
}

// ExecutorMetrics tracks parallel execution performance
type ExecutorMetrics struct {
	mu                sync.RWMutex
	totalTasks        int64
	completedTasks    int64
	failedTasks       int64
	retryCount        int64
	totalDuration     time.Duration
	avgDuration       time.Duration
	maxDuration       time.Duration
	minDuration       time.Duration
	queueSize         int64
	activeWorkers     int64
	throughput        float64
	lastMeasured      time.Time
}

// CompletedTasks returns the number of completed tasks
func (m *ExecutorMetrics) CompletedTasks() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.completedTasks
}

// FailedTasks returns the number of failed tasks
func (m *ExecutorMetrics) FailedTasks() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.failedTasks
}

// AvgDuration returns the average duration
func (m *ExecutorMetrics) AvgDuration() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.avgDuration
}

// Throughput returns the current throughput
func (m *ExecutorMetrics) Throughput() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.throughput
}

// ExecutorConfig contains configuration for the parallel executor
type ExecutorConfig struct {
	MaxWorkers   int
	QueueSize    int
	BatchSize    int
	RateLimit    int // requests per second
	RetryLimit   int
	Timeout      time.Duration
}

// DefaultExecutorConfig returns sensible defaults
func DefaultExecutorConfig() ExecutorConfig {
	numCPU := runtime.NumCPU()
	return ExecutorConfig{
		MaxWorkers: numCPU * 2,
		QueueSize:  1000,
		BatchSize:  10,
		RateLimit:  100,
		RetryLimit: 3,
		Timeout:    5 * time.Minute,
	}
}

// NewParallelExecutor creates a new parallel executor
func NewParallelExecutor(pool *ConnectionPool, config ExecutorConfig) *ParallelExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	
	executor := &ParallelExecutor{
		pool:         pool,
		maxWorkers:   config.MaxWorkers,
		taskQueue:    make(chan Task, config.QueueSize),
		results:      make(chan TaskResult, config.QueueSize),
		ctx:          ctx,
		cancel:       cancel,
		metrics:      &ExecutorMetrics{lastMeasured: time.Now()},
		errorHandler: errors.GetGlobalErrorHandler(),
		rateLimit:    NewRateLimiter(config.RateLimit),
		batchSize:    config.BatchSize,
	}

	// Start workers
	for i := 0; i < config.MaxWorkers; i++ {
		executor.wg.Add(1)
		go executor.worker(i)
	}

	// Start metrics collector
	go executor.metricsCollector()

	return executor
}

// Submit adds a task to the execution queue
func (e *ParallelExecutor) Submit(task Task) error {
	select {
	case e.taskQueue <- task:
		atomic.AddInt64(&e.metrics.queueSize, 1)
		atomic.AddInt64(&e.metrics.totalTasks, 1)
		return nil
	case <-e.ctx.Done():
		return errors.New(errors.ErrCodeTimeout, "Executor is shutting down")
	default:
		return errors.New(errors.ErrCodeInternal, "Task queue is full").
			WithContext("queue_size", len(e.taskQueue)).
			WithSuggestions(
				"Increase queue size",
				"Reduce submission rate",
				"Wait for tasks to complete",
			)
	}
}

// SubmitBatch submits multiple tasks as a batch
func (e *ParallelExecutor) SubmitBatch(tasks []Task) error {
	// Sort tasks by priority
	sortTasksByPriority(tasks)

	submitted := 0
	for _, task := range tasks {
		if err := e.Submit(task); err != nil {
			return fmt.Errorf("submitted %d/%d tasks before error: %w", submitted, len(tasks), err)
		}
		submitted++
	}
	return nil
}

// GetResults returns the results channel for processed tasks
func (e *ParallelExecutor) GetResults() <-chan TaskResult {
	return e.results
}

// WaitForCompletion waits for all submitted tasks to complete
func (e *ParallelExecutor) WaitForCompletion(timeout time.Duration) error {
	done := make(chan struct{})
	
	go func() {
		for atomic.LoadInt64(&e.metrics.queueSize) > 0 || atomic.LoadInt64(&e.metrics.activeWorkers) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New(errors.ErrCodeTimeout, "Timeout waiting for tasks to complete").
			WithContext("pending_tasks", atomic.LoadInt64(&e.metrics.queueSize)).
			WithContext("active_workers", atomic.LoadInt64(&e.metrics.activeWorkers))
	}
}

// Shutdown gracefully shuts down the executor
func (e *ParallelExecutor) Shutdown(timeout time.Duration) error {
	// Stop accepting new tasks
	e.cancel()

	// Wait for workers to finish
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(e.taskQueue)
		close(e.results)
		return nil
	case <-time.After(timeout):
		return errors.New(errors.ErrCodeTimeout, "Timeout during shutdown")
	}
}

// GetMetrics returns current execution metrics
func (e *ParallelExecutor) GetMetrics() ExecutorMetrics {
	e.metrics.mu.RLock()
	defer e.metrics.mu.RUnlock()
	return *e.metrics
}

// worker processes tasks from the queue
func (e *ParallelExecutor) worker(id int) {
	defer e.wg.Done()

	for {
		select {
		case task, ok := <-e.taskQueue:
			if !ok {
				return
			}

			atomic.AddInt64(&e.metrics.activeWorkers, 1)
			atomic.AddInt64(&e.metrics.queueSize, -1)

			result := e.executeTask(task)
			
			select {
			case e.results <- result:
			case <-e.ctx.Done():
				atomic.AddInt64(&e.metrics.activeWorkers, -1)
				return
			}

			atomic.AddInt64(&e.metrics.activeWorkers, -1)
			e.updateMetrics(result)

		case <-e.ctx.Done():
			return
		}
	}
}

// executeTask executes a single task
func (e *ParallelExecutor) executeTask(task Task) TaskResult {
	// Apply rate limiting
	e.rateLimit.Wait()

	startTime := time.Now()
	result := TaskResult{
		TaskID:     task.ID,
		StartedAt:  startTime,
		RetryCount: task.RetryCount,
	}

	// Get connection from pool
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	conn, err := e.pool.Get(ctx)
	if err != nil {
		result.Error = errors.Wrap(err, errors.ErrCodeConnectionFailed, "Failed to get connection from pool")
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result
	}
	defer e.pool.Put(conn)

	// Execute based on task type
	switch task.Type {
	case TaskTypeSQL:
		err = e.executeSQLTask(ctx, conn, task)
	case TaskTypeDDL:
		err = e.executeDDLTask(ctx, conn, task)
	case TaskTypeDML:
		err = e.executeDMLTask(ctx, conn, task)
	case TaskTypeFunction, TaskTypeProcedure:
		err = e.executeObjectTask(ctx, conn, task)
	case TaskTypeView:
		err = e.executeViewTask(ctx, conn, task)
	default:
		err = fmt.Errorf("unknown task type: %v", task.Type)
	}

	result.Error = err
	result.Success = err == nil
	result.CompletedAt = time.Now()
	result.Duration = time.Since(startTime)

	// Handle retry logic
	if err != nil && task.RetryCount < 3 {
		task.RetryCount++
		go func() {
			time.Sleep(time.Duration(task.RetryCount) * time.Second)
			e.Submit(task)
		}()
	}

	return result
}

// executeSQLTask executes a generic SQL task
func (e *ParallelExecutor) executeSQLTask(ctx context.Context, conn *PooledConnection, task Task) error {
	// Switch to the correct database and schema
	if _, err := conn.Execute(ctx, fmt.Sprintf("USE DATABASE %s", task.Database)); err != nil {
		return errors.SQLError("Failed to use database", fmt.Sprintf("USE DATABASE %s", task.Database), err)
	}

	if _, err := conn.Execute(ctx, fmt.Sprintf("USE SCHEMA %s", task.Schema)); err != nil {
		return errors.SQLError("Failed to use schema", fmt.Sprintf("USE SCHEMA %s", task.Schema), err)
	}

	// Execute the SQL content
	_, err := conn.Execute(ctx, task.Content)
	if err != nil {
		return errors.SQLError("Failed to execute SQL", task.Content, err).
			WithContext("file", task.FilePath)
	}

	return nil
}

// executeDDLTask executes DDL operations with special handling
func (e *ParallelExecutor) executeDDLTask(ctx context.Context, conn *PooledConnection, task Task) error {
	// DDL operations often can't be in transactions
	// Execute directly without transaction
	return e.executeSQLTask(ctx, conn, task)
}

// executeDMLTask executes DML operations with transaction support
func (e *ParallelExecutor) executeDMLTask(ctx context.Context, conn *PooledConnection, task Task) error {
	// Start transaction for DML operations
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "Failed to begin transaction")
	}

	// Create transaction handler
	txHandler := e.errorHandler.NewTransactionHandler(tx, tx.Rollback)

	return txHandler.Execute(func() error {
		// Switch context
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", task.Database)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s", task.Schema)); err != nil {
			return err
		}

		// Execute DML
		if _, err := tx.ExecContext(ctx, task.Content); err != nil {
			return errors.SQLError("Failed to execute DML", task.Content, err)
		}

		return tx.Commit()
	})
}

// executeObjectTask executes stored procedures and functions
func (e *ParallelExecutor) executeObjectTask(ctx context.Context, conn *PooledConnection, task Task) error {
	// Objects like functions and procedures need special handling
	// They often need CREATE OR REPLACE syntax
	return e.executeSQLTask(ctx, conn, task)
}

// executeViewTask executes view creation/updates
func (e *ParallelExecutor) executeViewTask(ctx context.Context, conn *PooledConnection, task Task) error {
	// Views might depend on other objects, so we need to handle dependencies
	return e.executeSQLTask(ctx, conn, task)
}

// updateMetrics updates execution metrics
func (e *ParallelExecutor) updateMetrics(result TaskResult) {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	if result.Success {
		e.metrics.completedTasks++
	} else {
		e.metrics.failedTasks++
	}

	// Update duration metrics
	e.metrics.totalDuration += result.Duration
	
	if e.metrics.minDuration == 0 || result.Duration < e.metrics.minDuration {
		e.metrics.minDuration = result.Duration
	}
	
	if result.Duration > e.metrics.maxDuration {
		e.metrics.maxDuration = result.Duration
	}

	// Calculate average
	totalProcessed := e.metrics.completedTasks + e.metrics.failedTasks
	if totalProcessed > 0 {
		e.metrics.avgDuration = e.metrics.totalDuration / time.Duration(totalProcessed)
	}
}

// metricsCollector periodically calculates throughput
func (e *ParallelExecutor) metricsCollector() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCompleted int64

	for {
		select {
		case <-ticker.C:
			e.metrics.mu.Lock()
			
			currentCompleted := e.metrics.completedTasks
			elapsed := time.Since(e.metrics.lastMeasured).Seconds()
			
			if elapsed > 0 {
				e.metrics.throughput = float64(currentCompleted-lastCompleted) / elapsed
			}
			
			e.metrics.lastMeasured = time.Now()
			lastCompleted = currentCompleted
			
			e.metrics.mu.Unlock()

		case <-e.ctx.Done():
			return
		}
	}
}

// Helper functions

func sortTasksByPriority(tasks []Task) {
	// Simple bubble sort for task priority
	for i := 0; i < len(tasks); i++ {
		for j := i + 1; j < len(tasks); j++ {
			if tasks[i].Priority < tasks[j].Priority {
				tasks[i], tasks[j] = tasks[j], tasks[i]
			}
		}
	}
}

// RateLimiter implements a simple token bucket rate limiter
type RateLimiter struct {
	rate       int
	tokens     chan struct{}
	stopFill   chan struct{}
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(ratePerSecond int) *RateLimiter {
	rl := &RateLimiter{
		rate:     ratePerSecond,
		tokens:   make(chan struct{}, ratePerSecond),
		stopFill: make(chan struct{}),
	}

	// Initially fill the bucket
	for i := 0; i < ratePerSecond; i++ {
		rl.tokens <- struct{}{}
	}

	// Start token refill routine
	go rl.refill()

	return rl
}

// Wait blocks until a token is available
func (rl *RateLimiter) Wait() {
	<-rl.tokens
}

// Stop stops the rate limiter
func (rl *RateLimiter) Stop() {
	close(rl.stopFill)
}

// refill periodically adds tokens to the bucket
func (rl *RateLimiter) refill() {
	interval := time.Second / time.Duration(rl.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case rl.tokens <- struct{}{}:
				// Token added
			default:
				// Bucket is full
			}
		case <-rl.stopFill:
			return
		}
	}
}