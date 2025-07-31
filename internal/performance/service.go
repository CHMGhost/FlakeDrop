package performance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// OptimizedService provides high-performance Snowflake operations
type OptimizedService struct {
	pool            *ConnectionPool
	executor        *ParallelExecutor
	batchProcessor  *BatchProcessor
	streamProcessor *StreamProcessor
	fileCache       *FileCache
	queryCache      *QueryCache
	monitor         *Monitor
	profiler        *Profiler
	config          *OptimizedConfig
	mu              sync.RWMutex
}

// OptimizedConfig contains configuration for the optimized service
type OptimizedConfig struct {
	// Connection pool settings
	PoolConfig PoolConfig

	// Parallel execution settings
	ExecutorConfig ExecutorConfig

	// Batch processing settings
	BatchConfig BatchConfig

	// Streaming settings
	StreamConfig StreamConfig

	// Cache settings
	FileCacheConfig  CacheConfig
	QueryCacheConfig CacheConfig

	// Monitoring settings
	MonitorConfig MonitorConfig

	// Feature flags
	EnableParallelProcessing bool
	EnableBatching           bool
	EnableStreaming          bool
	EnableCaching            bool
	EnableProfiling          bool

	// Thresholds
	LargeFileThreshold int64 // Bytes - files larger than this use streaming
	BatchingThreshold  int   // Number of files - more than this uses batching
}

// DefaultOptimizedConfig returns optimized default configuration
func DefaultOptimizedConfig() *OptimizedConfig {
	return &OptimizedConfig{
		PoolConfig:     DefaultPoolConfig(),
		ExecutorConfig: DefaultExecutorConfig(),
		BatchConfig:    DefaultBatchConfig(),
		StreamConfig:   DefaultStreamConfig(),
		FileCacheConfig: CacheConfig{
			MaxItems:        1000,
			MaxMemoryMB:     50,
			DefaultTTL:      5 * time.Minute,
			CleanupInterval: 1 * time.Minute,
		},
		QueryCacheConfig: CacheConfig{
			MaxItems:        5000,
			MaxMemoryMB:     100,
			DefaultTTL:      10 * time.Minute,
			CleanupInterval: 2 * time.Minute,
		},
		MonitorConfig: DefaultMonitorConfig(),

		// Enable all optimizations by default
		EnableParallelProcessing: true,
		EnableBatching:           true,
		EnableStreaming:          true,
		EnableCaching:            true,
		EnableProfiling:          true,

		// Thresholds
		LargeFileThreshold: 10 * 1024 * 1024, // 10MB
		BatchingThreshold:  10,
	}
}

// NewOptimizedService creates a new optimized Snowflake service
func NewOptimizedService(sfConfig snowflake.Config, optConfig *OptimizedConfig) (*OptimizedService, error) {
	if optConfig == nil {
		optConfig = DefaultOptimizedConfig()
	}

	// Create connection pool
	pool, err := NewConnectionPool(sfConfig, optConfig.PoolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create monitor
	monitor := NewMonitor(optConfig.MonitorConfig)

	// Create service
	service := &OptimizedService{
		pool:     pool,
		config:   optConfig,
		monitor:  monitor,
		profiler: NewProfiler(monitor),
	}

	// Initialize components based on config
	if optConfig.EnableParallelProcessing {
		service.executor = NewParallelExecutor(pool, optConfig.ExecutorConfig)
	}

	if optConfig.EnableBatching {
		service.batchProcessor = NewBatchProcessor(pool, optConfig.BatchConfig)
	}

	if optConfig.EnableStreaming {
		service.streamProcessor = NewStreamProcessor(pool, optConfig.StreamConfig)
	}

	if optConfig.EnableCaching {
		service.fileCache = NewFileCache(optConfig.FileCacheConfig)
		service.queryCache = NewQueryCache(optConfig.QueryCacheConfig)
	}

	return service, nil
}

// DeployFiles deploys multiple SQL files with optimal performance
func (s *OptimizedService) DeployFiles(ctx context.Context, files []string, database, schema string) error {
	timer := s.monitor.StartTimer("deployment.total", map[string]string{
		"database": database,
		"schema":   schema,
		"files":    fmt.Sprintf("%d", len(files)),
	})
	defer timer.Stop()

	// Start profiling if enabled
	if s.config.EnableProfiling {
		_ = s.profiler.StartProfile("deployment", map[string]string{
			"database": database,
			"schema":   schema,
		})
		defer s.profiler.EndProfile("deployment")
	}

	// Analyze files to determine optimal strategy
	strategy := s.analyzeDeploymentStrategy(files)
	s.monitor.RecordMetric("deployment.strategy", float64(strategy), "type", nil)

	switch strategy {
	case StrategyParallel:
		return s.deployParallel(ctx, files, database, schema)
	case StrategyBatch:
		return s.deployBatch(ctx, files, database, schema)
	case StrategyStream:
		return s.deployStream(ctx, files, database, schema)
	case StrategyHybrid:
		return s.deployHybrid(ctx, files, database, schema)
	default:
		return s.deploySequential(ctx, files, database, schema)
	}
}

// DeploymentStrategy represents the chosen deployment strategy
type DeploymentStrategy int

const (
	StrategySequential DeploymentStrategy = iota
	StrategyParallel
	StrategyBatch
	StrategyStream
	StrategyHybrid
)

// analyzeDeploymentStrategy determines the optimal deployment strategy
func (s *OptimizedService) analyzeDeploymentStrategy(files []string) DeploymentStrategy {
	if len(files) == 0 {
		return StrategySequential
	}

	// Check file sizes
	totalSize := int64(0)
	largeFiles := 0
	
	for _, file := range files {
		if info, err := os.Stat(file); err == nil {
			totalSize += info.Size()
			if info.Size() > s.config.LargeFileThreshold {
				largeFiles++
			}
		}
	}

	avgSize := totalSize / int64(len(files))

	// Decision logic
	if largeFiles > len(files)/2 && s.config.EnableStreaming {
		return StrategyStream
	}

	if len(files) > s.config.BatchingThreshold && avgSize < s.config.LargeFileThreshold && s.config.EnableBatching {
		return StrategyBatch
	}

	if len(files) > 5 && s.config.EnableParallelProcessing {
		return StrategyParallel
	}

	if largeFiles > 0 && len(files) > 10 {
		return StrategyHybrid
	}

	return StrategySequential
}

// deployParallel deploys files using parallel processing
func (s *OptimizedService) deployParallel(ctx context.Context, files []string, database, schema string) error {
	var span *Span
	if s.config.EnableProfiling && s.profiler != nil {
		span, _ = s.profiler.StartSpan("deployment", "parallel", nil)
		if span != nil {
			defer span.End()
		}
	}

	// Create tasks for each file
	tasks := make([]Task, 0, len(files))
	for i, file := range files {
		content, err := s.readFile(file)
		if err != nil {
			return err
		}

		task := Task{
			ID:       fmt.Sprintf("file_%d_%s", i, filepath.Base(file)),
			Type:     s.detectTaskType(file, content),
			FilePath: file,
			Content:  content,
			Database: database,
			Schema:   schema,
			Priority: s.calculatePriority(file),
		}
		tasks = append(tasks, task)
	}

	// Submit tasks
	if err := s.executor.SubmitBatch(tasks); err != nil {
		return err
	}

	// Process results
	resultChan := s.executor.GetResults()
	successCount := 0
	var errors []error

	for i := 0; i < len(tasks); i++ {
		select {
		case result := <-resultChan:
			if result.Success {
				successCount++
				s.monitor.RecordCount("deployment.files.success", nil)
			} else {
				errors = append(errors, result.Error)
				s.monitor.RecordCount("deployment.files.failed", nil)
			}
			s.monitor.RecordDuration("deployment.file.duration", result.Duration, map[string]string{
				"file": result.TaskID,
			})

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("deployment completed with %d errors: %v", len(errors), errors[0])
	}

	return nil
}

// deployBatch deploys files using batch processing
func (s *OptimizedService) deployBatch(ctx context.Context, files []string, database, schema string) error {
	var span *Span
	if s.config.EnableProfiling && s.profiler != nil {
		span, _ = s.profiler.StartSpan("deployment", "batch", nil)
		if span != nil {
			defer span.End()
		}
	}

	for _, file := range files {
		content, err := s.readFile(file)
		if err != nil {
			return err
		}

		// Split content into statements
		statements := s.splitStatements(content)
		
		for _, stmt := range statements {
			batchStmt := BatchStatement{
				ID:       fmt.Sprintf("%s_%d", filepath.Base(file), time.Now().UnixNano()),
				SQL:      stmt,
				FilePath: file,
				Size:     int64(len(stmt)),
			}

			if err := s.batchProcessor.Add(database, schema, batchStmt); err != nil {
				return err
			}
		}
	}

	// Flush remaining batches
	return s.batchProcessor.FlushAll()
}

// deployStream deploys files using streaming for large files
func (s *OptimizedService) deployStream(ctx context.Context, files []string, database, schema string) error {
	var span *Span
	if s.config.EnableProfiling && s.profiler != nil {
		span, _ = s.profiler.StartSpan("deployment", "stream", nil)
		if span != nil {
			defer span.End()
		}
	}

	for _, file := range files {
		fileTimer := s.monitor.StartTimer("deployment.stream.file", map[string]string{
			"file": filepath.Base(file),
		})

		if err := s.streamProcessor.ProcessFile(ctx, file, database, schema); err != nil {
			return fmt.Errorf("failed to stream %s: %w", file, err)
		}

		fileTimer.Stop()
	}

	return nil
}

// deployHybrid uses a combination of strategies
func (s *OptimizedService) deployHybrid(ctx context.Context, files []string, database, schema string) error {
	var span *Span
	if s.config.EnableProfiling && s.profiler != nil {
		span, _ = s.profiler.StartSpan("deployment", "hybrid", nil)
		if span != nil {
			defer span.End()
		}
	}

	// Categorize files
	var smallFiles, largeFiles []string
	
	for _, file := range files {
		if info, err := os.Stat(file); err == nil {
			if info.Size() > s.config.LargeFileThreshold {
				largeFiles = append(largeFiles, file)
			} else {
				smallFiles = append(smallFiles, file)
			}
		}
	}

	// Use parallel processing for small files
	if len(smallFiles) > 0 && s.config.EnableParallelProcessing {
		if err := s.deployParallel(ctx, smallFiles, database, schema); err != nil {
			return err
		}
	}

	// Use streaming for large files
	if len(largeFiles) > 0 && s.config.EnableStreaming {
		if err := s.deployStream(ctx, largeFiles, database, schema); err != nil {
			return err
		}
	}

	return nil
}

// deploySequential deploys files one by one (fallback)
func (s *OptimizedService) deploySequential(ctx context.Context, files []string, database, schema string) error {
	var span *Span
	if s.config.EnableProfiling && s.profiler != nil {
		span, _ = s.profiler.StartSpan("deployment", "sequential", nil)
		if span != nil {
			defer span.End()
		}
	}

	// Get a single connection
	conn, err := s.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	for _, file := range files {
		content, err := s.readFile(file)
		if err != nil {
			return err
		}

		if err := s.executeFile(ctx, conn, file, content, database, schema); err != nil {
			return err
		}
	}

	return nil
}

// readFile reads a file with caching support
func (s *OptimizedService) readFile(path string) (string, error) {
	// Check cache first
	if s.config.EnableCaching {
		if content, found := s.fileCache.GetFile(path); found {
			s.monitor.RecordCount("cache.file.hit", nil)
			return content, nil
		}
		s.monitor.RecordCount("cache.file.miss", nil)
	}

	// Validate and clean the path
	cleanedPath, err := common.CleanPath(path)
	if err != nil {
		return "", errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}

	// Read file
	content, err := os.ReadFile(cleanedPath) // #nosec G304 - path is validated
	if err != nil {
		return "", errors.Wrap(err, errors.ErrCodeFileNotFound, "Failed to read file").
			WithContext("file", path)
	}

	contentStr := string(content)

	// Cache the content
	if s.config.EnableCaching {
		s.fileCache.SetFile(path, contentStr)
	}

	return contentStr, nil
}

// executeFile executes a single file
func (s *OptimizedService) executeFile(ctx context.Context, conn *PooledConnection, file, content, database, schema string) error {
	timer := s.monitor.StartTimer("execution.file", map[string]string{
		"file": filepath.Base(file),
	})
	defer timer.Stop()

	// Set database and schema
	if _, err := conn.Execute(ctx, fmt.Sprintf("USE DATABASE %s", database)); err != nil {
		return err
	}
	if _, err := conn.Execute(ctx, fmt.Sprintf("USE SCHEMA %s", schema)); err != nil {
		return err
	}

	// Execute statements
	statements := s.splitStatements(content)
	for i, stmt := range statements {
		if _, err := conn.Execute(ctx, stmt); err != nil {
			return errors.SQLError(
				fmt.Sprintf("Failed to execute statement %d in %s", i+1, file),
				stmt,
				err,
			)
		}
	}

	return nil
}

// Helper methods

func (s *OptimizedService) splitStatements(sql string) []string {
	// Simple statement splitter - reuse from snowflake service
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := rune(0)

	for i, char := range sql {
		if !inString {
			if char == '\'' || char == '"' {
				inString = true
				stringChar = char
			} else if char == ';' {
				if i == 0 || sql[i-1] != '\\' {
					statements = append(statements, current.String())
					current.Reset()
					continue
				}
			}
		} else {
			if char == stringChar && (i == 0 || sql[i-1] != '\\') {
				inString = false
			}
		}
		current.WriteRune(char)
	}

	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

func (s *OptimizedService) detectTaskType(file, content string) TaskType {
	// Detect based on file name or content
	lower := strings.ToLower(file)
	
	switch {
	case strings.Contains(lower, "table"):
		return TaskTypeDDL
	case strings.Contains(lower, "view"):
		return TaskTypeView
	case strings.Contains(lower, "function"):
		return TaskTypeFunction
	case strings.Contains(lower, "procedure"):
		return TaskTypeProcedure
	case strings.Contains(lower, "data") || strings.Contains(lower, "insert"):
		return TaskTypeDML
	default:
		// Analyze content
		contentLower := strings.ToLower(content)
		if strings.Contains(contentLower, "create table") || strings.Contains(contentLower, "alter table") {
			return TaskTypeDDL
		}
		if strings.Contains(contentLower, "insert") || strings.Contains(contentLower, "update") || strings.Contains(contentLower, "delete") {
			return TaskTypeDML
		}
		return TaskTypeSQL
	}
}

func (s *OptimizedService) calculatePriority(file string) int {
	// Higher priority for DDL operations
	taskType := s.detectTaskType(file, "")
	
	switch taskType {
	case TaskTypeDDL:
		return 10
	case TaskTypeView:
		return 5
	case TaskTypeFunction, TaskTypeProcedure:
		return 3
	case TaskTypeDML:
		return 1
	default:
		return 0
	}
}

// Shutdown gracefully shuts down the service
func (s *OptimizedService) Shutdown(timeout time.Duration) error {
	// Stop monitor
	s.monitor.Stop()

	// Shutdown components
	var wg sync.WaitGroup
	errChan := make(chan error, 4)

	// Shutdown executor
	if s.executor != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.executor.Shutdown(timeout); err != nil {
				errChan <- fmt.Errorf("executor shutdown error: %w", err)
			}
		}()
	}

	// Shutdown batch processor
	if s.batchProcessor != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.batchProcessor.Stop(); err != nil {
				errChan <- fmt.Errorf("batch processor shutdown error: %w", err)
			}
		}()
	}

	// Close connection pool
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.pool.Close(); err != nil {
			errChan <- fmt.Errorf("pool close error: %w", err)
		}
	}()

	// Wait for shutdown
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(timeout):
		return errors.New(errors.ErrCodeTimeout, "Shutdown timeout")
	}

	// Check for errors
	close(errChan)
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	return nil
}

// GetReport generates a performance report
func (s *OptimizedService) GetReport() *Report {
	report := s.monitor.GenerateReport()
	
	// Add service-specific metrics
	report.Summary["pool_metrics"] = s.pool.GetMetrics()
	
	if s.executor != nil {
		report.Summary["executor_metrics"] = s.executor.GetMetrics()
	}
	
	if s.batchProcessor != nil {
		report.Summary["batch_metrics"] = s.batchProcessor.GetMetrics()
	}
	
	if s.streamProcessor != nil {
		report.Summary["stream_metrics"] = s.streamProcessor.GetMetrics()
	}
	
	if s.fileCache != nil {
		report.Summary["file_cache_stats"] = s.fileCache.GetStats()
	}
	
	if s.queryCache != nil {
		report.Summary["query_cache_stats"] = s.queryCache.cache.GetStats()
	}

	return report
}