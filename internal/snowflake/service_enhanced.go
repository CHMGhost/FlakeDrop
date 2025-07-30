package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// EnhancedService provides optimized Snowflake database operations
type EnhancedService struct {
	*Service
	connManager      *ConnectionManager
	queryOptimizer   *QueryOptimizer
	bulkOps          *BulkOperations
	asyncExecutor    *AsyncQueryExecutor
	preparedStmts    *PreparedStatementCache
	metricsCollector *ServiceMetricsCollector
	mu               sync.RWMutex
}

// PreparedStatementCache caches prepared statements
type PreparedStatementCache struct {
	statements map[string]*PreparedStatement
	maxSize    int
	mu         sync.RWMutex
}

// PreparedStatement represents a cached prepared statement
type PreparedStatement struct {
	ID          string
	SQL         string
	Stmt        *sql.Stmt
	CreatedAt   time.Time
	LastUsedAt  time.Time
	UseCount    int64
	Connection  *ManagedConnection
}

// ServiceMetricsCollector collects service-level metrics
type ServiceMetricsCollector struct {
	queries      []QueryMetric
	operations   []OperationMetric
	connections  []ConnectionMetric
	mu           sync.RWMutex
}

// QueryMetric tracks individual query metrics
type QueryMetric struct {
	QueryID       string
	SQL           string
	Duration      time.Duration
	RowsAffected  int64
	BytesScanned  int64
	Warehouse     string
	Timestamp     time.Time
}

// OperationMetric tracks operation metrics
type OperationMetric struct {
	OperationType string
	Duration      time.Duration
	Success       bool
	Error         string
	Timestamp     time.Time
}

// ConnectionMetric tracks connection metrics
type ConnectionMetric struct {
	ConnectionID   string
	Warehouse      string
	SessionTime    time.Duration
	QueriesExecuted int64
	BytesTransferred int64
	Timestamp       time.Time
}

// NewEnhancedService creates a new enhanced Snowflake service
func NewEnhancedService(config Config) (*EnhancedService, error) {
	// Create base service
	baseService := NewService(config)

	// Create connection manager
	connManager := NewConnectionManager(config)

	// Create enhanced service
	service := &EnhancedService{
		Service:          baseService,
		connManager:      connManager,
		queryOptimizer:   NewQueryOptimizer(baseService),
		preparedStmts:    NewPreparedStatementCache(100),
		metricsCollector: NewServiceMetricsCollector(),
	}

	// Initialize bulk operations
	service.bulkOps = NewBulkOperations(baseService, connManager)

	// Initialize async executor
	service.asyncExecutor = NewAsyncQueryExecutor(baseService, connManager, DefaultAsyncConfig())

	return service, nil
}

// ExecuteOptimized executes a query with optimization
func (es *EnhancedService) ExecuteOptimized(ctx context.Context, sql string) (*sql.Rows, error) {
	startTime := time.Now()

	// Analyze and optimize query
	optimizedSQL, plan, err := es.queryOptimizer.OptimizeQuery(ctx, sql)
	if err != nil {
		// Fall back to original query
		optimizedSQL = sql
	}

	// Get optimal connection
	warehouse := ""
	if plan != nil && plan.EstimatedCost > 1000 {
		warehouse = "COMPUTE_WH_LARGE"
	}

	conn, err := es.connManager.GetConnection(ctx, warehouse)
	if err != nil {
		return nil, err
	}
	defer es.connManager.ReleaseConnection(conn)

	// Execute query
	rows, err := conn.Query(ctx, optimizedSQL)

	// Record metrics
	es.recordQueryMetric(QueryMetric{
		SQL:       sql,
		Duration:  time.Since(startTime),
		Warehouse: warehouse,
		Timestamp: time.Now(),
	})

	return rows, err
}

// PrepareStatement prepares and caches a statement
func (es *EnhancedService) PrepareStatement(ctx context.Context, sql string) (*PreparedStatement, error) {
	// Check cache first
	if stmt := es.preparedStmts.Get(sql); stmt != nil {
		return stmt, nil
	}

	// Get connection
	conn, err := es.connManager.GetConnection(ctx, "")
	if err != nil {
		return nil, err
	}

	// Prepare statement
	sqlStmt, err := conn.db.PrepareContext(ctx, sql)
	if err != nil {
		es.connManager.ReleaseConnection(conn)
		return nil, errors.SQLError("Failed to prepare statement", sql, err)
	}

	// Create prepared statement
	stmt := &PreparedStatement{
		ID:         fmt.Sprintf("ps_%d", time.Now().UnixNano()),
		SQL:        sql,
		Stmt:       sqlStmt,
		CreatedAt:  time.Now(),
		LastUsedAt: time.Now(),
		Connection: conn,
	}

	// Cache it
	es.preparedStmts.Set(sql, stmt)

	return stmt, nil
}

// ExecutePrepared executes a prepared statement
func (es *EnhancedService) ExecutePrepared(ctx context.Context, stmt *PreparedStatement, args ...interface{}) (*sql.Rows, error) {
	stmt.LastUsedAt = time.Now()
	stmt.UseCount++

	return stmt.Stmt.QueryContext(ctx, args...)
}

// BulkInsert performs optimized bulk insert
func (es *EnhancedService) BulkInsert(ctx context.Context, table string, files []string, options BulkInsertOptions) (*BulkOperationResult, error) {
	// Set default options if not provided
	if options.Database == "" {
		options.Database = es.config.Database
	}
	if options.Schema == "" {
		options.Schema = es.config.Schema
	}
	options.Table = table

	return es.bulkOps.BulkInsertFromFile(ctx, files, options)
}

// ExecuteAsync executes a query asynchronously
func (es *EnhancedService) ExecuteAsync(ctx context.Context, sql string, options ...QueryOption) (*AsyncQuery, error) {
	return es.asyncExecutor.SubmitQuery(ctx, sql, options...)
}

// ExecuteBatch executes multiple queries in batch
func (es *EnhancedService) ExecuteBatch(ctx context.Context, queries []string) ([]*AsyncQuery, error) {
	batchExecutor := NewBatchAsyncExecutor(es.asyncExecutor, 5)
	return batchExecutor.ExecuteBatch(ctx, queries)
}

// GetQueryPlan gets the execution plan for a query
func (es *EnhancedService) GetQueryPlan(ctx context.Context, sql string) (*QueryPlan, error) {
	return es.queryOptimizer.AnalyzeQuery(ctx, sql)
}

// GetSlowQueries returns queries that exceeded the duration threshold
func (es *EnhancedService) GetSlowQueries(threshold time.Duration) []QueryMetrics {
	return es.queryOptimizer.metricsTracker.GetSlowQueries(threshold)
}

// GetWarehouseRecommendations provides warehouse sizing recommendations
func (es *EnhancedService) GetWarehouseRecommendations() map[string]string {
	return es.connManager.warehouseOptimizer.GetRecommendations()
}

// ExportData exports query results to file
func (es *EnhancedService) ExportData(ctx context.Context, query string, outputPath string, format string) (*ExportResult, error) {
	options := ExportOptions{
		Format:      format,
		Compression: "GZIP",
		Header:      true,
	}

	return es.bulkOps.BulkExport(ctx, query, outputPath, options)
}

// GetServiceMetrics returns service performance metrics
func (es *EnhancedService) GetServiceMetrics() ServiceMetrics {
	es.metricsCollector.mu.RLock()
	defer es.metricsCollector.mu.RUnlock()

	metrics := ServiceMetrics{
		TotalQueries:     len(es.metricsCollector.queries),
		TotalOperations:  len(es.metricsCollector.operations),
		TotalConnections: len(es.metricsCollector.connections),
	}

	// Calculate averages
	if metrics.TotalQueries > 0 {
		var totalDuration time.Duration
		var totalBytes int64
		for _, q := range es.metricsCollector.queries {
			totalDuration += q.Duration
			totalBytes += q.BytesScanned
		}
		metrics.AvgQueryDuration = totalDuration / time.Duration(metrics.TotalQueries)
		metrics.AvgBytesScanned = totalBytes / int64(metrics.TotalQueries)
	}

	// Success rate
	if metrics.TotalOperations > 0 {
		successCount := 0
		for _, op := range es.metricsCollector.operations {
			if op.Success {
				successCount++
			}
		}
		metrics.SuccessRate = float64(successCount) / float64(metrics.TotalOperations) * 100
	}

	return metrics
}

// Close closes all resources
func (es *EnhancedService) Close() error {
	es.mu.Lock()
	defer es.mu.Unlock()

	var errs []error

	// Close prepared statements
	es.preparedStmts.Clear()

	// Close connection manager
	if err := es.connManager.Close(); err != nil {
		errs = append(errs, err)
	}

	// Close base service
	if err := es.Service.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing service: %v", errs)
	}

	return nil
}

// Helper methods

func (es *EnhancedService) recordQueryMetric(metric QueryMetric) {
	es.metricsCollector.mu.Lock()
	defer es.metricsCollector.mu.Unlock()

	es.metricsCollector.queries = append(es.metricsCollector.queries, metric)

	// Keep only last 1000 queries
	if len(es.metricsCollector.queries) > 1000 {
		es.metricsCollector.queries = es.metricsCollector.queries[len(es.metricsCollector.queries)-1000:]
	}
}

func (es *EnhancedService) recordOperationMetric(metric OperationMetric) {
	es.metricsCollector.mu.Lock()
	defer es.metricsCollector.mu.Unlock()

	es.metricsCollector.operations = append(es.metricsCollector.operations, metric)

	// Keep only last 1000 operations
	if len(es.metricsCollector.operations) > 1000 {
		es.metricsCollector.operations = es.metricsCollector.operations[len(es.metricsCollector.operations)-1000:]
	}
}

// PreparedStatementCache methods

func NewPreparedStatementCache(maxSize int) *PreparedStatementCache {
	return &PreparedStatementCache{
		statements: make(map[string]*PreparedStatement),
		maxSize:    maxSize,
	}
}

func (psc *PreparedStatementCache) Get(sql string) *PreparedStatement {
	psc.mu.RLock()
	defer psc.mu.RUnlock()

	stmt, exists := psc.statements[sql]
	if !exists {
		return nil
	}

	// Check if statement is still valid
	if time.Since(stmt.LastUsedAt) > 30*time.Minute {
		// Statement might be stale
		return nil
	}

	return stmt
}

func (psc *PreparedStatementCache) Set(sql string, stmt *PreparedStatement) {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	// Check size limit
	if len(psc.statements) >= psc.maxSize {
		// Evict least recently used
		var lruKey string
		var lruTime time.Time
		for key, s := range psc.statements {
			if lruKey == "" || s.LastUsedAt.Before(lruTime) {
				lruKey = key
				lruTime = s.LastUsedAt
			}
		}
		if lruKey != "" {
			if oldStmt := psc.statements[lruKey]; oldStmt != nil {
				oldStmt.Stmt.Close()
			}
			delete(psc.statements, lruKey)
		}
	}

	psc.statements[sql] = stmt
}

func (psc *PreparedStatementCache) Clear() {
	psc.mu.Lock()
	defer psc.mu.Unlock()

	for _, stmt := range psc.statements {
		if stmt != nil && stmt.Stmt != nil {
			stmt.Stmt.Close()
		}
		if stmt != nil && stmt.Connection != nil {
			// Return connection to pool
			stmt.Connection.pool.Put(stmt.Connection)
		}
	}

	psc.statements = make(map[string]*PreparedStatement)
}

// ServiceMetricsCollector methods

func NewServiceMetricsCollector() *ServiceMetricsCollector {
	return &ServiceMetricsCollector{
		queries:     make([]QueryMetric, 0),
		operations:  make([]OperationMetric, 0),
		connections: make([]ConnectionMetric, 0),
	}
}

// ServiceMetrics contains aggregated service metrics
type ServiceMetrics struct {
	TotalQueries     int
	TotalOperations  int
	TotalConnections int
	AvgQueryDuration time.Duration
	AvgBytesScanned  int64
	SuccessRate      float64
	ActiveQueries    int
	CacheHitRate     float64
}

// StreamingOptions configures streaming operations
type StreamingOptions struct {
	BufferSize      int
	FlushInterval   time.Duration
	MaxRetries      int
	OnError         func(error)
	ProgressHandler func(int64)
}

// StreamInsert performs streaming insert operations
func (es *EnhancedService) StreamInsert(ctx context.Context, table string, reader StreamReader, options StreamingOptions) error {
	// Default options
	if options.BufferSize == 0 {
		options.BufferSize = 1024 * 1024 // 1MB
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = 5 * time.Second
	}

	// Create bulk insert options
	bulkOpts := BulkInsertOptions{
		Database:        es.config.Database,
		Schema:          es.config.Schema,
		Table:           table,
		FileFormat:      "CSV",
		BatchSize:       options.BufferSize,
		ContinueOnError: true,
	}

	// Stream data
	buffer := make([]byte, 0, options.BufferSize)
	ticker := time.NewTicker(options.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if len(buffer) > 0 {
				if err := es.flushBuffer(ctx, buffer, bulkOpts); err != nil {
					if options.OnError != nil {
						options.OnError(err)
					}
					if options.MaxRetries == 0 {
						return err
					}
				}
				buffer = buffer[:0]
			}

		default:
			data, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					// Final flush
					if len(buffer) > 0 {
						return es.flushBuffer(ctx, buffer, bulkOpts)
					}
					return nil
				}
				return err
			}

			buffer = append(buffer, data...)

			if len(buffer) >= options.BufferSize {
				if err := es.flushBuffer(ctx, buffer, bulkOpts); err != nil {
					if options.OnError != nil {
						options.OnError(err)
					}
					if options.MaxRetries == 0 {
						return err
					}
				}
				buffer = buffer[:0]

				if options.ProgressHandler != nil {
					options.ProgressHandler(int64(len(buffer)))
				}
			}
		}
	}
}

func (es *EnhancedService) flushBuffer(ctx context.Context, data []byte, options BulkInsertOptions) error {
	// Implementation would write buffer to temp file and bulk insert
	// This is simplified for the example
	return nil
}

// StreamReader interface for streaming data
type StreamReader interface {
	Read() ([]byte, error)
}