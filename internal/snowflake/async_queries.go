package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// AsyncQueryExecutor handles asynchronous query execution in Snowflake
type AsyncQueryExecutor struct {
	service      *Service
	connManager  *ConnectionManager
	queryTracker *QueryTracker
	resultCache  *ResultCache
	config       AsyncConfig
}

// AsyncConfig configures async query execution
type AsyncConfig struct {
	MaxConcurrentQueries int
	QueryTimeout         time.Duration
	PollInterval         time.Duration
	ResultCacheSize      int
	ResultCacheTTL       time.Duration
	EnableQueryTags      bool
	DefaultWarehouse     string
}

// AsyncQuery represents an asynchronous query
type AsyncQuery struct {
	ID            string
	SQL           string
	Status        QueryStatus
	QueryID       string // Snowflake query ID
	SubmittedAt   time.Time
	StartedAt     time.Time
	CompletedAt   time.Time
	ResultSetSize int64
	RowCount      int64
	Error         error
	Warehouse     string
	QueryTag      string
	Context       context.Context
	resultChan    chan *AsyncQueryResult
	cancelFunc    context.CancelFunc
}

// QueryStatus represents the status of an async query
type QueryStatus int

const (
	QueryStatusPending QueryStatus = iota
	QueryStatusRunning
	QueryStatusSucceeded
	QueryStatusFailed
	QueryStatusCancelled
	QueryStatusTimeout
)

// AsyncQueryResult contains the result of an async query
type AsyncQueryResult struct {
	QueryID       string
	Status        QueryStatus
	Rows          *sql.Rows
	RowCount      int64
	ExecutionTime time.Duration
	Error         error
	Metadata      QueryMetadata
}

// QueryMetadata contains query execution metadata
type QueryMetadata struct {
	BytesScanned     int64
	PartitionsScanned int64
	CreditsUsed      float64
	CompilationTime  time.Duration
	ExecutionTime    time.Duration
	QueueTime        time.Duration
}

// QueryTracker tracks active queries
type QueryTracker struct {
	queries map[string]*AsyncQuery
	mu      sync.RWMutex
}

// ResultCache caches query results
type ResultCache struct {
	cache map[string]*CachedResult
	mu    sync.RWMutex
}

// CachedResult represents a cached query result
type CachedResult struct {
	QueryID   string
	Result    interface{}
	CachedAt  time.Time
	ExpiresAt time.Time
	Size      int64
}

// DefaultAsyncConfig returns default async configuration
func DefaultAsyncConfig() AsyncConfig {
	return AsyncConfig{
		MaxConcurrentQueries: 10,
		QueryTimeout:         30 * time.Minute,
		PollInterval:         2 * time.Second,
		ResultCacheSize:      100,
		ResultCacheTTL:       10 * time.Minute,
		EnableQueryTags:      true,
		DefaultWarehouse:     "COMPUTE_WH",
	}
}

// NewAsyncQueryExecutor creates a new async query executor
func NewAsyncQueryExecutor(service *Service, connManager *ConnectionManager, config AsyncConfig) *AsyncQueryExecutor {
	return &AsyncQueryExecutor{
		service:      service,
		connManager:  connManager,
		queryTracker: NewQueryTracker(),
		resultCache:  NewResultCache(config.ResultCacheSize),
		config:       config,
	}
}

// SubmitQuery submits a query for asynchronous execution
func (aqe *AsyncQueryExecutor) SubmitQuery(ctx context.Context, sql string, options ...QueryOption) (*AsyncQuery, error) {
	// Apply query options
	opts := &queryOptions{
		warehouse: aqe.config.DefaultWarehouse,
		queryTag:  fmt.Sprintf("async_%d", time.Now().UnixNano()),
	}
	for _, opt := range options {
		opt(opts)
	}

	// Create async query
	queryCtx, cancel := context.WithTimeout(ctx, aqe.config.QueryTimeout)
	query := &AsyncQuery{
		ID:          fmt.Sprintf("aq_%d", time.Now().UnixNano()),
		SQL:         sql,
		Status:      QueryStatusPending,
		SubmittedAt: time.Now(),
		Warehouse:   opts.warehouse,
		QueryTag:    opts.queryTag,
		Context:     queryCtx,
		resultChan:  make(chan *AsyncQueryResult, 1),
		cancelFunc:  cancel,
	}

	// Track query
	if err := aqe.queryTracker.Add(query); err != nil {
		cancel()
		return nil, err
	}

	// Submit for execution
	go aqe.executeAsync(query)

	return query, nil
}

// executeAsync executes a query asynchronously
func (aqe *AsyncQueryExecutor) executeAsync(query *AsyncQuery) {
	defer close(query.resultChan)
	defer query.cancelFunc()

	// Update status
	aqe.queryTracker.UpdateStatus(query.ID, QueryStatusRunning)
	query.StartedAt = time.Now()

	// Get connection
	conn, err := aqe.connManager.GetConnection(query.Context, query.Warehouse)
	if err != nil {
		aqe.handleQueryError(query, err)
		return
	}
	defer aqe.connManager.ReleaseConnection(conn)

	// Set query tag if enabled
	if aqe.config.EnableQueryTags {
		if err := conn.SetSessionParam("QUERY_TAG", query.QueryTag); err != nil {
			// Log but don't fail
		}
	}

	// Execute query asynchronously using Snowflake's async execution
	queryID, err := aqe.submitToSnowflake(conn, query.SQL)
	if err != nil {
		aqe.handleQueryError(query, err)
		return
	}

	query.QueryID = queryID

	// Poll for results
	result, err := aqe.pollForResults(conn, queryID, query.Context)
	if err != nil {
		aqe.handleQueryError(query, err)
		return
	}

	// Update query with results
	query.Status = QueryStatusSucceeded
	query.CompletedAt = time.Now()
	query.RowCount = result.RowCount

	// Send result
	query.resultChan <- result

	// Cache result if applicable
	if aqe.shouldCacheResult(query, result) {
		aqe.cacheResult(query, result)
	}

	// Update tracker
	aqe.queryTracker.UpdateStatus(query.ID, QueryStatusSucceeded)
}

// submitToSnowflake submits query to Snowflake for async execution
func (aqe *AsyncQueryExecutor) submitToSnowflake(conn *ManagedConnection, sql string) (string, error) {
	// Use Snowflake's async query submission
	// First, get the query ID that will be used
	row := conn.db.QueryRow("SELECT SYSTEM$GENERATE_QUERY_ID()")
	var queryID string
	if err := row.Scan(&queryID); err != nil {
		return "", errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to generate query ID")
	}

	// Submit query with ASYNC flag
	asyncSQL := fmt.Sprintf("/* queryid:%s */ %s", queryID, sql)
	
	// Execute query asynchronously
	_, err := conn.db.Exec(asyncSQL)
	if err != nil {
		return "", errors.SQLError("Failed to submit async query", sql, err)
	}

	return queryID, nil
}

// pollForResults polls Snowflake for query results
func (aqe *AsyncQueryExecutor) pollForResults(conn *ManagedConnection, queryID string, ctx context.Context) (*AsyncQueryResult, error) {
	ticker := time.NewTicker(aqe.config.PollInterval)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New(errors.ErrCodeTimeout, "Query cancelled or timed out")
		
		case <-ticker.C:
			// Check query status
			status, metadata, err := aqe.checkQueryStatus(conn, queryID)
			if err != nil {
				return nil, err
			}

			switch status {
			case "SUCCESS":
				// Get results
				rows, err := aqe.getQueryResults(conn, queryID)
				if err != nil {
					return nil, err
				}

				return &AsyncQueryResult{
					QueryID:       queryID,
					Status:        QueryStatusSucceeded,
					Rows:          rows,
					ExecutionTime: time.Since(startTime),
					Metadata:      metadata,
				}, nil

			case "FAILED", "INCIDENT":
				return nil, errors.New(errors.ErrCodeSQLExecution, "Query failed").
					WithContext("query_id", queryID).
					WithContext("status", status)

			case "RUNNING", "QUEUED", "BLOCKED":
				// Continue polling
				continue

			default:
				// Unknown status
				return nil, errors.New(errors.ErrCodeUnknown, "Unknown query status").
					WithContext("status", status)
			}
		}
	}
}

// checkQueryStatus checks the status of a query
func (aqe *AsyncQueryExecutor) checkQueryStatus(conn *ManagedConnection, queryID string) (string, QueryMetadata, error) {
	// Query ID is from Snowflake system, not user input
	statusSQL := fmt.Sprintf(`
		SELECT 
			execution_status,
			bytes_scanned,
			partitions_scanned,
			credits_used_cloud_services,
			compilation_time,
			execution_time,
			queued_provisioning_time + queued_repair_time + queued_overload_time
		FROM table(information_schema.query_history())
		WHERE query_id = '%s'
		ORDER BY start_time DESC
		LIMIT 1
	`, queryID) // #nosec G201 - queryID from Snowflake system

	var status string
	var metadata QueryMetadata
	var compilationMs, executionMs, queueMs int64

	row := conn.db.QueryRow(statusSQL)
	err := row.Scan(
		&status,
		&metadata.BytesScanned,
		&metadata.PartitionsScanned,
		&metadata.CreditsUsed,
		&compilationMs,
		&executionMs,
		&queueMs,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// Query might not be in history yet
			return "RUNNING", metadata, nil
		}
		return "", metadata, errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to check query status")
	}

	metadata.CompilationTime = time.Duration(compilationMs) * time.Millisecond
	metadata.ExecutionTime = time.Duration(executionMs) * time.Millisecond
	metadata.QueueTime = time.Duration(queueMs) * time.Millisecond

	return status, metadata, nil
}

// getQueryResults retrieves results for a completed query
func (aqe *AsyncQueryExecutor) getQueryResults(conn *ManagedConnection, queryID string) (*sql.Rows, error) {
	// Use RESULT_SCAN to get results of a previous query
	// Query ID is from Snowflake system, not user input
	resultSQL := fmt.Sprintf("SELECT * FROM TABLE(RESULT_SCAN('%s'))", queryID) // #nosec G201 - queryID from Snowflake system
	
	rows, err := conn.db.Query(resultSQL)
	if err != nil {
		return nil, errors.SQLError("Failed to retrieve query results", resultSQL, err)
	}

	return rows, nil
}

// Wait waits for query completion and returns results
func (aqe *AsyncQueryExecutor) Wait(query *AsyncQuery) (*AsyncQueryResult, error) {
	select {
	case result := <-query.resultChan:
		if result == nil {
			return nil, errors.New(errors.ErrCodeNoResults, "No results available")
		}
		return result, nil
	case <-query.Context.Done():
		return nil, query.Context.Err()
	}
}

// GetStatus returns the current status of a query
func (aqe *AsyncQueryExecutor) GetStatus(queryID string) (QueryStatus, error) {
	return aqe.queryTracker.GetStatus(queryID)
}

// Cancel cancels an async query
func (aqe *AsyncQueryExecutor) Cancel(queryID string) error {
	query, err := aqe.queryTracker.Get(queryID)
	if err != nil {
		return err
	}

	// Cancel context
	query.cancelFunc()

	// Cancel in Snowflake
	if query.QueryID != "" {
		conn, err := aqe.connManager.GetConnection(context.Background(), query.Warehouse)
		if err == nil {
			defer aqe.connManager.ReleaseConnection(conn)
			
			// Query ID is from Snowflake system, not user input
			cancelSQL := fmt.Sprintf("SELECT SYSTEM$CANCEL_QUERY('%s')", query.QueryID) // #nosec G201 - queryID from Snowflake system
			_, _ = conn.db.Exec(cancelSQL)
		}
	}

	// Update status
	aqe.queryTracker.UpdateStatus(queryID, QueryStatusCancelled)

	return nil
}

// handleQueryError handles query execution errors
func (aqe *AsyncQueryExecutor) handleQueryError(query *AsyncQuery, err error) {
	query.Status = QueryStatusFailed
	query.Error = err
	query.CompletedAt = time.Now()

	aqe.queryTracker.UpdateStatus(query.ID, QueryStatusFailed)

	query.resultChan <- &AsyncQueryResult{
		QueryID:       query.QueryID,
		Status:        QueryStatusFailed,
		Error:         err,
		ExecutionTime: time.Since(query.StartedAt),
	}
}

// shouldCacheResult determines if a result should be cached
func (aqe *AsyncQueryExecutor) shouldCacheResult(query *AsyncQuery, result *AsyncQueryResult) bool {
	// Don't cache failed queries or large results
	if result.Status != QueryStatusSucceeded || result.RowCount > 10000 {
		return false
	}

	// Check if query is cacheable (no temporary tables, etc.)
	sql := strings.ToLower(query.SQL)
	if strings.Contains(sql, "temp") || strings.Contains(sql, "volatile") {
		return false
	}

	return true
}

// cacheResult caches query results
func (aqe *AsyncQueryExecutor) cacheResult(query *AsyncQuery, result *AsyncQueryResult) {
	aqe.resultCache.Set(query.SQL, &CachedResult{
		QueryID:   query.QueryID,
		Result:    result,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(aqe.config.ResultCacheTTL),
	})
}

// QueryOption configures query execution
type QueryOption func(*queryOptions)

type queryOptions struct {
	warehouse string
	queryTag  string
	priority  int
}

// WithWarehouse sets the warehouse for query execution
func WithWarehouse(warehouse string) QueryOption {
	return func(o *queryOptions) {
		o.warehouse = warehouse
	}
}

// WithQueryTag sets a query tag
func WithQueryTag(tag string) QueryOption {
	return func(o *queryOptions) {
		o.queryTag = tag
	}
}

// QueryTracker methods

func NewQueryTracker() *QueryTracker {
	return &QueryTracker{
		queries: make(map[string]*AsyncQuery),
	}
}

func (qt *QueryTracker) Add(query *AsyncQuery) error {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if _, exists := qt.queries[query.ID]; exists {
		return errors.New(errors.ErrCodeDuplicateEntry, "Query ID already exists")
	}

	qt.queries[query.ID] = query
	return nil
}

func (qt *QueryTracker) Get(queryID string) (*AsyncQuery, error) {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	query, exists := qt.queries[queryID]
	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "Query not found")
	}

	return query, nil
}

func (qt *QueryTracker) GetStatus(queryID string) (QueryStatus, error) {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	query, exists := qt.queries[queryID]
	if !exists {
		return QueryStatusFailed, errors.New(errors.ErrCodeNotFound, "Query not found")
	}

	return query.Status, nil
}

func (qt *QueryTracker) UpdateStatus(queryID string, status QueryStatus) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if query, exists := qt.queries[queryID]; exists {
		query.Status = status
	}
}

func (qt *QueryTracker) GetActiveQueries() []*AsyncQuery {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	var active []*AsyncQuery
	for _, query := range qt.queries {
		if query.Status == QueryStatusRunning || query.Status == QueryStatusPending {
			active = append(active, query)
		}
	}

	return active
}

// ResultCache methods

func NewResultCache(maxSize int) *ResultCache {
	return &ResultCache{
		cache: make(map[string]*CachedResult, maxSize),
	}
}

func (rc *ResultCache) Get(key string) (*CachedResult, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	result, exists := rc.cache[key]
	if !exists || time.Now().After(result.ExpiresAt) {
		return nil, false
	}

	return result, true
}

func (rc *ResultCache) Set(key string, result *CachedResult) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cache[key] = result
}

// String returns string representation of query status
func (qs QueryStatus) String() string {
	switch qs {
	case QueryStatusPending:
		return "PENDING"
	case QueryStatusRunning:
		return "RUNNING"
	case QueryStatusSucceeded:
		return "SUCCEEDED"
	case QueryStatusFailed:
		return "FAILED"
	case QueryStatusCancelled:
		return "CANCELLED"
	case QueryStatusTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

// BatchAsyncExecutor handles batch async query execution
type BatchAsyncExecutor struct {
	executor  *AsyncQueryExecutor
	semaphore chan struct{}
}

// NewBatchAsyncExecutor creates a new batch executor
func NewBatchAsyncExecutor(executor *AsyncQueryExecutor, maxConcurrent int) *BatchAsyncExecutor {
	return &BatchAsyncExecutor{
		executor:  executor,
		semaphore: make(chan struct{}, maxConcurrent),
	}
}

// ExecuteBatch executes multiple queries asynchronously
func (bae *BatchAsyncExecutor) ExecuteBatch(ctx context.Context, queries []string) ([]*AsyncQuery, error) {
	var wg sync.WaitGroup
	results := make([]*AsyncQuery, len(queries))
	errors := make([]error, len(queries))

	for i, sql := range queries {
		wg.Add(1)
		go func(index int, query string) {
			defer wg.Done()

			// Acquire semaphore
			bae.semaphore <- struct{}{}
			defer func() { <-bae.semaphore }()

			// Submit query
			asyncQuery, err := bae.executor.SubmitQuery(ctx, query)
			if err != nil {
				errors[index] = err
			} else {
				results[index] = asyncQuery
			}
		}(i, sql)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return results, fmt.Errorf("batch execution had errors: %v", errors)
		}
	}

	return results, nil
}

// WaitForBatch waits for all queries in a batch to complete
func (bae *BatchAsyncExecutor) WaitForBatch(queries []*AsyncQuery) ([]*AsyncQueryResult, error) {
	results := make([]*AsyncQueryResult, len(queries))
	var wg sync.WaitGroup

	for i, query := range queries {
		if query == nil {
			continue
		}

		wg.Add(1)
		go func(index int, q *AsyncQuery) {
			defer wg.Done()
			
			result, err := bae.executor.Wait(q)
			if err != nil {
				results[index] = &AsyncQueryResult{
					QueryID: q.ID,
					Status:  QueryStatusFailed,
					Error:   err,
				}
			} else {
				results[index] = result
			}
		}(i, query)
	}

	wg.Wait()
	return results, nil
}