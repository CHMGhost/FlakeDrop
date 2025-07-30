package snowflake

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// ConnectionManager provides advanced connection management for Snowflake
type ConnectionManager struct {
	config          Config
	pools           map[string]*ConnectionPool // Per-warehouse pools
	sessionManager  *SessionManager
	warehouseOptimizer *WarehouseOptimizer
	mu              sync.RWMutex
	closed          bool
}

// ConnectionPool represents a pool of connections for a specific warehouse
type ConnectionPool struct {
	warehouse    string
	connections  chan *ManagedConnection
	size         int
	maxSize      int
	minSize      int
	idleTimeout  time.Duration
	metrics      *PoolMetrics
	mu           sync.RWMutex
}

// ManagedConnection represents a managed Snowflake connection
type ManagedConnection struct {
	db              *sql.DB
	id              string
	warehouse       string
	sessionParams   map[string]string
	createdAt       time.Time
	lastUsedAt      time.Time
	useCount        int64
	pool            *ConnectionPool
}

// SessionManager manages session parameters and optimization
type SessionManager struct {
	defaultParams map[string]string
	mu            sync.RWMutex
}

// WarehouseOptimizer provides warehouse sizing recommendations
type WarehouseOptimizer struct {
	metrics      *MetricsCollector
	history      []WarehouseMetric
	mu           sync.RWMutex
}

// WarehouseMetric tracks warehouse performance metrics
type WarehouseMetric struct {
	Warehouse        string
	Size             string
	QueriesExecuted  int64
	AvgExecutionTime time.Duration
	AvgQueueTime     time.Duration
	CreditsUsed      float64
	Timestamp        time.Time
}

// PoolMetrics tracks connection pool statistics
type PoolMetrics struct {
	TotalConnections   int64
	ActiveConnections  int64
	IdleConnections    int64
	ConnectionRequests int64
	ConnectionFailures int64
	AvgWaitTime        time.Duration
	mu                 sync.RWMutex
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(config Config) *ConnectionManager {
	return &ConnectionManager{
		config:             config,
		pools:              make(map[string]*ConnectionPool),
		sessionManager:     NewSessionManager(),
		warehouseOptimizer: NewWarehouseOptimizer(),
	}
}

// GetConnection gets an optimized connection for the specified warehouse
func (cm *ConnectionManager) GetConnection(ctx context.Context, warehouse string) (*ManagedConnection, error) {
	cm.mu.RLock()
	if cm.closed {
		cm.mu.RUnlock()
		return nil, errors.New(errors.ErrCodeConnectionFailed, "Connection manager is closed")
	}
	cm.mu.RUnlock()

	// Use default warehouse if not specified
	if warehouse == "" {
		warehouse = cm.config.Warehouse
	}

	// Get or create pool for warehouse
	pool := cm.getOrCreatePool(warehouse)

	// Try to get connection with exponential backoff
	var conn *ManagedConnection
	err := cm.retryWithBackoff(ctx, func() error {
		var err error
		conn, err = pool.Get(ctx)
		return err
	})

	if err != nil {
		return nil, err
	}

	// Apply session optimizations
	if err := cm.sessionManager.OptimizeSession(conn); err != nil {
		pool.Put(conn)
		return nil, errors.Wrap(err, errors.ErrCodeConfiguration, "Failed to optimize session")
	}

	return conn, nil
}

// ReleaseConnection returns a connection to the pool
func (cm *ConnectionManager) ReleaseConnection(conn *ManagedConnection) {
	if conn == nil {
		return
	}

	conn.pool.Put(conn)
}

// getOrCreatePool gets or creates a connection pool for a warehouse
func (cm *ConnectionManager) getOrCreatePool(warehouse string) *ConnectionPool {
	cm.mu.RLock()
	pool, exists := cm.pools[warehouse]
	cm.mu.RUnlock()

	if exists {
		return pool
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, exists := cm.pools[warehouse]; exists {
		return pool
	}

	// Create new pool
	pool = &ConnectionPool{
		warehouse:    warehouse,
		connections:  make(chan *ManagedConnection, 20),
		minSize:      2,
		maxSize:      20,
		idleTimeout:  10 * time.Minute,
		metrics:      &PoolMetrics{},
	}

	// Initialize minimum connections
	for i := 0; i < pool.minSize; i++ {
		conn, err := cm.createConnection(warehouse, fmt.Sprintf("%s-%d", warehouse, i))
		if err == nil {
			pool.connections <- conn
			pool.size++
		}
	}

	cm.pools[warehouse] = pool

	// Start pool maintenance
	go pool.maintain()

	return pool
}

// createConnection creates a new Snowflake connection
func (cm *ConnectionManager) createConnection(warehouse, id string) (*ManagedConnection, error) {
	config := cm.config
	config.Warehouse = warehouse

	// Build connection string with optimizations
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s&query_timeout=300&client_session_keep_alive=true",
		config.Username,
		config.Password,
		config.Account,
		config.Database,
		config.Schema,
		warehouse,
		config.Role,
	)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, errors.ConnectionError("Failed to create connection", err).
			WithContext("warehouse", warehouse)
	}

	// Configure connection pool settings
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // Managed by our pool

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, errors.ConnectionError("Failed to ping connection", err)
	}

	return &ManagedConnection{
		db:            db,
		id:            id,
		warehouse:     warehouse,
		sessionParams: make(map[string]string),
		createdAt:     time.Now(),
		lastUsedAt:    time.Now(),
		pool:          nil, // Set by pool when added
	}, nil
}

// retryWithBackoff retries an operation with exponential backoff and jitter
func (cm *ConnectionManager) retryWithBackoff(ctx context.Context, operation func() error) error {
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		err := operation()
		if err == nil {
			return nil
		}

		// Check if error is retryable
		if !isRetryableError(err) {
			return err
		}

		if i == maxRetries-1 {
			return err
		}

		// Calculate next backoff with jitter using crypto/rand
		var b [8]byte
		_, _ = cryptorand.Read(b[:])
		jitter := float64(binary.LittleEndian.Uint64(b[:])) / float64(^uint64(0)) * 0.1
		nextBackoff := time.Duration(float64(backoff) * (1 + jitter))
		if nextBackoff > maxBackoff {
			nextBackoff = maxBackoff
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextBackoff):
			backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
		}
	}

	return errors.New(errors.ErrCodeMaxRetriesExceeded, "Maximum retries exceeded")
}

// ExecuteWithOptimalWarehouse executes a query on the most suitable warehouse
func (cm *ConnectionManager) ExecuteWithOptimalWarehouse(ctx context.Context, sql string, estimatedSize string) (*sql.Rows, error) {
	// Get warehouse recommendation
	warehouse := cm.warehouseOptimizer.RecommendWarehouse(sql, estimatedSize)

	// Get connection for recommended warehouse
	conn, err := cm.GetConnection(ctx, warehouse)
	if err != nil {
		return nil, err
	}
	defer cm.ReleaseConnection(conn)

	// Execute query
	return conn.Query(ctx, sql)
}

// Close closes all connection pools
func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return nil
	}

	cm.closed = true

	var errs []error
	for _, pool := range cm.pools {
		if err := pool.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pools: %v", errs)
	}

	return nil
}

// ConnectionPool methods

// Get retrieves a connection from the pool
func (cp *ConnectionPool) Get(ctx context.Context) (*ManagedConnection, error) {
	cp.metrics.incrementRequests()

	select {
	case conn := <-cp.connections:
		cp.metrics.decrementIdle()
		cp.metrics.incrementActive()
		
		// Check connection health
		if err := conn.Ping(ctx); err != nil {
			conn.Close()
			cp.size--
			return cp.Get(ctx) // Retry with another connection
		}

		conn.lastUsedAt = time.Now()
		conn.useCount++
		return conn, nil

	case <-ctx.Done():
		cp.metrics.incrementFailures()
		return nil, ctx.Err()

	case <-time.After(30 * time.Second):
		cp.metrics.incrementFailures()
		return nil, errors.New(errors.ErrCodeTimeout, "Timeout waiting for connection")
	}
}

// Put returns a connection to the pool
func (cp *ConnectionPool) Put(conn *ManagedConnection) {
	if conn == nil {
		return
	}

	cp.metrics.decrementActive()
	cp.metrics.incrementIdle()

	select {
	case cp.connections <- conn:
		// Successfully returned
	default:
		// Pool is full, close connection
		conn.Close()
		cp.size--
	}
}

// Close closes all connections in the pool
func (cp *ConnectionPool) Close() error {
	close(cp.connections)
	
	var errs []error
	for conn := range cp.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}

	return nil
}

// maintain performs pool maintenance
func (cp *ConnectionPool) maintain() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cp.mu.RLock()
		if cp.size > cp.minSize {
			cp.mu.RUnlock()
			
			// Check for idle connections to remove
			select {
			case conn := <-cp.connections:
				if time.Since(conn.lastUsedAt) > cp.idleTimeout {
					conn.Close()
					cp.mu.Lock()
					cp.size--
					cp.mu.Unlock()
				} else {
					// Return to pool
					cp.connections <- conn
				}
			default:
				// No idle connections
			}
		} else {
			cp.mu.RUnlock()
		}
	}
}

// ManagedConnection methods

// Query executes a query
func (mc *ManagedConnection) Query(ctx context.Context, sql string) (*sql.Rows, error) {
	return mc.db.QueryContext(ctx, sql)
}

// Execute executes a statement
func (mc *ManagedConnection) Execute(ctx context.Context, sql string) (sql.Result, error) {
	return mc.db.ExecContext(ctx, sql)
}

// Ping tests the connection
func (mc *ManagedConnection) Ping(ctx context.Context) error {
	return mc.db.PingContext(ctx)
}

// Close closes the connection
func (mc *ManagedConnection) Close() error {
	return mc.db.Close()
}

// SetSessionParam sets a session parameter
func (mc *ManagedConnection) SetSessionParam(param, value string) error {
	sql := fmt.Sprintf("ALTER SESSION SET %s = '%s'", param, value)
	_, err := mc.db.Exec(sql)
	if err == nil {
		mc.sessionParams[param] = value
	}
	return err
}

// SessionManager methods

func NewSessionManager() *SessionManager {
	return &SessionManager{
		defaultParams: map[string]string{
			"QUERY_TAG":                      "flakedrop",
			"STATEMENT_TIMEOUT_IN_SECONDS":   "300",
			"USE_CACHED_RESULT":              "TRUE",
			"ROWS_PER_RESULTSET":             "100000",
			"AUTOCOMMIT":                     "TRUE",
			"BINARY_INPUT_FORMAT":            "BASE64",
			"BINARY_OUTPUT_FORMAT":           "BASE64",
			"DATE_INPUT_FORMAT":              "AUTO",
			"DATE_OUTPUT_FORMAT":             "YYYY-MM-DD",
			"TIME_INPUT_FORMAT":              "AUTO",
			"TIME_OUTPUT_FORMAT":             "HH24:MI:SS",
			"TIMESTAMP_INPUT_FORMAT":         "AUTO",
			"TIMESTAMP_OUTPUT_FORMAT":        "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
			"TRANSACTION_DEFAULT_ISOLATION_LEVEL": "READ COMMITTED",
		},
	}
}

// OptimizeSession applies optimal session parameters
func (sm *SessionManager) OptimizeSession(conn *ManagedConnection) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for param, value := range sm.defaultParams {
		if err := conn.SetSessionParam(param, value); err != nil {
			// Log but don't fail on non-critical params
			continue
		}
	}

	return nil
}

// WarehouseOptimizer methods

func NewWarehouseOptimizer() *WarehouseOptimizer {
	return &WarehouseOptimizer{
		metrics: NewMetricsCollector(),
		history: make([]WarehouseMetric, 0),
	}
}

// RecommendWarehouse recommends the optimal warehouse for a query
func (wo *WarehouseOptimizer) RecommendWarehouse(sql, estimatedSize string) string {
	// Simple logic - can be enhanced with ML models
	sqlLower := strings.ToLower(sql)
	
	// Large analytical queries
	if strings.Contains(sqlLower, "group by") || strings.Contains(sqlLower, "order by") {
		if estimatedSize == "large" {
			return "COMPUTE_WH_LARGE"
		}
		return "COMPUTE_WH_MEDIUM"
	}
	
	// Simple queries
	if estimatedSize == "small" || !strings.Contains(sqlLower, "join") {
		return "COMPUTE_WH_SMALL"
	}
	
	// Default
	return "COMPUTE_WH_MEDIUM"
}

// RecordMetric records warehouse performance metrics
func (wo *WarehouseOptimizer) RecordMetric(metric WarehouseMetric) {
	wo.mu.Lock()
	defer wo.mu.Unlock()
	
	wo.history = append(wo.history, metric)
	
	// Keep only last 1000 metrics
	if len(wo.history) > 1000 {
		wo.history = wo.history[len(wo.history)-1000:]
	}
}

// GetRecommendations provides warehouse sizing recommendations
func (wo *WarehouseOptimizer) GetRecommendations() map[string]string {
	wo.mu.RLock()
	defer wo.mu.RUnlock()
	
	recommendations := make(map[string]string)
	
	// Analyze metrics by warehouse
	warehouseStats := make(map[string]*WarehouseStats)
	
	for _, metric := range wo.history {
		stats, exists := warehouseStats[metric.Warehouse]
		if !exists {
			stats = &WarehouseStats{}
			warehouseStats[metric.Warehouse] = stats
		}
		
		stats.TotalQueries++
		stats.TotalExecutionTime += metric.AvgExecutionTime
		stats.TotalQueueTime += metric.AvgQueueTime
		stats.TotalCredits += metric.CreditsUsed
	}
	
	// Generate recommendations
	for warehouse, stats := range warehouseStats {
		avgExecTime := stats.TotalExecutionTime / time.Duration(stats.TotalQueries)
		avgQueueTime := stats.TotalQueueTime / time.Duration(stats.TotalQueries)
		
		if avgQueueTime > 5*time.Second {
			recommendations[warehouse] = "Consider scaling up - high queue times detected"
		} else if avgExecTime < 1*time.Second && stats.TotalCredits > 100 {
			recommendations[warehouse] = "Consider scaling down - queries completing quickly with high credit usage"
		}
	}
	
	return recommendations
}

// Helper types and functions

type WarehouseStats struct {
	TotalQueries       int64
	TotalExecutionTime time.Duration
	TotalQueueTime     time.Duration
	TotalCredits       float64
}

// MetricsCollector collects connection and query metrics
type MetricsCollector struct {
	metrics map[string]interface{}
	mu      sync.RWMutex
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]interface{}),
	}
}

// PoolMetrics methods

func (pm *PoolMetrics) incrementRequests() {
	pm.mu.Lock()
	pm.ConnectionRequests++
	pm.mu.Unlock()
}

func (pm *PoolMetrics) incrementFailures() {
	pm.mu.Lock()
	pm.ConnectionFailures++
	pm.mu.Unlock()
}

func (pm *PoolMetrics) incrementActive() {
	pm.mu.Lock()
	pm.ActiveConnections++
	pm.mu.Unlock()
}

func (pm *PoolMetrics) decrementActive() {
	pm.mu.Lock()
	pm.ActiveConnections--
	pm.mu.Unlock()
}

func (pm *PoolMetrics) incrementIdle() {
	pm.mu.Lock()
	pm.IdleConnections++
	pm.mu.Unlock()
}

func (pm *PoolMetrics) decrementIdle() {
	pm.mu.Lock()
	pm.IdleConnections--
	pm.mu.Unlock()
}

// isRetryableError determines if an error should be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	retryablePatterns := []string{
		"connection reset",
		"connection refused", 
		"timeout",
		"temporary failure",
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