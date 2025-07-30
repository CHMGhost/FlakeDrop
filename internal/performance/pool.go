package performance

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// ConnectionPool manages a pool of Snowflake connections for optimal performance
type ConnectionPool struct {
	config       snowflake.Config
	connections  chan *PooledConnection
	mu           sync.RWMutex
	size         int
	maxSize      int
	metrics      *PoolMetrics
	closed       bool
	waitTimeout  time.Duration
	idleTimeout  time.Duration
	healthCheck  time.Duration
}

// PooledConnection wraps a database connection with metadata
type PooledConnection struct {
	db         *sql.DB
	id         int
	createdAt  time.Time
	lastUsedAt time.Time
	useCount   int64
	pool       *ConnectionPool
}

// PoolMetrics tracks connection pool performance
type PoolMetrics struct {
	mu              sync.RWMutex
	totalConns      int64
	activeConns     int64
	idleConns       int64
	waitTime        time.Duration
	waitCount       int64
	timeouts        int64
	errors          int64
	totalRequests   int64
	successRequests int64
}

// TotalConns returns the total number of connections
func (m *PoolMetrics) TotalConns() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalConns
}

// ActiveConns returns the number of active connections
func (m *PoolMetrics) ActiveConns() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeConns
}

// PoolConfig contains configuration for the connection pool
type PoolConfig struct {
	MinSize      int
	MaxSize      int
	WaitTimeout  time.Duration
	IdleTimeout  time.Duration
	HealthCheck  time.Duration
}

// DefaultPoolConfig returns sensible defaults for connection pooling
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinSize:     5,
		MaxSize:     20,
		WaitTimeout: 30 * time.Second,
		IdleTimeout: 10 * time.Minute,
		HealthCheck: 1 * time.Minute,
	}
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(sfConfig snowflake.Config, poolConfig PoolConfig) (*ConnectionPool, error) {
	if poolConfig.MinSize <= 0 {
		poolConfig.MinSize = 5
	}
	if poolConfig.MaxSize < poolConfig.MinSize {
		poolConfig.MaxSize = poolConfig.MinSize * 2
	}

	pool := &ConnectionPool{
		config:      sfConfig,
		connections: make(chan *PooledConnection, poolConfig.MaxSize),
		size:        0,
		maxSize:     poolConfig.MaxSize,
		metrics:     &PoolMetrics{},
		waitTimeout: poolConfig.WaitTimeout,
		idleTimeout: poolConfig.IdleTimeout,
		healthCheck: poolConfig.HealthCheck,
	}

	// Initialize minimum connections
	for i := 0; i < poolConfig.MinSize; i++ {
		conn, err := pool.createConnection(i)
		if err != nil {
			// Clean up any created connections
			pool.Close()
			return nil, fmt.Errorf("failed to create initial connection %d: %w", i, err)
		}
		pool.connections <- conn
		pool.size++
		pool.metrics.incrementTotalConns()
		pool.metrics.incrementIdleConns()
	}

	// Start health check routine
	go pool.healthCheckRoutine()

	// Start idle connection cleanup routine
	go pool.idleCleanupRoutine()

	return pool, nil
}

// Get retrieves a connection from the pool
func (pool *ConnectionPool) Get(ctx context.Context) (*PooledConnection, error) {
	pool.mu.RLock()
	if pool.closed {
		pool.mu.RUnlock()
		return nil, errors.New(errors.ErrCodeConnectionFailed, "Connection pool is closed")
	}
	pool.mu.RUnlock()

	pool.metrics.incrementTotalRequests()
	startWait := time.Now()

	select {
	case conn := <-pool.connections:
		pool.metrics.recordWaitTime(time.Since(startWait))
		pool.metrics.decrementIdleConns()
		pool.metrics.incrementActiveConns()
		
		// Check connection health
		if err := conn.ping(ctx); err != nil {
			pool.metrics.incrementErrors()
			// Try to create a new connection
			newConn, err := pool.createConnection(conn.id)
			if err != nil {
				return nil, err
			}
			conn.close()
			conn = newConn
		}
		
		conn.lastUsedAt = time.Now()
		conn.useCount++
		pool.metrics.incrementSuccessRequests()
		return conn, nil

	case <-time.After(pool.waitTimeout):
		pool.metrics.incrementTimeouts()
		return nil, errors.New(errors.ErrCodeTimeout, "Timeout waiting for connection from pool").
			WithContext("wait_time", pool.waitTimeout).
			WithSuggestions(
				"Increase pool size",
				"Reduce concurrent operations",
				"Check for connection leaks",
			)

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put returns a connection to the pool
func (pool *ConnectionPool) Put(conn *PooledConnection) {
	if conn == nil {
		return
	}

	pool.mu.RLock()
	if pool.closed {
		pool.mu.RUnlock()
		conn.close()
		return
	}
	pool.mu.RUnlock()

	pool.metrics.decrementActiveConns()
	pool.metrics.incrementIdleConns()

	// Return to pool
	select {
	case pool.connections <- conn:
		// Successfully returned
	default:
		// Pool is full, close the connection
		conn.close()
		pool.mu.Lock()
		pool.size--
		pool.mu.Unlock()
		pool.metrics.decrementTotalConns()
		pool.metrics.decrementIdleConns()
	}
}

// Close closes all connections in the pool
func (pool *ConnectionPool) Close() error {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}
	pool.closed = true
	pool.mu.Unlock()

	// Close all connections
	close(pool.connections)
	for conn := range pool.connections {
		conn.close()
	}

	return nil
}

// GetMetrics returns current pool metrics
func (pool *ConnectionPool) GetMetrics() PoolMetrics {
	pool.metrics.mu.RLock()
	defer pool.metrics.mu.RUnlock()
	return *pool.metrics
}

// createConnection creates a new database connection
func (pool *ConnectionPool) createConnection(id int) (*PooledConnection, error) {
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		pool.config.Username,
		pool.config.Password,
		pool.config.Account,
		pool.config.Database,
		pool.config.Schema,
		pool.config.Warehouse,
		pool.config.Role,
	)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, errors.ConnectionError("Failed to create connection", err).
			WithContext("account", pool.config.Account)
	}

	// Configure connection
	db.SetMaxOpenConns(1) // Each pooled connection is single
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // We manage lifetime

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, errors.ConnectionError("Failed to ping connection", err)
	}

	return &PooledConnection{
		db:         db,
		id:         id,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		pool:       pool,
	}, nil
}

// healthCheckRoutine periodically checks connection health
func (pool *ConnectionPool) healthCheckRoutine() {
	ticker := time.NewTicker(pool.healthCheck)
	defer ticker.Stop()

	for range ticker.C {
		pool.mu.RLock()
		if pool.closed {
			pool.mu.RUnlock()
			return
		}
		pool.mu.RUnlock()

		// Check a sample of connections
		select {
		case conn := <-pool.connections:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := conn.ping(ctx); err != nil {
				// Connection is bad, create a new one
				if newConn, err := pool.createConnection(conn.id); err == nil {
					conn.close()
					pool.connections <- newConn
				} else {
					// Failed to create new connection, reduce pool size
					pool.mu.Lock()
					pool.size--
					pool.mu.Unlock()
					pool.metrics.decrementTotalConns()
				}
			} else {
				// Connection is healthy, return it
				pool.connections <- conn
			}
			cancel()
		default:
			// No idle connections to check
		}
	}
}

// idleCleanupRoutine removes idle connections
func (pool *ConnectionPool) idleCleanupRoutine() {
	ticker := time.NewTicker(pool.idleTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		pool.mu.RLock()
		if pool.closed {
			pool.mu.RUnlock()
			return
		}
		currentSize := pool.size
		pool.mu.RUnlock()

		// Only cleanup if we have more than minimum connections
		if currentSize > 5 {
			select {
			case conn := <-pool.connections:
				if time.Since(conn.lastUsedAt) > pool.idleTimeout {
					// Close idle connection
					conn.close()
					pool.mu.Lock()
					pool.size--
					pool.mu.Unlock()
					pool.metrics.decrementTotalConns()
					pool.metrics.decrementIdleConns()
				} else {
					// Return to pool
					pool.connections <- conn
				}
			default:
				// No idle connections
			}
		}
	}
}

// PooledConnection methods

func (pc *PooledConnection) ping(ctx context.Context) error {
	return pc.db.PingContext(ctx)
}

func (pc *PooledConnection) close() error {
	return pc.db.Close()
}

// Execute runs a query on the pooled connection
func (pc *PooledConnection) Execute(ctx context.Context, query string) (sql.Result, error) {
	return pc.db.ExecContext(ctx, query)
}

// Query runs a query that returns rows
func (pc *PooledConnection) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return pc.db.QueryContext(ctx, query)
}

// BeginTx starts a transaction
func (pc *PooledConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return pc.db.BeginTx(ctx, opts)
}

// Metrics methods

func (m *PoolMetrics) incrementTotalConns() {
	m.mu.Lock()
	m.totalConns++
	m.mu.Unlock()
}

func (m *PoolMetrics) decrementTotalConns() {
	m.mu.Lock()
	m.totalConns--
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementActiveConns() {
	m.mu.Lock()
	m.activeConns++
	m.mu.Unlock()
}

func (m *PoolMetrics) decrementActiveConns() {
	m.mu.Lock()
	m.activeConns--
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementIdleConns() {
	m.mu.Lock()
	m.idleConns++
	m.mu.Unlock()
}

func (m *PoolMetrics) decrementIdleConns() {
	m.mu.Lock()
	m.idleConns--
	m.mu.Unlock()
}

func (m *PoolMetrics) recordWaitTime(duration time.Duration) {
	m.mu.Lock()
	m.waitTime += duration
	m.waitCount++
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementTimeouts() {
	m.mu.Lock()
	m.timeouts++
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementErrors() {
	m.mu.Lock()
	m.errors++
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementTotalRequests() {
	m.mu.Lock()
	m.totalRequests++
	m.mu.Unlock()
}

func (m *PoolMetrics) incrementSuccessRequests() {
	m.mu.Lock()
	m.successRequests++
	m.mu.Unlock()
}

// GetAverageWaitTime returns the average wait time for connections
func (m *PoolMetrics) GetAverageWaitTime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.waitCount == 0 {
		return 0
	}
	return m.waitTime / time.Duration(m.waitCount)
}

// GetSuccessRate returns the success rate of connection requests
func (m *PoolMetrics) GetSuccessRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.totalRequests == 0 {
		return 0
	}
	return float64(m.successRequests) / float64(m.totalRequests) * 100
}