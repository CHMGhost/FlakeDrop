package testutil

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// MockSnowflakeDB provides a mock implementation of Snowflake database operations
type MockSnowflakeDB struct {
	mu sync.Mutex
	
	// Connection state
	Connected bool
	DSN       string
	
	// Execution tracking
	ExecutedQueries []ExecutedQuery
	QueryResults    map[string]*sql.Result
	QueryErrors     map[string]error
	
	// Connection behavior
	ConnectError error
	PingError    error
	
	// Transaction support
	InTransaction bool
	TxCommitted   bool
	TxRolledBack  bool
}

// ExecutedQuery represents a query that was executed
type ExecutedQuery struct {
	Query     string
	Database  string
	Schema    string
	Timestamp time.Time
}

// NewMockSnowflakeDB creates a new mock Snowflake database
func NewMockSnowflakeDB() *MockSnowflakeDB {
	return &MockSnowflakeDB{
		ExecutedQueries: make([]ExecutedQuery, 0),
		QueryResults:    make(map[string]*sql.Result),
		QueryErrors:     make(map[string]error),
	}
}

// Connect simulates connecting to Snowflake
func (m *MockSnowflakeDB) Connect(dsn string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.ConnectError != nil {
		return m.ConnectError
	}
	
	m.DSN = dsn
	m.Connected = true
	return nil
}

// Close simulates closing the connection
func (m *MockSnowflakeDB) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.Connected = false
	return nil
}

// Ping simulates checking the connection
func (m *MockSnowflakeDB) Ping() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.Connected {
		return fmt.Errorf("not connected")
	}
	
	if m.PingError != nil {
		return m.PingError
	}
	
	return nil
}

// Execute simulates executing a query
func (m *MockSnowflakeDB) Execute(query, database, schema string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.Connected {
		return fmt.Errorf("not connected")
	}
	
	// Record the execution
	m.ExecutedQueries = append(m.ExecutedQueries, ExecutedQuery{
		Query:     query,
		Database:  database,
		Schema:    schema,
		Timestamp: time.Now(),
	})
	
	// Check for predefined error
	if err, exists := m.QueryErrors[query]; exists {
		return err
	}
	
	return nil
}

// BeginTransaction starts a mock transaction
func (m *MockSnowflakeDB) BeginTransaction() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.Connected {
		return fmt.Errorf("not connected")
	}
	
	if m.InTransaction {
		return fmt.Errorf("already in transaction")
	}
	
	m.InTransaction = true
	return nil
}

// Commit commits a mock transaction
func (m *MockSnowflakeDB) Commit() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.InTransaction {
		return fmt.Errorf("not in transaction")
	}
	
	m.InTransaction = false
	m.TxCommitted = true
	return nil
}

// Rollback rolls back a mock transaction
func (m *MockSnowflakeDB) Rollback() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.InTransaction {
		return fmt.Errorf("not in transaction")
	}
	
	m.InTransaction = false
	m.TxRolledBack = true
	return nil
}

// SetQueryError sets an error to be returned for a specific query
func (m *MockSnowflakeDB) SetQueryError(query string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.QueryErrors[query] = err
}

// GetExecutedQueries returns all executed queries
func (m *MockSnowflakeDB) GetExecutedQueries() []ExecutedQuery {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make([]ExecutedQuery, len(m.ExecutedQueries))
	copy(result, m.ExecutedQueries)
	return result
}

// Reset clears all execution history
func (m *MockSnowflakeDB) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.ExecutedQueries = make([]ExecutedQuery, 0)
	m.QueryResults = make(map[string]*sql.Result)
	m.QueryErrors = make(map[string]error)
	m.TxCommitted = false
	m.TxRolledBack = false
}

// MockSnowflakeService provides a higher-level mock for Snowflake operations
type MockSnowflakeService struct {
	DB *MockSnowflakeDB
	
	// Service configuration
	Account   string
	Username  string
	Warehouse string
	Role      string
	
	// Connection pooling simulation
	MaxConnections   int
	ActiveConnections int
}

// NewMockSnowflakeService creates a new mock Snowflake service
func NewMockSnowflakeService() *MockSnowflakeService {
	return &MockSnowflakeService{
		DB:             NewMockSnowflakeDB(),
		MaxConnections: 10,
	}
}

// TestConnection tests the Snowflake connection
func (s *MockSnowflakeService) TestConnection() error {
	if s.DB.ConnectError != nil {
		return s.DB.ConnectError
	}
	
	// Simulate connection string construction
	dsn := fmt.Sprintf("%s:%s@%s", s.Username, "***", s.Account)
	
	if err := s.DB.Connect(dsn); err != nil {
		return err
	}
	
	return s.DB.Ping()
}

// ExecuteFile simulates executing a SQL file
func (s *MockSnowflakeService) ExecuteFile(filepath, database, schema string) error {
	// In a real implementation, this would read the file
	// For testing, we'll just execute a mock query
	query := fmt.Sprintf("-- File: %s", filepath)
	return s.DB.Execute(query, database, schema)
}

// GetDatabases returns a list of mock databases
func (s *MockSnowflakeService) GetDatabases() ([]string, error) {
	if !s.DB.Connected {
		return nil, fmt.Errorf("not connected")
	}
	
	return []string{"DEV", "TEST", "PROD"}, nil
}

// GetSchemas returns a list of mock schemas for a database
func (s *MockSnowflakeService) GetSchemas(database string) ([]string, error) {
	if !s.DB.Connected {
		return nil, fmt.Errorf("not connected")
	}
	
	schemas := map[string][]string{
		"DEV":  {"PUBLIC", "ANALYTICS", "RAW"},
		"TEST": {"PUBLIC", "STAGING"},
		"PROD": {"PUBLIC", "ANALYTICS", "REPORTING"},
	}
	
	if s, exists := schemas[database]; exists {
		return s, nil
	}
	
	return []string{"PUBLIC"}, nil
}

// MockQueryResult provides a way to simulate query results
type MockQueryResult struct {
	AffectedRows int64
	LastInsertID int64
	Error        error
}

func (r *MockQueryResult) LastInsertId() (int64, error) {
	return r.LastInsertID, r.Error
}

func (r *MockQueryResult) RowsAffected() (int64, error) {
	return r.AffectedRows, r.Error
}