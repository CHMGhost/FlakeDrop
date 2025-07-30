package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
	"flakedrop/pkg/errors"
)

// Service provides Snowflake database operations
type Service struct {
	db         *sql.DB
	config     Config
	connected  bool
	queryCache map[string]*sql.Stmt
	errorHandler *errors.ErrorHandler
	circuitBreaker *errors.CircuitBreaker
}

// Config holds Snowflake connection configuration
type Config struct {
	Account   string
	Username  string
	Password  string
	Database  string
	Schema    string
	Warehouse string
	Role      string
	Timeout   time.Duration
}

// NewService creates a new Snowflake service
func NewService(config Config) *Service {
	errorHandler := errors.GetGlobalErrorHandler()
	return &Service{
		config:     config,
		queryCache: make(map[string]*sql.Stmt),
		errorHandler: errorHandler,
		circuitBreaker: errors.NewCircuitBreaker("snowflake", 5, 30*time.Second),
	}
}

// Connect establishes a connection to Snowflake
func (s *Service) Connect() error {
	if s.connected {
		return nil
	}

	// Use circuit breaker for connection attempts
	return s.circuitBreaker.Execute(context.Background(), func() error {
		return errors.RetryWithBackoff(context.Background(), func(ctx context.Context) error {
			dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
				s.config.Username,
				s.config.Password,
				s.config.Account,
				s.config.Database,
				s.config.Schema,
				s.config.Warehouse,
				s.config.Role,
			)

			db, err := sql.Open("snowflake", dsn)
			if err != nil {
				return errors.ConnectionError("Failed to open Snowflake connection", err).
					WithContext("account", s.config.Account).
					WithContext("warehouse", s.config.Warehouse)
			}

			// Set connection pool settings
			db.SetMaxOpenConns(10)
			db.SetMaxIdleConns(5)
			db.SetConnMaxLifetime(10 * time.Minute)

			// Test the connection
			connCtx, cancel := s.getContext()
			defer cancel()

			if err := db.PingContext(connCtx); err != nil {
				db.Close()
				
				// Check for specific error types
				if strings.Contains(err.Error(), "authentication") {
					return errors.New(errors.ErrCodeAuthenticationFailed, "Authentication failed").
						WithContext("user", s.config.Username).
						WithSuggestions(
							"Verify your username and password",
							"Check if your account is locked",
							"Ensure MFA is properly configured if required",
						)
				}
				
				return errors.ConnectionError("Failed to connect to Snowflake", err).
					WithContext("account", s.config.Account).
					AsRecoverable()
			}

			s.db = db
			s.connected = true
			return nil
		})
	})
}

// Close closes the database connection
func (s *Service) Close() error {
	if !s.connected {
		return nil
	}

	// Close all prepared statements
	for _, stmt := range s.queryCache {
		stmt.Close()
	}
	s.queryCache = make(map[string]*sql.Stmt)

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	s.connected = false
	return nil
}

// ExecuteFile executes SQL statements from a file
func (s *Service) ExecuteFile(filepath, database, schema string) error {
	if !s.connected {
		return fmt.Errorf("not connected to database")
	}

	// Note: filepath is already validated by the caller
	content, err := os.ReadFile(filepath) // #nosec G304 - path should be validated by caller
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filepath, err)
	}

	return s.ExecuteSQL(string(content), database, schema)
}

// ExecuteSQL executes SQL statements
func (s *Service) ExecuteSQL(sql, database, schema string) error {
	if !s.connected {
		return errors.New(errors.ErrCodeConnectionFailed, "Not connected to database").
			WithSuggestions("Call Connect() before executing SQL")
	}

	ctx, cancel := s.getContext()
	defer cancel()

	// Start a transaction for multiple statements
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "Failed to begin transaction")
	}

	// Create transaction handler for automatic rollback
	txHandler := s.errorHandler.NewTransactionHandler(tx, tx.Rollback)

	return txHandler.Execute(func() error {
		// Switch to the specified database and schema
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", database)); err != nil {
			return errors.SQLError(
				fmt.Sprintf("Failed to use database %s", database),
				fmt.Sprintf("USE DATABASE %s", database),
				err,
			).WithContext("database", database)
		}

		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s", schema)); err != nil {
			return errors.SQLError(
				fmt.Sprintf("Failed to use schema %s", schema),
				fmt.Sprintf("USE SCHEMA %s", schema),
				err,
			).WithContext("schema", schema)
		}

		// Split SQL into individual statements
		statements := s.splitStatements(sql)

		for i, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}

			// Execute statement
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				sqlErr := errors.SQLError(
					fmt.Sprintf("Failed to execute statement %d", i+1),
					stmt,
					err,
				).WithContext("statement_index", i+1).
					WithContext("total_statements", len(statements))

				// Check for specific SQL errors
				errStr := err.Error()
				if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found") {
					sqlErr.Code = errors.ErrCodeSQLObjectNotFound
					sqlErr.WithSuggestions(
						"Verify the object exists in the target database/schema",
						"Check for typos in object names",
						"Ensure you have the correct permissions",
					)
				} else if strings.Contains(errStr, "syntax error") {
					sqlErr.WithSuggestions(
						"Check SQL syntax near the error location",
						"Verify Snowflake-specific syntax requirements",
						"Use the Snowflake documentation for syntax reference",
					)
				}

				return sqlErr
			}
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, errors.ErrCodeSQLTransaction, "Failed to commit transaction")
		}

		return nil
	})
}

// ExecuteQuery executes a query and returns results
func (s *Service) ExecuteQuery(query string) (*sql.Rows, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected to database")
	}

	ctx, cancel := s.getContext()
	defer cancel()

	return s.db.QueryContext(ctx, query)
}

// GetDatabases returns a list of accessible databases
func (s *Service) GetDatabases() ([]string, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected to database")
	}

	rows, err := s.ExecuteQuery("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		// SHOW DATABASES returns multiple columns, we want the name column
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// The database name is typically in the second column
		if len(values) > 1 {
			if name, ok := values[1].(string); ok {
				databases = append(databases, name)
			}
		}
	}

	return databases, rows.Err()
}

// GetSchemas returns a list of schemas in a database
func (s *Service) GetSchemas(database string) ([]string, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected to database")
	}

	ctx, cancel := s.getContext()
	defer cancel()

	// Switch to the database
	if err := s.useDatabase(ctx, database); err != nil {
		return nil, err
	}

	rows, err := s.ExecuteQuery("SHOW SCHEMAS")
	if err != nil {
		return nil, fmt.Errorf("failed to get schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		// Similar to databases, we need to handle multiple columns
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// The schema name is typically in the second column
		if len(values) > 1 {
			if name, ok := values[1].(string); ok {
				schemas = append(schemas, name)
			}
		}
	}

	return schemas, rows.Err()
}

// BeginTransaction starts a new transaction
func (s *Service) BeginTransaction() (*sql.Tx, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected to database")
	}

	ctx, cancel := s.getContext()
	defer cancel()

	return s.db.BeginTx(ctx, nil)
}

// TestConnection tests the database connection
func (s *Service) TestConnection() error {
	if !s.connected {
		if err := s.Connect(); err != nil {
			return err
		}
	}

	ctx, cancel := s.getContext()
	defer cancel()

	return s.db.PingContext(ctx)
}

// Helper methods

func (s *Service) getContext() (context.Context, context.CancelFunc) {
	timeout := s.config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (s *Service) useDatabase(ctx context.Context, database string) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", database))
	if err != nil {
		return fmt.Errorf("failed to use database %s: %w", database, err)
	}
	return nil
}

func (s *Service) useSchema(ctx context.Context, schema string) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s", schema))
	if err != nil {
		return fmt.Errorf("failed to use schema %s: %w", schema, err)
	}
	return nil
}

func (s *Service) splitStatements(sql string) []string {
	// Simple statement splitter - splits on semicolons not within strings
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
				// Check if it's not escaped
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

	// Add the last statement if any
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

// ValidateConfig validates the Snowflake configuration
func ValidateConfig(config Config) error {
	if config.Account == "" {
		return fmt.Errorf("account is required")
	}
	if config.Username == "" {
		return fmt.Errorf("username is required")
	}
	if config.Password == "" {
		return fmt.Errorf("password is required")
	}
	if config.Warehouse == "" {
		return fmt.Errorf("warehouse is required")
	}
	if config.Role == "" {
		return fmt.Errorf("role is required")
	}
	return nil
}

// GetTableInfo returns information about a table
func (s *Service) GetTableInfo(database, schema, table string) (map[string]interface{}, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected to database")
	}

	ctx, cancel := s.getContext()
	defer cancel()

	// Switch context
	if err := s.useDatabase(ctx, database); err != nil {
		return nil, err
	}
	if err := s.useSchema(ctx, schema); err != nil {
		return nil, err
	}

	// Get table information
	query := fmt.Sprintf("DESCRIBE TABLE %s", table)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}
	defer rows.Close()

	info := make(map[string]interface{})
	var columns []map[string]string

	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		column := make(map[string]string)
		for i, col := range cols {
			if val, ok := values[i].(string); ok {
				column[col] = val
			}
		}
		columns = append(columns, column)
	}

	info["columns"] = columns
	info["database"] = database
	info["schema"] = schema
	info["table"] = table

	return info, rows.Err()
}

// ExecuteDirectory executes all SQL files in a directory
func (s *Service) ExecuteDirectory(dir, database, schema string, pattern string) error {
	if !s.connected {
		return fmt.Errorf("not connected to database")
	}

	if pattern == "" {
		pattern = "*.sql"
	}

	files, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	for _, file := range files {
		if err := s.ExecuteFile(file, database, schema); err != nil {
			return fmt.Errorf("failed to execute %s: %w", file, err)
		}
	}

	return nil
}

// GetDB returns the underlying database connection
func (s *Service) GetDB() *sql.DB {
	return s.db
}