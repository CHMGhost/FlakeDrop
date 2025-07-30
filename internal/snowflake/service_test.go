package snowflake

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	config := Config{
		Account:   "test123.us-east-1",
		Username:  "testuser",
		Password:  "testpass",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
		Warehouse: "TEST_WH",
		Role:      "SYSADMIN",
		Timeout:   30 * time.Second,
	}

	service := NewService(config)

	assert.NotNil(t, service)
	assert.Equal(t, config, service.config)
	assert.False(t, service.connected)
	assert.NotNil(t, service.queryCache)
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Password:  "testpass",
				Warehouse: "TEST_WH",
				Role:      "SYSADMIN",
			},
			wantError: false,
		},
		{
			name: "missing account",
			config: Config{
				Username:  "testuser",
				Password:  "testpass",
				Warehouse: "TEST_WH",
				Role:      "SYSADMIN",
			},
			wantError: true,
			errorMsg:  "account is required",
		},
		{
			name: "missing username",
			config: Config{
				Account:   "test123.us-east-1",
				Password:  "testpass",
				Warehouse: "TEST_WH",
				Role:      "SYSADMIN",
			},
			wantError: true,
			errorMsg:  "username is required",
		},
		{
			name: "missing password",
			config: Config{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Warehouse: "TEST_WH",
				Role:      "SYSADMIN",
			},
			wantError: true,
			errorMsg:  "password is required",
		},
		{
			name: "missing warehouse",
			config: Config{
				Account:  "test123.us-east-1",
				Username: "testuser",
				Password: "testpass",
				Role:     "SYSADMIN",
			},
			wantError: true,
			errorMsg:  "warehouse is required",
		},
		{
			name: "missing role",
			config: Config{
				Account:   "test123.us-east-1",
				Username:  "testuser",
				Password:  "testpass",
				Warehouse: "TEST_WH",
			},
			wantError: true,
			errorMsg:  "role is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.config)
			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSplitStatements(t *testing.T) {
	service := &Service{}

	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "single statement",
			sql:  "SELECT * FROM users",
			expected: []string{
				"SELECT * FROM users",
			},
		},
		{
			name: "multiple statements",
			sql:  "CREATE TABLE users (id INT); INSERT INTO users VALUES (1);",
			expected: []string{
				"CREATE TABLE users (id INT)",
				" INSERT INTO users VALUES (1)",
			},
		},
		{
			name: "statements with strings",
			sql:  "INSERT INTO logs VALUES ('test;data'); SELECT * FROM logs;",
			expected: []string{
				"INSERT INTO logs VALUES ('test;data')",
				" SELECT * FROM logs",
			},
		},
		{
			name: "statements with double quotes",
			sql:  `CREATE TABLE "test;table" (id INT); SELECT * FROM "test;table";`,
			expected: []string{
				`CREATE TABLE "test;table" (id INT)`,
				` SELECT * FROM "test;table"`,
			},
		},
		{
			name: "empty statements",
			sql:  "SELECT 1;;;SELECT 2;",
			expected: []string{
				"SELECT 1",
				"",
				"",
				"SELECT 2",
			},
		},
		{
			name: "statement without trailing semicolon",
			sql:  "SELECT * FROM users WHERE id = 1",
			expected: []string{
				"SELECT * FROM users WHERE id = 1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.splitStatements(tt.sql)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExecuteSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	tests := []struct {
		name      string
		sql       string
		database  string
		schema    string
		setupMock func()
		wantError bool
		errorMsg  string
	}{
		{
			name:     "successful execution",
			sql:      "CREATE TABLE users (id INT)",
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			},
			wantError: false,
		},
		{
			name:     "not connected",
			sql:      "SELECT 1",
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				service.connected = false
			},
			wantError: true,
			errorMsg:  "not connected to database",
		},
		{
			name:     "database switch failure",
			sql:      "SELECT 1",
			database: "INVALID_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				service.connected = true
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE INVALID_DB").WillReturnError(fmt.Errorf("database not found"))
				mock.ExpectRollback()
			},
			wantError: true,
			errorMsg:  "failed to use database",
		},
		{
			name:     "schema switch failure",
			sql:      "SELECT 1",
			database: "TEST_DB",
			schema:   "INVALID_SCHEMA",
			setupMock: func() {
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA INVALID_SCHEMA").WillReturnError(fmt.Errorf("schema not found"))
				mock.ExpectRollback()
			},
			wantError: true,
			errorMsg:  "Failed to use schema",
		},
		{
			name:     "statement execution failure",
			sql:      "INVALID SQL STATEMENT",
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("INVALID SQL STATEMENT").WillReturnError(fmt.Errorf("syntax error"))
				mock.ExpectRollback()
			},
			wantError: true,
			errorMsg:  "Failed to execute statement",
		},
		{
			name:     "multiple statements",
			sql:      "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT);",
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE t1").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE t2").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := service.ExecuteSQL(tt.sql, tt.database, tt.schema)

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}

			// Ensure all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil && !tt.wantError {
				t.Errorf("unfulfilled expectations: %s", err)
			}
		})
	}
}

func TestExecuteFile(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	// Create a temporary SQL file
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "test.sql")
	sqlContent := "CREATE TABLE test_table (id INT);"
	err = os.WriteFile(sqlFile, []byte(sqlContent), 0644)
	require.NoError(t, err)

	tests := []struct {
		name      string
		filepath  string
		database  string
		schema    string
		setupMock func()
		wantError bool
		errorMsg  string
	}{
		{
			name:     "successful file execution",
			filepath: sqlFile,
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				mock.ExpectBegin()
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE test_table").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			},
			wantError: false,
		},
		{
			name:      "file not found",
			filepath:  "/nonexistent/file.sql",
			database:  "TEST_DB",
			schema:    "PUBLIC",
			setupMock: func() {},
			wantError: true,
			errorMsg:  "failed to read file",
		},
		{
			name:     "not connected",
			filepath: sqlFile,
			database: "TEST_DB",
			schema:   "PUBLIC",
			setupMock: func() {
				service.connected = false
			},
			wantError: true,
			errorMsg:  "not connected to database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := service.ExecuteFile(tt.filepath, tt.database, tt.schema)

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}

			// Reset connection state
			service.connected = true
		})
	}
}

func TestGetDatabases(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	tests := []struct {
		name      string
		setupMock func()
		expected  []string
		wantError bool
		errorMsg  string
	}{
		{
			name: "successful get databases",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"created_on", "name", "is_default", "is_current", "origin", "owner", "comment", "options", "retention_time"}).
					AddRow("2023-01-01", "DEV", "N", "N", "", "SYSADMIN", "", "", "1").
					AddRow("2023-01-02", "TEST", "N", "Y", "", "SYSADMIN", "", "", "1").
					AddRow("2023-01-03", "PROD", "N", "N", "", "SYSADMIN", "", "", "1")
				mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
			},
			expected:  []string{"DEV", "TEST", "PROD"},
			wantError: false,
		},
		{
			name: "not connected",
			setupMock: func() {
				service.connected = false
			},
			wantError: true,
			errorMsg:  "not connected to database",
		},
		{
			name: "query error",
			setupMock: func() {
				service.connected = true
				mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("permission denied"))
			},
			wantError: true,
			errorMsg:  "failed to get databases",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			result, err := service.GetDatabases()

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}

			// Reset connection state
			service.connected = true
		})
	}
}

func TestGetSchemas(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	tests := []struct {
		name      string
		database  string
		setupMock func()
		expected  []string
		wantError bool
		errorMsg  string
	}{
		{
			name:     "successful get schemas",
			database: "TEST_DB",
			setupMock: func() {
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				rows := sqlmock.NewRows([]string{"created_on", "name", "is_default", "is_current", "database_name", "owner", "comment", "options", "retention_time"}).
					AddRow("2023-01-01", "PUBLIC", "Y", "Y", "TEST_DB", "SYSADMIN", "", "", "1").
					AddRow("2023-01-02", "ANALYTICS", "N", "N", "TEST_DB", "SYSADMIN", "", "", "1").
					AddRow("2023-01-03", "RAW", "N", "N", "TEST_DB", "SYSADMIN", "", "", "1")
				mock.ExpectQuery("SHOW SCHEMAS").WillReturnRows(rows)
			},
			expected:  []string{"PUBLIC", "ANALYTICS", "RAW"},
			wantError: false,
		},
		{
			name:     "database switch error",
			database: "INVALID_DB",
			setupMock: func() {
				mock.ExpectExec("USE DATABASE INVALID_DB").WillReturnError(fmt.Errorf("database not found"))
			},
			wantError: true,
			errorMsg:  "failed to use database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			result, err := service.GetSchemas(tt.database)

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBeginTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	t.Run("successful transaction", func(t *testing.T) {
		mock.ExpectBegin()
		
		tx, err := service.BeginTransaction()
		
		assert.NoError(t, err)
		assert.NotNil(t, tx)
	})

	t.Run("not connected", func(t *testing.T) {
		service.connected = false
		
		tx, err := service.BeginTransaction()
		
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected to database")
		assert.Nil(t, tx)
	})
}

func TestGetTableInfo(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	tests := []struct {
		name      string
		database  string
		schema    string
		table     string
		setupMock func()
		validate  func(t *testing.T, info map[string]interface{})
		wantError bool
		errorMsg  string
	}{
		{
			name:     "successful get table info",
			database: "TEST_DB",
			schema:   "PUBLIC",
			table:    "USERS",
			setupMock: func() {
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				rows := sqlmock.NewRows([]string{"name", "type", "kind", "null?", "default", "primary key", "unique key", "check", "expression", "comment"}).
					AddRow("ID", "NUMBER(38,0)", "COLUMN", "N", "", "Y", "N", "", "", "").
					AddRow("NAME", "VARCHAR(100)", "COLUMN", "Y", "", "N", "N", "", "", "").
					AddRow("EMAIL", "VARCHAR(200)", "COLUMN", "N", "", "N", "Y", "", "", "")
				mock.ExpectQuery("DESCRIBE TABLE USERS").WillReturnRows(rows)
			},
			validate: func(t *testing.T, info map[string]interface{}) {
				assert.Equal(t, "TEST_DB", info["database"])
				assert.Equal(t, "PUBLIC", info["schema"])
				assert.Equal(t, "USERS", info["table"])
				
				columns, ok := info["columns"].([]map[string]string)
				assert.True(t, ok)
				assert.Len(t, columns, 3)
			},
			wantError: false,
		},
		{
			name:     "table not found",
			database: "TEST_DB",
			schema:   "PUBLIC",
			table:    "NONEXISTENT",
			setupMock: func() {
				mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("DESCRIBE TABLE NONEXISTENT").WillReturnError(fmt.Errorf("table not found"))
			},
			wantError: true,
			errorMsg:  "failed to describe table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			info, err := service.GetTableInfo(tt.database, tt.schema, tt.table)

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				tt.validate(t, info)
			}
		})
	}
}

func TestExecuteDirectory(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	// Create temporary directory with SQL files
	tempDir := t.TempDir()
	
	// Create SQL files
	files := []struct {
		name    string
		content string
	}{
		{"table1.sql", "CREATE TABLE t1 (id INT);"},
		{"table2.sql", "CREATE TABLE t2 (id INT);"},
		{"view1.sql", "CREATE VIEW v1 AS SELECT * FROM t1;"},
		{"data.txt", "This is not a SQL file"},
	}

	for _, f := range files {
		err := os.WriteFile(filepath.Join(tempDir, f.name), []byte(f.content), 0644)
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		dir       string
		database  string
		schema    string
		pattern   string
		setupMock func()
		wantError bool
		errorMsg  string
	}{
		{
			name:     "execute all SQL files",
			dir:      tempDir,
			database: "TEST_DB",
			schema:   "PUBLIC",
			pattern:  "*.sql",
			setupMock: func() {
				// Expect 3 SQL files to be executed
				for i := 0; i < 3; i++ {
					mock.ExpectBegin()
					mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("CREATE").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectCommit()
				}
			},
			wantError: false,
		},
		{
			name:     "execute specific pattern",
			dir:      tempDir,
			database: "TEST_DB",
			schema:   "PUBLIC",
			pattern:  "table*.sql",
			setupMock: func() {
				// Expect 2 table SQL files to be executed
				for i := 0; i < 2; i++ {
					mock.ExpectBegin()
					mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectCommit()
				}
			},
			wantError: false,
		},
		{
			name:      "directory not found",
			dir:       "/nonexistent/directory",
			database:  "TEST_DB",
			schema:    "PUBLIC",
			pattern:   "*.sql",
			setupMock: func() {},
			wantError: false, // Glob returns empty list for non-existent directories
		},
		{
			name:     "not connected",
			dir:      tempDir,
			database: "TEST_DB",
			schema:   "PUBLIC",
			pattern:  "*.sql",
			setupMock: func() {
				service.connected = false
			},
			wantError: true,
			errorMsg:  "not connected to database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			err := service.ExecuteDirectory(tt.dir, tt.database, tt.schema, tt.pattern)

			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}

			// Reset connection state
			service.connected = true
		})
	}
}

func TestClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true
	// Don't add invalid statements to queryCache - it will be empty

	t.Run("successful close", func(t *testing.T) {
		mock.ExpectClose()

		err := service.Close()
		
		assert.NoError(t, err)
		assert.False(t, service.connected)
		assert.NotNil(t, service.queryCache) // queryCache should still exist but be empty
	})

	t.Run("already closed", func(t *testing.T) {
		service.connected = false
		
		err := service.Close()
		
		assert.NoError(t, err)
	})
}

// BenchmarkSplitStatements benchmarks the statement splitting function
func BenchmarkSplitStatements(b *testing.B) {
	service := &Service{}
	sql := `
		CREATE TABLE users (id INT, name VARCHAR(100));
		INSERT INTO users VALUES (1, 'John; Doe');
		UPDATE users SET name = 'Jane' WHERE id = 1;
		DELETE FROM users WHERE name LIKE '%;%';
		SELECT * FROM users WHERE created_at > '2023-01-01';
	`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = service.splitStatements(sql)
	}
}

// BenchmarkExecuteSQL benchmarks SQL execution
func BenchmarkExecuteSQL(b *testing.B) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	service := NewService(Config{
		Timeout: 5 * time.Second,
	})
	service.db = db
	service.connected = true

	sql := "CREATE TABLE benchmark_test (id INT, data VARCHAR(1000))"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mock.ExpectBegin()
		mock.ExpectExec("USE DATABASE TEST_DB").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("USE SCHEMA PUBLIC").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE benchmark_test").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()
		
		_ = service.ExecuteSQL(sql, "TEST_DB", "PUBLIC")
	}
}