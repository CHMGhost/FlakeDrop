package testutil

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
	
	"flakedrop/internal/common"
)

// TestHelper provides common test utilities
type TestHelper struct {
	t *testing.T
}

// NewTestHelper creates a new test helper
func NewTestHelper(t *testing.T) *TestHelper {
	return &TestHelper{t: t}
}

// TempDir creates a temporary directory and returns cleanup function
func (h *TestHelper) TempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "snowflake-deploy-test-*")
	if err != nil {
		h.t.Fatalf("Failed to create temp dir: %v", err)
	}
	
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			h.t.Errorf("Failed to clean up temp dir: %v", err)
		}
	}
	
	return dir, cleanup
}

// WriteFile writes content to a file in the given directory
func (h *TestHelper) WriteFile(dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	
	// Create parent directories if needed
	if err := os.MkdirAll(filepath.Dir(path), common.DirPermissionNormal); err != nil {
		h.t.Fatalf("Failed to create directories: %v", err)
	}
	
	if err := os.WriteFile(path, []byte(content), common.FilePermissionSecure); err != nil {
		h.t.Fatalf("Failed to write file %s: %v", path, err)
	}
	
	return path
}

// CreateTestRepository creates a test repository structure
func (h *TestHelper) CreateTestRepository(dir string) {
	// Create directory structure
	dirs := []string{
		"sql/tables",
		"sql/views",
		"sql/procedures",
		"sql/migrations",
		"configs",
	}
	
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(dir, d), common.DirPermissionNormal); err != nil {
			h.t.Fatalf("Failed to create directory %s: %v", d, err)
		}
	}
	
	// Create sample SQL files
	h.WriteFile(dir, "sql/tables/users.sql", `
-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`)
	
	h.WriteFile(dir, "sql/tables/orders.sql", `
-- Create orders table  
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);`)
	
	h.WriteFile(dir, "sql/views/user_orders.sql", `
-- Create user orders view
CREATE OR REPLACE VIEW user_orders AS
SELECT 
    u.username,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.username;`)
	
	h.WriteFile(dir, "sql/procedures/cleanup.sql", `
-- Cleanup procedure
CREATE OR REPLACE PROCEDURE cleanup_old_orders()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM orders WHERE created_at < DATEADD(year, -1, CURRENT_TIMESTAMP);
    RETURN 'Cleanup completed';
END;
$$;`)
	
	h.WriteFile(dir, "README.md", "# Test Repository\n\nThis is a test repository for Snowflake Deploy.")
}

// CaptureOutput captures stdout and stderr during function execution
func (h *TestHelper) CaptureOutput(f func()) (stdout, stderr string) {
	// Capture stdout
	oldStdout := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut
	
	// Capture stderr
	oldStderr := os.Stderr
	rErr, wErr, _ := os.Pipe()
	os.Stderr = wErr
	
	// Execute function
	f()
	
	// Restore and read stdout
	wOut.Close()
	os.Stdout = oldStdout
	outBytes, _ := io.ReadAll(rOut)
	stdout = string(outBytes)
	
	// Restore and read stderr
	wErr.Close()
	os.Stderr = oldStderr
	errBytes, _ := io.ReadAll(rErr)
	stderr = string(errBytes)
	
	return stdout, stderr
}

// AssertContains checks if a string contains a substring
func (h *TestHelper) AssertContains(haystack, needle string) {
	if !bytes.Contains([]byte(haystack), []byte(needle)) {
		h.t.Errorf("Expected to contain '%s', but got: %s", needle, haystack)
	}
}

// AssertNotContains checks if a string does not contain a substring
func (h *TestHelper) AssertNotContains(haystack, needle string) {
	if bytes.Contains([]byte(haystack), []byte(needle)) {
		h.t.Errorf("Expected not to contain '%s', but got: %s", needle, haystack)
	}
}

// AssertEqual checks if two values are equal
func (h *TestHelper) AssertEqual(expected, actual interface{}) {
	if expected != actual {
		h.t.Errorf("Expected %v, got %v", expected, actual)
	}
}

// AssertNotEqual checks if two values are not equal
func (h *TestHelper) AssertNotEqual(expected, actual interface{}) {
	if expected == actual {
		h.t.Errorf("Expected values to be different, but both are %v", expected)
	}
}

// AssertNil checks if a value is nil
func (h *TestHelper) AssertNil(value interface{}) {
	if value != nil {
		h.t.Errorf("Expected nil, got %v", value)
	}
}

// AssertNotNil checks if a value is not nil
func (h *TestHelper) AssertNotNil(value interface{}) {
	if value == nil {
		h.t.Error("Expected non-nil value, got nil")
	}
}

// AssertError checks if an error occurred
func (h *TestHelper) AssertError(err error) {
	if err == nil {
		h.t.Error("Expected error, got nil")
	}
}

// AssertNoError checks if no error occurred
func (h *TestHelper) AssertNoError(err error) {
	if err != nil {
		h.t.Errorf("Expected no error, got: %v", err)
	}
}

// WaitFor waits for a condition to be true within a timeout
func (h *TestHelper) WaitFor(condition func() bool, timeout time.Duration, message string) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		if condition() {
			return
		}
		
		<-ticker.C
		if time.Now().After(deadline) {
			h.t.Fatalf("Timeout waiting for: %s", message)
		}
	}
}

// MockEnv temporarily sets environment variables
func (h *TestHelper) MockEnv(key, value string) func() {
	oldValue, exists := os.LookupEnv(key)
	
	if err := os.Setenv(key, value); err != nil {
		h.t.Fatalf("Failed to set env var %s: %v", key, err)
	}
	
	return func() {
		if exists {
			os.Setenv(key, oldValue)
		} else {
			os.Unsetenv(key)
		}
	}
}

// GenerateTestData generates test data of various types
type TestDataGenerator struct {
	seed int
}

// NewTestDataGenerator creates a new test data generator
func NewTestDataGenerator() *TestDataGenerator {
	return &TestDataGenerator{seed: 1}
}

// NextString generates a unique string
func (g *TestDataGenerator) NextString(prefix string) string {
	result := fmt.Sprintf("%s_%d", prefix, g.seed)
	g.seed++
	return result
}

// NextInt generates a unique integer
func (g *TestDataGenerator) NextInt() int {
	result := g.seed
	g.seed++
	return result
}

// GenerateSQL generates sample SQL content
func (g *TestDataGenerator) GenerateSQL(tableNum int) string {
	return fmt.Sprintf(`
-- Generated table %d
CREATE TABLE IF NOT EXISTS test_table_%d (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO test_table_%d (id, name, value) VALUES
    (1, 'Item 1', 10.50),
    (2, 'Item 2', 20.75),
    (3, 'Item 3', 30.00);
`, tableNum, tableNum, tableNum)
}

// TestLogger provides a logger that writes to testing.T
type TestLogger struct {
	t *testing.T
}

// NewTestLogger creates a new test logger
func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{t: t}
}

// Debug logs a debug message
func (l *TestLogger) Debug(format string, args ...interface{}) {
	l.t.Logf("[DEBUG] "+format, args...)
}

// Info logs an info message
func (l *TestLogger) Info(format string, args ...interface{}) {
	l.t.Logf("[INFO] "+format, args...)
}

// Warn logs a warning message
func (l *TestLogger) Warn(format string, args ...interface{}) {
	l.t.Logf("[WARN] "+format, args...)
}

// Error logs an error message
func (l *TestLogger) Error(format string, args ...interface{}) {
	l.t.Logf("[ERROR] "+format, args...)
}