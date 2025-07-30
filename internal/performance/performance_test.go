package performance

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"flakedrop/internal/snowflake"
)

// TestConnectionPool tests connection pool functionality
func TestConnectionPool(t *testing.T) {
	// Create test configuration
	_ = snowflake.Config{
		Account:   "test_account",
		Username:  "test_user",
		Password:  "test_pass",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
		Warehouse: "TEST_WH",
		Role:      "TEST_ROLE",
	}

	_ = PoolConfig{
		MinSize:     2,
		MaxSize:     5,
		WaitTimeout: 2 * time.Second,
		IdleTimeout: 5 * time.Second,
		HealthCheck: 1 * time.Second,
	}

	t.Run("pool creation", func(t *testing.T) {
		// This would need a mock Snowflake connection in real tests
		t.Skip("Requires mock Snowflake connection")
	})

	t.Run("concurrent access", func(t *testing.T) {
		t.Skip("Requires mock implementation")
		
		// pool, err := NewConnectionPool(sfConfig, poolConfig)
		// if err != nil {
		// 	t.Fatalf("Failed to create pool: %v", err)
		// }
		// defer pool.Close()

		// var wg sync.WaitGroup
		// errors := make(chan error, 10)

		// // Simulate concurrent connection requests
		// for i := 0; i < 10; i++ {
		// 	wg.Add(1)
		// 	go func(id int) {
		// 		defer wg.Done()
				
		// 		ctx := context.Background()
		// 		conn, err := pool.Get(ctx)
		// 		if err != nil {
		// 			errors <- err
		// 			return
		// 		}
				
		// 		// Simulate work
		// 		time.Sleep(100 * time.Millisecond)
				
		// 		pool.Put(conn)
		// 	}(i)
		// }

		// wg.Wait()
		// close(errors)

		// // Check for errors
		// for err := range errors {
		// 	t.Errorf("Connection error: %v", err)
		// }
	})
}

// TestParallelExecutor tests parallel execution functionality
func TestParallelExecutor(t *testing.T) {
	t.Skip("Skipping test - requires proper database mocking")
	t.Run("task submission and execution", func(t *testing.T) {
		// Create mock pool
		mockPool := createMockConnectionPool()
		
		config := ExecutorConfig{
			MaxWorkers: 4,
			QueueSize:  100,
			BatchSize:  10,
			RateLimit:  10,
			RetryLimit: 2,
			Timeout:    1 * time.Minute,
		}
		
		executor := NewParallelExecutor(mockPool, config)
		defer executor.Shutdown(5 * time.Second)

		// Submit test tasks
		tasks := []Task{
			{
				ID:       "task1",
				Type:     TaskTypeSQL,
				FilePath: "test1.sql",
				Content:  "SELECT 1;",
				Database: "TEST_DB",
				Schema:   "PUBLIC",
				Priority: 1,
			},
			{
				ID:       "task2",
				Type:     TaskTypeDDL,
				FilePath: "test2.sql",
				Content:  "CREATE TABLE test (id INT);",
				Database: "TEST_DB",
				Schema:   "PUBLIC",
				Priority: 2,
			},
		}

		for _, task := range tasks {
			err := executor.Submit(task)
			if err != nil {
				t.Errorf("Failed to submit task %s: %v", task.ID, err)
			}
		}

		// Wait for completion
		err := executor.WaitForCompletion(5 * time.Second)
		if err != nil {
			t.Errorf("Failed to complete tasks: %v", err)
		}

		// Check metrics
		metrics := executor.GetMetrics()
		if metrics.completedTasks != int64(len(tasks)) {
			t.Errorf("Expected %d completed tasks, got %d", len(tasks), metrics.completedTasks)
		}
	})

	t.Run("rate limiting", func(t *testing.T) {
		limiter := NewRateLimiter(10) // 10 per second
		defer limiter.Stop()

		start := time.Now()
		
		// Try to consume 20 tokens
		for i := 0; i < 20; i++ {
			limiter.Wait()
		}
		
		duration := time.Since(start)
		
		// Should take at least 1 second for the second batch
		if duration < 900*time.Millisecond {
			t.Errorf("Rate limiting too fast: %v", duration)
		}
	})
}

// TestCache tests caching functionality
func TestCache(t *testing.T) {
	config := CacheConfig{
		MaxItems:        100,
		MaxMemoryMB:     10,
		DefaultTTL:      1 * time.Second,
		CleanupInterval: 500 * time.Millisecond,
	}

	cache := NewCache(config)
	defer cache.Stop()

	t.Run("basic operations", func(t *testing.T) {
		// Set value
		err := cache.Set("key1", "value1", 100)
		if err != nil {
			t.Errorf("Failed to set value: %v", err)
		}

		// Get value
		value, found := cache.Get("key1")
		if !found {
			t.Error("Expected to find key1")
		}
		if value != "value1" {
			t.Errorf("Expected value1, got %v", value)
		}

		// Delete value
		deleted := cache.Delete("key1")
		if !deleted {
			t.Error("Expected to delete key1")
		}

		// Verify deletion
		_, found = cache.Get("key1")
		if found {
			t.Error("Expected key1 to be deleted")
		}
	})

	t.Run("TTL expiration", func(t *testing.T) {
		// Set with short TTL
		err := cache.SetWithTTL("key2", "value2", 100, 500*time.Millisecond)
		if err != nil {
			t.Errorf("Failed to set value: %v", err)
		}

		// Should exist immediately
		_, found := cache.Get("key2")
		if !found {
			t.Error("Expected to find key2")
		}

		// Wait for expiration
		time.Sleep(1 * time.Second)

		// Should be expired
		_, found = cache.Get("key2")
		if found {
			t.Error("Expected key2 to be expired")
		}
	})

	t.Run("LRU eviction", func(t *testing.T) {
		smallCache := NewCache(CacheConfig{
			MaxItems:        3,
			MaxMemoryMB:     1,
			DefaultTTL:      1 * time.Minute,
			CleanupInterval: 10 * time.Second,
		})
		defer smallCache.Stop()

		// Fill cache
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			smallCache.Set(key, value, 100)
		}

		// Access key0 to make it recently used
		smallCache.Get("key0")

		// Add new item, should evict key1 (least recently used)
		smallCache.Set("key3", "value3", 100)

		// Check what's in cache
		_, found0 := smallCache.Get("key0")
		_, found1 := smallCache.Get("key1")
		_, found2 := smallCache.Get("key2")
		_, found3 := smallCache.Get("key3")

		if !found0 {
			t.Error("Expected key0 to be in cache")
		}
		if found1 {
			t.Error("Expected key1 to be evicted")
		}
		if !found2 {
			t.Error("Expected key2 to be in cache")
		}
		if !found3 {
			t.Error("Expected key3 to be in cache")
		}
	})
}

// TestBatchProcessor tests batch processing functionality
func TestBatchProcessor(t *testing.T) {
	t.Run("batch accumulation and flush", func(t *testing.T) {
		t.Skip("Skipping test - requires proper database connection mocking")
		mockPool := createMockConnectionPool()
		
		config := BatchConfig{
			BatchSize:      3,
			FlushInterval:  1 * time.Second,
			MaxRetries:     2,
			MaxBatchMemory: 1024 * 1024,
		}
		
		processor := NewBatchProcessor(mockPool, config)
		defer processor.Stop()

		// Add statements
		statements := []BatchStatement{
			{ID: "1", SQL: "INSERT INTO test VALUES (1);", Size: 30},
			{ID: "2", SQL: "INSERT INTO test VALUES (2);", Size: 30},
			{ID: "3", SQL: "INSERT INTO test VALUES (3);", Size: 30},
		}

		for _, stmt := range statements {
			err := processor.Add("TEST_DB", "PUBLIC", stmt)
			if err != nil {
				t.Errorf("Failed to add statement: %v", err)
			}
		}

		// Should trigger automatic flush at batch size
		time.Sleep(100 * time.Millisecond)

		metrics := processor.GetMetrics()
		if metrics.TotalBatches != 1 {
			t.Errorf("Expected 1 batch, got %d", metrics.TotalBatches)
		}
	})

	t.Run("statement type detection", func(t *testing.T) {
		tests := []struct {
			sql      string
			expected StatementType
		}{
			{"INSERT INTO table VALUES (1)", StatementTypeInsert},
			{"UPDATE table SET col = 1", StatementTypeUpdate},
			{"DELETE FROM table WHERE id = 1", StatementTypeDelete},
			{"CREATE TABLE test (id INT)", StatementTypeCreate},
			{"ALTER TABLE test ADD COLUMN", StatementTypeAlter},
			{"DROP TABLE test", StatementTypeDrop},
			{"MERGE INTO target USING source", StatementTypeMerge},
			{"COPY INTO table FROM stage", StatementTypeCopy},
			{"SELECT * FROM table", StatementTypeUnknown},
		}

		for _, test := range tests {
			result := detectStatementType(test.sql)
			if result != test.expected {
				t.Errorf("SQL: %s, expected %v, got %v", test.sql, test.expected, result)
			}
		}
	})
}

// TestStreamProcessor tests streaming functionality
func TestStreamProcessor(t *testing.T) {
	t.Run("chunk reading", func(t *testing.T) {
		// Create test SQL content
		sqlContent := `
CREATE TABLE test1 (id INT);
INSERT INTO test1 VALUES (1);
INSERT INTO test1 VALUES (2);
CREATE TABLE test2 (
    id INT,
    name VARCHAR(100)
);
INSERT INTO test2 VALUES (1, 'test');
`
		reader := strings.NewReader(sqlContent)
		streamReader := &StreamReader{
			reader:       bufio.NewReader(reader),
			maxChunkSize: 1024,
			metrics:      &StreamMetrics{},
		}

		// Read chunk
		chunk, err := streamReader.ReadChunk()
		if err != nil {
			t.Fatalf("Failed to read chunk: %v", err)
		}

		// Should have parsed statements
		if len(chunk.Statements) < 2 {
			t.Errorf("Expected at least 2 statements, got %d", len(chunk.Statements))
		}

		// Verify first statement
		expectedFirst := "CREATE TABLE test1 (id INT)"
		if chunk.Statements[0] != expectedFirst {
			t.Errorf("Expected first statement: %s, got: %s", expectedFirst, chunk.Statements[0])
		}
	})
}

// TestMonitor tests performance monitoring
func TestMonitor(t *testing.T) {
	config := MonitorConfig{
		CollectionInterval: 100 * time.Millisecond,
		HistorySize:        100,
	}

	monitor := NewMonitor(config)
	defer monitor.Stop()

	t.Run("metric recording", func(t *testing.T) {
		// Record metrics
		monitor.RecordMetric("test.value", 10.5, "ms", nil)
		monitor.RecordMetric("test.value", 15.2, "ms", nil)
		monitor.RecordMetric("test.value", 12.8, "ms", nil)

		// Get metric
		metric, found := monitor.GetMetric("test.value")
		if !found {
			t.Fatal("Expected to find metric")
		}

		// Check calculations
		if metric.Count != 3 {
			t.Errorf("Expected count 3, got %d", metric.Count)
		}
		if metric.Min != 10.5 {
			t.Errorf("Expected min 10.5, got %f", metric.Min)
		}
		if metric.Max != 15.2 {
			t.Errorf("Expected max 15.2, got %f", metric.Max)
		}
		
		expectedAvg := (10.5 + 15.2 + 12.8) / 3
		if abs(metric.Avg-expectedAvg) > 0.01 {
			t.Errorf("Expected avg %f, got %f", expectedAvg, metric.Avg)
		}
	})

	t.Run("timer functionality", func(t *testing.T) {
		timer := monitor.StartTimer("operation.duration", nil)
		time.Sleep(100 * time.Millisecond)
		duration := timer.Stop()

		if duration < 100*time.Millisecond {
			t.Errorf("Expected duration >= 100ms, got %v", duration)
		}

		// Check recorded metric
		metric, found := monitor.GetMetric("operation.duration")
		if !found {
			t.Fatal("Expected to find timer metric")
		}
		if metric.Value < 0.1 {
			t.Errorf("Expected value >= 0.1, got %f", metric.Value)
		}
	})

	t.Run("resource stats collection", func(t *testing.T) {
		// Wait for collection
		time.Sleep(200 * time.Millisecond)

		stats := monitor.GetResourceStats()
		
		// Should have collected goroutine count
		if stats.GoroutineCount == 0 {
			t.Error("Expected goroutine count > 0")
		}

		// Should have memory stats
		if stats.HeapAllocMB == 0 {
			t.Error("Expected heap alloc > 0")
		}
	})
}

// TestOptimizedService tests the integrated optimized service
func TestOptimizedService(t *testing.T) {
	t.Run("deployment strategy selection", func(t *testing.T) {
		_ = snowflake.Config{
			Account:  "test",
			Username: "test",
			Password: "test",
			Database: "TEST_DB",
			Schema:   "PUBLIC",
		}

		_ = DefaultOptimizedConfig()
		
		// Would need mock implementation
		t.Skip("Requires mock Snowflake connection")
		
		// service, err := NewOptimizedService(sfConfig, optConfig)
		// if err != nil {
		// 	t.Fatalf("Failed to create service: %v", err)
		// }
		// defer service.Shutdown(5 * time.Second)

		// Test strategy selection logic
		// Small files should use parallel
		// Large files should use streaming
		// Many small files should use batching
	})
}

// Benchmark tests
func BenchmarkConnectionPool(b *testing.B) {
	b.Skip("Requires mock implementation")
	
	// pool := createMockConnectionPool()
	// defer pool.Close()
	
	// ctx := context.Background()
	
	// b.ResetTimer()
	// b.RunParallel(func(pb *testing.PB) {
	// 	for pb.Next() {
	// 		conn, err := pool.Get(ctx)
	// 		if err != nil {
	// 			b.Fatal(err)
	// 		}
	// 		pool.Put(conn)
	// 	}
	// })
}

func BenchmarkCache(b *testing.B) {
	cache := NewCache(DefaultCacheConfig())
	defer cache.Stop()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		cache.Set(key, value, 100)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%1000)
			cache.Get(key)
			i++
		}
	})
}

// Helper functions

// MockPooledConnection implements a mock connection for testing
type MockPooledConnection struct {
	*PooledConnection
}

func (m *MockPooledConnection) Execute(ctx context.Context, query string) (sql.Result, error) {
	return mockResult{}, nil
}

type mockResult struct{}

func (m mockResult) LastInsertId() (int64, error) { return 0, nil }
func (m mockResult) RowsAffected() (int64, error) { return 1, nil }

func createMockConnectionPool() *ConnectionPool {
	// Create mock connections
	pool := &ConnectionPool{
		connections: make(chan *PooledConnection, 5),
		maxSize:     5,
		metrics:     &PoolMetrics{},
	}
	
	// Pre-populate with mock connections using sql.Open with a dummy driver
	// In real tests, we would use sqlmock
	for i := 0; i < 5; i++ {
		conn := &PooledConnection{
			db:        nil, // Would be a real mock in proper tests
			id:        i,
			createdAt: time.Now(),
			pool:      pool,
		}
		pool.connections <- conn
	}
	
	return pool
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}