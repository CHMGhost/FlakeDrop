package snowflake

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEnhancedService(t *testing.T) {
	// Skip if no Snowflake credentials
	if testing.Short() {
		t.Skip("Skipping Snowflake integration test")
	}

	config := Config{
		Account:   "test_account",
		Username:  "test_user",
		Password:  "test_pass",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
		Warehouse: "COMPUTE_WH",
		Role:      "SYSADMIN",
		Timeout:   30 * time.Second,
	}

	service, err := NewEnhancedService(config)
	if err != nil {
		t.Fatalf("Failed to create enhanced service: %v", err)
	}
	defer service.Close()

	ctx := context.Background()

	t.Run("OptimizedQuery", func(t *testing.T) {
		// Test query optimization
		sql := "SELECT * FROM large_table WHERE status = 'active'"
		
		plan, err := service.GetQueryPlan(ctx, sql)
		if err != nil {
			t.Errorf("Failed to get query plan: %v", err)
		}

		if plan != nil && len(plan.Suggestions) > 0 {
			t.Logf("Query optimization suggestions:")
			for _, suggestion := range plan.Suggestions {
				t.Logf("- %s: %s", suggestion.Type, suggestion.Description)
			}
		}
	})

	t.Run("AsyncQuery", func(t *testing.T) {
		// Test async query execution
		sql := "SELECT COUNT(*) FROM orders WHERE order_date > '2024-01-01'"
		
		asyncQuery, err := service.ExecuteAsync(ctx, sql, 
			WithWarehouse("COMPUTE_WH_LARGE"),
			WithQueryTag("test_async_query"),
		)
		if err != nil {
			t.Errorf("Failed to submit async query: %v", err)
		}

		// Wait for completion
		result, err := service.asyncExecutor.Wait(asyncQuery)
		if err != nil {
			t.Errorf("Failed to wait for async query: %v", err)
		}

		if result != nil {
			t.Logf("Async query completed in %v", result.ExecutionTime)
			t.Logf("Bytes scanned: %d", result.Metadata.BytesScanned)
		}
	})

	t.Run("BulkInsert", func(t *testing.T) {
		// Test bulk insert
		options := BulkInsertOptions{
			FileFormat:      "CSV",
			Compression:     "GZIP",
			FieldDelimiter:  ",",
			SkipHeader:      1,
			ContinueOnError: true,
			PurgeAfterLoad:  true,
		}

		files := []string{"data/orders_2024.csv.gz"}
		
		result, err := service.BulkInsert(ctx, "orders", files, options)
		if err != nil {
			t.Errorf("Failed to bulk insert: %v", err)
		}

		if result != nil {
			t.Logf("Bulk insert results:")
			t.Logf("- Rows loaded: %d", result.RowsLoaded)
			t.Logf("- Rows rejected: %d", result.RowsRejected)
			t.Logf("- Duration: %v", result.Duration)
		}
	})

	t.Run("PreparedStatement", func(t *testing.T) {
		// Test prepared statements
		sql := "SELECT * FROM customers WHERE customer_id = ?"
		
		stmt, err := service.PrepareStatement(ctx, sql)
		if err != nil {
			t.Errorf("Failed to prepare statement: %v", err)
		}

		// Execute prepared statement multiple times
		customerIDs := []int{1001, 1002, 1003}
		for _, id := range customerIDs {
			rows, err := service.ExecutePrepared(ctx, stmt, id)
			if err != nil {
				t.Errorf("Failed to execute prepared statement: %v", err)
				continue
			}
			rows.Close()
		}
	})

	t.Run("ServiceMetrics", func(t *testing.T) {
		// Get service metrics
		metrics := service.GetServiceMetrics()
		
		t.Logf("Service metrics:")
		t.Logf("- Total queries: %d", metrics.TotalQueries)
		t.Logf("- Average query duration: %v", metrics.AvgQueryDuration)
		t.Logf("- Success rate: %.2f%%", metrics.SuccessRate)
	})
}

func TestQueryOptimizer(t *testing.T) {
	service := &Service{}
	optimizer := NewQueryOptimizer(service)

	testCases := []struct {
		name     string
		sql      string
		expected []string // Expected suggestion types
	}{
		{
			name:     "SelectStar",
			sql:      "SELECT * FROM orders",
			expected: []string{"select_star"},
		},
		{
			name:     "MissingFilter",
			sql:      "SELECT col1, col2 FROM large_table",
			expected: []string{"missing_filter"},
		},
		{
			name:     "OrderWithoutLimit",
			sql:      "SELECT id FROM users ORDER BY created_at DESC",
			expected: []string{"order_without_limit"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plan := &QueryPlan{
				SQL: tc.sql,
				Operations: []PlanOperation{
					{Operation: "TableScan", Object: "large_table", EstRows: 2000000},
				},
			}

			suggestions := optimizer.generateSuggestions(tc.sql, plan)
			
			// Check expected suggestions
			suggestionTypes := make(map[string]bool)
			for _, s := range suggestions {
				suggestionTypes[s.Type] = true
			}

			for _, expected := range tc.expected {
				if !suggestionTypes[expected] {
					t.Errorf("Expected suggestion type %s not found", expected)
				}
			}
		})
	}
}

func TestConnectionManager(t *testing.T) {
	config := Config{
		Account:   "test_account",
		Username:  "test_user",
		Password:  "test_pass",
		Database:  "TEST_DB",
		Schema:    "PUBLIC",
		Warehouse: "COMPUTE_WH",
		Role:      "SYSADMIN",
	}

	manager := NewConnectionManager(config)
	defer manager.Close()

	t.Run("GetConnection", func(t *testing.T) {
		ctx := context.Background()
		
		// Get connection from default warehouse
		conn1, err := manager.GetConnection(ctx, "")
		if err != nil {
			t.Skipf("Skipping: %v", err)
		}
		defer manager.ReleaseConnection(conn1)

		// Get connection from specific warehouse
		conn2, err := manager.GetConnection(ctx, "COMPUTE_WH_LARGE")
		if err != nil {
			t.Skipf("Skipping: %v", err)
		}
		defer manager.ReleaseConnection(conn2)

		// Verify different warehouses
		if conn1.warehouse == conn2.warehouse {
			t.Error("Expected different warehouses")
		}
	})

	t.Run("WarehouseRecommendations", func(t *testing.T) {
		// Test warehouse recommendations
		recommendations := map[string]string{
			"SELECT * FROM small_table":          "COMPUTE_WH_SMALL",
			"SELECT * FROM t1 JOIN t2 GROUP BY x ORDER BY y": "COMPUTE_WH_MEDIUM",
		}

		for sql, expected := range recommendations {
			warehouse := manager.warehouseOptimizer.RecommendWarehouse(sql, "medium")
			if warehouse != expected {
				t.Errorf("Expected warehouse %s for query, got %s", expected, warehouse)
			}
		}
	})
}

func TestBulkOperations(t *testing.T) {
	service := &Service{}
	connManager := NewConnectionManager(Config{})
	bulkOps := NewBulkOperations(service, connManager)

	t.Run("ValidateOptions", func(t *testing.T) {
		// Test option validation
		options := BulkInsertOptions{
			FileFormat: "INVALID",
		}

		err := bulkOps.validateOptions(options)
		if err == nil {
			t.Error("Expected validation error for invalid format")
		}

		// Valid options
		options = BulkInsertOptions{
			Database:   "DB",
			Schema:     "SCHEMA",
			Table:      "TABLE",
			FileFormat: "CSV",
		}

		err = bulkOps.validateOptions(options)
		if err != nil {
			t.Errorf("Unexpected validation error: %v", err)
		}
	})

	t.Run("FileFormatOptions", func(t *testing.T) {
		options := BulkInsertOptions{
			FileFormat:      "CSV",
			Compression:     "GZIP",
			FieldDelimiter:  "|",
			RecordDelimiter: "\n",
			SkipHeader:      1,
		}

		formatOptions := bulkOps.buildFileFormatOptions(options)
		
		if !strings.Contains(formatOptions, "COMPRESSION = GZIP") {
			t.Error("Expected compression option")
		}
		if !strings.Contains(formatOptions, "FIELD_DELIMITER = '|'") {
			t.Error("Expected field delimiter option")
		}
		if !strings.Contains(formatOptions, "SKIP_HEADER = 1") {
			t.Error("Expected skip header option")
		}
	})
}

func TestMonitoring(t *testing.T) {
	service := &Service{}
	config := DefaultMonitorConfig()
	monitor := NewMonitor(service, config)

	t.Run("AlertThresholds", func(t *testing.T) {
		// Test query alert detection
		query := MonitoredQuery{
			QueryID:       "test_query_1",
			ExecutionTime: 10 * time.Minute, // Exceeds threshold
			QueuedTime:    45 * time.Second,  // Exceeds threshold
			SpillingBytes: 2 * 1024 * 1024 * 1024, // 2GB
		}

		// Check alerts would be triggered
		monitor.checkQueryAlerts(query)
		
		// Verify alerts
		alerts := monitor.alertManager.GetRecentAlerts(10)
		if len(alerts) < 2 {
			t.Error("Expected at least 2 alerts to be triggered")
		}
	})

	t.Run("DashboardData", func(t *testing.T) {
		// Test dashboard data generation
		data := monitor.GetDashboardData()
		
		if data.Timestamp.IsZero() {
			t.Error("Expected dashboard timestamp to be set")
		}
		
		// Verify structure
		_ = data.QuerySummary
		_ = data.WarehouseSummary
		_ = data.CreditSummary
		_ = data.RecentAlerts
	})
}

// Benchmark tests

func BenchmarkPreparedStatements(b *testing.B) {
	cache := NewPreparedStatementCache(100)
	
	// Pre-populate cache
	for i := 0; i < 50; i++ {
		sql := fmt.Sprintf("SELECT * FROM table_%d WHERE id = ?", i)
		stmt := &PreparedStatement{
			ID:         fmt.Sprintf("stmt_%d", i),
			SQL:        sql,
			CreatedAt:  time.Now(),
			LastUsedAt: time.Now(),
		}
		cache.Set(sql, stmt)
	}

	b.ResetTimer()

	b.Run("CacheHit", func(b *testing.B) {
		sql := "SELECT * FROM table_25 WHERE id = ?"
		for i := 0; i < b.N; i++ {
			_ = cache.Get(sql)
		}
	})

	b.Run("CacheMiss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sql := fmt.Sprintf("SELECT * FROM table_%d WHERE id = ?", i+100)
			_ = cache.Get(sql)
		}
	})
}

func BenchmarkQueryOptimization(b *testing.B) {
	service := &Service{}
	optimizer := NewQueryOptimizer(service)
	
	queries := []string{
		"SELECT * FROM orders WHERE status = 'pending'",
		"SELECT id, name FROM customers ORDER BY created_at",
		"SELECT COUNT(*) FROM transactions GROUP BY user_id",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sql := queries[i%len(queries)]
		plan := &QueryPlan{SQL: sql}
		_ = optimizer.generateSuggestions(sql, plan)
	}
}