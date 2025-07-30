//go:build example
// +build example

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"flakedrop/internal/snowflake"
)

func main() {
	// Configure Snowflake connection
	config := snowflake.Config{
		Account:   os.Getenv("SNOWFLAKE_ACCOUNT"),
		Username:  os.Getenv("SNOWFLAKE_USER"),
		Password:  os.Getenv("SNOWFLAKE_PASSWORD"),
		Database:  os.Getenv("SNOWFLAKE_DATABASE"),
		Schema:    os.Getenv("SNOWFLAKE_SCHEMA"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Role:      os.Getenv("SNOWFLAKE_ROLE"),
		Timeout:   30 * time.Second,
	}

	// Create enhanced service
	service, err := snowflake.NewEnhancedService(config)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}
	defer service.Close()

	ctx := context.Background()

	// Demonstrate various optimizations
	fmt.Println("=== Snowflake Operations Optimization Demo ===\n")

	// 1. Query Optimization
	demoQueryOptimization(ctx, service)

	// 2. Async Query Execution
	demoAsyncQueries(ctx, service)

	// 3. Bulk Operations
	demoBulkOperations(ctx, service)

	// 4. Connection Pool Management
	demoConnectionManagement(ctx, service)

	// 5. Monitoring and Alerts
	demoMonitoring(ctx, service)

	// 6. Performance Metrics
	demoPerformanceMetrics(service)
}

func demoQueryOptimization(ctx context.Context, service *snowflake.EnhancedService) {
	fmt.Println("1. Query Optimization Demo")
	fmt.Println("--------------------------")

	// Analyze a poorly written query
	poorQuery := `
		SELECT * 
		FROM orders o
		JOIN customers c
		JOIN products p
		ORDER BY o.order_date DESC
	`

	fmt.Println("Analyzing query for optimization opportunities...")
	
	plan, err := service.GetQueryPlan(ctx, poorQuery)
	if err != nil {
		log.Printf("Error getting query plan: %v", err)
		return
	}

	if plan != nil && len(plan.Suggestions) > 0 {
		fmt.Println("\nOptimization Suggestions:")
		for _, suggestion := range plan.Suggestions {
			fmt.Printf("- [%s] %s: %s\n", 
				suggestion.Severity, 
				suggestion.Type, 
				suggestion.Description)
			fmt.Printf("  Impact: %s\n", suggestion.Impact)
		}
	}

	// Execute optimized query
	fmt.Println("\nExecuting optimized query...")
	rows, err := service.ExecuteOptimized(ctx, poorQuery)
	if err != nil {
		log.Printf("Error executing query: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Query executed successfully with optimizations applied.\n")
}

func demoAsyncQueries(ctx context.Context, service *snowflake.EnhancedService) {
	fmt.Println("2. Async Query Execution Demo")
	fmt.Println("-----------------------------")

	// Submit multiple queries asynchronously
	queries := []string{
		"SELECT COUNT(*) FROM orders WHERE order_date >= '2024-01-01'",
		"SELECT customer_id, SUM(amount) FROM transactions GROUP BY customer_id",
		"SELECT product_id, COUNT(*) FROM order_items GROUP BY product_id ORDER BY 2 DESC LIMIT 10",
	}

	fmt.Println("Submitting queries asynchronously...")
	
	asyncQueries, err := service.ExecuteBatch(ctx, queries)
	if err != nil {
		log.Printf("Error submitting batch: %v", err)
		return
	}

	// Wait for all queries to complete
	fmt.Println("Waiting for queries to complete...")
	
	for i, asyncQuery := range asyncQueries {
		if asyncQuery == nil {
			continue
		}

		result, err := service.AsyncExecutor().Wait(asyncQuery)
		if err != nil {
			log.Printf("Query %d failed: %v", i+1, err)
			continue
		}

		fmt.Printf("Query %d completed in %v\n", i+1, result.ExecutionTime)
		fmt.Printf("  - Bytes scanned: %d\n", result.Metadata.BytesScanned)
		fmt.Printf("  - Credits used: %.4f\n", result.Metadata.CreditsUsed)
		
		if result.Rows != nil {
			result.Rows.Close()
		}
	}
	fmt.Println()
}

func demoBulkOperations(ctx context.Context, service *snowflake.EnhancedService) {
	fmt.Println("3. Bulk Operations Demo")
	fmt.Println("-----------------------")

	// Configure bulk insert
	options := snowflake.BulkInsertOptions{
		FileFormat:      "CSV",
		Compression:     "GZIP",
		FieldDelimiter:  ",",
		RecordDelimiter: "\n",
		SkipHeader:      1,
		BatchSize:       10000,
		Parallelism:     4,
		UsePut:          true,
		PurgeAfterLoad:  true,
		ContinueOnError: true,
		MaxErrors:       100,
	}

	// Simulate bulk load
	fmt.Println("Configuring bulk load with:")
	fmt.Printf("  - Format: %s\n", options.FileFormat)
	fmt.Printf("  - Compression: %s\n", options.Compression)
	fmt.Printf("  - Batch size: %d\n", options.BatchSize)
	fmt.Printf("  - Parallelism: %d\n", options.Parallelism)
	
	// Example files (would be actual data files in production)
	files := []string{
		"data/orders_2024_01.csv.gz",
		"data/orders_2024_02.csv.gz",
		"data/orders_2024_03.csv.gz",
	}

	fmt.Printf("\nBulk loading %d files...\n", len(files))
	
	startTime := time.Now()
	result, err := service.BulkInsert(ctx, "orders_staging", files, options)
	
	if err != nil {
		log.Printf("Bulk insert error: %v", err)
		// In production, would handle partial failures
	} else if result != nil {
		fmt.Printf("\nBulk Load Results:\n")
		fmt.Printf("  - Status: %s\n", result.Status)
		fmt.Printf("  - Duration: %v\n", result.Duration)
		fmt.Printf("  - Rows loaded: %d\n", result.RowsLoaded)
		fmt.Printf("  - Rows rejected: %d\n", result.RowsRejected)
		fmt.Printf("  - Load rate: %.0f rows/sec\n", 
			float64(result.RowsLoaded)/time.Since(startTime).Seconds())
	}
	fmt.Println()
}

func demoConnectionManagement(ctx context.Context, service *snowflake.EnhancedService) {
	fmt.Println("4. Connection Pool Management Demo")
	fmt.Println("----------------------------------")

	// Get warehouse recommendations
	recommendations := service.GetWarehouseRecommendations()
	
	if len(recommendations) > 0 {
		fmt.Println("Warehouse Recommendations:")
		for warehouse, recommendation := range recommendations {
			fmt.Printf("  - %s: %s\n", warehouse, recommendation)
		}
	}

	// Demonstrate optimal warehouse selection
	queries := map[string]string{
		"Simple query":      "SELECT COUNT(*) FROM small_table",
		"Complex analytics": "SELECT customer_id, product_id, SUM(amount) FROM large_fact_table GROUP BY 1,2",
		"Heavy JOIN":        "SELECT * FROM t1 JOIN t2 JOIN t3 JOIN t4 WHERE complex_conditions",
	}

	fmt.Println("\nOptimal warehouse selection:")
	for queryType, sql := range queries {
		warehouse := service.ConnManager().WarehouseOptimizer().RecommendWarehouse(sql, "medium")
		fmt.Printf("  - %s -> %s\n", queryType, warehouse)
	}
	fmt.Println()
}

func demoMonitoring(ctx context.Context, service *snowflake.EnhancedService) {
	fmt.Println("5. Monitoring and Alerts Demo")
	fmt.Println("-----------------------------")

	// Create monitor
	monitorConfig := snowflake.DefaultMonitorConfig()
	monitorConfig.AlertThresholds.SlowQueryDuration = 30 * time.Second
	monitorConfig.AlertThresholds.HighCreditUsage = 100.0
	
	monitor := snowflake.NewMonitor(service.Service, monitorConfig)

	// Register alert handlers
	monitor.AlertManager().RegisterHandler(snowflake.AlertTypeSlowQuery, 
		func(alert snowflake.Alert) error {
			fmt.Printf("\n🚨 ALERT: %s\n", alert.Title)
			fmt.Printf("   %s\n", alert.Description)
			return nil
		})

	monitor.AlertManager().RegisterHandler(snowflake.AlertTypeHighCreditUsage,
		func(alert snowflake.Alert) error {
			fmt.Printf("\n💰 COST ALERT: %s\n", alert.Title)
			fmt.Printf("   %s\n", alert.Description)
			return nil
		})

	// Start monitoring
	monitorCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	if err := monitor.Start(monitorCtx); err != nil {
		log.Printf("Error starting monitor: %v", err)
		return
	}

	fmt.Println("Monitoring active. Alerts will be displayed if thresholds are exceeded.")
	
	// Get dashboard data
	dashboard := monitor.GetDashboardData()
	fmt.Printf("\nCurrent Status:\n")
	fmt.Printf("  - Total queries: %d\n", dashboard.QuerySummary.TotalQueries)
	fmt.Printf("  - Average execution time: %v\n", dashboard.QuerySummary.AvgExecutionTime)
	fmt.Printf("  - Active warehouses: %d\n", dashboard.WarehouseSummary.ActiveWarehouses)
	fmt.Printf("  - Credits used today: %.2f\n", dashboard.CreditSummary.TotalCredits)
	fmt.Println()
}

func demoPerformanceMetrics(service *snowflake.EnhancedService) {
	fmt.Println("6. Performance Metrics Demo")
	fmt.Println("---------------------------")

	// Get service metrics
	metrics := service.GetServiceMetrics()
	
	fmt.Printf("Service Performance Metrics:\n")
	fmt.Printf("  - Total queries executed: %d\n", metrics.TotalQueries)
	fmt.Printf("  - Average query duration: %v\n", metrics.AvgQueryDuration)
	fmt.Printf("  - Average bytes scanned: %d\n", metrics.AvgBytesScanned)
	fmt.Printf("  - Success rate: %.2f%%\n", metrics.SuccessRate)
	fmt.Printf("  - Cache hit rate: %.2f%%\n", metrics.CacheHitRate)
	
	// Get slow queries
	slowQueries := service.GetSlowQueries(1 * time.Minute)
	if len(slowQueries) > 0 {
		fmt.Printf("\nSlow Queries (>1 minute):\n")
		for i, query := range slowQueries {
			if i >= 5 {
				break // Show only top 5
			}
			fmt.Printf("  %d. Query ID: %s\n", i+1, query.QueryID)
			fmt.Printf("     Duration: %v\n", query.ExecutionTime)
			fmt.Printf("     Warehouse: %s\n", query.WarehouseName)
		}
	}
	
	fmt.Println("\n=== Demo Complete ===")
}

// Helper to get AsyncExecutor (would be exposed in real implementation)
func (es *snowflake.EnhancedService) AsyncExecutor() *snowflake.AsyncQueryExecutor {
	return es.asyncExecutor
}

// Helper to get ConnectionManager (would be exposed in real implementation)
func (es *snowflake.EnhancedService) ConnManager() *snowflake.ConnectionManager {
	return es.connManager
}

// Helper to get AlertManager from Monitor
func (m *snowflake.Monitor) AlertManager() *snowflake.AlertManager {
	return m.alertManager
}