package performance

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"flakedrop/internal/testutil"
)

// TestLargeScaleDeployment tests deployment performance with large numbers of files
func TestLargeScaleDeployment(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	scenarios := []struct {
		name           string
		fileCount      int
		avgFileSize    int // lines of SQL
		concurrency    int
		expectedMaxDur time.Duration
	}{
		{
			name:           "Small deployment",
			fileCount:      10,
			avgFileSize:    50,
			concurrency:    1,
			expectedMaxDur: 5 * time.Second,
		},
		{
			name:           "Medium deployment",
			fileCount:      100,
			avgFileSize:    100,
			concurrency:    5,
			expectedMaxDur: 30 * time.Second,
		},
		{
			name:           "Large deployment",
			fileCount:      1000,
			avgFileSize:    200,
			concurrency:    10,
			expectedMaxDur: 2 * time.Minute,
		},
		{
			name:           "Extra large deployment",
			fileCount:      5000,
			avgFileSize:    150,
			concurrency:    20,
			expectedMaxDur: 5 * time.Minute,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup
			snowflakeService := testutil.NewMockSnowflakeService()
			snowflakeService.TestConnection()

			gitService := testutil.NewMockGitService()
			generator := testutil.NewTestDataGenerator()

			// Generate files
			var files []string
			fileContents := make(map[string]string)

			for i := 0; i < scenario.fileCount; i++ {
				filename := fmt.Sprintf("sql/perf/file_%05d.sql", i)
				files = append(files, filename)
				
				// Generate SQL content
				content := generateSQLContent(generator, scenario.avgFileSize)
				fileContents[filename] = content
			}

			// Add commit with all files
			gitService.AddCommit(testutil.MockCommit{
				Message: fmt.Sprintf("Performance test - %d files", scenario.fileCount),
				Files:   files,
			})

			// Setup file content responses
			for file, content := range fileContents {
				gitService.SetFileContent(file, content)
			}

			// Measure deployment
			start := time.Now()
			
			// Deployment metrics
			var (
				filesProcessed int64
				errors         int64
				bytesProcessed int64
			)

			// Create worker pool
			fileChan := make(chan string, scenario.fileCount)
			var wg sync.WaitGroup

			// Start workers
			for i := 0; i < scenario.concurrency; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for file := range fileChan {
						content := fileContents[file]
						atomic.AddInt64(&bytesProcessed, int64(len(content)))

						err := snowflakeService.DB.Execute(content, "PERF_DB", "PUBLIC")
						if err != nil {
							atomic.AddInt64(&errors, 1)
						} else {
							atomic.AddInt64(&filesProcessed, 1)
						}
					}
				}(i)
			}

			// Send files to workers
			for _, file := range files {
				fileChan <- file
			}
			close(fileChan)

			// Wait for completion
			wg.Wait()
			duration := time.Since(start)

			// Calculate metrics
			throughput := float64(filesProcessed) / duration.Seconds()
			avgLatency := duration / time.Duration(filesProcessed)
			mbProcessed := float64(bytesProcessed) / (1024 * 1024)
			mbPerSecond := mbProcessed / duration.Seconds()

			// Log results
			t.Logf("Performance Metrics for %s:", scenario.name)
			t.Logf("  Total files: %d", scenario.fileCount)
			t.Logf("  Files processed: %d", filesProcessed)
			t.Logf("  Errors: %d", errors)
			t.Logf("  Duration: %v", duration)
			t.Logf("  Throughput: %.2f files/sec", throughput)
			t.Logf("  Average latency: %v per file", avgLatency)
			t.Logf("  Data processed: %.2f MB", mbProcessed)
			t.Logf("  Data throughput: %.2f MB/sec", mbPerSecond)

			// Assertions
			if duration > scenario.expectedMaxDur {
				t.Errorf("Deployment took too long: %v (expected < %v)", duration, scenario.expectedMaxDur)
			}

			if errors > 0 {
				t.Errorf("Deployment had %d errors", errors)
			}

			if filesProcessed != int64(scenario.fileCount) {
				t.Errorf("Not all files processed: %d/%d", filesProcessed, scenario.fileCount)
			}
		})
	}
}

// TestConcurrentDeployments tests multiple concurrent deployments
func TestConcurrentDeployments(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Test configuration
	numDeployments := 10
	filesPerDeployment := 50
	maxConcurrent := 5

	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()

	// Create semaphore for concurrency control
	sem := make(chan struct{}, maxConcurrent)
	
	// Metrics
	var (
		totalDurationNs int64  // nanoseconds for atomic operations
		totalFiles      int64
		completedDeploys int64
		failedDeploys   int64
	)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < numDeployments; i++ {
		wg.Add(1)
		
		go func(deployID int) {
			defer wg.Done()
			
			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			deployStart := time.Now()

			// Create deployment-specific git service
			gitService := testutil.NewMockGitService()
			
			// Generate files for this deployment
			var files []string
			for j := 0; j < filesPerDeployment; j++ {
				files = append(files, fmt.Sprintf("sql/deploy%d/file%d.sql", deployID, j))
			}

			gitService.AddCommit(testutil.MockCommit{
				Message: fmt.Sprintf("Deployment %d", deployID),
				Files:   files,
			})

			// Execute deployment
			successCount := 0
			commits, _ := gitService.GetCommits(1)
			deployFiles, _ := gitService.GetCommitFiles(commits[0].Hash)

			for _, file := range deployFiles {
				content, _ := gitService.GetFileContent(commits[0].Hash, file)
				err := snowflakeService.DB.Execute(content, fmt.Sprintf("DB_%d", deployID), "PUBLIC")
				
				if err == nil {
					successCount++
					atomic.AddInt64(&totalFiles, 1)
				}
			}

			deployDuration := time.Since(deployStart)
			atomic.AddInt64(&totalDurationNs, deployDuration.Nanoseconds())

			if successCount == len(files) {
				atomic.AddInt64(&completedDeploys, 1)
			} else {
				atomic.AddInt64(&failedDeploys, 1)
			}

			t.Logf("Deployment %d completed in %v (%d/%d files)", deployID, deployDuration, successCount, len(files))
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(start)

	// Calculate metrics
	avgDeployTime := time.Duration(totalDurationNs) / time.Duration(numDeployments)
	totalFilesExpected := int64(numDeployments * filesPerDeployment)

	t.Logf("\nConcurrent Deployment Summary:")
	t.Logf("  Total deployments: %d", numDeployments)
	t.Logf("  Successful deployments: %d", completedDeploys)
	t.Logf("  Failed deployments: %d", failedDeploys)
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Average deployment time: %v", avgDeployTime)
	t.Logf("  Total files processed: %d/%d", totalFiles, totalFilesExpected)
	t.Logf("  Parallelism efficiency: %.2f%%", float64(totalDurationNs)/float64(totalTime.Nanoseconds())*100/float64(numDeployments))
}

// TestMemoryUsage tests memory consumption during large deployments
func TestMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Force GC before starting
	runtime.GC()
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)

	// Setup large deployment
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()

	gitService := testutil.NewMockGitService()
	
	// Generate large number of files with substantial content
	fileCount := 1000
	var files []string
	
	for i := 0; i < fileCount; i++ {
		files = append(files, fmt.Sprintf("sql/mem/large_file_%04d.sql", i))
	}

	gitService.AddCommit(testutil.MockCommit{
		Message: "Memory test deployment",
		Files:   files,
	})

	// Track memory usage during deployment
	memSamples := []uint64{}
	done := make(chan bool)

	// Start memory monitoring
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				memSamples = append(memSamples, mem.Alloc)
			case <-done:
				return
			}
		}
	}()

	// Execute deployment
	commits, _ := gitService.GetCommits(1)
	deployFiles, _ := gitService.GetCommitFiles(commits[0].Hash)

	for _, file := range deployFiles {
		// Simulate large SQL content
		content := generateLargeSQLContent(10000) // 10k lines
		gitService.SetFileContent(file, content)
		
		err := snowflakeService.DB.Execute(content, "MEM_TEST", "PUBLIC")
		if err != nil {
			t.Errorf("Deployment failed: %v", err)
		}
	}

	// Stop monitoring
	done <- true

	// Force GC and get final memory stats
	runtime.GC()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	// Calculate memory statistics
	var maxMem, avgMem uint64
	for _, sample := range memSamples {
		if sample > maxMem {
			maxMem = sample
		}
		avgMem += sample
	}
	if len(memSamples) > 0 {
		avgMem /= uint64(len(memSamples))
	}

	// Convert to MB for readability
	startMemMB := float64(startMem.Alloc) / 1024 / 1024
	endMemMB := float64(endMem.Alloc) / 1024 / 1024
	maxMemMB := float64(maxMem) / 1024 / 1024
	avgMemMB := float64(avgMem) / 1024 / 1024

	t.Logf("\nMemory Usage Statistics:")
	t.Logf("  Start memory: %.2f MB", startMemMB)
	t.Logf("  End memory: %.2f MB", endMemMB)
	t.Logf("  Peak memory: %.2f MB", maxMemMB)
	t.Logf("  Average memory: %.2f MB", avgMemMB)
	t.Logf("  Memory growth: %.2f MB", endMemMB-startMemMB)
	t.Logf("  GC runs: %d", endMem.NumGC-startMem.NumGC)

	// Memory leak detection
	acceptableGrowth := 100.0 // MB
	if endMemMB-startMemMB > acceptableGrowth {
		t.Errorf("Possible memory leak detected: growth of %.2f MB exceeds threshold of %.2f MB",
			endMemMB-startMemMB, acceptableGrowth)
	}
}

// BenchmarkDeploymentOperations benchmarks various deployment operations
func BenchmarkDeploymentOperations(b *testing.B) {
	snowflakeService := testutil.NewMockSnowflakeService()
	snowflakeService.TestConnection()

	b.Run("SingleFileDeployment", func(b *testing.B) {
		content := generateSQLContent(testutil.NewTestDataGenerator(), 100)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = snowflakeService.DB.Execute(content, "BENCH_DB", "PUBLIC")
		}
	})

	b.Run("ParallelDeployment", func(b *testing.B) {
		contents := make([]string, 10)
		for i := range contents {
			contents[i] = generateSQLContent(testutil.NewTestDataGenerator(), 50)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			for _, content := range contents {
				wg.Add(1)
				go func(sql string) {
					defer wg.Done()
					_ = snowflakeService.DB.Execute(sql, "BENCH_DB", "PUBLIC")
				}(content)
			}
			wg.Wait()
		}
	})

	b.Run("TransactionalDeployment", func(b *testing.B) {
		contents := make([]string, 5)
		for i := range contents {
			contents[i] = generateSQLContent(testutil.NewTestDataGenerator(), 50)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = snowflakeService.DB.BeginTransaction()
			for _, content := range contents {
				_ = snowflakeService.DB.Execute(content, "BENCH_DB", "PUBLIC")
			}
			_ = snowflakeService.DB.Commit()
		}
	})
}

// TestDeploymentLatency measures deployment latency under various conditions
func TestDeploymentLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	scenarios := []struct {
		name         string
		fileSize     int    // lines of SQL
		networkDelay time.Duration
		expectedP99  time.Duration
	}{
		{
			name:         "Small file, low latency",
			fileSize:     10,
			networkDelay: 1 * time.Millisecond,
			expectedP99:  50 * time.Millisecond,
		},
		{
			name:         "Medium file, medium latency",
			fileSize:     100,
			networkDelay: 10 * time.Millisecond,
			expectedP99:  200 * time.Millisecond,
		},
		{
			name:         "Large file, high latency",
			fileSize:     1000,
			networkDelay: 50 * time.Millisecond,
			expectedP99:  1 * time.Second,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup service with simulated network delay
			snowflakeService := testutil.NewMockSnowflakeService()
			snowflakeService.TestConnection()

			// Generate test content
			content := generateSQLContent(testutil.NewTestDataGenerator(), scenario.fileSize)

			// Measure latencies
			iterations := 100
			latencies := make([]time.Duration, iterations)

			for i := 0; i < iterations; i++ {
				// Simulate network delay
				time.Sleep(scenario.networkDelay)

				start := time.Now()
				err := snowflakeService.DB.Execute(content, "LATENCY_TEST", "PUBLIC")
				latencies[i] = time.Since(start)

				if err != nil {
					t.Errorf("Execution failed: %v", err)
				}
			}

			// Calculate percentiles
			p50 := calculatePercentile(latencies, 50)
			p95 := calculatePercentile(latencies, 95)
			p99 := calculatePercentile(latencies, 99)

			t.Logf("Latency statistics for %s:", scenario.name)
			t.Logf("  P50: %v", p50)
			t.Logf("  P95: %v", p95)
			t.Logf("  P99: %v", p99)

			if p99 > scenario.expectedP99 {
				t.Errorf("P99 latency %v exceeds expected %v", p99, scenario.expectedP99)
			}
		})
	}
}

// Helper functions

func generateSQLContent(generator *testutil.TestDataGenerator, lines int) string {
	var content string
	for i := 0; i < lines; i++ {
		switch rand.Intn(5) {
		case 0:
			content += fmt.Sprintf("INSERT INTO table_%d VALUES (%d, '%s', %f);\n",
				rand.Intn(10), generator.NextInt(), generator.NextString("data"), rand.Float64()*1000)
		case 1:
			content += fmt.Sprintf("UPDATE table_%d SET value = %d WHERE id = %d;\n",
				rand.Intn(10), rand.Intn(1000), rand.Intn(100))
		case 2:
			content += fmt.Sprintf("DELETE FROM table_%d WHERE created_at < '2023-01-01';\n",
				rand.Intn(10))
		case 3:
			content += fmt.Sprintf("CREATE INDEX idx_%s ON table_%d(column_%d);\n",
				generator.NextString("idx"), rand.Intn(10), rand.Intn(5))
		default:
			content += fmt.Sprintf("SELECT * FROM table_%d WHERE status = 'active';\n",
				rand.Intn(10))
		}
	}
	return content
}

func generateLargeSQLContent(lines int) string {
	var content string
	for i := 0; i < lines; i++ {
		// Generate realistic looking SQL with some complexity
		content += fmt.Sprintf(`
-- Query %d: Complex join operation
SELECT 
    t1.id,
    t1.name,
    t1.created_at,
    t2.value,
    t3.status,
    COUNT(t4.id) as related_count,
    SUM(t5.amount) as total_amount
FROM table_1 t1
    JOIN table_2 t2 ON t1.id = t2.table_1_id
    LEFT JOIN table_3 t3 ON t2.id = t3.table_2_id
    LEFT JOIN table_4 t4 ON t3.id = t4.table_3_id
    LEFT JOIN table_5 t5 ON t4.id = t5.table_4_id
WHERE 
    t1.status = 'active'
    AND t2.created_at >= DATEADD(day, -30, CURRENT_DATE())
    AND (t3.type IN ('A', 'B', 'C') OR t3.type IS NULL)
GROUP BY 
    t1.id, t1.name, t1.created_at, t2.value, t3.status
HAVING 
    COUNT(t4.id) > 0 OR SUM(t5.amount) > 1000
ORDER BY 
    t1.created_at DESC, total_amount DESC
LIMIT 1000;

`, i)
	}
	return content
}

func calculatePercentile(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Sort latencies
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Calculate percentile index
	index := (percentile * len(sorted)) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}