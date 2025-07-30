package performance

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	
	"flakedrop/internal/common"
)

// Benchmark provides comprehensive performance benchmarking
type Benchmark struct {
	config   *BenchmarkConfig
	service  *OptimizedService
	results  *BenchmarkResults
	mu       sync.RWMutex
}

// BenchmarkConfig contains benchmark configuration
type BenchmarkConfig struct {
	// Test scenarios
	Scenarios []BenchmarkScenario

	// Test data configuration
	TestDataDir      string
	GenerateTestData bool
	CleanupAfter     bool

	// Execution configuration
	WarmupRuns       int
	TestRuns         int
	ConcurrentUsers  int
	Duration         time.Duration

	// Output configuration
	OutputFormat     string // "json", "csv", "text"
	OutputFile       string
	Verbose          bool
}

// BenchmarkScenario represents a test scenario
type BenchmarkScenario struct {
	Name        string
	Description string
	FileCount   int
	FileSize    FileSizeRange
	Complexity  ComplexityLevel
	Operations  []OperationType
	Weight      int // Relative frequency in mixed workloads
}

// FileSizeRange defines file size parameters
type FileSizeRange struct {
	MinLines int
	MaxLines int
	MinBytes int64
	MaxBytes int64
}

// ComplexityLevel represents SQL complexity
type ComplexityLevel int

const (
	ComplexitySimple ComplexityLevel = iota
	ComplexityMedium
	ComplexityComplex
)

// OperationType represents types of SQL operations
type OperationType int

const (
	OpCreateTable OperationType = iota
	OpInsertData
	OpUpdateData
	OpSelectData
	OpJoinQueries
	OpAggregations
	OpStoredProcs
	OpViews
)

// BenchmarkResults stores benchmark results
type BenchmarkResults struct {
	mu            sync.RWMutex
	StartTime     time.Time
	EndTime       time.Time
	Scenarios     map[string]*ScenarioResult
	SystemMetrics *SystemMetrics
	Summary       *BenchmarkSummary
}

// ScenarioResult stores results for a single scenario
type ScenarioResult struct {
	Name            string
	Runs            []*RunResult
	TotalDuration   time.Duration
	AvgDuration     time.Duration
	MinDuration     time.Duration
	MaxDuration     time.Duration
	Throughput      float64
	ErrorRate       float64
	SuccessRate     float64
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
}

// RunResult stores results for a single run
type RunResult struct {
	ID         int
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
	Success    bool
	Error      error
	Operations int
	Bytes      int64
}

// SystemMetrics stores system resource metrics during benchmark
type SystemMetrics struct {
	CPUUsage    []float64
	MemoryUsage []float64
	Goroutines  []int
	GCPauses    []time.Duration
}

// BenchmarkSummary provides overall summary
type BenchmarkSummary struct {
	TotalRuns          int
	TotalDuration      time.Duration
	TotalOperations    int64
	TotalBytes         int64
	OverallThroughput  float64
	OverallSuccessRate float64
	BestScenario       string
	WorstScenario      string
}

// DefaultBenchmarkConfig returns default benchmark configuration
func DefaultBenchmarkConfig() *BenchmarkConfig {
	return &BenchmarkConfig{
		Scenarios: []BenchmarkScenario{
			{
				Name:        "small_files",
				Description: "Small file deployment",
				FileCount:   100,
				FileSize:    FileSizeRange{MinLines: 10, MaxLines: 50},
				Complexity:  ComplexitySimple,
				Operations:  []OperationType{OpCreateTable, OpInsertData},
				Weight:      30,
			},
			{
				Name:        "large_files",
				Description: "Large file deployment",
				FileCount:   10,
				FileSize:    FileSizeRange{MinLines: 1000, MaxLines: 5000},
				Complexity:  ComplexityMedium,
				Operations:  []OperationType{OpCreateTable, OpInsertData, OpUpdateData},
				Weight:      20,
			},
			{
				Name:        "complex_queries",
				Description: "Complex query execution",
				FileCount:   50,
				FileSize:    FileSizeRange{MinLines: 100, MaxLines: 500},
				Complexity:  ComplexityComplex,
				Operations:  []OperationType{OpJoinQueries, OpAggregations, OpViews},
				Weight:      25,
			},
			{
				Name:        "mixed_workload",
				Description: "Mixed operation workload",
				FileCount:   200,
				FileSize:    FileSizeRange{MinLines: 20, MaxLines: 200},
				Complexity:  ComplexityMedium,
				Operations:  []OperationType{OpCreateTable, OpInsertData, OpUpdateData, OpSelectData},
				Weight:      25,
			},
		},
		TestDataDir:      "benchmark_data",
		GenerateTestData: true,
		CleanupAfter:     true,
		WarmupRuns:       2,
		TestRuns:         10,
		ConcurrentUsers:  1,
		Duration:         5 * time.Minute,
		OutputFormat:     "text",
		Verbose:          false,
	}
}

// NewBenchmark creates a new benchmark instance
func NewBenchmark(service *OptimizedService, config *BenchmarkConfig) *Benchmark {
	if config == nil {
		config = DefaultBenchmarkConfig()
	}

	return &Benchmark{
		config:  config,
		service: service,
		results: &BenchmarkResults{
			Scenarios:     make(map[string]*ScenarioResult),
			SystemMetrics: &SystemMetrics{},
			Summary:       &BenchmarkSummary{},
		},
	}
}

// Run executes the benchmark
func (b *Benchmark) Run(ctx context.Context) error {
	b.results.StartTime = time.Now()

	// Generate test data if needed
	if b.config.GenerateTestData {
		if err := b.generateTestData(); err != nil {
			return fmt.Errorf("failed to generate test data: %w", err)
		}
	}

	// Start system metrics collection
	stopMetrics := b.startMetricsCollection(ctx)
	defer stopMetrics()

	// Run warmup
	if b.config.WarmupRuns > 0 {
		if b.config.Verbose {
			fmt.Printf("Running %d warmup iterations...\n", b.config.WarmupRuns)
		}
		for i := 0; i < b.config.WarmupRuns; i++ {
			b.runScenarios(ctx, true)
		}
	}

	// Run actual benchmarks
	if b.config.Verbose {
		fmt.Printf("Running %d test iterations...\n", b.config.TestRuns)
	}

	var wg sync.WaitGroup
	for i := 0; i < b.config.TestRuns; i++ {
		for j := 0; j < b.config.ConcurrentUsers; j++ {
			wg.Add(1)
			go func(runID int) {
				defer wg.Done()
				b.runScenarios(ctx, false)
			}(i*b.config.ConcurrentUsers + j)
		}
	}
	wg.Wait()

	b.results.EndTime = time.Now()

	// Calculate summary
	b.calculateSummary()

	// Cleanup if configured
	if b.config.CleanupAfter {
		b.cleanup()
	}

	// Output results
	return b.outputResults()
}

// runScenarios executes all scenarios once
func (b *Benchmark) runScenarios(ctx context.Context, isWarmup bool) {
	for _, scenario := range b.config.Scenarios {
		select {
		case <-ctx.Done():
			return
		default:
			b.runScenario(ctx, scenario, isWarmup)
		}
	}
}

// runScenario executes a single scenario
func (b *Benchmark) runScenario(ctx context.Context, scenario BenchmarkScenario, isWarmup bool) {
	runResult := &RunResult{
		ID:        int(time.Now().UnixNano()),
		StartTime: time.Now(),
		Success:   true,
	}

	// Get test files for scenario
	files := b.getScenarioFiles(scenario)
	if len(files) == 0 {
		runResult.Success = false
		runResult.Error = fmt.Errorf("no test files found for scenario %s", scenario.Name)
		return
	}

	// Calculate total size
	for _, file := range files {
		if info, err := os.Stat(file); err == nil {
			runResult.Bytes += info.Size()
		}
	}
	runResult.Operations = len(files)

	// Execute deployment
	err := b.service.DeployFiles(ctx, files, "BENCHMARK_DB", "PUBLIC")
	runResult.EndTime = time.Now()
	runResult.Duration = runResult.EndTime.Sub(runResult.StartTime)

	if err != nil {
		runResult.Success = false
		runResult.Error = err
	}

	// Record result if not warmup
	if !isWarmup {
		b.recordResult(scenario.Name, runResult)
	}
}

// generateTestData generates test SQL files
func (b *Benchmark) generateTestData() error {
	// Create test data directory
	if err := os.MkdirAll(b.config.TestDataDir, common.DirPermissionNormal); err != nil {
		return err
	}

	generator := &TestDataGenerator{
		seed: time.Now().UnixNano(),
	}

	for _, scenario := range b.config.Scenarios {
		scenarioDir := filepath.Join(b.config.TestDataDir, scenario.Name)
		if err := os.MkdirAll(scenarioDir, common.DirPermissionNormal); err != nil {
			return err
		}

		// Generate files for scenario
		for i := 0; i < scenario.FileCount; i++ {
			fileName := filepath.Join(scenarioDir, fmt.Sprintf("file_%04d.sql", i))
			content := generator.GenerateSQL(scenario)
			
			if err := os.WriteFile(fileName, []byte(content), common.FilePermissionSecure); err != nil {
				return err
			}
		}
	}

	return nil
}

// getScenarioFiles returns test files for a scenario
func (b *Benchmark) getScenarioFiles(scenario BenchmarkScenario) []string {
	scenarioDir := filepath.Join(b.config.TestDataDir, scenario.Name)
	pattern := filepath.Join(scenarioDir, "*.sql")
	
	files, _ := filepath.Glob(pattern)
	return files
}

// recordResult records a run result
func (b *Benchmark) recordResult(scenarioName string, result *RunResult) {
	b.results.mu.Lock()
	defer b.results.mu.Unlock()

	scenarioResult, exists := b.results.Scenarios[scenarioName]
	if !exists {
		scenarioResult = &ScenarioResult{
			Name: scenarioName,
			Runs: make([]*RunResult, 0),
		}
		b.results.Scenarios[scenarioName] = scenarioResult
	}

	scenarioResult.Runs = append(scenarioResult.Runs, result)
}

// startMetricsCollection starts collecting system metrics
func (b *Benchmark) startMetricsCollection(ctx context.Context) func() {
	stopChan := make(chan struct{})
	
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Collect system metrics
				// This is simplified - in production, use proper system monitoring
				b.results.SystemMetrics.Goroutines = append(b.results.SystemMetrics.Goroutines, runtime.NumGoroutine())
				
			case <-stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		close(stopChan)
	}
}

// calculateSummary calculates benchmark summary
func (b *Benchmark) calculateSummary() {
	b.results.mu.Lock()
	defer b.results.mu.Unlock()

	summary := b.results.Summary
	var bestThroughput float64
	var worstThroughput float64 = 1e9

	for name, scenario := range b.results.Scenarios {
		// Calculate scenario metrics
		b.calculateScenarioMetrics(scenario)

		// Update summary
		summary.TotalRuns += len(scenario.Runs)
		summary.TotalDuration += scenario.TotalDuration
		
		for _, run := range scenario.Runs {
			summary.TotalOperations += int64(run.Operations)
			summary.TotalBytes += run.Bytes
			if run.Success {
				summary.OverallSuccessRate++
			}
		}

		// Track best/worst scenarios
		if scenario.Throughput > bestThroughput {
			bestThroughput = scenario.Throughput
			summary.BestScenario = name
		}
		if scenario.Throughput < worstThroughput {
			worstThroughput = scenario.Throughput
			summary.WorstScenario = name
		}
	}

	// Calculate overall metrics
	if summary.TotalDuration > 0 {
		summary.OverallThroughput = float64(summary.TotalOperations) / summary.TotalDuration.Seconds()
	}
	if summary.TotalRuns > 0 {
		summary.OverallSuccessRate = (summary.OverallSuccessRate / float64(summary.TotalRuns)) * 100
	}
}

// calculateScenarioMetrics calculates metrics for a scenario
func (b *Benchmark) calculateScenarioMetrics(scenario *ScenarioResult) {
	if len(scenario.Runs) == 0 {
		return
	}

	// Calculate durations
	var totalDuration time.Duration
	var successCount int
	durations := make([]time.Duration, 0, len(scenario.Runs))

	scenario.MinDuration = scenario.Runs[0].Duration
	scenario.MaxDuration = scenario.Runs[0].Duration

	for _, run := range scenario.Runs {
		totalDuration += run.Duration
		durations = append(durations, run.Duration)
		
		if run.Success {
			successCount++
		}
		
		if run.Duration < scenario.MinDuration {
			scenario.MinDuration = run.Duration
		}
		if run.Duration > scenario.MaxDuration {
			scenario.MaxDuration = run.Duration
		}
	}

	scenario.TotalDuration = totalDuration
	scenario.AvgDuration = totalDuration / time.Duration(len(scenario.Runs))
	scenario.SuccessRate = float64(successCount) / float64(len(scenario.Runs)) * 100
	scenario.ErrorRate = 100 - scenario.SuccessRate

	// Calculate throughput
	if totalDuration > 0 {
		totalOps := 0
		for _, run := range scenario.Runs {
			totalOps += run.Operations
		}
		scenario.Throughput = float64(totalOps) / totalDuration.Seconds()
	}

	// Calculate percentiles
	scenario.P50Latency = calculatePercentileDuration(durations, 50)
	scenario.P95Latency = calculatePercentileDuration(durations, 95)
	scenario.P99Latency = calculatePercentileDuration(durations, 99)
}

// cleanup removes test data
func (b *Benchmark) cleanup() {
	os.RemoveAll(b.config.TestDataDir)
}

// outputResults outputs benchmark results
func (b *Benchmark) outputResults() error {
	switch b.config.OutputFormat {
	case "json":
		return b.outputJSON()
	case "csv":
		return b.outputCSV()
	default:
		return b.outputText()
	}
}

// outputText outputs results as formatted text
func (b *Benchmark) outputText() error {
	var output strings.Builder

	output.WriteString("\n=== Snowflake Deploy Performance Benchmark Results ===\n\n")
	output.WriteString(fmt.Sprintf("Total Duration: %v\n", b.results.EndTime.Sub(b.results.StartTime)))
	output.WriteString(fmt.Sprintf("Total Runs: %d\n", b.results.Summary.TotalRuns))
	output.WriteString(fmt.Sprintf("Total Operations: %d\n", b.results.Summary.TotalOperations))
	output.WriteString(fmt.Sprintf("Overall Throughput: %.2f ops/sec\n", b.results.Summary.OverallThroughput))
	output.WriteString(fmt.Sprintf("Overall Success Rate: %.2f%%\n\n", b.results.Summary.OverallSuccessRate))

	// Scenario results
	output.WriteString("Scenario Results:\n")
	output.WriteString(strings.Repeat("-", 80) + "\n")
	
	for _, scenario := range b.results.Scenarios {
		output.WriteString(fmt.Sprintf("\nScenario: %s\n", scenario.Name))
		output.WriteString(fmt.Sprintf("  Runs: %d\n", len(scenario.Runs)))
		output.WriteString(fmt.Sprintf("  Success Rate: %.2f%%\n", scenario.SuccessRate))
		output.WriteString(fmt.Sprintf("  Throughput: %.2f ops/sec\n", scenario.Throughput))
		output.WriteString(fmt.Sprintf("  Avg Duration: %v\n", scenario.AvgDuration))
		output.WriteString(fmt.Sprintf("  Min Duration: %v\n", scenario.MinDuration))
		output.WriteString(fmt.Sprintf("  Max Duration: %v\n", scenario.MaxDuration))
		output.WriteString(fmt.Sprintf("  P50 Latency: %v\n", scenario.P50Latency))
		output.WriteString(fmt.Sprintf("  P95 Latency: %v\n", scenario.P95Latency))
		output.WriteString(fmt.Sprintf("  P99 Latency: %v\n", scenario.P99Latency))
	}

	output.WriteString("\n" + strings.Repeat("-", 80) + "\n")
	output.WriteString(fmt.Sprintf("Best Performing Scenario: %s\n", b.results.Summary.BestScenario))
	output.WriteString(fmt.Sprintf("Worst Performing Scenario: %s\n", b.results.Summary.WorstScenario))

	// Output to file or stdout
	if b.config.OutputFile != "" {
		return os.WriteFile(b.config.OutputFile, []byte(output.String()), common.FilePermissionSecure)
	}
	
	fmt.Print(output.String())
	return nil
}

// outputJSON outputs results as JSON
func (b *Benchmark) outputJSON() error {
	// Implementation omitted for brevity
	return fmt.Errorf("JSON output not implemented")
}

// outputCSV outputs results as CSV
func (b *Benchmark) outputCSV() error {
	// Implementation omitted for brevity
	return fmt.Errorf("CSV output not implemented")
}

// TestDataGenerator generates test SQL data
type TestDataGenerator struct {
	seed int64
	rand *rand.Rand
}

// GenerateSQL generates SQL content for a scenario
func (g *TestDataGenerator) GenerateSQL(scenario BenchmarkScenario) string {
	if g.rand == nil {
		g.rand = rand.New(rand.NewSource(g.seed)) // #nosec G404 - Using math/rand for deterministic test data generation
	}

	var content strings.Builder
	lines := g.rand.Intn(scenario.FileSize.MaxLines-scenario.FileSize.MinLines) + scenario.FileSize.MinLines

	for i := 0; i < lines; i++ {
		opType := scenario.Operations[g.rand.Intn(len(scenario.Operations))]
		content.WriteString(g.generateStatement(opType, scenario.Complexity))
		content.WriteString("\n")
	}

	return content.String()
}

// generateStatement generates a SQL statement
func (g *TestDataGenerator) generateStatement(opType OperationType, complexity ComplexityLevel) string {
	switch opType {
	case OpCreateTable:
		return g.generateCreateTable(complexity)
	case OpInsertData:
		return g.generateInsert(complexity)
	case OpUpdateData:
		return g.generateUpdate(complexity)
	case OpSelectData:
		return g.generateSelect(complexity)
	case OpJoinQueries:
		return g.generateJoin(complexity)
	case OpAggregations:
		return g.generateAggregation(complexity)
	case OpStoredProcs:
		return g.generateStoredProc(complexity)
	case OpViews:
		return g.generateView(complexity)
	default:
		return "-- Unknown operation"
	}
}

// Generate methods for different statement types
func (g *TestDataGenerator) generateCreateTable(complexity ComplexityLevel) string {
	tableName := fmt.Sprintf("test_table_%d", g.rand.Intn(1000))
	
	switch complexity {
	case ComplexitySimple:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`, tableName)
	
	case ComplexityMedium:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    status VARCHAR(20) DEFAULT 'active',
    score DECIMAL(10,2),
    metadata VARIANT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`, tableName)
	
	case ComplexityComplex:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    uuid VARCHAR(36) DEFAULT UUID_STRING(),
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    country VARCHAR(2) DEFAULT 'US',
    postal_code VARCHAR(10),
    status VARCHAR(20) DEFAULT 'active',
    category VARCHAR(50),
    tags ARRAY,
    metadata VARIANT,
    score DECIMAL(10,2),
    balance DECIMAL(15,2) DEFAULT 0,
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_status CHECK (status IN ('active', 'inactive', 'suspended')),
    INDEX idx_email (email),
    INDEX idx_status_created (status, created_at)
);`, tableName)
	
	default:
		return "-- Invalid complexity"
	}
}

func (g *TestDataGenerator) generateInsert(complexity ComplexityLevel) string {
	tableName := fmt.Sprintf("test_table_%d", g.rand.Intn(100))
	
	switch complexity {
	case ComplexitySimple:
		return fmt.Sprintf("INSERT INTO %s (name) VALUES ('Test User %d');", 
			tableName, g.rand.Intn(10000))
	
	case ComplexityMedium:
		values := make([]string, 5)
		for i := 0; i < 5; i++ {
			values[i] = fmt.Sprintf("('User %d', 'user%d@example.com', %d)", 
				g.rand.Intn(10000), g.rand.Intn(10000), g.rand.Intn(100))
		}
		return fmt.Sprintf("INSERT INTO %s (name, email, score) VALUES\n%s;",
			tableName, strings.Join(values, ",\n"))
	
	case ComplexityComplex:
		return fmt.Sprintf(`INSERT INTO %s (name, email, phone, metadata, tags, score)
SELECT 
    'User ' || seq4(),
    'user' || seq4() || '@example.com',
    '+1' || UNIFORM(1000000000, 9999999999, RANDOM())::VARCHAR,
    OBJECT_CONSTRUCT('type', 'premium', 'level', UNIFORM(1, 10, RANDOM())),
    ARRAY_CONSTRUCT('tag1', 'tag2', 'tag3'),
    UNIFORM(0, 100, RANDOM())::DECIMAL(10,2)
FROM TABLE(GENERATOR(ROWCOUNT => 100));`, tableName)
	
	default:
		return "-- Invalid complexity"
	}
}

func (g *TestDataGenerator) generateUpdate(complexity ComplexityLevel) string {
	tableName := fmt.Sprintf("test_table_%d", g.rand.Intn(100))
	
	switch complexity {
	case ComplexitySimple:
		return fmt.Sprintf("UPDATE %s SET updated_at = CURRENT_TIMESTAMP WHERE id = %d;",
			tableName, g.rand.Intn(1000))
	
	case ComplexityMedium:
		return fmt.Sprintf(`UPDATE %s 
SET score = score * 1.1,
    status = 'premium',
    updated_at = CURRENT_TIMESTAMP
WHERE score > 80 AND status = 'active';`, tableName)
	
	case ComplexityComplex:
		return fmt.Sprintf(`UPDATE %s t1
SET score = t2.avg_score * 1.05,
    metadata = OBJECT_INSERT(metadata, 'updated', TRUE),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT category, AVG(score) as avg_score
    FROM %s
    WHERE created_at > DATEADD(day, -30, CURRENT_DATE())
    GROUP BY category
) t2
WHERE t1.category = t2.category
  AND t1.score < t2.avg_score;`, tableName, tableName)
	
	default:
		return "-- Invalid complexity"
	}
}

func (g *TestDataGenerator) generateSelect(complexity ComplexityLevel) string {
	tableName := fmt.Sprintf("test_table_%d", g.rand.Intn(100))
	
	switch complexity {
	case ComplexitySimple:
		return fmt.Sprintf("SELECT * FROM %s WHERE id = %d;", tableName, g.rand.Intn(1000))
	
	case ComplexityMedium:
		return fmt.Sprintf(`SELECT 
    id, name, email, score,
    CASE 
        WHEN score > 90 THEN 'A'
        WHEN score > 80 THEN 'B'
        WHEN score > 70 THEN 'C'
        ELSE 'D'
    END as grade
FROM %s
WHERE status = 'active'
  AND created_at > DATEADD(day, -7, CURRENT_DATE())
ORDER BY score DESC
LIMIT 100;`, tableName)
	
	case ComplexityComplex:
		return fmt.Sprintf(`WITH ranked_users AS (
    SELECT 
        id, name, email, score, category,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) as rank,
        AVG(score) OVER (PARTITION BY category) as category_avg,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) OVER (PARTITION BY category) as median_score
    FROM %s
    WHERE status = 'active'
),
category_stats AS (
    SELECT 
        category,
        COUNT(*) as user_count,
        AVG(score) as avg_score,
        STDDEV(score) as stddev_score
    FROM %s
    GROUP BY category
)
SELECT 
    ru.*,
    cs.user_count,
    cs.stddev_score,
    ru.score - ru.category_avg as score_diff
FROM ranked_users ru
JOIN category_stats cs ON ru.category = cs.category
WHERE ru.rank <= 10
ORDER BY ru.category, ru.rank;`, tableName, tableName)
	
	default:
		return "-- Invalid complexity"
	}
}

func (g *TestDataGenerator) generateJoin(complexity ComplexityLevel) string {
	// Implementation for join queries
	return fmt.Sprintf(`SELECT 
    t1.*, t2.order_count, t3.total_revenue
FROM test_table_%d t1
LEFT JOIN test_table_%d t2 ON t1.id = t2.user_id
LEFT JOIN test_table_%d t3 ON t1.id = t3.customer_id
WHERE t1.status = 'active';`, 
		g.rand.Intn(100), g.rand.Intn(100), g.rand.Intn(100))
}

func (g *TestDataGenerator) generateAggregation(complexity ComplexityLevel) string {
	// Implementation for aggregation queries
	tableName := fmt.Sprintf("test_table_%d", g.rand.Intn(100))
	return fmt.Sprintf(`SELECT 
    DATE_TRUNC('day', created_at) as day,
    COUNT(*) as count,
    AVG(score) as avg_score,
    MAX(score) as max_score,
    MIN(score) as min_score
FROM %s
WHERE created_at > DATEADD(day, -30, CURRENT_DATE())
GROUP BY DATE_TRUNC('day', created_at)
ORDER BY day DESC;`, tableName)
}

func (g *TestDataGenerator) generateStoredProc(complexity ComplexityLevel) string {
	// Implementation for stored procedures
	procName := fmt.Sprintf("sp_test_%d", g.rand.Intn(1000))
	return fmt.Sprintf(`CREATE OR REPLACE PROCEDURE %s(threshold FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE test_table_1 SET status = 'premium' WHERE score > threshold;
    RETURN 'Updated ' || SQLROWCOUNT || ' rows';
END;
$$;`, procName)
}

func (g *TestDataGenerator) generateView(complexity ComplexityLevel) string {
	// Implementation for views
	viewName := fmt.Sprintf("v_test_%d", g.rand.Intn(1000))
	return fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
SELECT 
    category,
    COUNT(*) as user_count,
    AVG(score) as avg_score
FROM test_table_1
WHERE status = 'active'
GROUP BY category;`, viewName)
}

// Helper function to calculate percentile for durations
func calculatePercentileDuration(durations []time.Duration, percentile int) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	// Sort durations
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	
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