package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// QueryOptimizer provides query optimization and analysis for Snowflake
type QueryOptimizer struct {
	service       *Service
	cache         *QueryPlanCache
	analyzer      *QueryAnalyzer
	rewriter      *QueryRewriter
	metricsTracker *QueryMetricsTracker
}

// QueryPlan represents a Snowflake query execution plan
type QueryPlan struct {
	QueryID       string
	SQL           string
	EstimatedCost float64
	EstimatedRows int64
	Operations    []PlanOperation
	Warnings      []string
	Suggestions   []OptimizationSuggestion
	CreatedAt     time.Time
}

// PlanOperation represents a single operation in the query plan
type PlanOperation struct {
	ID           int
	Operation    string
	Object       string
	EstRows      int64
	EstBytes     int64
	EstCost      float64
	PartitionInfo string
}

// OptimizationSuggestion provides optimization recommendations
type OptimizationSuggestion struct {
	Type        string
	Severity    string // "high", "medium", "low"
	Description string
	Impact      string
	SQL         string // Optional rewritten SQL
}

// QueryMetrics tracks query performance metrics
type QueryMetrics struct {
	QueryID            string
	SQL                string
	ExecutionTime      time.Duration
	CompilationTime    time.Duration
	QueuedTime         time.Duration
	BytesScanned       int64
	BytesWritten       int64
	RowsProduced       int64
	CreditsUsed        float64
	WarehouseName      string
	WarehouseSize      string
	ClusterNumber      int
	QueryTag           string
	ResourceMonitor    string
	PartitionsScanned  int64
	PartitionsTotal    int64
	SpillingToLocal    int64
	SpillingToRemote   int64
	CreatedAt          time.Time
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(service *Service) *QueryOptimizer {
	return &QueryOptimizer{
		service:        service,
		cache:          NewQueryPlanCache(),
		analyzer:       NewQueryAnalyzer(),
		rewriter:       NewQueryRewriter(),
		metricsTracker: NewQueryMetricsTracker(),
	}
}

// AnalyzeQuery analyzes a query and provides optimization suggestions
func (qo *QueryOptimizer) AnalyzeQuery(ctx context.Context, sql string) (*QueryPlan, error) {
	// Check cache first
	if cachedPlan := qo.cache.Get(sql); cachedPlan != nil {
		return cachedPlan, nil
	}

	// Get query plan using EXPLAIN
	planSQL := fmt.Sprintf("EXPLAIN %s", sql)
	rows, err := qo.service.ExecuteQuery(planSQL)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to get query plan")
	}
	defer rows.Close()

	plan := &QueryPlan{
		SQL:       sql,
		CreatedAt: time.Now(),
	}

	// Parse the execution plan
	if err := qo.parsePlan(rows, plan); err != nil {
		return nil, err
	}

	// Analyze the query structure
	qo.analyzer.Analyze(sql, plan)

	// Generate optimization suggestions
	suggestions := qo.generateSuggestions(sql, plan)
	plan.Suggestions = suggestions

	// Cache the plan
	qo.cache.Set(sql, plan)

	return plan, nil
}

// OptimizeQuery rewrites a query for better performance
func (qo *QueryOptimizer) OptimizeQuery(ctx context.Context, sql string) (string, *QueryPlan, error) {
	// First analyze the query
	plan, err := qo.AnalyzeQuery(ctx, sql)
	if err != nil {
		return sql, nil, err
	}

	// Apply query rewrites based on suggestions
	optimizedSQL := qo.rewriter.Rewrite(sql, plan.Suggestions)

	// Re-analyze the optimized query
	optimizedPlan, err := qo.AnalyzeQuery(ctx, optimizedSQL)
	if err != nil {
		// Return original if optimization fails
		return sql, plan, nil
	}

	// Only return optimized version if it's actually better
	if optimizedPlan.EstimatedCost < plan.EstimatedCost {
		return optimizedSQL, optimizedPlan, nil
	}

	return sql, plan, nil
}

// ExecuteWithMetrics executes a query and collects performance metrics
func (qo *QueryOptimizer) ExecuteWithMetrics(ctx context.Context, sql string, queryTag string) (*sql.Rows, *QueryMetrics, error) {
	// Set query tag for tracking
	if queryTag != "" {
		if _, err := qo.service.db.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET QUERY_TAG = '%s'", queryTag)); err != nil {
			return nil, nil, errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to set query tag")
		}
	}

	// Get query ID before execution
	var queryID string
	row := qo.service.db.QueryRowContext(ctx, "SELECT LAST_QUERY_ID()")
	_ = row.Scan(&queryID)

	startTime := time.Now()

	// Execute the query
	rows, err := qo.service.ExecuteQuery(sql)
	if err != nil {
		return nil, nil, err
	}

	executionTime := time.Since(startTime)

	// Collect metrics asynchronously
	go func() {
		metrics := qo.collectMetrics(queryID, sql, queryTag, executionTime)
		qo.metricsTracker.Record(metrics)
	}()

	return rows, nil, nil
}

// parsePlan parses the execution plan from EXPLAIN output
func (qo *QueryOptimizer) parsePlan(rows *sql.Rows, plan *QueryPlan) error {
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}

		// Parse the plan line
		// Snowflake EXPLAIN output format varies, this is a simplified parser
		op := qo.parsePlanLine(line)
		if op != nil {
			plan.Operations = append(plan.Operations, *op)
			plan.EstimatedCost += op.EstCost
			plan.EstimatedRows += op.EstRows
		}
	}

	return rows.Err()
}

// parsePlanLine parses a single line from the execution plan
func (qo *QueryOptimizer) parsePlanLine(line string) *PlanOperation {
	// This is a simplified parser - real implementation would be more sophisticated
	// Example line: "1:0     ->  TableScan[ORDERS] (cost=1000.0, rows=10000)"
	
	op := &PlanOperation{}
	
	// Extract operation type
	if strings.Contains(line, "TableScan") {
		op.Operation = "TableScan"
		// Extract table name
		if match := regexp.MustCompile(`TableScan\[(\w+)\]`).FindStringSubmatch(line); len(match) > 1 {
			op.Object = match[1]
		}
	} else if strings.Contains(line, "Filter") {
		op.Operation = "Filter"
	} else if strings.Contains(line, "Join") {
		op.Operation = "Join"
	} else if strings.Contains(line, "Aggregate") {
		op.Operation = "Aggregate"
	}

	// Extract cost and rows
	if match := regexp.MustCompile(`cost=([0-9.]+)`).FindStringSubmatch(line); len(match) > 1 {
		fmt.Sscanf(match[1], "%f", &op.EstCost)
	}
	if match := regexp.MustCompile(`rows=([0-9]+)`).FindStringSubmatch(line); len(match) > 1 {
		fmt.Sscanf(match[1], "%d", &op.EstRows)
	}

	return op
}

// generateSuggestions generates optimization suggestions based on the query plan
func (qo *QueryOptimizer) generateSuggestions(sql string, plan *QueryPlan) []OptimizationSuggestion {
	var suggestions []OptimizationSuggestion

	sqlLower := strings.ToLower(sql)

	// Check for missing WHERE clause on large tables
	for _, op := range plan.Operations {
		if op.Operation == "TableScan" && op.EstRows > 1000000 {
			if !strings.Contains(sqlLower, "where") {
				suggestions = append(suggestions, OptimizationSuggestion{
					Type:        "missing_filter",
					Severity:    "high",
					Description: fmt.Sprintf("Full table scan on large table %s without WHERE clause", op.Object),
					Impact:      "Can significantly reduce data scanned and improve performance",
				})
			}
		}
	}

	// Check for SELECT *
	if strings.Contains(sqlLower, "select *") || strings.Contains(sqlLower, "select\n*") {
		suggestions = append(suggestions, OptimizationSuggestion{
			Type:        "select_star",
			Severity:    "medium",
			Description: "Query uses SELECT * which retrieves all columns",
			Impact:      "Selecting only needed columns reduces data transfer and improves performance",
		})
	}

	// Check for missing JOIN conditions
	if strings.Contains(sqlLower, "join") && !strings.Contains(sqlLower, " on ") {
		suggestions = append(suggestions, OptimizationSuggestion{
			Type:        "cartesian_join",
			Severity:    "high",
			Description: "Possible cartesian join detected",
			Impact:      "Can result in exponential row growth and poor performance",
		})
	}

	// Check for ORDER BY without LIMIT
	if strings.Contains(sqlLower, "order by") && !strings.Contains(sqlLower, "limit") {
		suggestions = append(suggestions, OptimizationSuggestion{
			Type:        "order_without_limit",
			Severity:    "low",
			Description: "ORDER BY without LIMIT sorts entire result set",
			Impact:      "Adding LIMIT can reduce sorting overhead if only top results needed",
		})
	}

	// Check for subqueries that could be CTEs
	if strings.Count(sqlLower, "select") > 1 && strings.Contains(sqlLower, "from (") {
		suggestions = append(suggestions, OptimizationSuggestion{
			Type:        "subquery_to_cte",
			Severity:    "medium",
			Description: "Consider using CTEs instead of nested subqueries",
			Impact:      "CTEs can improve readability and may enable better optimization",
		})
	}

	// Check for DISTINCT on large result sets
	if strings.Contains(sqlLower, "distinct") {
		for _, op := range plan.Operations {
			if op.EstRows > 100000 {
				suggestions = append(suggestions, OptimizationSuggestion{
					Type:        "expensive_distinct",
					Severity:    "medium",
					Description: "DISTINCT operation on large dataset",
					Impact:      "Consider if DISTINCT is necessary or if data can be deduplicated earlier",
				})
				break
			}
		}
	}

	return suggestions
}

// collectMetrics collects query execution metrics
func (qo *QueryOptimizer) collectMetrics(queryID, sql, queryTag string, executionTime time.Duration) *QueryMetrics {
	metrics := &QueryMetrics{
		QueryID:       queryID,
		SQL:           sql,
		QueryTag:      queryTag,
		ExecutionTime: executionTime,
		CreatedAt:     time.Now(),
	}

	// Query Snowflake's query history for detailed metrics
	// Query ID is from Snowflake system, not user input
	metricSQL := fmt.Sprintf(`
		SELECT 
			compilation_time,
			queued_provisioning_time + queued_repair_time + queued_overload_time as queued_time,
			bytes_scanned,
			bytes_written,
			rows_produced,
			credits_used_cloud_services,
			warehouse_name,
			warehouse_size,
			cluster_number,
			partitions_scanned,
			partitions_total,
			bytes_spilled_to_local_storage,
			bytes_spilled_to_remote_storage
		FROM table(information_schema.query_history())
		WHERE query_id = '%s'
		ORDER BY start_time DESC
		LIMIT 1
	`, queryID) // #nosec G201 - queryID from Snowflake system

	row := qo.service.db.QueryRow(metricSQL)
	
	var compilationTime, queuedTime int64
	row.Scan(
		&compilationTime,
		&queuedTime,
		&metrics.BytesScanned,
		&metrics.BytesWritten,
		&metrics.RowsProduced,
		&metrics.CreditsUsed,
		&metrics.WarehouseName,
		&metrics.WarehouseSize,
		&metrics.ClusterNumber,
		&metrics.PartitionsScanned,
		&metrics.PartitionsTotal,
		&metrics.SpillingToLocal,
		&metrics.SpillingToRemote,
	)

	metrics.CompilationTime = time.Duration(compilationTime) * time.Millisecond
	metrics.QueuedTime = time.Duration(queuedTime) * time.Millisecond

	return metrics
}

// QueryAnalyzer analyzes query structure
type QueryAnalyzer struct {
	patterns []AnalysisPattern
}

type AnalysisPattern struct {
	Name    string
	Pattern *regexp.Regexp
	Analyze func(sql string, plan *QueryPlan)
}

func NewQueryAnalyzer() *QueryAnalyzer {
	return &QueryAnalyzer{
		patterns: []AnalysisPattern{
			{
				Name:    "large_in_clause",
				Pattern: regexp.MustCompile(`\sIN\s*\([^)]{1000,}\)`),
				Analyze: func(sql string, plan *QueryPlan) {
					plan.Warnings = append(plan.Warnings, "Large IN clause detected - consider using a temporary table or JOIN")
				},
			},
			{
				Name:    "case_sensitive_comparison",
				Pattern: regexp.MustCompile(`\sLIKE\s+'[^']+'`),
				Analyze: func(sql string, plan *QueryPlan) {
					if !strings.Contains(strings.ToLower(sql), "ilike") {
						plan.Warnings = append(plan.Warnings, "LIKE is case-sensitive in Snowflake - use ILIKE for case-insensitive matching")
					}
				},
			},
		},
	}
}

func (qa *QueryAnalyzer) Analyze(sql string, plan *QueryPlan) {
	for _, pattern := range qa.patterns {
		if pattern.Pattern.MatchString(sql) {
			pattern.Analyze(sql, plan)
		}
	}
}

// QueryRewriter rewrites queries for optimization
type QueryRewriter struct {
	rules []RewriteRule
}

type RewriteRule struct {
	Name     string
	Applies  func(sql string, suggestions []OptimizationSuggestion) bool
	Rewrite  func(sql string) string
}

func NewQueryRewriter() *QueryRewriter {
	return &QueryRewriter{
		rules: []RewriteRule{
			{
				Name: "select_star_to_columns",
				Applies: func(sql string, suggestions []OptimizationSuggestion) bool {
					for _, s := range suggestions {
						if s.Type == "select_star" {
							return true
						}
					}
					return false
				},
				Rewrite: func(sql string) string {
					// This is a simplified example - real implementation would parse SQL properly
					// and determine actual columns needed
					return sql
				},
			},
		},
	}
}

func (qr *QueryRewriter) Rewrite(sql string, suggestions []OptimizationSuggestion) string {
	rewritten := sql
	for _, rule := range qr.rules {
		if rule.Applies(rewritten, suggestions) {
			rewritten = rule.Rewrite(rewritten)
		}
	}
	return rewritten
}

// QueryPlanCache caches query execution plans
type QueryPlanCache struct {
	plans map[string]*QueryPlan
	mu    sync.RWMutex
}

func NewQueryPlanCache() *QueryPlanCache {
	return &QueryPlanCache{
		plans: make(map[string]*QueryPlan),
	}
}

func (qpc *QueryPlanCache) Get(sql string) *QueryPlan {
	qpc.mu.RLock()
	defer qpc.mu.RUnlock()
	
	plan, exists := qpc.plans[sql]
	if !exists || time.Since(plan.CreatedAt) > 1*time.Hour {
		return nil
	}
	return plan
}

func (qpc *QueryPlanCache) Set(sql string, plan *QueryPlan) {
	qpc.mu.Lock()
	defer qpc.mu.Unlock()
	qpc.plans[sql] = plan
}

// QueryMetricsTracker tracks query performance over time
type QueryMetricsTracker struct {
	metrics []QueryMetrics
	mu      sync.RWMutex
}

func NewQueryMetricsTracker() *QueryMetricsTracker {
	return &QueryMetricsTracker{
		metrics: make([]QueryMetrics, 0),
	}
}

func (qmt *QueryMetricsTracker) Record(metrics *QueryMetrics) {
	qmt.mu.Lock()
	defer qmt.mu.Unlock()
	qmt.metrics = append(qmt.metrics, *metrics)
	
	// Keep only last 1000 metrics
	if len(qmt.metrics) > 1000 {
		qmt.metrics = qmt.metrics[len(qmt.metrics)-1000:]
	}
}

func (qmt *QueryMetricsTracker) GetMetrics() []QueryMetrics {
	qmt.mu.RLock()
	defer qmt.mu.RUnlock()
	
	result := make([]QueryMetrics, len(qmt.metrics))
	copy(result, qmt.metrics)
	return result
}

// GetSlowQueries returns queries that exceeded the duration threshold
func (qmt *QueryMetricsTracker) GetSlowQueries(threshold time.Duration) []QueryMetrics {
	qmt.mu.RLock()
	defer qmt.mu.RUnlock()
	
	var slow []QueryMetrics
	for _, m := range qmt.metrics {
		if m.ExecutionTime > threshold {
			slow = append(slow, m)
		}
	}
	return slow
}