package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// Monitor provides comprehensive monitoring for Snowflake operations
type Monitor struct {
	service           *Service
	queryMonitor      *QueryMonitor
	warehouseMonitor  *WarehouseMonitor
	creditMonitor     *CreditMonitor
	alertManager      *AlertManager
	dashboardServer   *DashboardServer
	config            MonitorConfig
}

// MonitorConfig configures the monitoring system
type MonitorConfig struct {
	EnableQueryMonitoring     bool
	EnableWarehouseMonitoring bool
	EnableCreditMonitoring    bool
	EnableAlerts              bool
	QueryHistoryRetention     time.Duration
	MetricsInterval           time.Duration
	AlertThresholds           AlertThresholds
	DashboardPort             int
}

// AlertThresholds defines thresholds for alerts
type AlertThresholds struct {
	SlowQueryDuration      time.Duration
	HighCreditUsage        float64
	LongQueueTime          time.Duration
	HighMemorySpillage     int64
	WarehouseUtilization   float64
	ConcurrentQueryLimit   int
}

// QueryMonitor monitors query performance
type QueryMonitor struct {
	queries      map[string]*MonitoredQuery
	history      []QueryHistory
	mu           sync.RWMutex
	alertChan    chan Alert
}

// MonitoredQuery represents a query being monitored
type MonitoredQuery struct {
	QueryID          string
	SQL              string
	StartTime        time.Time
	EndTime          time.Time
	Status           string
	Warehouse        string
	User             string
	Application      string
	ExecutionTime    time.Duration
	CompilationTime  time.Duration
	QueuedTime       time.Duration
	BytesScanned     int64
	BytesWritten     int64
	RowsProduced     int64
	PartitionsScanned int64
	SpillingBytes    int64
	Credits          float64
}

// QueryHistory stores historical query data
type QueryHistory struct {
	Timestamp time.Time
	Queries   []MonitoredQuery
	Summary   QuerySummary
}

// QuerySummary provides query statistics summary
type QuerySummary struct {
	TotalQueries      int
	SuccessfulQueries int
	FailedQueries     int
	AvgExecutionTime  time.Duration
	MaxExecutionTime  time.Duration
	TotalBytesScanned int64
	TotalCreditsUsed  float64
}

// WarehouseMonitor monitors warehouse performance
type WarehouseMonitor struct {
	warehouses map[string]*WarehouseStatus
	history    []WarehouseHistory
	mu         sync.RWMutex
}

// WarehouseStatus represents current warehouse status
type WarehouseStatus struct {
	Name              string
	Size              string
	State             string
	RunningQueries    int
	QueuedQueries     int
	Utilization       float64
	CreditsPerHour    float64
	AutoSuspend       int
	AutoResume        bool
	LastActivityTime  time.Time
	CreatedAt         time.Time
}

// WarehouseHistory stores historical warehouse data
type WarehouseHistory struct {
	Timestamp  time.Time
	Warehouses []WarehouseStatus
	Summary    WarehouseSummary
}

// WarehouseSummary provides warehouse statistics summary
type WarehouseSummary struct {
	TotalWarehouses     int
	ActiveWarehouses    int
	TotalRunningQueries int
	TotalQueuedQueries  int
	AvgUtilization      float64
	TotalCreditsPerHour float64
}

// CreditMonitor monitors credit usage
type CreditMonitor struct {
	usage    map[string]*CreditUsage
	history  []CreditHistory
	mu       sync.RWMutex
	budget   CreditBudget
}

// CreditUsage tracks credit consumption
type CreditUsage struct {
	Warehouse       string
	Date            time.Time
	ComputeCredits  float64
	CloudCredits    float64
	TotalCredits    float64
	QueryCount      int
	AvgCreditsQuery float64
}

// CreditHistory stores historical credit data
type CreditHistory struct {
	Timestamp time.Time
	Usage     []CreditUsage
	Summary   CreditSummary
}

// CreditSummary provides credit usage summary
type CreditSummary struct {
	TotalCredits        float64
	ComputeCredits      float64
	CloudCredits        float64
	DailyAverage        float64
	ProjectedMonthly    float64
	BudgetUtilization   float64
}

// CreditBudget defines credit budget constraints
type CreditBudget struct {
	DailyLimit   float64
	MonthlyLimit float64
	Alerts       []float64 // Percentage thresholds for alerts
}

// AlertManager manages monitoring alerts
type AlertManager struct {
	alerts   []Alert
	handlers map[AlertType][]AlertHandler
	mu       sync.RWMutex
}

// Alert represents a monitoring alert
type Alert struct {
	ID          string
	Type        AlertType
	Severity    AlertSeverity
	Title       string
	Description string
	Details     map[string]interface{}
	Timestamp   time.Time
	Resolved    bool
	ResolvedAt  time.Time
}

// AlertType defines types of alerts
type AlertType string

const (
	AlertTypeSlowQuery         AlertType = "SLOW_QUERY"
	AlertTypeHighCreditUsage   AlertType = "HIGH_CREDIT_USAGE" // #nosec G101 - Not a credential, just an alert type name
	AlertTypeLongQueueTime     AlertType = "LONG_QUEUE_TIME"
	AlertTypeMemorySpillage    AlertType = "MEMORY_SPILLAGE"
	AlertTypeWarehouseOverload AlertType = "WAREHOUSE_OVERLOAD"
	AlertTypeConnectionFailure AlertType = "CONNECTION_FAILURE"
)

// AlertSeverity defines alert severity levels
type AlertSeverity string

const (
	AlertSeverityInfo     AlertSeverity = "INFO"
	AlertSeverityWarning  AlertSeverity = "WARNING"
	AlertSeverityCritical AlertSeverity = "CRITICAL"
)

// AlertHandler handles alerts
type AlertHandler func(alert Alert) error

// DashboardServer provides monitoring dashboard
type DashboardServer struct {
	monitor *Monitor
	port    int
	mu      sync.RWMutex
}

// DefaultMonitorConfig returns default monitoring configuration
func DefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		EnableQueryMonitoring:     true,
		EnableWarehouseMonitoring: true,
		EnableCreditMonitoring:    true,
		EnableAlerts:              true,
		QueryHistoryRetention:     24 * time.Hour,
		MetricsInterval:           1 * time.Minute,
		AlertThresholds: AlertThresholds{
			SlowQueryDuration:    5 * time.Minute,
			HighCreditUsage:      1000.0,
			LongQueueTime:        30 * time.Second,
			HighMemorySpillage:   1024 * 1024 * 1024, // 1GB
			WarehouseUtilization: 0.9,                 // 90%
			ConcurrentQueryLimit: 100,
		},
		DashboardPort: 8080,
	}
}

// NewMonitor creates a new monitoring system
func NewMonitor(service *Service, config MonitorConfig) *Monitor {
	monitor := &Monitor{
		service:          service,
		queryMonitor:     NewQueryMonitor(),
		warehouseMonitor: NewWarehouseMonitor(),
		creditMonitor:    NewCreditMonitor(),
		alertManager:     NewAlertManager(),
		config:           config,
	}

	if config.DashboardPort > 0 {
		monitor.dashboardServer = NewDashboardServer(monitor, config.DashboardPort)
	}

	return monitor
}

// Start starts the monitoring system
func (m *Monitor) Start(ctx context.Context) error {
	// Start monitoring goroutines
	if m.config.EnableQueryMonitoring {
		go m.monitorQueries(ctx)
	}

	if m.config.EnableWarehouseMonitoring {
		go m.monitorWarehouses(ctx)
	}

	if m.config.EnableCreditMonitoring {
		go m.monitorCredits(ctx)
	}

	// Start dashboard server
	if m.dashboardServer != nil {
		go func() { _ = m.dashboardServer.Start() }()
	}

	return nil
}

// monitorQueries monitors query performance
func (m *Monitor) monitorQueries(ctx context.Context) {
	ticker := time.NewTicker(m.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.collectQueryMetrics(); err != nil {
				// Log error but continue monitoring
				continue
			}
		}
	}
}

// collectQueryMetrics collects query performance metrics
func (m *Monitor) collectQueryMetrics() error {
	// Query for recent queries
	query := `
		SELECT 
			query_id,
			query_text,
			start_time,
			end_time,
			execution_status,
			warehouse_name,
			user_name,
			client_application_id,
			total_elapsed_time,
			compilation_time,
			queued_provisioning_time + queued_repair_time + queued_overload_time,
			bytes_scanned,
			bytes_written,
			rows_produced,
			partitions_scanned,
			bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage,
			credits_used_cloud_services
		FROM table(information_schema.query_history(
			end_time_range_start => dateadd('minute', -5, current_timestamp()),
			end_time_range_end => current_timestamp()
		))
		WHERE warehouse_name IS NOT NULL
		ORDER BY start_time DESC
	`

	rows, err := m.service.ExecuteQuery(query)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to query history")
	}
	defer rows.Close()

	var queries []MonitoredQuery

	for rows.Next() {
		var q MonitoredQuery
		var startTime, endTime sql.NullTime
		var compilationMs, totalMs, queuedMs sql.NullInt64

		err := rows.Scan(
			&q.QueryID,
			&q.SQL,
			&startTime,
			&endTime,
			&q.Status,
			&q.Warehouse,
			&q.User,
			&q.Application,
			&totalMs,
			&compilationMs,
			&queuedMs,
			&q.BytesScanned,
			&q.BytesWritten,
			&q.RowsProduced,
			&q.PartitionsScanned,
			&q.SpillingBytes,
			&q.Credits,
		)
		if err != nil {
			continue
		}

		if startTime.Valid {
			q.StartTime = startTime.Time
		}
		if endTime.Valid {
			q.EndTime = endTime.Time
		}
		if totalMs.Valid {
			q.ExecutionTime = time.Duration(totalMs.Int64) * time.Millisecond
		}
		if compilationMs.Valid {
			q.CompilationTime = time.Duration(compilationMs.Int64) * time.Millisecond
		}
		if queuedMs.Valid {
			q.QueuedTime = time.Duration(queuedMs.Int64) * time.Millisecond
		}

		queries = append(queries, q)

		// Check for alerts
		m.checkQueryAlerts(q)
	}

	// Update monitor
	m.queryMonitor.Update(queries)

	return rows.Err()
}

// checkQueryAlerts checks for query-related alerts
func (m *Monitor) checkQueryAlerts(query MonitoredQuery) {
	// Slow query alert
	if query.ExecutionTime > m.config.AlertThresholds.SlowQueryDuration {
		alert := Alert{
			ID:       fmt.Sprintf("slow_query_%s", query.QueryID),
			Type:     AlertTypeSlowQuery,
			Severity: AlertSeverityWarning,
			Title:    "Slow Query Detected",
			Description: fmt.Sprintf("Query %s took %v to execute", 
				query.QueryID, query.ExecutionTime),
			Details: map[string]interface{}{
				"query_id":       query.QueryID,
				"execution_time": query.ExecutionTime,
				"warehouse":      query.Warehouse,
				"bytes_scanned":  query.BytesScanned,
			},
			Timestamp: time.Now(),
		}
		m.alertManager.SendAlert(alert)
	}

	// Long queue time alert
	if query.QueuedTime > m.config.AlertThresholds.LongQueueTime {
		alert := Alert{
			ID:       fmt.Sprintf("queue_time_%s", query.QueryID),
			Type:     AlertTypeLongQueueTime,
			Severity: AlertSeverityWarning,
			Title:    "Long Queue Time Detected",
			Description: fmt.Sprintf("Query %s was queued for %v", 
				query.QueryID, query.QueuedTime),
			Details: map[string]interface{}{
				"query_id":    query.QueryID,
				"queue_time":  query.QueuedTime,
				"warehouse":   query.Warehouse,
			},
			Timestamp: time.Now(),
		}
		m.alertManager.SendAlert(alert)
	}

	// Memory spillage alert
	if query.SpillingBytes > m.config.AlertThresholds.HighMemorySpillage {
		alert := Alert{
			ID:       fmt.Sprintf("spillage_%s", query.QueryID),
			Type:     AlertTypeMemorySpillage,
			Severity: AlertSeverityCritical,
			Title:    "High Memory Spillage",
			Description: fmt.Sprintf("Query %s spilled %d bytes to storage", 
				query.QueryID, query.SpillingBytes),
			Details: map[string]interface{}{
				"query_id":       query.QueryID,
				"spilling_bytes": query.SpillingBytes,
				"warehouse":      query.Warehouse,
			},
			Timestamp: time.Now(),
		}
		m.alertManager.SendAlert(alert)
	}
}

// monitorWarehouses monitors warehouse utilization
func (m *Monitor) monitorWarehouses(ctx context.Context) {
	ticker := time.NewTicker(m.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.collectWarehouseMetrics(); err != nil {
				// Log error but continue monitoring
				continue
			}
		}
	}
}

// collectWarehouseMetrics collects warehouse metrics
func (m *Monitor) collectWarehouseMetrics() error {
	// Query warehouse status
	query := `
		SELECT 
			warehouse_name,
			warehouse_size,
			state,
			auto_suspend,
			auto_resume,
			created_on,
			resumed_on
		FROM table(information_schema.warehouse_metering_history(
			date_range_start => dateadd('hour', -1, current_timestamp())
		))
		GROUP BY 1,2,3,4,5,6,7
	`

	rows, err := m.service.ExecuteQuery(query)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to query warehouse status")
	}
	defer rows.Close()

	var warehouses []WarehouseStatus

	for rows.Next() {
		var w WarehouseStatus
		var autoSuspend sql.NullInt64
		var createdOn, resumedOn sql.NullTime

		err := rows.Scan(
			&w.Name,
			&w.Size,
			&w.State,
			&autoSuspend,
			&w.AutoResume,
			&createdOn,
			&resumedOn,
		)
		if err != nil {
			continue
		}

		if autoSuspend.Valid {
			w.AutoSuspend = int(autoSuspend.Int64)
		}
		if createdOn.Valid {
			w.CreatedAt = createdOn.Time
		}
		if resumedOn.Valid {
			w.LastActivityTime = resumedOn.Time
		}

		// Get running and queued queries
		w.RunningQueries, w.QueuedQueries = m.getWarehouseQueryCounts(w.Name)
		
		// Calculate utilization
		if w.State == "RUNNING" {
			w.Utilization = float64(w.RunningQueries) / float64(m.getWarehouseCapacity(w.Size))
		}

		warehouses = append(warehouses, w)

		// Check for alerts
		m.checkWarehouseAlerts(w)
	}

	// Update monitor
	m.warehouseMonitor.Update(warehouses)

	return rows.Err()
}

// getWarehouseQueryCounts gets running and queued query counts
func (m *Monitor) getWarehouseQueryCounts(warehouse string) (running, queued int) {
	// Simplified - would query actual counts
	return 0, 0
}

// getWarehouseCapacity returns warehouse capacity based on size
func (m *Monitor) getWarehouseCapacity(size string) int {
	capacityMap := map[string]int{
		"X-SMALL": 8,
		"SMALL":   16,
		"MEDIUM":  32,
		"LARGE":   64,
		"X-LARGE": 128,
		"2X-LARGE": 256,
		"3X-LARGE": 512,
		"4X-LARGE": 1024,
	}
	
	if capacity, ok := capacityMap[size]; ok {
		return capacity
	}
	return 8
}

// checkWarehouseAlerts checks for warehouse-related alerts
func (m *Monitor) checkWarehouseAlerts(warehouse WarehouseStatus) {
	// High utilization alert
	if warehouse.Utilization > m.config.AlertThresholds.WarehouseUtilization {
		alert := Alert{
			ID:       fmt.Sprintf("warehouse_util_%s", warehouse.Name),
			Type:     AlertTypeWarehouseOverload,
			Severity: AlertSeverityWarning,
			Title:    "High Warehouse Utilization",
			Description: fmt.Sprintf("Warehouse %s is at %.1f%% utilization", 
				warehouse.Name, warehouse.Utilization*100),
			Details: map[string]interface{}{
				"warehouse":       warehouse.Name,
				"utilization":     warehouse.Utilization,
				"running_queries": warehouse.RunningQueries,
				"queued_queries":  warehouse.QueuedQueries,
			},
			Timestamp: time.Now(),
		}
		m.alertManager.SendAlert(alert)
	}
}

// monitorCredits monitors credit usage
func (m *Monitor) monitorCredits(ctx context.Context) {
	ticker := time.NewTicker(m.config.MetricsInterval * 10) // Less frequent
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.collectCreditMetrics(); err != nil {
				// Log error but continue monitoring
				continue
			}
		}
	}
}

// collectCreditMetrics collects credit usage metrics
func (m *Monitor) collectCreditMetrics() error {
	// Query credit usage
	query := `
		SELECT 
			warehouse_name,
			start_time::date as usage_date,
			SUM(credits_used) as compute_credits,
			SUM(credits_used_cloud_services) as cloud_credits,
			COUNT(*) as query_count
		FROM table(information_schema.warehouse_metering_history(
			date_range_start => dateadd('day', -7, current_date())
		))
		GROUP BY 1, 2
		ORDER BY 2 DESC, 1
	`

	rows, err := m.service.ExecuteQuery(query)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLExecution, "Failed to query credit usage")
	}
	defer rows.Close()

	usage := make(map[string]*CreditUsage)

	for rows.Next() {
		var warehouse string
		var date time.Time
		var computeCredits, cloudCredits float64
		var queryCount int

		err := rows.Scan(
			&warehouse,
			&date,
			&computeCredits,
			&cloudCredits,
			&queryCount,
		)
		if err != nil {
			continue
		}

		key := fmt.Sprintf("%s_%s", warehouse, date.Format("2006-01-02"))
		usage[key] = &CreditUsage{
			Warehouse:       warehouse,
			Date:            date,
			ComputeCredits:  computeCredits,
			CloudCredits:    cloudCredits,
			TotalCredits:    computeCredits + cloudCredits,
			QueryCount:      queryCount,
			AvgCreditsQuery: (computeCredits + cloudCredits) / float64(queryCount),
		}
	}

	// Update monitor
	m.creditMonitor.Update(usage)

	// Check credit alerts
	m.checkCreditAlerts()

	return rows.Err()
}

// checkCreditAlerts checks for credit-related alerts
func (m *Monitor) checkCreditAlerts() {
	summary := m.creditMonitor.GetSummary()
	
	if summary.TotalCredits > m.config.AlertThresholds.HighCreditUsage {
		alert := Alert{
			ID:       fmt.Sprintf("credit_usage_%d", time.Now().Unix()),
			Type:     AlertTypeHighCreditUsage,
			Severity: AlertSeverityCritical,
			Title:    "High Credit Usage",
			Description: fmt.Sprintf("Total credit usage is %.2f credits", 
				summary.TotalCredits),
			Details: map[string]interface{}{
				"total_credits":     summary.TotalCredits,
				"compute_credits":   summary.ComputeCredits,
				"cloud_credits":     summary.CloudCredits,
				"projected_monthly": summary.ProjectedMonthly,
			},
			Timestamp: time.Now(),
		}
		m.alertManager.SendAlert(alert)
	}
}

// GetDashboardData returns data for monitoring dashboard
func (m *Monitor) GetDashboardData() DashboardData {
	return DashboardData{
		QuerySummary:     m.queryMonitor.GetSummary(),
		WarehouseSummary: m.warehouseMonitor.GetSummary(),
		CreditSummary:    m.creditMonitor.GetSummary(),
		RecentAlerts:     m.alertManager.GetRecentAlerts(10),
		Timestamp:        time.Now(),
	}
}

// DashboardData contains data for monitoring dashboard
type DashboardData struct {
	QuerySummary     QuerySummary
	WarehouseSummary WarehouseSummary
	CreditSummary    CreditSummary
	RecentAlerts     []Alert
	Timestamp        time.Time
}

// Helper constructors and methods for sub-components

func NewQueryMonitor() *QueryMonitor {
	return &QueryMonitor{
		queries:   make(map[string]*MonitoredQuery),
		history:   make([]QueryHistory, 0),
		alertChan: make(chan Alert, 100),
	}
}

func (qm *QueryMonitor) Update(queries []MonitoredQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	// Update current queries
	for _, q := range queries {
		qm.queries[q.QueryID] = &q
	}

	// Add to history
	history := QueryHistory{
		Timestamp: time.Now(),
		Queries:   queries,
		Summary:   qm.calculateSummary(queries),
	}
	qm.history = append(qm.history, history)

	// Cleanup old history
	cutoff := time.Now().Add(-24 * time.Hour)
	var newHistory []QueryHistory
	for _, h := range qm.history {
		if h.Timestamp.After(cutoff) {
			newHistory = append(newHistory, h)
		}
	}
	qm.history = newHistory
}

func (qm *QueryMonitor) calculateSummary(queries []MonitoredQuery) QuerySummary {
	summary := QuerySummary{}
	
	var totalExecTime time.Duration
	var maxExecTime time.Duration

	for _, q := range queries {
		summary.TotalQueries++
		if q.Status == "SUCCESS" {
			summary.SuccessfulQueries++
		} else {
			summary.FailedQueries++
		}
		
		totalExecTime += q.ExecutionTime
		if q.ExecutionTime > maxExecTime {
			maxExecTime = q.ExecutionTime
		}
		
		summary.TotalBytesScanned += q.BytesScanned
		summary.TotalCreditsUsed += q.Credits
	}

	if summary.TotalQueries > 0 {
		summary.AvgExecutionTime = totalExecTime / time.Duration(summary.TotalQueries)
	}
	summary.MaxExecutionTime = maxExecTime

	return summary
}

func (qm *QueryMonitor) GetSummary() QuerySummary {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	if len(qm.history) > 0 {
		return qm.history[len(qm.history)-1].Summary
	}
	return QuerySummary{}
}

func NewWarehouseMonitor() *WarehouseMonitor {
	return &WarehouseMonitor{
		warehouses: make(map[string]*WarehouseStatus),
		history:    make([]WarehouseHistory, 0),
	}
}

func (wm *WarehouseMonitor) Update(warehouses []WarehouseStatus) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Update current warehouses
	for _, w := range warehouses {
		wm.warehouses[w.Name] = &w
	}

	// Add to history
	history := WarehouseHistory{
		Timestamp:  time.Now(),
		Warehouses: warehouses,
		Summary:    wm.calculateSummary(warehouses),
	}
	wm.history = append(wm.history, history)
}

func (wm *WarehouseMonitor) calculateSummary(warehouses []WarehouseStatus) WarehouseSummary {
	summary := WarehouseSummary{
		TotalWarehouses: len(warehouses),
	}

	var totalUtilization float64

	for _, w := range warehouses {
		if w.State == "RUNNING" {
			summary.ActiveWarehouses++
		}
		summary.TotalRunningQueries += w.RunningQueries
		summary.TotalQueuedQueries += w.QueuedQueries
		totalUtilization += w.Utilization
		summary.TotalCreditsPerHour += w.CreditsPerHour
	}

	if summary.TotalWarehouses > 0 {
		summary.AvgUtilization = totalUtilization / float64(summary.TotalWarehouses)
	}

	return summary
}

func (wm *WarehouseMonitor) GetSummary() WarehouseSummary {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if len(wm.history) > 0 {
		return wm.history[len(wm.history)-1].Summary
	}
	return WarehouseSummary{}
}

func NewCreditMonitor() *CreditMonitor {
	return &CreditMonitor{
		usage:   make(map[string]*CreditUsage),
		history: make([]CreditHistory, 0),
	}
}

func (cm *CreditMonitor) Update(usage map[string]*CreditUsage) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.usage = usage

	// Calculate summary
	var usageList []CreditUsage
	for _, u := range usage {
		usageList = append(usageList, *u)
	}

	history := CreditHistory{
		Timestamp: time.Now(),
		Usage:     usageList,
		Summary:   cm.calculateSummary(usageList),
	}
	cm.history = append(cm.history, history)
}

func (cm *CreditMonitor) calculateSummary(usage []CreditUsage) CreditSummary {
	summary := CreditSummary{}

	daysTracked := make(map[string]bool)

	for _, u := range usage {
		summary.TotalCredits += u.TotalCredits
		summary.ComputeCredits += u.ComputeCredits
		summary.CloudCredits += u.CloudCredits
		daysTracked[u.Date.Format("2006-01-02")] = true
	}

	if len(daysTracked) > 0 {
		summary.DailyAverage = summary.TotalCredits / float64(len(daysTracked))
		summary.ProjectedMonthly = summary.DailyAverage * 30
	}

	if cm.budget.MonthlyLimit > 0 {
		summary.BudgetUtilization = summary.ProjectedMonthly / cm.budget.MonthlyLimit * 100
	}

	return summary
}

func (cm *CreditMonitor) GetSummary() CreditSummary {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if len(cm.history) > 0 {
		return cm.history[len(cm.history)-1].Summary
	}
	return CreditSummary{}
}

func NewAlertManager() *AlertManager {
	return &AlertManager{
		alerts:   make([]Alert, 0),
		handlers: make(map[AlertType][]AlertHandler),
	}
}

func (am *AlertManager) SendAlert(alert Alert) {
	am.mu.Lock()
	am.alerts = append(am.alerts, alert)
	
	// Keep only last 1000 alerts
	if len(am.alerts) > 1000 {
		am.alerts = am.alerts[len(am.alerts)-1000:]
	}
	am.mu.Unlock()

	// Call handlers
	am.mu.RLock()
	handlers := am.handlers[alert.Type]
	am.mu.RUnlock()

	for _, handler := range handlers {
		go func() { _ = handler(alert) }()
	}
}

func (am *AlertManager) RegisterHandler(alertType AlertType, handler AlertHandler) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.handlers[alertType] = append(am.handlers[alertType], handler)
}

func (am *AlertManager) GetRecentAlerts(limit int) []Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	start := len(am.alerts) - limit
	if start < 0 {
		start = 0
	}

	result := make([]Alert, 0, limit)
	for i := len(am.alerts) - 1; i >= start; i-- {
		result = append(result, am.alerts[i])
	}

	return result
}

func NewDashboardServer(monitor *Monitor, port int) *DashboardServer {
	return &DashboardServer{
		monitor: monitor,
		port:    port,
	}
}

func (ds *DashboardServer) Start() error {
	// Create a new mux to avoid global state
	mux := http.NewServeMux()
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		data := ds.monitor.GetDashboardData()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	})

	// Create server with proper timeouts
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", ds.port),
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return server.ListenAndServe()
}