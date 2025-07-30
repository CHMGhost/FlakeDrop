package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// HealthStatus represents the health status of a component
type HealthStatus int

const (
	HealthStatusUp HealthStatus = iota
	HealthStatusDown
	HealthStatusDegraded
	HealthStatusUnknown
)

var statusNames = map[HealthStatus]string{
	HealthStatusUp:       "UP",
	HealthStatusDown:     "DOWN",
	HealthStatusDegraded: "DEGRADED",
	HealthStatusUnknown:  "UNKNOWN",
}

// HealthCheck represents a health check
type HealthCheck interface {
	Name() string
	Check(ctx context.Context) HealthResult
}

// HealthResult represents the result of a health check
type HealthResult struct {
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Duration  time.Duration          `json:"duration_ms"`
	Timestamp time.Time              `json:"timestamp"`
}

// HealthReport represents the overall health report
type HealthReport struct {
	Status     HealthStatus              `json:"status"`
	Timestamp  time.Time                 `json:"timestamp"`
	Duration   time.Duration             `json:"duration_ms"`
	Components map[string]HealthResult   `json:"components"`
	Metadata   map[string]interface{}    `json:"metadata,omitempty"`
}

// HealthManager manages health checks
type HealthManager struct {
	mu        sync.RWMutex
	checks    map[string]HealthCheck
	timeout   time.Duration
	metadata  map[string]interface{}
	logger    *Logger
}

// NewHealthManager creates a new health manager
func NewHealthManager(timeout time.Duration, logger *Logger) *HealthManager {
	return &HealthManager{
		checks:   make(map[string]HealthCheck),
		timeout:  timeout,
		metadata: make(map[string]interface{}),
		logger:   logger,
	}
}

// RegisterCheck registers a health check
func (hm *HealthManager) RegisterCheck(check HealthCheck) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.checks[check.Name()] = check
}

// UnregisterCheck unregisters a health check
func (hm *HealthManager) UnregisterCheck(name string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.checks, name)
}

// SetMetadata sets metadata for the health report
func (hm *HealthManager) SetMetadata(key string, value interface{}) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.metadata[key] = value
}

// CheckHealth performs all health checks and returns a report
func (hm *HealthManager) CheckHealth(ctx context.Context) HealthReport {
	start := time.Now()
	
	hm.mu.RLock()
	checks := make(map[string]HealthCheck)
	for name, check := range hm.checks {
		checks[name] = check
	}
	metadata := make(map[string]interface{})
	for k, v := range hm.metadata {
		metadata[k] = v
	}
	hm.mu.RUnlock()
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, hm.timeout)
	defer cancel()
	
	// Run all checks concurrently
	results := make(chan struct {
		name   string
		result HealthResult
	}, len(checks))
	
	for name, check := range checks {
		go func(name string, check HealthCheck) {
			checkStart := time.Now()
			result := check.Check(ctx)
			result.Duration = time.Since(checkStart)
			result.Timestamp = time.Now()
			
			results <- struct {
				name   string
				result HealthResult
			}{name, result}
		}(name, check)
	}
	
	// Collect results
	components := make(map[string]HealthResult)
	overallStatus := HealthStatusUp
	
	for i := 0; i < len(checks); i++ {
		result := <-results
		components[result.name] = result.result
		
		// Update overall status
		switch result.result.Status {
		case HealthStatusDown:
			overallStatus = HealthStatusDown
		case HealthStatusDegraded:
			if overallStatus == HealthStatusUp {
				overallStatus = HealthStatusDegraded
			}
		case HealthStatusUnknown:
			if overallStatus == HealthStatusUp {
				overallStatus = HealthStatusUnknown
			}
		}
	}
	
	report := HealthReport{
		Status:     overallStatus,
		Timestamp:  time.Now(),
		Duration:   time.Since(start),
		Components: components,
		Metadata:   metadata,
	}
	
	// Log health check results
	if hm.logger != nil {
		hm.logger.InfoWithFields("Health check completed", map[string]interface{}{
			"status":       statusNames[overallStatus],
			"duration_ms":  report.Duration.Milliseconds(),
			"components":   len(components),
		})
	}
	
	return report
}

// CheckHealthForComponent performs a health check for a specific component
func (hm *HealthManager) CheckHealthForComponent(ctx context.Context, name string) (HealthResult, error) {
	hm.mu.RLock()
	check, exists := hm.checks[name]
	hm.mu.RUnlock()
	
	if !exists {
		return HealthResult{}, fmt.Errorf("health check %s not found", name)
	}
	
	ctx, cancel := context.WithTimeout(ctx, hm.timeout)
	defer cancel()
	
	start := time.Now()
	result := check.Check(ctx)
	result.Duration = time.Since(start)
	result.Timestamp = time.Now()
	
	return result, nil
}

// HealthHandler returns an HTTP handler for health checks
func (hm *HealthManager) HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		report := hm.CheckHealth(ctx)
		
		w.Header().Set("Content-Type", "application/json")
		
		// Set HTTP status based on health status
		switch report.Status {
		case HealthStatusUp:
			w.WriteHeader(http.StatusOK)
		case HealthStatusDegraded:
			w.WriteHeader(http.StatusOK) // Still 200 for degraded
		case HealthStatusDown:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(report)
	}
}

// ReadinessHandler returns an HTTP handler for readiness checks
func (hm *HealthManager) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		report := hm.CheckHealth(ctx)
		
		w.Header().Set("Content-Type", "application/json")
		
		// Readiness is more strict - only UP is considered ready
		if report.Status == HealthStatusUp {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(report)
	}
}

// LivenessHandler returns an HTTP handler for liveness checks
func (hm *HealthManager) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// For liveness, we only check if the service is responsive
		// A simple "alive" response is sufficient
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		
		response := map[string]interface{}{
			"status":    "UP",
			"timestamp": time.Now(),
		}
		
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
	}
}

// Built-in health checks

// DatabaseHealthCheck checks database connectivity
type DatabaseHealthCheck struct {
	name        string
	connString  string
	testQuery   string
	timeout     time.Duration
	testConnFn  func(ctx context.Context) error
}

// NewDatabaseHealthCheck creates a new database health check
func NewDatabaseHealthCheck(name, connString, testQuery string, timeout time.Duration, testConnFn func(ctx context.Context) error) *DatabaseHealthCheck {
	return &DatabaseHealthCheck{
		name:       name,
		connString: connString,
		testQuery:  testQuery,
		timeout:    timeout,
		testConnFn: testConnFn,
	}
}

// Name returns the check name
func (d *DatabaseHealthCheck) Name() string {
	return d.name
}

// Check performs the database health check
func (d *DatabaseHealthCheck) Check(ctx context.Context) HealthResult {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	
	err := d.testConnFn(ctx)
	if err != nil {
		return HealthResult{
			Status:  HealthStatusDown,
			Message: fmt.Sprintf("Database connection failed: %v", err),
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	return HealthResult{
		Status:  HealthStatusUp,
		Message: "Database connection successful",
	}
}

// ServiceHealthCheck checks external service connectivity
type ServiceHealthCheck struct {
	name     string
	url      string
	timeout  time.Duration
	client   *http.Client
}

// NewServiceHealthCheck creates a new service health check
func NewServiceHealthCheck(name, url string, timeout time.Duration) *ServiceHealthCheck {
	return &ServiceHealthCheck{
		name:    name,
		url:     url,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
}

// Name returns the check name
func (s *ServiceHealthCheck) Name() string {
	return s.name
}

// Check performs the service health check
func (s *ServiceHealthCheck) Check(ctx context.Context) HealthResult {
	req, err := http.NewRequestWithContext(ctx, "GET", s.url, nil)
	if err != nil {
		return HealthResult{
			Status:  HealthStatusDown,
			Message: fmt.Sprintf("Failed to create request: %v", err),
		}
	}
	
	resp, err := s.client.Do(req)
	if err != nil {
		return HealthResult{
			Status:  HealthStatusDown,
			Message: fmt.Sprintf("Service request failed: %v", err),
			Details: map[string]interface{}{
				"url":   s.url,
				"error": err.Error(),
			},
		}
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return HealthResult{
			Status:  HealthStatusUp,
			Message: "Service is healthy",
			Details: map[string]interface{}{
				"url":         s.url,
				"status_code": resp.StatusCode,
			},
		}
	}
	
	return HealthResult{
		Status:  HealthStatusDegraded,
		Message: fmt.Sprintf("Service returned status %d", resp.StatusCode),
		Details: map[string]interface{}{
			"url":         s.url,
			"status_code": resp.StatusCode,
		},
	}
}

// MemoryHealthCheck checks memory usage
type MemoryHealthCheck struct {
	name        string
	threshold   float64 // Memory usage threshold (0.0 to 1.0)
	criticalThreshold float64 // Critical threshold
}

// NewMemoryHealthCheck creates a new memory health check
func NewMemoryHealthCheck(name string, threshold, criticalThreshold float64) *MemoryHealthCheck {
	return &MemoryHealthCheck{
		name:              name,
		threshold:         threshold,
		criticalThreshold: criticalThreshold,
	}
}

// Name returns the check name
func (m *MemoryHealthCheck) Name() string {
	return m.name
}

// Check performs the memory health check
func (m *MemoryHealthCheck) Check(ctx context.Context) HealthResult {
	// Simple memory check using runtime.MemStats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Calculate memory usage percentage (this is a simplified calculation)
	heapUsed := float64(memStats.HeapAlloc)
	heapTotal := float64(memStats.HeapSys)
	
	if heapTotal == 0 {
		return HealthResult{
			Status:  HealthStatusUnknown,
			Message: "Unable to determine memory usage",
		}
	}
	
	usage := heapUsed / heapTotal
	
	details := map[string]interface{}{
		"heap_used_mb":  heapUsed / 1024 / 1024,
		"heap_total_mb": heapTotal / 1024 / 1024,
		"usage_percent": usage * 100,
	}
	
	if usage >= m.criticalThreshold {
		return HealthResult{
			Status:  HealthStatusDown,
			Message: fmt.Sprintf("Critical memory usage: %.1f%%", usage*100),
			Details: details,
		}
	}
	
	if usage >= m.threshold {
		return HealthResult{
			Status:  HealthStatusDegraded,
			Message: fmt.Sprintf("High memory usage: %.1f%%", usage*100),
			Details: details,
		}
	}
	
	return HealthResult{
		Status:  HealthStatusUp,
		Message: fmt.Sprintf("Memory usage normal: %.1f%%", usage*100),
		Details: details,
	}
}

// Global health manager
var defaultHealthManager = NewHealthManager(10*time.Second, GetDefaultLogger())

// GetDefaultHealthManager returns the global health manager
func GetDefaultHealthManager() *HealthManager {
	return defaultHealthManager
}

// RegisterHealthCheck registers a health check with the default manager
func RegisterHealthCheck(check HealthCheck) {
	defaultHealthManager.RegisterCheck(check)
}

// CheckHealth performs health checks using the default manager
func CheckHealth(ctx context.Context) HealthReport {
	return defaultHealthManager.CheckHealth(ctx)
}