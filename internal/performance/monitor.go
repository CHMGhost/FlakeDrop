package performance

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Monitor tracks performance metrics and system resources
type Monitor struct {
	mu            sync.RWMutex
	startTime     time.Time
	metrics       map[string]*Metric
	resourceStats *ResourceStats
	stopChan      chan struct{}
	interval      time.Duration
	history       *MetricHistory
}

// Metric represents a performance metric
type Metric struct {
	Name        string
	Value       float64
	Unit        string
	Min         float64
	Max         float64
	Avg         float64
	Count       int64
	LastUpdated time.Time
	Tags        map[string]string
}

// ResourceStats tracks system resource usage
type ResourceStats struct {
	mu             sync.RWMutex
	CPUPercent     float64
	MemoryUsedMB   float64
	MemoryPercent  float64
	GoroutineCount int
	GCRuns         uint32
	GCPauseMs      float64
	HeapAllocMB    float64
	HeapSysMB      float64
}

// MetricHistory stores historical metric data
type MetricHistory struct {
	mu         sync.RWMutex
	maxEntries int
	entries    map[string][]HistoryEntry
}

// HistoryEntry represents a point-in-time metric value
type HistoryEntry struct {
	Timestamp time.Time
	Value     float64
}

// MonitorConfig contains monitor configuration
type MonitorConfig struct {
	CollectionInterval time.Duration
	HistorySize        int
}

// DefaultMonitorConfig returns sensible defaults
func DefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		CollectionInterval: 5 * time.Second,
		HistorySize:        1000,
	}
}

// NewMonitor creates a new performance monitor
func NewMonitor(config MonitorConfig) *Monitor {
	m := &Monitor{
		startTime:     time.Now(),
		metrics:       make(map[string]*Metric),
		resourceStats: &ResourceStats{},
		stopChan:      make(chan struct{}),
		interval:      config.CollectionInterval,
		history: &MetricHistory{
			maxEntries: config.HistorySize,
			entries:    make(map[string][]HistoryEntry),
		},
	}

	// Start resource monitoring
	go m.collectResourceStats()

	return m
}

// RecordMetric records a metric value
func (m *Monitor) RecordMetric(name string, value float64, unit string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metric, exists := m.metrics[name]
	if !exists {
		metric = &Metric{
			Name:  name,
			Unit:  unit,
			Min:   value,
			Max:   value,
			Tags:  tags,
		}
		m.metrics[name] = metric
	}

	// Update metric
	metric.Value = value
	metric.Count++
	metric.LastUpdated = time.Now()

	// Update min/max
	if value < metric.Min {
		metric.Min = value
	}
	if value > metric.Max {
		metric.Max = value
	}

	// Update average
	metric.Avg = ((metric.Avg * float64(metric.Count-1)) + value) / float64(metric.Count)

	// Record history
	m.history.Add(name, value)
}

// RecordDuration records a duration metric
func (m *Monitor) RecordDuration(name string, duration time.Duration, tags map[string]string) {
	m.RecordMetric(name, duration.Seconds(), "seconds", tags)
}

// RecordCount increments a counter metric
func (m *Monitor) RecordCount(name string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metric, exists := m.metrics[name]
	if !exists {
		metric = &Metric{
			Name: name,
			Unit: "count",
			Tags: tags,
		}
		m.metrics[name] = metric
	}

	metric.Value++
	metric.Count++
	metric.LastUpdated = time.Now()
}

// GetMetric returns a specific metric
func (m *Monitor) GetMetric(name string) (*Metric, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metric, exists := m.metrics[name]
	if exists {
		// Return a copy
		metricCopy := *metric
		return &metricCopy, true
	}
	return nil, false
}

// GetAllMetrics returns all metrics
func (m *Monitor) GetAllMetrics() map[string]Metric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := make(map[string]Metric)
	for name, metric := range m.metrics {
		metrics[name] = *metric
	}
	return metrics
}

// GetResourceStats returns current resource statistics
func (m *Monitor) GetResourceStats() ResourceStats {
	m.resourceStats.mu.RLock()
	defer m.resourceStats.mu.RUnlock()
	return *m.resourceStats
}

// GetHistory returns metric history
func (m *Monitor) GetHistory(metricName string, duration time.Duration) []HistoryEntry {
	return m.history.Get(metricName, duration)
}

// Stop stops the monitor
func (m *Monitor) Stop() {
	close(m.stopChan)
}

// collectResourceStats collects system resource statistics
func (m *Monitor) collectResourceStats() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	var lastGCStats runtime.MemStats
	runtime.ReadMemStats(&lastGCStats)

	for {
		select {
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			m.resourceStats.mu.Lock()

			// Memory statistics
			m.resourceStats.HeapAllocMB = float64(memStats.HeapAlloc) / 1024 / 1024
			m.resourceStats.HeapSysMB = float64(memStats.HeapSys) / 1024 / 1024
			m.resourceStats.MemoryUsedMB = float64(memStats.Alloc) / 1024 / 1024

			// GC statistics
			m.resourceStats.GCRuns = memStats.NumGC - lastGCStats.NumGC
			if memStats.NumGC > lastGCStats.NumGC {
				m.resourceStats.GCPauseMs = float64(memStats.PauseNs[(memStats.NumGC+255)%256]) / 1e6
			}

			// Goroutine count
			m.resourceStats.GoroutineCount = runtime.NumGoroutine()

			m.resourceStats.mu.Unlock()

			lastGCStats = memStats

			// Record as metrics
			m.RecordMetric("system.memory.heap_alloc_mb", m.resourceStats.HeapAllocMB, "MB", nil)
			m.RecordMetric("system.memory.heap_sys_mb", m.resourceStats.HeapSysMB, "MB", nil)
			m.RecordMetric("system.goroutines", float64(m.resourceStats.GoroutineCount), "count", nil)
			m.RecordMetric("system.gc.runs", float64(m.resourceStats.GCRuns), "count", nil)
			m.RecordMetric("system.gc.pause_ms", m.resourceStats.GCPauseMs, "ms", nil)

		case <-m.stopChan:
			return
		}
	}
}

// MetricHistory methods

func (mh *MetricHistory) Add(metricName string, value float64) {
	mh.mu.Lock()
	defer mh.mu.Unlock()

	entry := HistoryEntry{
		Timestamp: time.Now(),
		Value:     value,
	}

	entries := mh.entries[metricName]
	entries = append(entries, entry)

	// Trim old entries
	if len(entries) > mh.maxEntries {
		entries = entries[len(entries)-mh.maxEntries:]
	}

	mh.entries[metricName] = entries
}

func (mh *MetricHistory) Get(metricName string, duration time.Duration) []HistoryEntry {
	mh.mu.RLock()
	defer mh.mu.RUnlock()

	entries, exists := mh.entries[metricName]
	if !exists {
		return nil
	}

	cutoff := time.Now().Add(-duration)
	var result []HistoryEntry

	for _, entry := range entries {
		if entry.Timestamp.After(cutoff) {
			result = append(result, entry)
		}
	}

	return result
}

// Profiler provides performance profiling capabilities
type Profiler struct {
	mu       sync.RWMutex
	profiles map[string]*Profile
	monitor  *Monitor
}

// Profile represents a performance profile
type Profile struct {
	Name      string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Spans     []*Span
	Tags      map[string]string
}

// Span represents a timed operation within a profile
type Span struct {
	Name      string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Tags      map[string]string
	Children  []*Span
	parent    *Span
}

// NewProfiler creates a new profiler
func NewProfiler(monitor *Monitor) *Profiler {
	return &Profiler{
		profiles: make(map[string]*Profile),
		monitor:  monitor,
	}
}

// StartProfile starts a new profile
func (p *Profiler) StartProfile(name string, tags map[string]string) *Profile {
	profile := &Profile{
		Name:      name,
		StartTime: time.Now(),
		Tags:      tags,
		Spans:     make([]*Span, 0),
	}

	p.mu.Lock()
	p.profiles[name] = profile
	p.mu.Unlock()

	return profile
}

// EndProfile ends a profile
func (p *Profiler) EndProfile(name string) (*Profile, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	profile, exists := p.profiles[name]
	if !exists {
		return nil, fmt.Errorf("profile %s not found", name)
	}

	profile.EndTime = time.Now()
	profile.Duration = profile.EndTime.Sub(profile.StartTime)

	// Record metric
	if p.monitor != nil {
		p.monitor.RecordDuration(fmt.Sprintf("profile.%s.duration", name), profile.Duration, profile.Tags)
	}

	delete(p.profiles, name)
	return profile, nil
}

// StartSpan starts a new span within a profile
func (p *Profiler) StartSpan(profileName, spanName string, tags map[string]string) (*Span, error) {
	p.mu.RLock()
	profile, exists := p.profiles[profileName]
	p.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("profile %s not found", profileName)
	}

	span := &Span{
		Name:      spanName,
		StartTime: time.Now(),
		Tags:      tags,
		Children:  make([]*Span, 0),
	}

	profile.Spans = append(profile.Spans, span)
	return span, nil
}

// EndSpan ends a span
func (s *Span) End() {
	s.EndTime = time.Now()
	s.Duration = s.EndTime.Sub(s.StartTime)
}

// StartChild starts a child span
func (s *Span) StartChild(name string, tags map[string]string) *Span {
	child := &Span{
		Name:      name,
		StartTime: time.Now(),
		Tags:      tags,
		Children:  make([]*Span, 0),
		parent:    s,
	}

	s.Children = append(s.Children, child)
	return child
}

// Timer provides a convenient way to time operations
type Timer struct {
	name      string
	startTime time.Time
	monitor   *Monitor
	tags      map[string]string
}

// StartTimer starts a new timer
func (m *Monitor) StartTimer(name string, tags map[string]string) *Timer {
	return &Timer{
		name:      name,
		startTime: time.Now(),
		monitor:   m,
		tags:      tags,
	}
}

// Stop stops the timer and records the duration
func (t *Timer) Stop() time.Duration {
	duration := time.Since(t.startTime)
	if t.monitor != nil {
		t.monitor.RecordDuration(t.name, duration, t.tags)
	}
	return duration
}

// WithTimer executes a function and times it
func (m *Monitor) WithTimer(name string, tags map[string]string, fn func() error) error {
	timer := m.StartTimer(name, tags)
	err := fn()
	timer.Stop()
	return err
}

// Report generates a performance report
type Report struct {
	StartTime     time.Time
	EndTime       time.Time
	Duration      time.Duration
	Metrics       map[string]Metric
	ResourceStats ResourceStats
	Summary       map[string]interface{}
}

// GenerateReport generates a performance report
func (m *Monitor) GenerateReport() *Report {
	report := &Report{
		StartTime:     m.startTime,
		EndTime:       time.Now(),
		Duration:      time.Since(m.startTime),
		Metrics:       m.GetAllMetrics(),
		ResourceStats: m.GetResourceStats(),
		Summary:       make(map[string]interface{}),
	}

	// Calculate summary statistics
	var totalRequests, successfulRequests, failedRequests float64
	var avgDuration time.Duration
	var throughput float64

	for name, metric := range report.Metrics {
		switch {
		case strings.Contains(name, "requests.total"):
			totalRequests = metric.Value
		case strings.Contains(name, "requests.success"):
			successfulRequests = metric.Value
		case strings.Contains(name, "requests.failed"):
			failedRequests = metric.Value
		case strings.Contains(name, "duration.avg"):
			avgDuration = time.Duration(metric.Avg * float64(time.Second))
		case strings.Contains(name, "throughput"):
			throughput = metric.Value
		}
	}

	// Add to summary
	report.Summary["total_requests"] = totalRequests
	report.Summary["success_rate"] = (successfulRequests / totalRequests) * 100
	report.Summary["error_rate"] = (failedRequests / totalRequests) * 100
	report.Summary["avg_duration"] = avgDuration
	report.Summary["throughput"] = throughput
	report.Summary["uptime"] = report.Duration

	return report
}