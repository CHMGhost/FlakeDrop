package observability

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MetricType represents the type of metric
type MetricType int

const (
	CounterType MetricType = iota
	GaugeType
	HistogramType
	SummaryType
)

// Metric represents a generic metric interface
type Metric interface {
	Type() MetricType
	Name() string
	Help() string
	Labels() map[string]string
}

// Counter represents a monotonic counter metric
type Counter struct {
	name   string
	help   string
	labels map[string]string
	value  float64
	mu     sync.RWMutex
}

// NewCounter creates a new counter metric
func NewCounter(name, help string, labels map[string]string) *Counter {
	return &Counter{
		name:   name,
		help:   help,
		labels: labels,
	}
}

// Inc increments the counter by 1
func (c *Counter) Inc() {
	c.Add(1)
}

// Add adds the given value to the counter
func (c *Counter) Add(delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += delta
}

// Value returns the current counter value
func (c *Counter) Value() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// Type returns the metric type
func (c *Counter) Type() MetricType {
	return CounterType
}

// Name returns the metric name
func (c *Counter) Name() string {
	return c.name
}

// Help returns the metric help text
func (c *Counter) Help() string {
	return c.help
}

// Labels returns the metric labels
func (c *Counter) Labels() map[string]string {
	return c.labels
}

// Gauge represents a gauge metric
type Gauge struct {
	name   string
	help   string
	labels map[string]string
	value  float64
	mu     sync.RWMutex
}

// NewGauge creates a new gauge metric
func NewGauge(name, help string, labels map[string]string) *Gauge {
	return &Gauge{
		name:   name,
		help:   help,
		labels: labels,
	}
}

// Set sets the gauge value
func (g *Gauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

// Inc increments the gauge by 1
func (g *Gauge) Inc() {
	g.Add(1)
}

// Dec decrements the gauge by 1
func (g *Gauge) Dec() {
	g.Add(-1)
}

// Add adds the given value to the gauge
func (g *Gauge) Add(delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value += delta
}

// Value returns the current gauge value
func (g *Gauge) Value() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// Type returns the metric type
func (g *Gauge) Type() MetricType {
	return GaugeType
}

// Name returns the metric name
func (g *Gauge) Name() string {
	return g.name
}

// Help returns the metric help text
func (g *Gauge) Help() string {
	return g.help
}

// Labels returns the metric labels
func (g *Gauge) Labels() map[string]string {
	return g.labels
}

// Histogram represents a histogram metric
type Histogram struct {
	name    string
	help    string
	labels  map[string]string
	buckets []float64
	counts  map[float64]uint64
	sum     float64
	count   uint64
	mu      sync.RWMutex
}

// NewHistogram creates a new histogram metric
func NewHistogram(name, help string, labels map[string]string, buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	
	counts := make(map[float64]uint64)
	for _, b := range buckets {
		counts[b] = 0
	}
	
	return &Histogram{
		name:    name,
		help:    help,
		labels:  labels,
		buckets: buckets,
		counts:  counts,
	}
}

// Observe adds an observation to the histogram
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	h.sum += value
	h.count++
	
	for _, bucket := range h.buckets {
		if value <= bucket {
			h.counts[bucket]++
		}
	}
}

// Count returns the total count of observations
func (h *Histogram) Count() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

// Sum returns the sum of all observations
func (h *Histogram) Sum() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sum
}

// Type returns the metric type
func (h *Histogram) Type() MetricType {
	return HistogramType
}

// Name returns the metric name
func (h *Histogram) Name() string {
	return h.name
}

// Help returns the metric help text
func (h *Histogram) Help() string {
	return h.help
}

// Labels returns the metric labels
func (h *Histogram) Labels() map[string]string {
	return h.labels
}

// MetricsRegistry manages all metrics
type MetricsRegistry struct {
	mu       sync.RWMutex
	metrics  map[string]Metric
	prefix   string
}

// NewMetricsRegistry creates a new metrics registry
func NewMetricsRegistry(prefix string) *MetricsRegistry {
	return &MetricsRegistry{
		metrics: make(map[string]Metric),
		prefix:  prefix,
	}
}

// Register registers a new metric
func (r *MetricsRegistry) Register(metric Metric) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	name := metric.Name()
	if r.prefix != "" {
		name = r.prefix + "_" + name
	}
	
	if _, exists := r.metrics[name]; exists {
		return fmt.Errorf("metric %s already registered", name)
	}
	
	r.metrics[name] = metric
	return nil
}

// GetMetric returns a metric by name
func (r *MetricsRegistry) GetMetric(name string) (Metric, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	if r.prefix != "" {
		name = r.prefix + "_" + name
	}
	
	metric, exists := r.metrics[name]
	return metric, exists
}

// GetCounter returns a counter metric by name
func (r *MetricsRegistry) GetCounter(name string) (*Counter, bool) {
	metric, exists := r.GetMetric(name)
	if !exists {
		return nil, false
	}
	
	counter, ok := metric.(*Counter)
	return counter, ok
}

// GetGauge returns a gauge metric by name
func (r *MetricsRegistry) GetGauge(name string) (*Gauge, bool) {
	metric, exists := r.GetMetric(name)
	if !exists {
		return nil, false
	}
	
	gauge, ok := metric.(*Gauge)
	return gauge, ok
}

// GetHistogram returns a histogram metric by name
func (r *MetricsRegistry) GetHistogram(name string) (*Histogram, bool) {
	metric, exists := r.GetMetric(name)
	if !exists {
		return nil, false
	}
	
	histogram, ok := metric.(*Histogram)
	return histogram, ok
}

// GetAllMetrics returns all registered metrics
func (r *MetricsRegistry) GetAllMetrics() map[string]Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	metrics := make(map[string]Metric)
	for name, metric := range r.metrics {
		metrics[name] = metric
	}
	
	return metrics
}

// PrometheusExporter exports metrics in Prometheus format
type PrometheusExporter struct {
	registry *MetricsRegistry
}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter(registry *MetricsRegistry) *PrometheusExporter {
	return &PrometheusExporter{
		registry: registry,
	}
}

// Export exports metrics in Prometheus format
func (e *PrometheusExporter) Export() string {
	metrics := e.registry.GetAllMetrics()
	var output string
	
	for _, metric := range metrics {
		switch m := metric.(type) {
		case *Counter:
			output += e.formatCounter(m)
		case *Gauge:
			output += e.formatGauge(m)
		case *Histogram:
			output += e.formatHistogram(m)
		}
	}
	
	return output
}

// formatCounter formats a counter metric
func (e *PrometheusExporter) formatCounter(c *Counter) string {
	output := fmt.Sprintf("# HELP %s %s\n", c.Name(), c.Help())
	output += fmt.Sprintf("# TYPE %s counter\n", c.Name())
	output += fmt.Sprintf("%s%s %f\n", c.Name(), formatLabels(c.Labels()), c.Value())
	return output
}

// formatGauge formats a gauge metric
func (e *PrometheusExporter) formatGauge(g *Gauge) string {
	output := fmt.Sprintf("# HELP %s %s\n", g.Name(), g.Help())
	output += fmt.Sprintf("# TYPE %s gauge\n", g.Name())
	output += fmt.Sprintf("%s%s %f\n", g.Name(), formatLabels(g.Labels()), g.Value())
	return output
}

// formatHistogram formats a histogram metric
func (e *PrometheusExporter) formatHistogram(h *Histogram) string {
	output := fmt.Sprintf("# HELP %s %s\n", h.Name(), h.Help())
	output += fmt.Sprintf("# TYPE %s histogram\n", h.Name())
	
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	// Bucket values
	for _, bucket := range h.buckets {
		labels := copyLabels(h.Labels())
		labels["le"] = fmt.Sprintf("%g", bucket)
		output += fmt.Sprintf("%s_bucket%s %d\n", h.Name(), formatLabels(labels), h.counts[bucket])
	}
	
	// +Inf bucket
	labels := copyLabels(h.Labels())
	labels["le"] = "+Inf"
	output += fmt.Sprintf("%s_bucket%s %d\n", h.Name(), formatLabels(labels), h.count)
	
	// Sum and count
	output += fmt.Sprintf("%s_sum%s %f\n", h.Name(), formatLabels(h.Labels()), h.sum)
	output += fmt.Sprintf("%s_count%s %d\n", h.Name(), formatLabels(h.Labels()), h.count)
	
	return output
}

// formatLabels formats metric labels
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	
	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}
	
	return "{" + strings.Join(parts, ",") + "}"
}

// copyLabels creates a copy of labels map
func copyLabels(labels map[string]string) map[string]string {
	copy := make(map[string]string)
	for k, v := range labels {
		copy[k] = v
	}
	return copy
}

// MetricsHandler returns an HTTP handler for metrics endpoint
func (e *PrometheusExporter) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, e.Export())
	}
}

// MetricsCollector collects various system and application metrics
type MetricsCollector struct {
	registry *MetricsRegistry
	logger   *Logger
	interval time.Duration
	stopCh   chan struct{}
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(registry *MetricsRegistry, logger *Logger, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		registry: registry,
		logger:   logger,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the metrics collection
func (c *MetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.collectMetrics()
		}
	}
}

// Stop stops the metrics collection
func (c *MetricsCollector) Stop() {
	close(c.stopCh)
}

// collectMetrics collects various metrics
func (c *MetricsCollector) collectMetrics() {
	// This is a placeholder for actual metric collection
	// In a real implementation, this would collect various system and application metrics
}

// Global metrics registry
var defaultRegistry = NewMetricsRegistry("snowflake_deploy")

// GetDefaultRegistry returns the global default metrics registry
func GetDefaultRegistry() *MetricsRegistry {
	return defaultRegistry
}

// Standard metrics for the application
var (
	// Deployment metrics
	DeploymentTotal = NewCounter(
		"deployments_total",
		"Total number of deployments",
		nil,
	)
	
	DeploymentSuccess = NewCounter(
		"deployments_success_total",
		"Total number of successful deployments",
		nil,
	)
	
	DeploymentFailure = NewCounter(
		"deployments_failure_total",
		"Total number of failed deployments",
		nil,
	)
	
	DeploymentDuration = NewHistogram(
		"deployment_duration_seconds",
		"Deployment duration in seconds",
		nil,
		[]float64{1, 5, 10, 30, 60, 120, 300, 600},
	)
	
	// Git operations metrics
	GitOperationsTotal = NewCounter(
		"git_operations_total",
		"Total number of git operations",
		nil,
	)
	
	GitOperationDuration = NewHistogram(
		"git_operation_duration_seconds",
		"Git operation duration in seconds",
		nil,
		[]float64{0.1, 0.5, 1, 2, 5, 10},
	)
	
	// Snowflake connection metrics
	SnowflakeConnectionsActive = NewGauge(
		"snowflake_connections_active",
		"Number of active Snowflake connections",
		nil,
	)
	
	SnowflakeQueriesTotal = NewCounter(
		"snowflake_queries_total",
		"Total number of Snowflake queries",
		nil,
	)
	
	SnowflakeQueryDuration = NewHistogram(
		"snowflake_query_duration_seconds",
		"Snowflake query duration in seconds",
		nil,
		[]float64{0.1, 0.5, 1, 5, 10, 30, 60},
	)
)

// RegisterStandardMetrics registers all standard metrics
func RegisterStandardMetrics() {
	_ = defaultRegistry.Register(DeploymentTotal)
	_ = defaultRegistry.Register(DeploymentSuccess)
	_ = defaultRegistry.Register(DeploymentFailure)
	defaultRegistry.Register(DeploymentDuration)
	defaultRegistry.Register(GitOperationsTotal)
	defaultRegistry.Register(GitOperationDuration)
	defaultRegistry.Register(SnowflakeConnectionsActive)
	defaultRegistry.Register(SnowflakeQueriesTotal)
	defaultRegistry.Register(SnowflakeQueryDuration)
}