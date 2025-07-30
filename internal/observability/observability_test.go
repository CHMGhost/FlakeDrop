package observability

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLogger(t *testing.T) {
	// Test JSON logger
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:   DebugLevel,
		Output:  &buf,
		Service: "test-service",
		Version: "1.0.0",
		Encoder: NewJSONEncoder(false),
	})
	
	logger.Info("test message")
	
	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected log output to contain 'test message', got: %s", output)
	}
	if !strings.Contains(output, "test-service") {
		t.Errorf("Expected log output to contain service name, got: %s", output)
	}
}

func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:   InfoLevel,
		Output:  &buf,
		Service: "test-service",
		Encoder: NewJSONEncoder(false),
	})
	
	logger.InfoWithFields("test message", map[string]interface{}{
		"user_id": 123,
		"action":  "login",
	})
	
	output := buf.String()
	if !strings.Contains(output, "user_id") {
		t.Errorf("Expected log output to contain 'user_id', got: %s", output)
	}
	if !strings.Contains(output, "123") {
		t.Errorf("Expected log output to contain '123', got: %s", output)
	}
}

func TestMetricsCounter(t *testing.T) {
	counter := NewCounter("test_counter", "Test counter", nil)
	
	if counter.Value() != 0 {
		t.Errorf("Expected initial counter value to be 0, got: %f", counter.Value())
	}
	
	counter.Inc()
	if counter.Value() != 1 {
		t.Errorf("Expected counter value to be 1 after Inc(), got: %f", counter.Value())
	}
	
	counter.Add(5)
	if counter.Value() != 6 {
		t.Errorf("Expected counter value to be 6 after Add(5), got: %f", counter.Value())
	}
}

func TestMetricsGauge(t *testing.T) {
	gauge := NewGauge("test_gauge", "Test gauge", nil)
	
	if gauge.Value() != 0 {
		t.Errorf("Expected initial gauge value to be 0, got: %f", gauge.Value())
	}
	
	gauge.Set(10.5)
	if gauge.Value() != 10.5 {
		t.Errorf("Expected gauge value to be 10.5 after Set(10.5), got: %f", gauge.Value())
	}
	
	gauge.Inc()
	if gauge.Value() != 11.5 {
		t.Errorf("Expected gauge value to be 11.5 after Inc(), got: %f", gauge.Value())
	}
	
	gauge.Dec()
	if gauge.Value() != 10.5 {
		t.Errorf("Expected gauge value to be 10.5 after Dec(), got: %f", gauge.Value())
	}
}

func TestMetricsHistogram(t *testing.T) {
	histogram := NewHistogram("test_histogram", "Test histogram", nil, []float64{0.1, 0.5, 1.0, 5.0})
	
	if histogram.Count() != 0 {
		t.Errorf("Expected initial histogram count to be 0, got: %d", histogram.Count())
	}
	
	histogram.Observe(0.3)
	histogram.Observe(0.8)
	histogram.Observe(2.0)
	
	if histogram.Count() != 3 {
		t.Errorf("Expected histogram count to be 3, got: %d", histogram.Count())
	}
	
	expectedSum := 0.3 + 0.8 + 2.0
	if histogram.Sum() != expectedSum {
		t.Errorf("Expected histogram sum to be %f, got: %f", expectedSum, histogram.Sum())
	}
}

func TestMetricsRegistry(t *testing.T) {
	registry := NewMetricsRegistry("test")
	
	counter := NewCounter("requests_total", "Total requests", nil)
	err := registry.Register(counter)
	if err != nil {
		t.Errorf("Expected no error registering metric, got: %v", err)
	}
	
	// Test duplicate registration
	err = registry.Register(counter)
	if err == nil {
		t.Error("Expected error when registering duplicate metric")
	}
	
	// Test retrieval
	retrievedCounter, exists := registry.GetCounter("requests_total")
	if !exists {
		t.Error("Expected to find registered counter")
	}
	if retrievedCounter != counter {
		t.Error("Expected retrieved counter to be the same instance")
	}
}

func TestPrometheusExporter(t *testing.T) {
	registry := NewMetricsRegistry("test")
	
	counter := NewCounter("requests_total", "Total requests", map[string]string{"method": "GET"})
	counter.Add(42)
	_ = registry.Register(counter)
	
	gauge := NewGauge("memory_usage", "Memory usage", nil)
	gauge.Set(123.45)
	_ = registry.Register(gauge)
	
	exporter := NewPrometheusExporter(registry)
	output := exporter.Export()
	
	if !strings.Contains(output, "requests_total") {
		t.Errorf("Expected Prometheus output to contain counter metric, got: %s", output)
	}
	if !strings.Contains(output, "memory_usage") {
		t.Errorf("Expected Prometheus output to contain gauge metric, got: %s", output)
	}
	if !strings.Contains(output, "42") {
		t.Errorf("Expected Prometheus output to contain counter value, got: %s", output)
	}
	if !strings.Contains(output, "123.45") {
		t.Errorf("Expected Prometheus output to contain gauge value, got: %s", output)
	}
}

func TestTracing(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:   InfoLevel,
		Output:  &buf,
		Service: "test-service",
		Encoder: NewJSONEncoder(false),
	})
	
	exporter := NewLoggingSpanExporter(logger)
	tracer := NewTracer("test-service", exporter, AlwaysSample{})
	
	span := tracer.StartSpan("test_operation")
	span.SetTag("user_id", 123)
	span.LogEvent("processing started")
	span.Finish()
	
	// Check that span was exported to logs
	output := buf.String()
	if !strings.Contains(output, "test_operation") {
		t.Errorf("Expected log output to contain span operation name, got: %s", output)
	}
}

func TestTracingWithContext(t *testing.T) {
	tracer := NewTracer("test-service", nil, AlwaysSample{})
	
	ctx := context.Background()
	span, ctx := tracer.StartSpanFromContext(ctx, "parent_operation")
	
	// Start child span
	childSpan, _ := tracer.StartSpanFromContext(ctx, "child_operation")
	
	if childSpan.TraceID != span.TraceID {
		t.Error("Expected child span to have same trace ID as parent")
	}
	if childSpan.ParentID != span.SpanID {
		t.Error("Expected child span to have parent span ID")
	}
	
	childSpan.Finish()
	span.Finish()
}

func TestHealthChecks(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:   InfoLevel,
		Output:  &buf,
		Service: "test-service",
		Encoder: NewJSONEncoder(false),
	})
	
	healthManager := NewHealthManager(5*time.Second, logger)
	
	// Add a simple health check
	simpleCheck := &testHealthCheck{name: "test_check", status: HealthStatusUp}
	healthManager.RegisterCheck(simpleCheck)
	
	ctx := context.Background()
	report := healthManager.CheckHealth(ctx)
	
	if report.Status != HealthStatusUp {
		t.Errorf("Expected overall health status to be UP, got: %v", report.Status)
	}
	
	if len(report.Components) != 1 {
		t.Errorf("Expected 1 component in health report, got: %d", len(report.Components))
	}
	
	if _, exists := report.Components["test_check"]; !exists {
		t.Error("Expected health report to contain test_check component")
	}
}

func TestHealthCheckDown(t *testing.T) {
	healthManager := NewHealthManager(5*time.Second, nil)
	
	// Add a failing health check
	failingCheck := &testHealthCheck{name: "failing_check", status: HealthStatusDown}
	healthManager.RegisterCheck(failingCheck)
	
	ctx := context.Background()
	report := healthManager.CheckHealth(ctx)
	
	if report.Status != HealthStatusDown {
		t.Errorf("Expected overall health status to be DOWN, got: %v", report.Status)
	}
}

func TestMemoryHealthCheck(t *testing.T) {
	memCheck := NewMemoryHealthCheck("memory", 0.8, 0.95)
	
	ctx := context.Background()
	result := memCheck.Check(ctx)
	
	// This should typically pass unless running under extreme memory pressure
	if result.Status == HealthStatusUnknown {
		t.Error("Memory health check returned unknown status")
	}
	
	if result.Details == nil {
		t.Error("Expected memory health check to include details")
	}
}

func TestObservabilitySystem(t *testing.T) {
	config := DefaultObservabilityConfig()
	config.DashboardEnabled = false // Disable dashboard for test
	
	system, err := NewObservabilitySystem(config)
	if err != nil {
		t.Fatalf("Failed to create observability system: %v", err)
	}
	
	if system.GetLogger() == nil {
		t.Error("Expected observability system to have a logger")
	}
	
	if system.GetRegistry() == nil {
		t.Error("Expected observability system to have a metrics registry")
	}
	
	if system.GetTracer() == nil {
		t.Error("Expected observability system to have a tracer")
	}
	
	if system.GetHealthManager() == nil {
		t.Error("Expected observability system to have a health manager")
	}
}

func TestObservabilitySystemStartStop(t *testing.T) {
	config := DefaultObservabilityConfig()
	config.DashboardEnabled = false // Disable dashboard for test
	config.DashboardPort = 0        // Use any available port
	
	system, err := NewObservabilitySystem(config)
	if err != nil {
		t.Fatalf("Failed to create observability system: %v", err)
	}
	
	ctx := context.Background()
	
	err = system.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start observability system: %v", err)
	}
	
	// Test double start
	err = system.Start(ctx)
	if err == nil {
		t.Error("Expected error when starting already started system")
	}
	
	err = system.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop observability system: %v", err)
	}
}

func TestInstrumentHTTPHandler(t *testing.T) {
	config := DefaultObservabilityConfig()
	config.DashboardEnabled = false
	
	system, err := NewObservabilitySystem(config)
	if err != nil {
		t.Fatalf("Failed to create observability system: %v", err)
	}
	
	// Create a test handler
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}
	
	// Instrument the handler
	instrumentedHandler := system.InstrumentHTTPHandler("test", testHandler)
	
	// This test just ensures the instrumented handler doesn't panic
	// In a real test, you'd want to make actual HTTP requests and verify metrics
	if instrumentedHandler == nil {
		t.Error("Expected instrumented handler to not be nil")
	}
}

// Test helper types

type testHealthCheck struct {
	name   string
	status HealthStatus
}

func (t *testHealthCheck) Name() string {
	return t.name
}

func (t *testHealthCheck) Check(ctx context.Context) HealthResult {
	return HealthResult{
		Status:  t.status,
		Message: "Test health check",
	}
}

func TestLogLevelFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"DEBUG", DebugLevel},
		{"debug", DebugLevel},
		{"INFO", InfoLevel},
		{"info", InfoLevel},
		{"WARN", WarnLevel},
		{"warn", WarnLevel},
		{"WARNING", WarnLevel},
		{"ERROR", ErrorLevel},
		{"error", ErrorLevel},
		{"FATAL", FatalLevel},
		{"fatal", FatalLevel},
		{"UNKNOWN", InfoLevel}, // Default
	}
	
	for _, test := range tests {
		result := LogLevelFromString(test.input)
		if result != test.expected {
			t.Errorf("LogLevelFromString(%s) = %v, expected %v", test.input, result, test.expected)
		}
	}
}

func TestProbabilitySampler(t *testing.T) {
	sampler := NewProbabilitySampler(0.0) // Never sample
	
	// Test multiple times to ensure consistent behavior
	for i := 0; i < 10; i++ {
		traceID := generateTraceID()
		spanID := generateSpanID()
		if sampler.ShouldSample(traceID, spanID, "test") {
			t.Error("Expected 0.0 probability sampler to never sample")
		}
	}
	
	alwaysSampler := NewProbabilitySampler(1.0) // Always sample
	
	for i := 0; i < 10; i++ {
		traceID := generateTraceID()
		spanID := generateSpanID()
		if !alwaysSampler.ShouldSample(traceID, spanID, "test") {
			t.Error("Expected 1.0 probability sampler to always sample")
		}
	}
}

func TestSpanOperations(t *testing.T) {
	tracer := NewTracer("test-service", nil, AlwaysSample{})
	span := tracer.StartSpan("test_operation")
	
	// Test setting tags
	span.SetTag("key1", "value1")
	span.SetTag("key2", 42)
	
	if span.Tags["key1"] != "value1" {
		t.Error("Expected tag to be set correctly")
	}
	if span.Tags["key2"] != 42 {
		t.Error("Expected numeric tag to be set correctly")
	}
	
	// Test logging
	span.LogEvent("test event")
	span.LogFields(map[string]interface{}{
		"field1": "value1",
		"field2": 123,
	})
	
	if len(span.Logs) != 2 {
		t.Errorf("Expected 2 log entries, got %d", len(span.Logs))
	}
	
	// Test status
	span.SetStatus(SpanStatusError)
	if span.Status != SpanStatusError {
		t.Error("Expected span status to be set to error")
	}
	
	// Test finish
	if span.IsFinished() {
		t.Error("Expected span to not be finished initially")
	}
	
	span.Finish()
	
	if !span.IsFinished() {
		t.Error("Expected span to be finished after calling Finish()")
	}
	
	if span.Duration == 0 {
		t.Error("Expected span to have non-zero duration after finishing")
	}
}