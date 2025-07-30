package observability

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// ObservabilityConfig contains configuration for the observability system
type ObservabilityConfig struct {
	// Logging configuration
	LogLevel        LogLevel `yaml:"log_level"`
	LogFormat       string   `yaml:"log_format"` // "json" or "text"
	LogOutput       string   `yaml:"log_output"` // "stdout", "stderr", or file path
	
	// Metrics configuration
	MetricsEnabled  bool          `yaml:"metrics_enabled"`
	MetricsPrefix   string        `yaml:"metrics_prefix"`
	MetricsInterval time.Duration `yaml:"metrics_interval"`
	
	// Tracing configuration
	TracingEnabled     bool    `yaml:"tracing_enabled"`
	TracingSampleRate  float64 `yaml:"tracing_sample_rate"`
	TracingServiceName string  `yaml:"tracing_service_name"`
	
	// Health check configuration
	HealthEnabled      bool          `yaml:"health_enabled"`
	HealthTimeout      time.Duration `yaml:"health_timeout"`
	HealthInterval     time.Duration `yaml:"health_interval"`
	
	// Dashboard configuration
	DashboardEnabled bool   `yaml:"dashboard_enabled"`
	DashboardPort    int    `yaml:"dashboard_port"`
	DashboardPath    string `yaml:"dashboard_path"`
	
	// Alerting configuration
	AlertingEnabled bool `yaml:"alerting_enabled"`
	
	// Service information
	ServiceName    string `yaml:"service_name"`
	ServiceVersion string `yaml:"service_version"`
	Environment    string `yaml:"environment"`
}

// DefaultObservabilityConfig returns default configuration
func DefaultObservabilityConfig() ObservabilityConfig {
	return ObservabilityConfig{
		LogLevel:           InfoLevel,
		LogFormat:          "json",
		LogOutput:          "stdout",
		MetricsEnabled:     true,
		MetricsPrefix:      "snowflake_deploy",
		MetricsInterval:    30 * time.Second,
		TracingEnabled:     true,
		TracingSampleRate:  1.0,
		TracingServiceName: "flakedrop",
		HealthEnabled:      true,
		HealthTimeout:      10 * time.Second,
		HealthInterval:     30 * time.Second,
		DashboardEnabled:   true,
		DashboardPort:      8080,
		DashboardPath:      "/dashboard",
		AlertingEnabled:    true,
		ServiceName:        "flakedrop",
		ServiceVersion:     "1.0.0",
		Environment:        "development",
	}
}

// ObservabilitySystem manages all observability components
type ObservabilitySystem struct {
	config         ObservabilityConfig
	logger         *Logger
	registry       *MetricsRegistry
	tracer         *Tracer
	healthManager  *HealthManager
	dashboardServer *DashboardServer
	alertManager   *AlertManager
	httpServer     *http.Server
	started        bool
}

// NewObservabilitySystem creates a new observability system
func NewObservabilitySystem(config ObservabilityConfig) (*ObservabilitySystem, error) {
	// Initialize logger
	logger, err := initializeLogger(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}
	
	// Initialize metrics registry
	var registry *MetricsRegistry
	if config.MetricsEnabled {
		registry = NewMetricsRegistry(config.MetricsPrefix)
		RegisterStandardMetrics()
	}
	
	// Initialize tracer
	var tracer *Tracer
	if config.TracingEnabled {
		spanExporter := NewLoggingSpanExporter(logger)
		sampler := NewProbabilitySampler(config.TracingSampleRate)
		tracer = NewTracer(config.TracingServiceName, spanExporter, sampler)
		InitTracing(config.TracingServiceName, spanExporter, sampler)
	}
	
	// Initialize health manager
	var healthManager *HealthManager
	if config.HealthEnabled {
		healthManager = NewHealthManager(config.HealthTimeout, logger)
		initializeDefaultHealthChecks(healthManager, logger)
	}
	
	// Initialize dashboard server
	var dashboardServer *DashboardServer
	if config.DashboardEnabled && registry != nil && healthManager != nil && tracer != nil {
		dashboardServer = NewDashboardServer(registry, healthManager, tracer, logger)
		SetVersion(config.ServiceVersion)
	}
	
	// Initialize alert manager
	var alertManager *AlertManager
	if config.AlertingEnabled {
		alertManager = NewAlertManager(logger)
		for _, rule := range GetDefaultAlertRules() {
			alertManager.AddRule(rule)
		}
	}
	
	system := &ObservabilitySystem{
		config:          config,
		logger:          logger,
		registry:        registry,
		tracer:          tracer,
		healthManager:   healthManager,
		dashboardServer: dashboardServer,
		alertManager:    alertManager,
	}
	
	return system, nil
}

// Start starts the observability system
func (os *ObservabilitySystem) Start(ctx context.Context) error {
	if os.started {
		return fmt.Errorf("observability system already started")
	}
	
	os.logger.Info("Starting observability system")
	
	// Start metrics collection
	if os.registry != nil {
		collector := NewMetricsCollector(os.registry, os.logger, os.config.MetricsInterval)
		go collector.Start(ctx)
	}
	
	// Start dashboard server
	if os.dashboardServer != nil {
		mux := http.NewServeMux()
		
		// Dashboard routes
		mux.Handle("/", os.dashboardServer)
		mux.Handle("/dashboard", os.dashboardServer)
		mux.Handle("/dashboard/", os.dashboardServer)
		mux.Handle("/api/", os.dashboardServer)
		
		// Health check routes
		if os.healthManager != nil {
			mux.Handle("/health", os.healthManager.HealthHandler())
			mux.Handle("/health/ready", os.healthManager.ReadinessHandler())
			mux.Handle("/health/live", os.healthManager.LivenessHandler())
		}
		
		// Metrics endpoint (Prometheus format)
		if os.registry != nil {
			exporter := NewPrometheusExporter(os.registry)
			mux.Handle("/metrics", exporter.MetricsHandler())
		}
		
		// Start HTTP server with proper timeouts
		os.httpServer = &http.Server{
			Addr:              fmt.Sprintf(":%d", os.config.DashboardPort),
			Handler:           mux,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
		}
		
		go func() {
			os.logger.InfoWithFields("Dashboard server starting", map[string]interface{}{
				"port": os.config.DashboardPort,
				"path": os.config.DashboardPath,
			})
			
			if err := os.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				os.logger.ErrorWithFields("Dashboard server error", map[string]interface{}{
					"error": err.Error(),
				})
			}
		}()
	}
	
	// Start alerting
	if os.alertManager != nil && os.dashboardServer != nil {
		go os.startAlerting(ctx)
	}
	
	os.started = true
	os.logger.Info("Observability system started successfully")
	
	return nil
}

// Stop stops the observability system
func (os *ObservabilitySystem) Stop(ctx context.Context) error {
	if !os.started {
		return nil
	}
	
	os.logger.Info("Stopping observability system")
	
	// Stop HTTP server
	if os.httpServer != nil {
		if err := os.httpServer.Shutdown(ctx); err != nil {
			os.logger.ErrorWithFields("Error stopping dashboard server", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}
	
	os.started = false
	os.logger.Info("Observability system stopped")
	
	return nil
}

// GetLogger returns the logger
func (os *ObservabilitySystem) GetLogger() *Logger {
	return os.logger
}

// GetRegistry returns the metrics registry
func (os *ObservabilitySystem) GetRegistry() *MetricsRegistry {
	return os.registry
}

// GetTracer returns the tracer
func (os *ObservabilitySystem) GetTracer() *Tracer {
	return os.tracer
}

// GetHealthManager returns the health manager
func (os *ObservabilitySystem) GetHealthManager() *HealthManager {
	return os.healthManager
}

// RegisterHealthCheck registers a custom health check
func (os *ObservabilitySystem) RegisterHealthCheck(check HealthCheck) {
	if os.healthManager != nil {
		os.healthManager.RegisterCheck(check)
	}
}

// startAlerting starts the alerting system
func (os *ObservabilitySystem) startAlerting(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute) // Check alerts every minute
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if os.dashboardServer != nil {
				data := os.dashboardServer.collectDashboardData(ctx)
				os.alertManager.CheckAlerts(data)
			}
		}
	}
}

// initializeLogger initializes the logger based on configuration
func initializeLogger(config ObservabilityConfig) (*Logger, error) {
	var output *os.File
	var err error
	
	switch config.LogOutput {
	case "stdout":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		output, err = os.OpenFile(config.LogOutput, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
	}
	
	var encoder LogEncoder
	if config.LogFormat == "json" {
		encoder = NewJSONEncoder(false)
	} else {
		encoder = NewJSONEncoder(true) // Pretty print for text format
	}
	
	loggerConfig := LoggerConfig{
		Level:       config.LogLevel,
		Output:      output,
		Service:     config.ServiceName,
		Version:     config.ServiceVersion,
		Environment: config.Environment,
		Encoder:     encoder,
	}
	
	logger := NewLogger(loggerConfig)
	SetDefaultLogger(logger)
	
	return logger, nil
}

// initializeDefaultHealthChecks initializes default health checks
func initializeDefaultHealthChecks(healthManager *HealthManager, logger *Logger) {
	// Memory health check
	memoryCheck := NewMemoryHealthCheck("memory", 0.8, 0.95)
	healthManager.RegisterCheck(memoryCheck)
	
	// Basic service health check
	serviceCheck := &BasicServiceHealthCheck{
		name:   "service",
		logger: logger,
	}
	healthManager.RegisterCheck(serviceCheck)
}

// BasicServiceHealthCheck is a basic service health check
type BasicServiceHealthCheck struct {
	name   string
	logger *Logger
}

// Name returns the check name
func (b *BasicServiceHealthCheck) Name() string {
	return b.name
}

// Check performs the basic service health check
func (b *BasicServiceHealthCheck) Check(ctx context.Context) HealthResult {
	// This is a basic check that always returns UP
	// In a real implementation, this could check database connections,
	// external service dependencies, etc.
	
	return HealthResult{
		Status:  HealthStatusUp,
		Message: "Service is running",
		Details: map[string]interface{}{
			"uptime": time.Since(startTime).String(),
		},
	}
}

// InstrumentHTTPHandler instruments an HTTP handler with observability
func (os *ObservabilitySystem) InstrumentHTTPHandler(name string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		
		// Start tracing
		var span *Span
		if os.tracer != nil {
			span, ctx = os.tracer.StartSpanFromContext(ctx, fmt.Sprintf("http.%s", name))
			defer span.Finish()
			
			span.SetTag("http.method", r.Method)
			span.SetTag("http.url", r.URL.String())
			span.SetTag("http.remote_addr", r.RemoteAddr)
		}
		
		// Update request context
		r = r.WithContext(ctx)
		
		// Track metrics
		start := time.Now()
		
		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: 200}
		
		// Execute handler
		handler(wrapped, r)
		
		duration := time.Since(start)
		
		// Record metrics
		if os.registry != nil {
			if counter, ok := os.registry.GetCounter("http_requests_total"); ok {
				counter.Inc()
			}
			
			if histogram, ok := os.registry.GetHistogram("http_request_duration_seconds"); ok {
				histogram.Observe(duration.Seconds())
			}
		}
		
		// Update span
		if span != nil {
			span.SetTag("http.status_code", wrapped.statusCode)
			if wrapped.statusCode >= 400 {
				span.SetStatus(SpanStatusError)
			}
		}
		
		// Log request
		if os.logger != nil {
			os.logger.InfoWithFields("HTTP request", map[string]interface{}{
				"method":      r.Method,
				"url":         r.URL.String(),
				"status_code": wrapped.statusCode,
				"duration_ms": duration.Milliseconds(),
				"remote_addr": r.RemoteAddr,
			})
		}
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Global observability system
var globalSystem *ObservabilitySystem

// Initialize initializes the global observability system
func Initialize(config ObservabilityConfig) error {
	system, err := NewObservabilitySystem(config)
	if err != nil {
		return err
	}
	
	globalSystem = system
	return nil
}

// Start starts the global observability system
func Start(ctx context.Context) error {
	if globalSystem == nil {
		return fmt.Errorf("observability system not initialized")
	}
	return globalSystem.Start(ctx)
}

// Stop stops the global observability system
func Stop(ctx context.Context) error {
	if globalSystem == nil {
		return nil
	}
	return globalSystem.Stop(ctx)
}

// GetGlobalSystem returns the global observability system
func GetGlobalSystem() *ObservabilitySystem {
	return globalSystem
}