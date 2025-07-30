# Observability System

This package provides comprehensive observability capabilities for the Snowflake deployment CLI, including structured logging, metrics collection, distributed tracing, health checks, and real-time monitoring dashboards.

## Features

### 🔍 Structured Logging
- JSON-formatted logs with contextual information
- Multiple log levels (DEBUG, INFO, WARN, ERROR, FATAL)
- Automatic trace ID and span ID correlation  
- Caller information and stack traces for errors
- Configurable outputs (stdout, stderr, files)

### 📊 Metrics Collection
- Prometheus-compatible metrics
- Support for counters, gauges, and histograms
- Built-in system metrics (memory, CPU, goroutines)
- Application-specific metrics (deployments, queries, etc.)
- HTTP endpoint for metrics scraping

### 🔗 Distributed Tracing
- OpenTelemetry-compatible tracing
- Hierarchical span relationships
- Automatic context propagation
- Configurable sampling rates
- Span tags, logs, and status tracking

### ❤️ Health Checks
- Comprehensive health monitoring
- Multiple health check types (memory, database, custom)
- HTTP endpoints for health, readiness, and liveness probes
- Detailed health status reporting
- Kubernetes-compatible probe endpoints

### 📈 Real-time Dashboard
- Web-based monitoring dashboard
- Live metrics visualization
- Health status overview
- System information display
- Auto-refreshing data

### 🚨 Alerting
- Rule-based alerting system
- Multiple severity levels
- Configurable alert destinations
- Built-in alert cooldowns
- Custom alert conditions

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "flakedrop/internal/observability"
)

func main() {
    // Initialize with default configuration
    config := observability.DefaultObservabilityConfig()
    config.ServiceName = "my-service"
    
    err := observability.Initialize(config)
    if err != nil {
        panic(err)
    }
    
    ctx := context.Background()
    err = observability.Start(ctx)
    if err != nil {
        panic(err)
    }
    defer observability.Stop(ctx)
    
    // Use observability components
    logger := observability.GetDefaultLogger()
    logger.Info("Service started")
    
    // Dashboard available at http://localhost:8080/dashboard
}
```

### Logging

```go
logger := observability.GetDefaultLogger()

// Simple logging
logger.Info("Operation completed")
logger.Error("Something went wrong")

// Structured logging with fields
logger.InfoWithFields("User logged in", map[string]interface{}{
    "user_id": 123,
    "method":  "oauth",
})

// Context-aware logging
logger.WithContext(ctx).Info("Processing request")
```

### Metrics

```go
registry := observability.GetDefaultRegistry()

// Create metrics
counter := observability.NewCounter("requests_total", "Total requests", nil)
registry.Register(counter)

gauge := observability.NewGauge("queue_size", "Current queue size", nil)
registry.Register(gauge)

histogram := observability.NewHistogram("duration_seconds", "Operation duration", nil, nil)
registry.Register(histogram)

// Use metrics
counter.Inc()
gauge.Set(42)
histogram.Observe(1.5)
```

### Tracing

```go
// Start a span
span, ctx := observability.StartSpanFromContext(ctx, "my_operation")
defer span.Finish()

// Add metadata
span.SetTag("user_id", 123)
span.LogEvent("processing started")

// Handle errors
if err != nil {
    span.LogError(err)
}

// Trace function calls
err := observability.TraceFunction(ctx, "database_query", func(ctx context.Context) error {
    // Your code here
    return nil
})
```

### Health Checks

```go
// Built-in health checks
memCheck := observability.NewMemoryHealthCheck("memory", 0.8, 0.95)
observability.RegisterHealthCheck(memCheck)

// Custom health checks
type MyHealthCheck struct {
    name string
}

func (h *MyHealthCheck) Name() string {
    return h.name
}

func (h *MyHealthCheck) Check(ctx context.Context) observability.HealthResult {
    return observability.HealthResult{
        Status:  observability.HealthStatusUp,
        Message: "All systems operational",
    }
}

observability.RegisterHealthCheck(&MyHealthCheck{name: "my_service"})
```

## Configuration

### Environment Variables

```bash
SNOWFLAKE_DEPLOY_LOG_LEVEL=info
SNOWFLAKE_DEPLOY_LOG_FORMAT=json
SNOWFLAKE_DEPLOY_METRICS_ENABLED=true
SNOWFLAKE_DEPLOY_TRACING_ENABLED=true
SNOWFLAKE_DEPLOY_DASHBOARD_PORT=8080
```

### Configuration File

```yaml
service_name: my-service
service_version: 1.0.0
environment: production

logging:
  level: info
  format: json
  output: stdout

metrics:
  enabled: true
  prefix: my_service
  collection_interval: 30s

tracing:
  enabled: true
  sample_rate: 1.0
  service_name: my-service

health:
  enabled: true
  timeout: 10s

dashboard:
  enabled: true
  port: 8080
```

## HTTP Endpoints

When the dashboard is enabled, the following endpoints are available:

- `GET /dashboard` - Web dashboard interface
- `GET /metrics` - Prometheus metrics endpoint
- `GET /health` - Health check endpoint
- `GET /health/ready` - Readiness probe
- `GET /health/live` - Liveness probe
- `GET /api/data` - JSON dashboard data
- `GET /api/traces` - Active traces data

## Integration

### Prometheus

Add scrape configuration:

```yaml
scrape_configs:
  - job_name: 'flakedrop'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
```

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: app
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8080
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8080
```

### Grafana

Import dashboards for visualization of collected metrics.

## Architecture

The observability system is composed of several components:

- **Logger**: Structured logging with context propagation
- **MetricsRegistry**: Metric collection and management
- **Tracer**: Distributed tracing with span management
- **HealthManager**: Health check coordination
- **DashboardServer**: Web interface for monitoring
- **AlertManager**: Rule-based alerting system

All components are integrated and work together to provide comprehensive observability.

## Performance

The observability system is designed to be lightweight and performant:

- Minimal overhead for metric collection
- Efficient trace context propagation
- Non-blocking health checks
- Configurable sampling rates
- Async logging and metric updates

## Testing

Run the test suite:

```bash
go test ./internal/observability -v
```

Run the demo application:

```bash
go run ./examples/observability_demo.go
```

## Documentation

For detailed documentation, see:

- [Observability Guide](../../docs/OBSERVABILITY.md)
- [API Documentation](https://pkg.go.dev/...)
- [Configuration Reference](../../configs/observability.yaml)

## Contributing

When adding new observability features:

1. Add comprehensive tests
2. Update documentation
3. Follow existing patterns
4. Consider performance impact
5. Add examples where appropriate