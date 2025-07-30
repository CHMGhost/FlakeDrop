//go:build example
// +build example

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flakedrop/internal/observability"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "observability" {
		observabilityDemo()
	} else {
		errorHandlingDemo()
	}
}

func observabilityDemo() {
	fmt.Println("🎯 FlakeDrop Observability Demo")
	fmt.Println("======================================")
	
	// Initialize observability with demo configuration
	config := observability.DefaultObservabilityConfig()
	config.ServiceName = "observability-demo"
	config.ServiceVersion = "1.0.0"
	config.Environment = "demo"
	config.DashboardPort = 8090
	
	// Initialize the observability system
	err := observability.Initialize(config)
	if err != nil {
		log.Fatalf("Failed to initialize observability: %v", err)
	}
	
	ctx := context.Background()
	
	// Start the observability system
	err = observability.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start observability: %v", err)
	}
	defer observability.Stop(ctx)
	
	system := observability.GetGlobalSystem()
	logger := system.GetLogger()
	registry := system.GetRegistry()
	
	logger.Info("Observability demo started")
	
	// Print access information
	fmt.Printf("\n🌐 Access Information:\n")
	fmt.Printf("   Dashboard: http://localhost:%d/dashboard\n", config.DashboardPort)
	fmt.Printf("   Metrics:   http://localhost:%d/metrics\n", config.DashboardPort)
	fmt.Printf("   Health:    http://localhost:%d/health\n", config.DashboardPort)
	fmt.Printf("\n")
	
	// Add custom health check
	system.RegisterHealthCheck(&DemoHealthCheck{name: "demo_service"})
	
	// Start demo workload
	go runDemoWorkload(ctx, logger, registry)
	
	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	fmt.Println("💡 Demo is running... Press Ctrl+C to stop")
	fmt.Println("   Try accessing the dashboard in your browser!")
	<-sigCh
	
	logger.Info("Observability demo stopping")
	fmt.Println("\n👋 Demo stopped. Thank you!")
}

func runDemoWorkload(ctx context.Context, logger *observability.Logger, registry *observability.MetricsRegistry) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	operationCount := 0
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationCount++
			
			// Simulate a deployment operation with tracing
			span, spanCtx := observability.StartSpanFromContext(ctx, "demo.deployment")
			span.SetTag("operation_id", operationCount)
			span.SetTag("demo", true)
			
			logger.InfoWithFields("Starting demo deployment", map[string]interface{}{
				"operation_id": operationCount,
				"demo":         true,
			})
			
			// Simulate some work
			err := simulateDeployment(spanCtx, logger, registry, operationCount)
			if err != nil {
				span.LogError(err)
				logger.ErrorWithFields("Demo deployment failed", map[string]interface{}{
					"operation_id": operationCount,
					"error":        err.Error(),
				})
			} else {
				logger.InfoWithFields("Demo deployment completed", map[string]interface{}{
					"operation_id": operationCount,
				})
			}
			
			span.Finish()
		}
	}
}

func simulateDeployment(ctx context.Context, logger *observability.Logger, registry *observability.MetricsRegistry, operationID int) error {
	// Track deployment start
	if counter, ok := registry.GetCounter("deployments_total"); ok {
		counter.Inc()
	}
	
	// Simulate connecting to Snowflake
	err := observability.TraceFunction(ctx, "snowflake.connect", func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		logger.Debug("Connected to Snowflake (simulated)")
		return nil
	})
	if err != nil {
		return err
	}
	
	// Simulate processing files
	files := []string{"table1.sql", "view1.sql", "procedure1.sql"}
	
	for i, file := range files {
		fileSpan, _ := observability.StartSpanFromContext(ctx, "deployment.process_file")
		fileSpan.SetTag("file", file)
		fileSpan.SetTag("file_index", i+1)
		
		// Simulate file processing
		time.Sleep(200 * time.Millisecond)
		
		// Track metrics
		if histogram, ok := registry.GetHistogram("deployment_duration_seconds"); ok {
			histogram.Observe(0.2) // 200ms
		}
		
		// Simulate occasional errors
		if operationID%7 == 0 && i == 1 {
			err := fmt.Errorf("simulated error processing %s", file)
			fileSpan.LogError(err)
			fileSpan.Finish()
			
			if counter, ok := registry.GetCounter("deployments_failure_total"); ok {
				counter.Inc()
			}
			
			return err
		}
		
		logger.DebugWithFields("Processed file", map[string]interface{}{
			"file": file,
		})
		
		fileSpan.Finish()
	}
	
	// Track successful deployment
	if counter, ok := registry.GetCounter("deployments_success_total"); ok {
		counter.Inc()
	}
	
	return nil
}

// DemoHealthCheck is a demo health check
type DemoHealthCheck struct {
	name string
}

func (d *DemoHealthCheck) Name() string {
	return d.name
}

func (d *DemoHealthCheck) Check(ctx context.Context) observability.HealthResult {
	// Simulate varying health status
	now := time.Now()
	
	switch now.Second() % 20 {
	case 0, 1, 2:
		return observability.HealthResult{
			Status:  observability.HealthStatusDegraded,
			Message: "Demo service experiencing high latency",
			Details: map[string]interface{}{
				"latency_ms": 250,
				"queue_size": 15,
			},
		}
	case 19:
		return observability.HealthResult{
			Status:  observability.HealthStatusDown,
			Message: "Demo service connection failed",
			Details: map[string]interface{}{
				"error": "connection timeout",
				"retry_count": 3,
			},
		}
	default:
		return observability.HealthResult{
			Status:  observability.HealthStatusUp,
			Message: "Demo service is healthy",
			Details: map[string]interface{}{
				"latency_ms": 50,
				"queue_size": 3,
				"connections": 5,
			},
		}
	}
}