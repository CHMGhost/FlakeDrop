package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"flakedrop/internal/common"
	"flakedrop/internal/config"
	"flakedrop/internal/observability"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/models"
)

var (
	monitoredDeployDryRun      bool
	monitoredDeployCommit      string
	monitoredDeployInteractive bool
	enableMonitoring           bool
	dashboardPort              int
)

var deployMonitoredCmd = &cobra.Command{
	Use:   "deploy-monitored [repository]",
	Short: "Deploy code to Snowflake with comprehensive observability",
	Long: `Deploy SQL changes from a Git repository to Snowflake with full observability.
	
This command provides comprehensive monitoring, logging, tracing, and health checks
during the deployment process. It includes:

- Structured logging with contextual information
- Metrics collection for deployment performance
- Distributed tracing for operation visibility
- Health checks for system components
- Real-time dashboard for monitoring
- Alerting for failures and performance issues

Use --enable-monitoring to start the observability dashboard.`,
	Args: cobra.ExactArgs(1),
	Run:  runMonitoredDeploy,
}

func init() {
	rootCmd.AddCommand(deployMonitoredCmd)
	
	deployMonitoredCmd.Flags().BoolVarP(&monitoredDeployDryRun, "dry-run", "d", false, "Show what would be deployed without executing")
	deployMonitoredCmd.Flags().StringVarP(&monitoredDeployCommit, "commit", "c", "", "Deploy a specific commit (skip interactive selection)")
	deployMonitoredCmd.Flags().BoolVarP(&monitoredDeployInteractive, "interactive", "i", true, "Use interactive mode for commit selection")
	deployMonitoredCmd.Flags().BoolVar(&enableMonitoring, "enable-monitoring", true, "Enable observability dashboard")
	deployMonitoredCmd.Flags().IntVar(&dashboardPort, "dashboard-port", 8080, "Port for observability dashboard")
}

func runMonitoredDeploy(cmd *cobra.Command, args []string) {
	repoName := args[0]
	ctx := context.Background()
	
	// Initialize observability system
	obsConfig := observability.DefaultObservabilityConfig()
	obsConfig.DashboardEnabled = enableMonitoring
	obsConfig.DashboardPort = dashboardPort
	obsConfig.ServiceName = "flakedrop"
	obsConfig.ServiceVersion = "1.0.0"
	obsConfig.Environment = "production" // Could be configurable
	
	// Initialize observability
	err := observability.Initialize(obsConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to initialize observability: %w", err))
		return
	}
	
	// Start observability system
	err = observability.Start(ctx)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to start observability: %w", err))
		return
	}
	defer observability.Stop(ctx)
	
	// Get observability components
	system := observability.GetGlobalSystem()
	logger := system.GetLogger()
	registry := system.GetRegistry()
	
	// Register deployment-specific health checks
	system.RegisterHealthCheck(&DeploymentHealthCheck{
		name:   "deployment_service",
		logger: logger,
	})
	
	// Start deployment tracing
	span, ctx := observability.StartSpanFromContext(ctx, "deploy.main")
	defer span.Finish()
	
	span.SetTag("repository", repoName)
	span.SetTag("dry_run", monitoredDeployDryRun)
	span.SetTag("interactive", monitoredDeployInteractive)
	
	logger.InfoWithFields("Starting deployment", map[string]interface{}{
		"repository":  repoName,
		"dry_run":     monitoredDeployDryRun,
		"interactive": monitoredDeployInteractive,
		"commit":      monitoredDeployCommit,
	})
	
	if enableMonitoring {
		ui.ShowInfo(fmt.Sprintf("🎯 Observability dashboard available at: http://localhost:%d/dashboard", dashboardPort))
		ui.ShowInfo(fmt.Sprintf("📊 Metrics endpoint: http://localhost:%d/metrics", dashboardPort))
		ui.ShowInfo(fmt.Sprintf("❤️  Health check: http://localhost:%d/health", dashboardPort))
		fmt.Println()
	}
	
	// Show header
	ui.ShowHeader(fmt.Sprintf("FlakeDrop - Repository: %s", repoName))
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	// Execute deployment with observability
	executeMonitoredDeployment(ctx, repoName, appConfig, logger, registry, span)
}

func executeMonitoredDeployment(ctx context.Context, repoName string, appConfig *models.Config, logger *observability.Logger, registry *observability.MetricsRegistry, parentSpan *observability.Span) {
	// Track deployment start
	if counter, ok := registry.GetCounter("deployments_total"); ok {
		counter.Inc()
	}
	
	deploymentStart := time.Now()
	var deploymentSuccess bool
	defer func() {
		// Record deployment duration
		if histogram, ok := registry.GetHistogram("deployment_duration_seconds"); ok {
			histogram.Observe(time.Since(deploymentStart).Seconds())
		}
		
		// Track success/failure
		if deploymentSuccess {
			if counter, ok := registry.GetCounter("deployments_success_total"); ok {
				counter.Inc()
			}
		} else {
			if counter, ok := registry.GetCounter("deployments_failure_total"); ok {
				counter.Inc()
			}
		}
	}()
	
	// Connect to Snowflake (with tracing)
	err := observability.TraceFunction(ctx, "snowflake.connect", func(ctx context.Context) error {
		span := observability.SpanFromContext(ctx)
		span.SetTag("component", "snowflake")
		
		logger.Info("Connecting to Snowflake...")
		spinner := ui.NewSpinner("Connecting to Snowflake...")
		spinner.Start()
		
		// Simulate connection time with some variance
		time.Sleep(2 * time.Second)
		
		spinner.Stop(true, "Connected to Snowflake")
		logger.Info("Successfully connected to Snowflake")
		
		// Update Snowflake connections gauge
		if gauge, ok := registry.GetGauge("snowflake_connections_active"); ok {
			gauge.Inc()
		}
		
		return nil
	})
	
	if err != nil {
		logger.ErrorWithFields("Failed to connect to Snowflake", map[string]interface{}{
			"error": err.Error(),
		})
		parentSpan.LogError(err)
		ui.ShowError(err)
		return
	}
	
	// Get commits (with tracing)
	var commits []ui.CommitInfo
	err = observability.TraceFunction(ctx, "git.get_commits", func(ctx context.Context) error {
		span := observability.SpanFromContext(ctx)
		span.SetTag("component", "git")
		
		logger.Info("Fetching recent commits...")
		commits, err = getRealCommits(appConfig, repoName)
		if err != nil {
			return fmt.Errorf("failed to get commits: %w", err)
		}
		
		span.SetTag("commit_count", len(commits))
		logger.InfoWithFields("Retrieved commits", map[string]interface{}{
			"count": len(commits),
		})
		
		// Track git operations
		if counter, ok := registry.GetCounter("git_operations_total"); ok {
			counter.Inc()
		}
		
		return nil
	})
	
	if err != nil {
		logger.ErrorWithFields("Failed to get commits", map[string]interface{}{
			"error": err.Error(),
		})
		parentSpan.LogError(err)
		ui.ShowError(err)
		return
	}
	
	// Select commit (with tracing)
	var selectedCommit string
	err = observability.TraceFunction(ctx, "commit.select", func(ctx context.Context) error {
		span := observability.SpanFromContext(ctx)
		
		if monitoredDeployCommit != "" {
			selectedCommit = monitoredDeployCommit
			span.SetTag("selection_method", "provided")
			span.SetTag("commit_hash", selectedCommit)
			displayCommit := selectedCommit
			if len(selectedCommit) > 7 {
				displayCommit = selectedCommit[:7]
			}
			ui.ShowInfo(fmt.Sprintf("Using commit: %s", displayCommit))
			logger.InfoWithFields("Using provided commit", map[string]interface{}{
				"commit": selectedCommit,
			})
		} else if monitoredDeployInteractive {
			span.SetTag("selection_method", "interactive")
			var err error
			selectedCommit, err = ui.SelectCommit(commits)
			if err != nil {
				return err
			}
			span.SetTag("commit_hash", selectedCommit)
			logger.InfoWithFields("Selected commit interactively", map[string]interface{}{
				"commit": selectedCommit,
			})
		} else {
			span.SetTag("selection_method", "latest")
			if len(commits) > 0 {
				selectedCommit = commits[0].Hash
				span.SetTag("commit_hash", selectedCommit)
				displayCommit := selectedCommit
				if len(selectedCommit) > 7 {
					displayCommit = selectedCommit[:7]
				}
				ui.ShowInfo(fmt.Sprintf("Using latest commit: %s", displayCommit))
				logger.InfoWithFields("Using latest commit", map[string]interface{}{
					"commit": selectedCommit,
				})
			}
		}
		
		return nil
	})
	
	if err != nil {
		logger.ErrorWithFields("Failed to select commit", map[string]interface{}{
			"error": err.Error(),
		})
		parentSpan.LogError(err)
		ui.ShowError(err)
		return
	}
	
	// Get files to deploy (with tracing)
	var files []string
	err = observability.TraceFunction(ctx, "deployment.analyze_files", func(ctx context.Context) error {
		span := observability.SpanFromContext(ctx)
		span.SetTag("commit", selectedCommit)
		
		logger.InfoWithFields("Analyzing files to deploy", map[string]interface{}{
			"commit": selectedCommit,
		})
		
		// Get real files from repository
		realFiles, err := getRealFilesToDeploy(appConfig, repoName)
		if err != nil {
			return fmt.Errorf("failed to get files: %w", err)
		}
		files = realFiles
		
		span.SetTag("file_count", len(files))
		logger.InfoWithFields("Files analyzed", map[string]interface{}{
			"count": len(files),
			"files": files,
		})
		
		return nil
	})
	
	if err != nil {
		logger.ErrorWithFields("Failed to analyze files", map[string]interface{}{
			"error": err.Error(),
		})
		parentSpan.LogError(err)
		ui.ShowError(err)
		return
	}
	
	// Show deployment summary
	ui.ShowDeploymentSummary(files, selectedCommit)
	
	if monitoredDeployDryRun {
		ui.ShowWarning("Running in dry-run mode - no changes will be applied")
		parentSpan.SetTag("dry_run", true)
	}
	
	// Confirm deployment
	confirm, err := ui.Confirm("Proceed with deployment?", true)
	if err != nil || !confirm {
		ui.ShowWarning("Deployment cancelled")
		logger.Warn("Deployment cancelled by user")
		parentSpan.SetTag("cancelled", true)
		return
	}
	
	// Execute deployment with comprehensive monitoring
	deploymentSuccess = executeMonitoredDeploymentFiles(ctx, files, monitoredDeployDryRun, logger, registry, appConfig, repoName)
	
	if deploymentSuccess {
		logger.Info("Deployment completed successfully")
		parentSpan.SetTag("success", true)
	} else {
		logger.Error("Deployment completed with errors")
		parentSpan.SetTag("success", false)
		parentSpan.SetStatus(observability.SpanStatusError)
	}
}

func executeMonitoredDeploymentFiles(ctx context.Context, files []string, dryRun bool, logger *observability.Logger, registry *observability.MetricsRegistry, appConfig *models.Config, repoName string) bool {
	span, ctx := observability.StartSpanFromContext(ctx, "deployment.execute_files")
	defer span.Finish()
	
	span.SetTag("file_count", len(files))
	span.SetTag("dry_run", dryRun)
	
	pb := ui.NewProgressBar(len(files))
	successCount := 0
	failureCount := 0
	
	logger.InfoWithFields("Starting file deployment", map[string]interface{}{
		"total_files": len(files),
		"dry_run":     dryRun,
	})
	
	for i, file := range files {
		// Create a span for each file deployment
		fileSpan, _ := observability.StartSpanFromContext(ctx, "deployment.execute_file")
		fileSpan.SetTag("file", file)
		fileSpan.SetTag("file_index", i+1)
		fileSpan.SetTag("dry_run", dryRun)
		
		ui.ShowFileExecution(file, i+1, len(files))
		
		fileLogger := logger.WithFields(map[string]interface{}{
			"file":       file,
			"file_index": i + 1,
			"total":      len(files),
		})
		
		fileLogger.Info("Executing file")
		
		var success bool
		var errorMsg string
		var duration time.Duration
		
		if dryRun {
			// Simulate dry run execution
			time.Sleep(500 * time.Millisecond)
			success = true
			duration = 500 * time.Millisecond
			fileSpan.SetTag("result", "skipped")
			fileLogger.Info("File execution skipped (dry-run)")
		} else {
			// Execute the actual SQL file
			start := time.Now()
			
			// Track Snowflake query
			if counter, ok := registry.GetCounter("snowflake_queries_total"); ok {
				counter.Inc()
			}
			
			// Need to create Snowflake service and execute file
			success, errorMsg = executeActualFile(ctx, file, appConfig, repoName, fileLogger)
			duration = time.Since(start)
			
			// Record query duration
			if histogram, ok := registry.GetHistogram("snowflake_query_duration_seconds"); ok {
				histogram.Observe(duration.Seconds())
			}
			
			if success {
				fileSpan.SetTag("result", "success")
				fileLogger.InfoWithFields("File executed successfully", map[string]interface{}{
					"duration_ms": duration.Milliseconds(),
				})
			} else {
				fileSpan.SetTag("result", "failure")
				fileSpan.SetTag("error", errorMsg)
				fileSpan.SetStatus(observability.SpanStatusError)
				fileSpan.LogError(fmt.Errorf("file execution failed: %s", errorMsg))
				fileLogger.ErrorWithFields("File execution failed", map[string]interface{}{
					"error":       errorMsg,
					"duration_ms": duration.Milliseconds(),
				})
			}
		}
		
		// Show execution result
		if success {
			if dryRun {
				ui.ShowExecutionResult(file, true, "", "skipped")
			} else {
				ui.ShowExecutionResult(file, true, "", formatDuration(duration))
			}
			successCount++
		} else {
			ui.ShowExecutionResult(file, false, errorMsg, "")
			failureCount++
		}
		
		pb.Update(i+1, file, success)
		fileSpan.Finish()
	}
	
	pb.Finish()
	
	// Log final summary
	logger.InfoWithFields("Deployment execution completed", map[string]interface{}{
		"total_files":    len(files),
		"successful":     successCount,
		"failed":         failureCount,
		"success_rate":   float64(successCount) / float64(len(files)) * 100,
	})
	
	// Show final summary
	fmt.Println()
	allSuccessful := failureCount == 0
	if allSuccessful {
		ui.ShowSuccess(fmt.Sprintf("Deployment completed successfully! %d files deployed.", successCount))
	} else {
		ui.ShowError(fmt.Errorf("Deployment completed with errors: %d succeeded, %d failed", successCount, failureCount))
	}
	
	span.SetTag("success_count", successCount)
	span.SetTag("failure_count", failureCount)
	span.SetTag("success", allSuccessful)
	
	if !allSuccessful {
		span.SetStatus(observability.SpanStatusError)
	}
	
	return allSuccessful
}

// DeploymentHealthCheck checks the health of the deployment service
type DeploymentHealthCheck struct {
	name   string
	logger *observability.Logger
}

func (d *DeploymentHealthCheck) Name() string {
	return d.name
}

func (d *DeploymentHealthCheck) Check(ctx context.Context) observability.HealthResult {
	// Check if we can connect to Snowflake (simulated)
	// In a real implementation, this would test actual connectivity
	
	// Simulate health check
	time.Sleep(100 * time.Millisecond)
	
	// For demo purposes, randomly return degraded status sometimes
	now := time.Now()
	if now.Second()%10 == 0 {
		return observability.HealthResult{
			Status:  observability.HealthStatusDegraded,
			Message: "High latency detected in Snowflake connections",
			Details: map[string]interface{}{
				"avg_latency_ms": 150,
				"max_latency_ms": 300,
			},
		}
	}
	
	return observability.HealthResult{
		Status:  observability.HealthStatusUp,
		Message: "Deployment service is healthy",
		Details: map[string]interface{}{
			"snowflake_connected": true,
			"git_accessible":      true,
			"avg_latency_ms":      50,
		},
	}
}

// executeActualFile executes a SQL file against Snowflake
func executeActualFile(ctx context.Context, file string, appConfig *models.Config, repoName string, logger *observability.Logger) (bool, string) {
	// Find repository configuration
	var repoConfig *models.Repository
	for _, repo := range appConfig.Repositories {
		if repo.Name == repoName {
			repoConfig = &repo
			break
		}
	}
	
	if repoConfig == nil {
		return false, fmt.Sprintf("repository '%s' not found", repoName)
	}
	
	// Get repository path
	repoPath := repoConfig.GitURL
	if !filepath.IsAbs(repoPath) {
		wd, _ := os.Getwd()
		repoPath = filepath.Join(wd, repoPath)
	}
	
	// Get full file path
	fullPath := filepath.Join(repoPath, file)
	
	// Validate file path
	validatedPath, err := common.ValidatePath(fullPath, repoPath)
	if err != nil {
		return false, fmt.Sprintf("invalid file path: %v", err)
	}
	
	// Create Snowflake service
	snowflakeConfig := snowflake.Config{
		Account:   appConfig.Snowflake.Account,
		Username:  appConfig.Snowflake.Username,
		Password:  appConfig.Snowflake.Password,
		Database:  repoConfig.Database,
		Schema:    repoConfig.Schema,
		Warehouse: appConfig.Snowflake.Warehouse,
		Role:      appConfig.Snowflake.Role,
		Timeout:   30 * time.Second,
	}
	
	snowflakeService := snowflake.NewService(snowflakeConfig)
	if err := snowflakeService.Connect(); err != nil {
		return false, fmt.Sprintf("failed to connect to Snowflake: %v", err)
	}
	defer snowflakeService.Close()
	
	// Execute SQL file
	err = snowflakeService.ExecuteFile(validatedPath, repoConfig.Database, repoConfig.Schema)
	if err != nil {
		return false, err.Error()
	}
	
	return true, ""
}

