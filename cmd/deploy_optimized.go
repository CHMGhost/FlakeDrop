package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"flakedrop/internal/config"
	"flakedrop/internal/git"
	"flakedrop/internal/performance"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

var (
	// Performance flags
	enableParallel   bool
	enableBatching   bool
	enableStreaming  bool
	enableCaching    bool
	maxWorkers       int
	batchSize        int
	streamThreshold  string
	showMetrics      bool
	profileMode      bool
	benchmarkMode    bool
)

var deployOptimizedCmd = &cobra.Command{
	Use:   "deploy-optimized [repository]",
	Short: "Deploy code to Snowflake with performance optimizations",
	Long: `Deploy SQL changes from a Git repository to Snowflake using advanced performance optimizations.

This command provides:
- Parallel processing for multi-file deployments
- Connection pooling for optimal resource usage
- Intelligent batching for small files
- Streaming for large files
- Caching for frequently accessed data
- Performance monitoring and profiling

Example:
  flakedrop deploy-optimized myrepo --parallel --workers 8 --show-metrics`,
	Args: cobra.ExactArgs(1),
	RunE: runOptimizedDeploy,
}

func init() {
	rootCmd.AddCommand(deployOptimizedCmd)

	// Performance optimization flags
	deployOptimizedCmd.Flags().BoolVar(&enableParallel, "parallel", true, "Enable parallel processing")
	deployOptimizedCmd.Flags().BoolVar(&enableBatching, "batching", true, "Enable batch processing for small files")
	deployOptimizedCmd.Flags().BoolVar(&enableStreaming, "streaming", true, "Enable streaming for large files")
	deployOptimizedCmd.Flags().BoolVar(&enableCaching, "caching", true, "Enable caching")
	deployOptimizedCmd.Flags().IntVar(&maxWorkers, "workers", 0, "Maximum number of parallel workers (0 = auto)")
	deployOptimizedCmd.Flags().IntVar(&batchSize, "batch-size", 100, "Maximum statements per batch")
	deployOptimizedCmd.Flags().StringVar(&streamThreshold, "stream-threshold", "10MB", "File size threshold for streaming")
	deployOptimizedCmd.Flags().BoolVar(&showMetrics, "show-metrics", false, "Show performance metrics after deployment")
	deployOptimizedCmd.Flags().BoolVar(&profileMode, "profile", false, "Enable profiling mode")
	deployOptimizedCmd.Flags().BoolVar(&benchmarkMode, "benchmark", false, "Run in benchmark mode")

	// Reuse flags from regular deploy command
	deployOptimizedCmd.Flags().BoolVarP(&deployDryRun, "dry-run", "d", false, "Show what would be deployed without executing")
	deployOptimizedCmd.Flags().StringVarP(&deployCommit, "commit", "c", "", "Deploy a specific commit")
	deployOptimizedCmd.Flags().BoolVarP(&deployInteractive, "interactive", "i", true, "Use interactive mode")
}

func runOptimizedDeploy(cmd *cobra.Command, args []string) error {
	repoName := args[0]
	ctx := context.Background()

	// Show header
	ui.ShowHeader(fmt.Sprintf("Snowflake Deploy (Optimized) - Repository: %s", repoName))

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeConfigInvalid, "Failed to load configuration")
	}

	// Find repository configuration
	var repoConfig *models.Repository
	for _, repo := range cfg.Repositories {
		if repo.Name == repoName {
			repoConfig = &repo
			break
		}
	}

	if repoConfig == nil {
		return errors.New(errors.ErrCodeNotFound, "Repository not found").
			WithContext("repository", repoName).
			WithSuggestions(
				"Check repository name in config file",
				"Run 'flakedrop repo list' to see available repositories",
			)
	}

	// Create optimized configuration
	optConfig := createOptimizedConfig()

	// Show optimization settings
	if showMetrics || profileMode {
		ui.ShowInfo("Performance optimizations enabled:")
		if enableParallel {
			workers := maxWorkers
			if workers == 0 {
				workers = optConfig.ExecutorConfig.MaxWorkers
			}
			ui.ShowInfo(fmt.Sprintf("  • Parallel processing: %d workers", workers))
		}
		if enableBatching {
			ui.ShowInfo(fmt.Sprintf("  • Batch processing: %d statements/batch", batchSize))
		}
		if enableStreaming {
			ui.ShowInfo(fmt.Sprintf("  • Streaming: files > %s", streamThreshold))
		}
		if enableCaching {
			ui.ShowInfo("  • Caching: enabled")
		}
	}

	// Create Snowflake configuration
	sfConfig := snowflake.Config{
		Account:   cfg.Snowflake.Account,
		Username:  cfg.Snowflake.Username,
		Password:  cfg.Snowflake.Password,
		Database:  repoConfig.Database,
		Schema:    repoConfig.Schema,
		Warehouse: cfg.Snowflake.Warehouse,
		Role:      cfg.Snowflake.Role,
		Timeout:   30 * time.Second,
	}

	// Create optimized service
	progressBar := ui.NewProgressBar(1)
	progressBar.Update(0, "Initializing optimized service...", true)
	
	service, err := performance.NewOptimizedService(sfConfig, optConfig)
	if err != nil {
		progressBar.Update(1, "Failed to initialize", false)
		return errors.Wrap(err, errors.ErrCodeInitialization, "Failed to create optimized service")
	}
	defer service.Shutdown(30 * time.Second)
	
	progressBar.Update(1, "Service initialized", true)
	progressBar.Finish()

	// Run benchmark mode if requested
	if benchmarkMode {
		return runBenchmark(ctx, service)
	}

	// Get commits (placeholder implementation)
	var commits []git.CommitInfo
	// TODO: Implement proper commit retrieval
	commits = []git.CommitInfo{
		{Hash: "abc123", Message: "Test commit", Author: "Demo", Date: time.Now()},
	}
	if len(commits) == 0 {
		return errors.New(errors.ErrCodeGit, "No commits found")
	}

	// Select commit
	var selectedCommit string
	if deployCommit != "" {
		selectedCommit = deployCommit
		ui.ShowInfo(fmt.Sprintf("Using specified commit: %s", selectedCommit[:7]))
	} else if deployInteractive && !deployDryRun {
		selectedCommit, err = ui.SelectCommit(convertCommits(commits))
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeUserInput, "Failed to select commit")
		}
	} else if len(commits) > 0 {
		selectedCommit = commits[0].Hash
		ui.ShowInfo(fmt.Sprintf("Using latest commit: %s", selectedCommit[:7]))
	} else {
		return errors.New(errors.ErrCodeGit, "No commits found")
	}

	// Get files to deploy (placeholder implementation)
	files := []string{"demo.sql", "schema.sql"} // TODO: Implement proper file retrieval

	// Filter SQL files
	var sqlFiles []string
	for _, file := range files {
		if strings.HasSuffix(strings.ToLower(file), ".sql") {
			sqlFiles = append(sqlFiles, filepath.Join(repoConfig.Path, file))
		}
	}

	if len(sqlFiles) == 0 {
		ui.ShowWarning("No SQL files found in commit")
		return nil
	}

	// Show deployment summary
	ui.ShowDeploymentSummary(sqlFiles, selectedCommit)

	// Check dry run
	if deployDryRun {
		ui.ShowWarning("Running in dry-run mode - no changes will be applied")
		return nil
	}

	// Confirm deployment
	confirm, err := ui.Confirm(fmt.Sprintf("Deploy %d files to %s.%s?", 
		len(sqlFiles), repoConfig.Database, repoConfig.Schema), true)
	if err != nil || !confirm {
		ui.ShowWarning("Deployment cancelled")
		return nil
	}

	// Start deployment with progress tracking
	ui.ShowHeader("Starting Optimized Deployment")
	
	startTime := time.Now()
	deploymentErr := deployWithProgress(ctx, service, sqlFiles, repoConfig.Database, repoConfig.Schema)
	duration := time.Since(startTime)

	// Show results
	if deploymentErr != nil {
		ui.ShowError(deploymentErr)
	} else {
		ui.ShowSuccess(fmt.Sprintf("Deployment completed successfully in %v!", duration))
	}

	// Show metrics if requested
	if showMetrics || profileMode {
		showPerformanceMetrics(service, duration)
	}

	return deploymentErr
}

func createOptimizedConfig() *performance.OptimizedConfig {
	config := performance.DefaultOptimizedConfig()

	// Apply command line overrides
	config.EnableParallelProcessing = enableParallel
	config.EnableBatching = enableBatching
	config.EnableStreaming = enableStreaming
	config.EnableCaching = enableCaching
	config.EnableProfiling = profileMode

	if maxWorkers > 0 {
		config.ExecutorConfig.MaxWorkers = maxWorkers
	}

	if batchSize > 0 {
		config.BatchConfig.BatchSize = batchSize
	}

	// Parse stream threshold
	if threshold, err := parseSize(streamThreshold); err == nil {
		config.LargeFileThreshold = threshold
	}

	return config
}

func deployWithProgress(ctx context.Context, service *performance.OptimizedService, files []string, database, schema string) error {
	// Create progress bar
	progressBar := ui.NewProgressBar(len(files))
	
	// Create a channel to receive progress updates
	progressChan := make(chan progressUpdate, len(files))
	
	// Start deployment in background
	go func() {
		// This is a simplified version - in reality, the OptimizedService
		// would need to support progress callbacks
		err := service.DeployFiles(ctx, files, database, schema)
		
		// Send completion
		for i, file := range files {
			progressChan <- progressUpdate{
				index:    i + 1,
				file:     filepath.Base(file),
				success:  err == nil,
				duration: time.Since(time.Now()),
			}
		}
		close(progressChan)
	}()

	// Update progress bar
	successCount := 0
	failureCount := 0
	
	for update := range progressChan {
		progressBar.Update(update.index, update.file, update.success)
		
		if update.success {
			successCount++
		} else {
			failureCount++
		}
	}
	
	progressBar.Finish()

	// Show summary
	fmt.Println()
	if failureCount == 0 {
		ui.ShowSuccess(fmt.Sprintf("All %d files deployed successfully!", successCount))
		return nil
	} else if successCount > 0 {
		ui.ShowWarning(fmt.Sprintf("Deployment completed with mixed results: %d succeeded, %d failed", 
			successCount, failureCount))
	} else {
		ui.ShowError(fmt.Errorf("All %d deployments failed", failureCount))
	}

	return fmt.Errorf("deployment had %d failures", failureCount)
}

type progressUpdate struct {
	index    int
	file     string
	success  bool
	duration time.Duration
}

func showPerformanceMetrics(service *performance.OptimizedService, totalDuration time.Duration) {
	report := service.GetReport()
	
	ui.ShowHeader("Performance Metrics")
	
	// Overall metrics
	fmt.Printf("\nDeployment Performance:\n")
	fmt.Printf("  Total Duration: %v\n", totalDuration)
	
	if throughput, ok := report.Summary["throughput"].(float64); ok && throughput > 0 {
		fmt.Printf("  Throughput: %.2f files/sec\n", throughput)
	}
	
	// Connection pool metrics
	if poolMetrics, ok := report.Summary["pool_metrics"].(performance.PoolMetrics); ok {
		fmt.Printf("\nConnection Pool:\n")
		fmt.Printf("  Total Connections: %d\n", poolMetrics.TotalConns())
		fmt.Printf("  Active Connections: %d\n", poolMetrics.ActiveConns()) 
		fmt.Printf("  Success Rate: %.2f%%\n", poolMetrics.GetSuccessRate())
		fmt.Printf("  Avg Wait Time: %v\n", poolMetrics.GetAverageWaitTime())
	}
	
	// Executor metrics
	if execMetrics, ok := report.Summary["executor_metrics"].(performance.ExecutorMetrics); ok {
		fmt.Printf("\nParallel Executor:\n")
		fmt.Printf("  Completed Tasks: %d\n", execMetrics.CompletedTasks())
		fmt.Printf("  Failed Tasks: %d\n", execMetrics.FailedTasks())
		fmt.Printf("  Avg Duration: %v\n", execMetrics.AvgDuration())
		fmt.Printf("  Throughput: %.2f tasks/sec\n", execMetrics.Throughput())
	}
	
	// Cache metrics
	if cacheStats, ok := report.Summary["file_cache_stats"].(performance.CacheStats); ok {
		fmt.Printf("\nFile Cache:\n")
		fmt.Printf("  Hit Rate: %.2f%%\n", cacheStats.GetHitRate())
		fmt.Printf("  Items Cached: %d\n", cacheStats.ItemCount)
		fmt.Printf("  Memory Used: %.2f MB\n", float64(cacheStats.TotalSize)/1024/1024)
	}
	
	// Resource usage
	fmt.Printf("\nResource Usage:\n")
	fmt.Printf("  Memory: %.2f MB\n", report.ResourceStats.MemoryUsedMB)
	fmt.Printf("  Goroutines: %d\n", report.ResourceStats.GoroutineCount)
	fmt.Printf("  GC Runs: %d\n", report.ResourceStats.GCRuns)
}

func runBenchmark(ctx context.Context, service *performance.OptimizedService) error {
	ui.ShowHeader("Running Performance Benchmark")
	
	// Create benchmark configuration
	benchConfig := performance.DefaultBenchmarkConfig()
	benchConfig.TestRuns = 5
	benchConfig.Verbose = true
	
	// Create and run benchmark
	benchmark := performance.NewBenchmark(service, benchConfig)
	
	ui.ShowInfo("Generating test data...")
	ui.ShowInfo("Running benchmark scenarios...")
	
	if err := benchmark.Run(ctx); err != nil {
		return errors.Wrap(err, errors.ErrCodeInternal, "Benchmark failed")
	}
	
	ui.ShowSuccess("Benchmark completed successfully!")
	return nil
}

func convertCommits(gitCommits []git.CommitInfo) []ui.CommitInfo {
	commits := make([]ui.CommitInfo, len(gitCommits))
	for i, gc := range gitCommits {
		commits[i] = ui.CommitInfo{
			Hash:      gc.Hash,
			ShortHash: gc.Hash[:7],
			Message:   gc.Message,
			Author:    gc.Author,
			Time:      gc.Date,
			Files:     0, // TODO: Get actual file count from git manager
		}
	}
	return commits
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	
	multiplier := int64(1)
	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	}
	
	var value int64
	_, err := fmt.Sscanf(s, "%d", &value)
	if err != nil {
		return 0, err
	}
	
	return value * multiplier, nil
}