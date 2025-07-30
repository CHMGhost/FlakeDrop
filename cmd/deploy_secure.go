package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"flakedrop/internal/common"
	"flakedrop/internal/config"
	"flakedrop/internal/git"
	"flakedrop/internal/security"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/models"

	"github.com/spf13/cobra"
)

// SecureDeployService wraps deployment with security features
type SecureDeployService struct {
	config          *models.Config
	securityManager *security.Manager
	gitService      *git.Service
	snowflakeService *snowflake.Service
	ui              *ui.UI
}

// NewSecureDeployService creates a new secure deployment service
func NewSecureDeployService() (*SecureDeployService, error) {
	// Initialize UI
	ui := ui.NewUI(false, false)
	
	// Initialize secure config loader
	secLoader, err := config.NewSecureLoader()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize secure loader: %w", err)
	}
	
	// Load configuration securely
	cfg, err := secLoader.LoadSecure()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}
	
	// Get security manager
	secManager := secLoader.GetSecurityManager()
	
	// Initialize git service
	gitService := git.NewService()
	
	// Initialize Snowflake service with secure credentials
	sfConfig := snowflake.Config{
		Account:   cfg.Snowflake.Account,
		Username:  cfg.Snowflake.Username,
		Password:  cfg.Snowflake.Password, // Already decrypted by secure loader
		Role:      cfg.Snowflake.Role,
		Warehouse: cfg.Snowflake.Warehouse,
		Database:  "",
		Schema:    "",
		Timeout:   30 * time.Second,
	}
	
	sfService := snowflake.NewService(sfConfig)
	
	return &SecureDeployService{
		config:           cfg,
		securityManager:  secManager,
		gitService:       gitService,
		snowflakeService: sfService,
		ui:              ui,
	}, nil
}

// ExecuteSecureDeployment performs deployment with security checks
func (sds *SecureDeployService) ExecuteSecureDeployment(repoName, commitHash string, dryRun bool) error {
	// Find repository configuration
	var repoConfig *models.Repository
	for _, repo := range sds.config.Repositories {
		if repo.Name == repoName {
			repoConfig = &repo
			break
		}
	}
	
	if repoConfig == nil {
		return fmt.Errorf("repository '%s' not found in configuration", repoName)
	}
	
	// Check deployment access
	if err := sds.securityManager.CheckDeploymentAccess(repoName, "deploy"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	
	// Log deployment start
	_ = sds.securityManager.LogDeployment(repoName, commitHash, "started", map[string]interface{}{
		"dry_run": dryRun,
		"user":    os.Getenv("USER"),
	})
	
	// Clone/update repository
	sds.ui.StartProgress("Updating repository...")
	repoPath := filepath.Join(os.TempDir(), "flakedrop", repoName)
	
	if err := sds.gitService.CloneOrUpdate(repoConfig.GitURL, repoPath, repoConfig.Branch); err != nil {
		sds.ui.StopProgress()
		_ = sds.securityManager.LogDeployment(repoName, commitHash, "failed", map[string]interface{}{
			"error": err.Error(),
			"stage": "git_clone",
		})
		return fmt.Errorf("failed to update repository: %w", err)
	}
	sds.ui.StopProgress()
	
	// Get changed files
	files, err := sds.gitService.GetChangedFiles(repoPath, commitHash)
	if err != nil {
		_ = sds.securityManager.LogDeployment(repoName, commitHash, "failed", map[string]interface{}{
			"error": err.Error(),
			"stage": "get_files",
		})
		return fmt.Errorf("failed to get changed files: %w", err)
	}
	
	// Security scan files before deployment
	sds.ui.StartProgress("Performing security scan...")
	scanner := sds.securityManager.GetScanner()
	
	criticalIssues := 0
	for _, file := range files {
		if filepath.Ext(file) == ".sql" {
			filePath := filepath.Join(repoPath, file)
			report, err := scanner.ScanFile(filePath)
			if err != nil {
				sds.ui.Warning(fmt.Sprintf("Failed to scan %s: %v", file, err))
				continue
			}
			
			if report.Summary.Critical > 0 {
				criticalIssues += report.Summary.Critical
				sds.ui.Error(fmt.Sprintf("Critical security issues in %s:", file))
				for _, finding := range report.Findings {
					if finding.Severity == security.SeverityCritical {
						sds.ui.Print(fmt.Sprintf("  - %s: %s", finding.RuleID, finding.Message))
					}
				}
			}
		}
	}
	sds.ui.StopProgress()
	
	// Abort if critical issues found
	if criticalIssues > 0 && !dryRun {
		_ = sds.securityManager.LogDeployment(repoName, commitHash, "aborted", map[string]interface{}{
			"reason":          "security_scan_failed",
			"critical_issues": criticalIssues,
		})
		return fmt.Errorf("deployment aborted: %d critical security issues found", criticalIssues)
	}
	
	// Verify SSL certificate
	sds.ui.Info("Verifying Snowflake SSL certificate...")
	certReport, err := sds.securityManager.VerifySnowflakeSSL(sds.config.Snowflake.Account)
	if err != nil {
		sds.ui.Warning(fmt.Sprintf("SSL verification failed: %v", err))
	} else if !certReport.IsValid {
		sds.ui.Warning("SSL certificate issues detected:")
		for _, issue := range certReport.Issues {
			sds.ui.Print(fmt.Sprintf("  - %s", issue))
		}
	} else {
		sds.ui.Success("SSL certificate valid")
	}
	
	// Connect to Snowflake
	sds.ui.StartProgress("Connecting to Snowflake...")
	if err := sds.snowflakeService.Connect(); err != nil {
		sds.ui.StopProgress()
		_ = sds.securityManager.LogDeployment(repoName, commitHash, "failed", map[string]interface{}{
			"error": err.Error(),
			"stage": "snowflake_connect",
		})
		return fmt.Errorf("failed to connect to Snowflake: %w", err)
	}
	defer sds.snowflakeService.Close()
	sds.ui.StopProgress()
	sds.ui.Success("Connected to Snowflake")
	
	// Show deployment summary
	sds.ui.Info("\nDeployment Summary:")
	sds.ui.Print(fmt.Sprintf("  Repository: %s", repoName))
	sds.ui.Print(fmt.Sprintf("  Commit: %s", commitHash[:8]))
	sds.ui.Print(fmt.Sprintf("  Database: %s", repoConfig.Database))
	sds.ui.Print(fmt.Sprintf("  Schema: %s", repoConfig.Schema))
	sds.ui.Print(fmt.Sprintf("  Files to deploy: %d", len(files)))
	
	if dryRun {
		sds.ui.Warning("\n🔍 DRY RUN MODE - No changes will be applied")
	}
	
	// Execute deployment
	successCount := 0
	failureCount := 0
	deploymentDetails := make(map[string]interface{})
	
	pb := ui.NewProgressBar(len(files))
	
	for i, file := range files {
		if filepath.Ext(file) != ".sql" {
			pb.Update(i+1, file, true)
			continue
		}
		
		sds.ui.Print(fmt.Sprintf("\n[%d/%d] Deploying: %s", i+1, len(files), file))
		
		filePath := filepath.Join(repoPath, file)
		
		// Validate file path to prevent directory traversal
		validatedPath, err := common.ValidatePath(filePath, repoPath)
		if err != nil {
			sds.ui.Error(fmt.Sprintf("  ✗ Invalid file path: %v", err))
			failureCount++
			continue
		}
		
		if dryRun {
			// In dry run, just validate SQL
			content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
			if err != nil {
				sds.ui.Error(fmt.Sprintf("  ✗ Failed to read file: %v", err))
				failureCount++
			} else {
				sds.ui.Success(fmt.Sprintf("  ✓ Would execute %d bytes of SQL", len(content)))
				successCount++
			}
		} else {
			// Execute SQL file
			start := time.Now()
			err := sds.snowflakeService.ExecuteFile(validatedPath, repoConfig.Database, repoConfig.Schema)
			duration := time.Since(start)
			
			if err != nil {
				sds.ui.Error(fmt.Sprintf("  ✗ Failed: %v", err))
				failureCount++
				deploymentDetails[file] = map[string]interface{}{
					"status":   "failed",
					"error":    err.Error(),
					"duration": duration.String(),
				}
			} else {
				sds.ui.Success(fmt.Sprintf("  ✓ Success (%s)", duration))
				successCount++
				deploymentDetails[file] = map[string]interface{}{
					"status":   "success",
					"duration": duration.String(),
				}
			}
		}
		
		pb.Update(i+1, file, err == nil)
	}
	
	pb.Finish()
	
	// Final status
	status := "completed"
	if failureCount > 0 {
		status = "completed_with_errors"
	}
	
	// Log deployment completion
	_ = sds.securityManager.LogDeployment(repoName, commitHash, status, map[string]interface{}{
		"success_count": successCount,
		"failure_count": failureCount,
		"dry_run":       dryRun,
		"details":       deploymentDetails,
	})
	
	// Show final summary
	fmt.Println()
	if failureCount == 0 {
		sds.ui.Success(fmt.Sprintf("✅ Deployment completed successfully! %d files deployed.", successCount))
	} else {
		sds.ui.Error(fmt.Sprintf("⚠️  Deployment completed with errors: %d succeeded, %d failed", 
			successCount, failureCount))
	}
	
	return nil
}

// Enhanced deploy command with security
func runSecureDeploy(cmd *cobra.Command, args []string) {
	repoName := args[0]
	
	// Initialize secure deployment service
	sds, err := NewSecureDeployService()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	// Get commit hash
	var commitHash string
	if deployCommit != "" {
		commitHash = deployCommit
	} else if deployInteractive {
		// Interactive commit selection would go here
		// For now, use HEAD
		commitHash = "HEAD"
	} else {
		commitHash = "HEAD"
	}
	
	// Execute deployment
	if err := sds.ExecuteSecureDeployment(repoName, commitHash, deployDryRun); err != nil {
		fmt.Fprintf(os.Stderr, "Deployment failed: %v\n", err)
		os.Exit(1)
	}
}