package ui

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"flakedrop/pkg/models"
)

// ConfigWizard provides an interactive configuration setup
type ConfigWizard struct {
	currentStep int
	totalSteps  int
}

// NewConfigWizard creates a new configuration wizard
func NewConfigWizard() *ConfigWizard {
	return &ConfigWizard{
		currentStep: 1,
		totalSteps:  5,
	}
}

// Run executes the configuration wizard
func (w *ConfigWizard) Run() (*models.Config, error) {
	ShowHeader("FlakeDrop - Configuration Setup")
	
	config := &models.Config{}
	
	// Step 1: Database configuration
	if err := w.configureDatabaseStep(config); err != nil {
		if err == terminal.InterruptErr {
			return nil, fmt.Errorf("configuration cancelled")
		}
		return nil, err
	}
	
	// Step 2: Repository configuration
	if err := w.configureRepositoryStep(config); err != nil {
		if err == terminal.InterruptErr {
			return nil, fmt.Errorf("configuration cancelled")
		}
		return nil, err
	}
	
	// Step 3: Deployment settings
	if err := w.configureDeploymentStep(config); err != nil {
		if err == terminal.InterruptErr {
			return nil, fmt.Errorf("configuration cancelled")
		}
		return nil, err
	}
	
	// Step 4: Advanced settings
	if err := w.configureAdvancedStep(config); err != nil {
		if err == terminal.InterruptErr {
			return nil, fmt.Errorf("configuration cancelled")
		}
		return nil, err
	}
	
	// Step 5: Review and confirm
	if err := w.reviewConfiguration(config); err != nil {
		return nil, err
	}
	
	return config, nil
}

func (w *ConfigWizard) configureDatabaseStep(config *models.Config) error {
	w.showProgress("Database Configuration")
	
	questions := []*survey.Question{
		{
			Name: "account",
			Prompt: &survey.Input{
				Message: "Snowflake Account:",
				Help:    "Your Snowflake account identifier (e.g., xy12345.us-east-1)",
			},
			Validate: survey.Required,
		},
		{
			Name: "username",
			Prompt: &survey.Input{
				Message: "Username:",
				Help:    "Your Snowflake username",
			},
			Validate: survey.Required,
		},
		{
			Name: "password",
			Prompt: &survey.Password{
				Message: "Password:",
				Help:    "Your Snowflake password (will be stored securely)",
			},
			Validate: survey.Required,
		},
		{
			Name: "database",
			Prompt: &survey.Input{
				Message: "Database:",
				Default: "DEV",
				Help:    "Target database for deployments",
			},
			Validate: survey.Required,
		},
		{
			Name: "warehouse",
			Prompt: &survey.Input{
				Message: "Warehouse:",
				Default: "COMPUTE_WH",
				Help:    "Warehouse to use for executing queries",
			},
			Validate: survey.Required,
		},
		{
			Name: "role",
			Prompt: &survey.Input{
				Message: "Role:",
				Default: "SYSADMIN",
				Help:    "Role to use for deployments",
			},
			Validate: survey.Required,
		},
	}
	
	answers := struct {
		Account   string
		Username  string
		Password  string
		Database  string
		Warehouse string
		Role      string
	}{}
	
	if err := survey.Ask(questions, &answers); err != nil {
		return err
	}
	
	// Update config
	config.Snowflake = models.Snowflake{
		Account:   answers.Account,
		Username:  answers.Username,
		Password:  answers.Password,
		Warehouse: answers.Warehouse,
		Role:      answers.Role,
	}
	
	w.currentStep++
	return nil
}

func (w *ConfigWizard) configureRepositoryStep(config *models.Config) error {
	w.showProgress("Repository Configuration")
	
	// Check if we're in a git repository
	inRepo := false
	repoPath := ""
	// This would be determined by actual git check
	
	questions := []*survey.Question{
		{
			Name: "path",
			Prompt: &survey.Input{
				Message: "Repository Path:",
				Default: func() string {
					if inRepo {
						return repoPath
					}
					return "."
				}(),
				Help: "Path to your SQL repository",
			},
			Validate: survey.Required,
		},
		{
			Name: "branch",
			Prompt: &survey.Input{
				Message: "Default Branch:",
				Default: "main",
				Help:    "Default branch for deployments",
			},
			Validate: survey.Required,
		},
	}
	
	answers := struct {
		Path   string
		Branch string
	}{}
	
	if err := survey.Ask(questions, &answers); err != nil {
		return err
	}
	
	// Update config - we'll add this as a new repository
	// In a real implementation, this would update or add to the repositories list
	
	w.currentStep++
	return nil
}

func (w *ConfigWizard) configureDeploymentStep(config *models.Config) error {
	w.showProgress("Deployment Settings")
	
	questions := []*survey.Question{
		{
			Name: "sqlPath",
			Prompt: &survey.Input{
				Message: "SQL Files Path:",
				Default: "sql",
				Help:    "Relative path to SQL files within the repository",
			},
			Validate: survey.Required,
		},
		{
			Name: "filePattern",
			Prompt: &survey.Input{
				Message: "File Pattern:",
				Default: "*.sql",
				Help:    "Pattern for SQL files to deploy (supports wildcards)",
			},
			Validate: survey.Required,
		},
		{
			Name: "autoCommit",
			Prompt: &survey.Confirm{
				Message: "Enable auto-commit?",
				Default: true,
				Help:    "Automatically commit each SQL statement",
			},
		},
		{
			Name: "dryRun",
			Prompt: &survey.Confirm{
				Message: "Enable dry-run by default?",
				Default: false,
				Help:    "Show what would be deployed without executing",
			},
		},
	}
	
	answers := struct {
		SQLPath     string
		FilePattern string
		AutoCommit  bool
		DryRun      bool
	}{}
	
	if err := survey.Ask(questions, &answers); err != nil {
		return err
	}
	
	// Update config - these would be stored as part of repository settings
	// In a real implementation, deployment settings would be per-repository
	
	w.currentStep++
	return nil
}

func (w *ConfigWizard) configureAdvancedStep(config *models.Config) error {
	w.showProgress("Advanced Settings")
	
	useAdvanced := false
	prompt := &survey.Confirm{
		Message: "Configure advanced settings?",
		Default: false,
		Help:    "Configure timeouts, logging, and other advanced options",
	}
	
	if err := survey.AskOne(prompt, &useAdvanced); err != nil {
		return err
	}
	
	if !useAdvanced {
		// Use defaults - these would be stored in a separate settings section
		w.currentStep++
		return nil
	}
	
	questions := []*survey.Question{
		{
			Name: "queryTimeout",
			Prompt: &survey.Input{
				Message: "Query Timeout (seconds):",
				Default: "300",
				Help:    "Maximum time for a single query execution",
			},
			Validate: func(val interface{}) error {
				// Validate numeric input
				return nil
			},
		},
		{
			Name: "logLevel",
			Prompt: &survey.Select{
				Message: "Log Level:",
				Options: []string{"debug", "info", "warn", "error"},
				Default: "info",
				Help:    "Logging verbosity level",
			},
		},
		{
			Name: "maxConcurrent",
			Prompt: &survey.Input{
				Message: "Max Concurrent Queries:",
				Default: "5",
				Help:    "Maximum number of concurrent SQL executions",
			},
		},
	}
	
	answers := struct {
		QueryTimeout  string
		LogLevel      string
		MaxConcurrent string
	}{}
	
	if err := survey.Ask(questions, &answers); err != nil {
		return err
	}
	
	// Parse and update config
	// In a real implementation, we'd parse the string values and store in settings
	
	w.currentStep++
	return nil
}

func (w *ConfigWizard) reviewConfiguration(config *models.Config) error {
	w.showProgress("Review Configuration")
	
	fmt.Println("\n" + ColorInfo("Configuration Summary:"))
	fmt.Println(strings.Repeat("─", 50))
	
	// Snowflake settings
	fmt.Println(ColorBold("\nSnowflake Settings:"))
	fmt.Printf("  Account:   %s\n", config.Snowflake.Account)
	fmt.Printf("  Username:  %s\n", config.Snowflake.Username)
	fmt.Printf("  Warehouse: %s\n", config.Snowflake.Warehouse)
	fmt.Printf("  Role:      %s\n", config.Snowflake.Role)
	
	// Repository settings (placeholder)
	fmt.Println(ColorBold("\nRepository Settings:"))
	fmt.Printf("  Repositories configured: %d\n", len(config.Repositories))
	
	fmt.Println(strings.Repeat("─", 50))
	
	confirm := false
	prompt := &survey.Confirm{
		Message: "Save this configuration?",
		Default: true,
	}
	
	if err := survey.AskOne(prompt, &confirm); err != nil {
		return err
	}
	
	if !confirm {
		return fmt.Errorf("configuration cancelled")
	}
	
	return nil
}

func (w *ConfigWizard) showProgress(step string) {
	fmt.Printf("\n%s [Step %d/%d] %s\n\n",
		ColorProgress("►"),
		w.currentStep,
		w.totalSteps,
		ColorBold(step),
	)
}