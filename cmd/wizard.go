package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"flakedrop/internal/common"
	"flakedrop/internal/template"
	"flakedrop/internal/scaffold"
	"flakedrop/internal/config"
	"flakedrop/pkg/models"
	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cobra"
)

var wizardCmd = &cobra.Command{
	Use:   "wizard",
	Short: "Interactive project setup wizard",
	Long: `An interactive wizard that guides you through setting up a new FlakeDrop project
with best practices, templates, and configurations tailored to your needs.`,
	Run: runWizard,
}

func init() {
	rootCmd.AddCommand(wizardCmd)
}

func runWizard(cmd *cobra.Command, args []string) {
	fmt.Println("🧙 FlakeDrop Project Wizard")
	fmt.Println("==================================")
	fmt.Println()
	fmt.Println("This wizard will guide you through creating a new FlakeDrop project.")
	fmt.Println()

	// Step 1: Project basics
	projectInfo := getProjectInfo()
	
	// Step 2: Project type and features
	projectType, features := selectProjectFeatures()
	
	// Step 3: Environments
	environments := configureEnvironments()
	
	// Step 4: Snowflake configuration
	snowflakeConfig := configureSnowflake(environments)
	
	// Step 5: CI/CD setup
	cicdPlatforms := selectCICDPlatforms()
	
	// Step 6: Additional features
	additionalFeatures := selectAdditionalFeatures()
	
	// Step 7: Review and confirm
	if !reviewConfiguration(projectInfo, projectType, features, environments, cicdPlatforms) {
		fmt.Println("Wizard cancelled.")
		return
	}
	
	// Step 8: Create project
	if err := createProjectFromWizard(projectInfo, projectType, features, environments, snowflakeConfig, cicdPlatforms, additionalFeatures); err != nil {
		fmt.Printf("Error creating project: %v\n", err)
		os.Exit(1)
	}
	
	// Step 9: Next steps
	showNextSteps(projectInfo.Name, cicdPlatforms)
}

func getProjectInfo() ProjectInfo {
	var info ProjectInfo
	
	// Project name
	namePrompt := &survey.Input{
		Message: "Project name:",
		Default: "snowflake-project",
		Help:    "The name of your project directory",
	}
	survey.AskOne(namePrompt, &info.Name)
	
	// Company/Organization
	companyPrompt := &survey.Input{
		Message: "Company/Organization name:",
		Help:    "Used in documentation and configurations",
	}
	survey.AskOne(companyPrompt, &info.Company)
	
	// Author
	authorPrompt := &survey.Input{
		Message: "Author name:",
		Default: os.Getenv("USER"),
		Help:    "Your name for documentation purposes",
	}
	survey.AskOne(authorPrompt, &info.Author)
	
	// Description
	descPrompt := &survey.Input{
		Message: "Project description:",
		Help:    "Brief description of your project",
	}
	survey.AskOne(descPrompt, &info.Description)
	
	return info
}

func selectProjectFeatures() (string, []string) {
	// Project type
	var projectType string
	typePrompt := &survey.Select{
		Message: "Select project type:",
		Options: []string{
			"minimal - Simple project with basic structure",
			"standard - Recommended for most projects",
			"enterprise - Full-featured with compliance and monitoring",
		},
		Default: "standard - Recommended for most projects",
	}
	survey.AskOne(typePrompt, &projectType)
	projectType = strings.Split(projectType, " - ")[0]
	
	// Features based on project type
	var features []string
	
	switch projectType {
	case "enterprise":
		// Enterprise gets all features by default
		features = []string{
			"monitoring",
			"security",
			"compliance",
			"backup",
			"multi-region",
			"advanced-rbac",
			"audit-logging",
		}
	case "standard":
		// Let user select features for standard
		featurePrompt := &survey.MultiSelect{
			Message: "Select additional features:",
			Options: []string{
				"monitoring - Performance monitoring and alerts",
				"security - Enhanced security features",
				"backup - Automated backup and recovery",
				"testing - Comprehensive test suite",
				"documentation - Extended documentation",
			},
		}
		
		var selectedFeatures []string
		survey.AskOne(featurePrompt, &selectedFeatures)
		
		for _, f := range selectedFeatures {
			features = append(features, strings.Split(f, " - ")[0])
		}
	case "minimal":
		// Minimal gets no extra features
		features = []string{}
	}
	
	return projectType, features
}

func configureEnvironments() []string {
	var environments []string
	
	// Predefined environment sets
	envPrompt := &survey.Select{
		Message: "Select environment configuration:",
		Options: []string{
			"basic - dev, prod",
			"standard - dev, staging, prod",
			"extended - dev, test, staging, prod",
			"enterprise - dev, test, staging, prod, dr",
			"custom - Define your own",
		},
		Default: "standard - dev, staging, prod",
	}
	
	var envChoice string
	survey.AskOne(envPrompt, &envChoice)
	envChoice = strings.Split(envChoice, " - ")[0]
	
	switch envChoice {
	case "basic":
		environments = []string{"dev", "prod"}
	case "standard":
		environments = []string{"dev", "staging", "prod"}
	case "extended":
		environments = []string{"dev", "test", "staging", "prod"}
	case "enterprise":
		environments = []string{"dev", "test", "staging", "prod", "dr"}
	case "custom":
		customPrompt := &survey.Input{
			Message: "Enter environments (comma-separated):",
			Default: "dev,staging,prod",
		}
		var customEnvs string
		survey.AskOne(customPrompt, &customEnvs)
		
		for _, env := range strings.Split(customEnvs, ",") {
			env = strings.TrimSpace(env)
			if env != "" {
				environments = append(environments, env)
			}
		}
	}
	
	return environments
}

func configureSnowflake(environments []string) map[string]models.Environment {
	configs := make(map[string]models.Environment)
	
	fmt.Println("\n📊 Snowflake Configuration")
	fmt.Println("Configure connection details for each environment.")
	fmt.Println()
	
	// Quick setup option
	var quickSetup bool
	quickPrompt := &survey.Confirm{
		Message: "Use quick setup with defaults?",
		Default: true,
		Help:    "Quick setup uses sensible defaults that you can customize later",
	}
	survey.AskOne(quickPrompt, &quickSetup)
	
	if quickSetup {
		// Account format
		var accountFormat string
		accountPrompt := &survey.Input{
			Message: "Snowflake account format (e.g., xy12345.us-east-1):",
			Help:    "Your organization's Snowflake account identifier",
		}
		survey.AskOne(accountPrompt, &accountFormat)
		
		// Generate configs with defaults
		for _, env := range environments {
			isProduction := env == "prod" || env == "production"
			configs[env] = models.Environment{
				Name:     env,
				Account:  accountFormat,
				Username: fmt.Sprintf("%s_DEPLOY_USER", strings.ToUpper(env)),
				Password: fmt.Sprintf("${SNOWFLAKE_%s_PASSWORD}", strings.ToUpper(env)),
				Database: fmt.Sprintf("%s_DB", strings.ToUpper(env)),
				Schema:   "PUBLIC",
				Warehouse: func() string {
					if isProduction {
						return "PROD_WH"
					}
					return "DEV_WH"
				}(),
				Role: fmt.Sprintf("%s_DEPLOY_ROLE", strings.ToUpper(env)),
			}
		}
	} else {
		// Detailed configuration for each environment
		for _, env := range environments {
			fmt.Printf("\nConfiguring %s environment:\n", env)
			
			var envConfig models.Environment
			envConfig.Name = env
			
			questions := []*survey.Question{
				{
					Name: "account",
					Prompt: &survey.Input{
						Message: "Account:",
						Help:    "Snowflake account identifier",
					},
					Validate: survey.Required,
				},
				{
					Name: "username",
					Prompt: &survey.Input{
						Message: "Username:",
						Default: fmt.Sprintf("%s_DEPLOY_USER", strings.ToUpper(env)),
					},
					Validate: survey.Required,
				},
				{
					Name: "database",
					Prompt: &survey.Input{
						Message: "Database:",
						Default: fmt.Sprintf("%s_DB", strings.ToUpper(env)),
					},
					Validate: survey.Required,
				},
				{
					Name: "schema",
					Prompt: &survey.Input{
						Message: "Schema:",
						Default: "PUBLIC",
					},
					Validate: survey.Required,
				},
				{
					Name: "warehouse",
					Prompt: &survey.Input{
						Message: "Warehouse:",
						Default: func() string {
							if env == "prod" {
								return "PROD_WH"
							}
							return "DEV_WH"
						}(),
					},
					Validate: survey.Required,
				},
				{
					Name: "role",
					Prompt: &survey.Input{
						Message: "Role:",
						Default: fmt.Sprintf("%s_DEPLOY_ROLE", strings.ToUpper(env)),
					},
					Validate: survey.Required,
				},
			}
			
			survey.Ask(questions, &envConfig)
			envConfig.Password = fmt.Sprintf("${SNOWFLAKE_%s_PASSWORD}", strings.ToUpper(env))
			configs[env] = envConfig
		}
	}
	
	return configs
}

func selectCICDPlatforms() []string {
	var platforms []string
	
	platformPrompt := &survey.MultiSelect{
		Message: "Select CI/CD platforms to configure:",
		Options: []string{
			"github - GitHub Actions",
			"gitlab - GitLab CI",
			"jenkins - Jenkins Pipeline", 
			"azure - Azure DevOps",
			"none - Skip CI/CD setup",
		},
	}
	
	var selections []string
	survey.AskOne(platformPrompt, &selections)
	
	for _, s := range selections {
		platform := strings.Split(s, " - ")[0]
		if platform != "none" {
			platforms = append(platforms, platform)
		}
	}
	
	return platforms
}

func selectAdditionalFeatures() AdditionalFeatures {
	var features AdditionalFeatures
	
	// Git initialization
	gitPrompt := &survey.Confirm{
		Message: "Initialize Git repository?",
		Default: true,
	}
	survey.AskOne(gitPrompt, &features.InitGit)
	
	// Example files
	examplesPrompt := &survey.Confirm{
		Message: "Create example migration files?",
		Default: true,
		Help:    "Creates sample SQL files to help you get started",
	}
	survey.AskOne(examplesPrompt, &features.CreateExamples)
	
	// VSCode configuration
	vscodePrompt := &survey.Confirm{
		Message: "Create VSCode configuration?",
		Default: true,
		Help:    "Creates .vscode folder with recommended settings",
	}
	survey.AskOne(vscodePrompt, &features.VSCodeConfig)
	
	// Docker support
	dockerPrompt := &survey.Confirm{
		Message: "Add Docker support?",
		Default: false,
		Help:    "Creates Dockerfile and docker-compose.yml",
	}
	survey.AskOne(dockerPrompt, &features.DockerSupport)
	
	return features
}

func reviewConfiguration(info ProjectInfo, projectType string, features []string, environments []string, platforms []string) bool {
	fmt.Println("\n📋 Configuration Review")
	fmt.Println("======================")
	fmt.Printf("Project Name: %s\n", info.Name)
	fmt.Printf("Company: %s\n", info.Company)
	fmt.Printf("Type: %s\n", projectType)
	fmt.Printf("Environments: %s\n", strings.Join(environments, ", "))
	
	if len(features) > 0 {
		fmt.Printf("Features: %s\n", strings.Join(features, ", "))
	}
	
	if len(platforms) > 0 {
		fmt.Printf("CI/CD: %s\n", strings.Join(platforms, ", "))
	}
	
	fmt.Println()
	
	var confirm bool
	confirmPrompt := &survey.Confirm{
		Message: "Create project with this configuration?",
		Default: true,
	}
	survey.AskOne(confirmPrompt, &confirm)
	
	return confirm
}

func createProjectFromWizard(info ProjectInfo, projectType string, features []string, 
	environments []string, snowflakeConfig map[string]models.Environment, 
	platforms []string, additional AdditionalFeatures) error {
	
	// Create project directory
	projectDir, err := filepath.Abs(info.Name)
	if err != nil {
		return err
	}
	
	// Check if directory exists
	if _, err := os.Stat(projectDir); err == nil {
		return fmt.Errorf("directory %s already exists", info.Name)
	}
	
	fmt.Printf("\n🚀 Creating project: %s\n", info.Name)
	
	// Use template manager
	tm := template.NewTemplateManager()
	_, err = tm.GetTemplate(projectType)
	if err != nil {
		return err
	}
	
	// Prepare template variables
	vars := map[string]interface{}{
		"ProjectName":       info.Name,
		"Company":           info.Company,
		"Author":            info.Author,
		"Description":       info.Description,
		"Environments":      environments,
		"Features":          features,
		"DefaultWarehouse":  "COMPUTE_WH",
		"DefaultRole":       "DEVELOPER",
		"Database":          "DEV_DB",
		"Schema":            "PUBLIC",
	}
	
	// Apply template
	if err := tm.ApplyTemplate(projectType, projectDir, vars); err != nil {
		return err
	}
	
	// Create configuration
	cfg := &models.Config{
		Repositories: []models.Repository{
			{
				Name:     "main",
				GitURL:   "local",
				Branch:   "main",
				Path:     "migrations",
				Database: snowflakeConfig[environments[0]].Database,
				Schema:   snowflakeConfig[environments[0]].Schema,
			},
		},
		Environments: func() []models.Environment {
			envs := []models.Environment{}
			for _, env := range environments {
				if config, ok := snowflakeConfig[env]; ok {
					envs = append(envs, config)
				}
			}
			return envs
		}(),
		Deployment: models.Deployment{
			Timeout:    "30m",
			MaxRetries: 3,
			BatchSize:  10,
			DryRun:     true,
			Rollback: models.RollbackConfig{
				Enabled:         true,
				OnFailure:       true,
				BackupRetention: 7,
				Strategy:        "immediate",
			},
			Validation: models.ValidationConfig{
				Enabled:     true,
				SyntaxCheck: true,
				DryRun:      true,
			},
		},
	}
	
	// Save configuration
	if err := config.Save(cfg); err != nil {
		return err
	}
	
	// Generate CI/CD configurations
	gen := scaffold.NewGenerator(projectDir, &scaffold.Config{
		ProjectName:  info.Name,
		Company:      info.Company,
		Author:       info.Author,
		Environments: environments,
		Features:     features,
	})
	
	for _, platform := range platforms {
		if _, err := gen.GeneratePipeline(platform); err != nil {
			fmt.Printf("Warning: Failed to generate %s configuration: %v\n", platform, err)
		}
	}
	
	// Create example files if requested
	if additional.CreateExamples {
		createWizardExamples(projectDir, snowflakeConfig[environments[0]])
	}
	
	// Initialize Git if requested
	if additional.InitGit {
		initializeGitRepo(projectDir)
	}
	
	// Create VSCode configuration if requested
	if additional.VSCodeConfig {
		createVSCodeConfig(projectDir)
	}
	
	// Create Docker files if requested
	if additional.DockerSupport {
		createDockerSupport(projectDir)
	}
	
	fmt.Println("✅ Project created successfully!")
	
	return nil
}

func createWizardExamples(projectDir string, envConfig models.Environment) error {
	examples := map[string]string{
		"migrations/schema/001_initial_setup.sql": fmt.Sprintf(`-- Initial database setup
-- Author: System
-- Date: %s

USE DATABASE %s;
USE SCHEMA %s;

-- Create a sample table
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER AUTOINCREMENT PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create an index
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- @ROLLBACK
-- DROP INDEX IF EXISTS idx_users_email;
-- DROP TABLE IF EXISTS users;
`, time.Now().Format("2006-01-02"), envConfig.Database, envConfig.Schema),
	}
	
	for path, content := range examples {
		fullPath := filepath.Join(projectDir, path)
		if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
			return err
		}
	}
	
	return nil
}

func createVSCodeConfig(projectDir string) error {
	vscodeDir := filepath.Join(projectDir, ".vscode")
	if err := os.MkdirAll(vscodeDir, common.DirPermissionNormal); err != nil {
		return err
	}
	
	// Settings
	settings := `{
    "files.exclude": {
        "**/.git": true,
        "**/.DS_Store": true,
        "**/node_modules": true,
        "**/__pycache__": true,
        "**/*.pyc": true
    },
    "search.exclude": {
        "**/backups": true,
        "**/logs": true,
        "**/.git": true
    },
    "editor.formatOnSave": true,
    "sql.format.enable": true,
    "[sql]": {
        "editor.defaultFormatter": "mtxr.sqltools"
    }
}`
	
	settingsPath := filepath.Join(vscodeDir, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(settings), 0600); err != nil {
		return err
	}
	
	// Extensions
	extensions := `{
    "recommendations": [
        "mtxr.sqltools",
        "mtxr.sqltools-driver-pg",
        "redhat.vscode-yaml",
        "yzhang.markdown-all-in-one",
        "davidanson.vscode-markdownlint",
        "eamodio.gitlens"
    ]
}`
	
	extensionsPath := filepath.Join(vscodeDir, "extensions.json")
	if err := os.WriteFile(extensionsPath, []byte(extensions), 0600); err != nil {
		return err
	}
	
	return nil
}

func createDockerSupport(projectDir string) error {
	// Dockerfile
	dockerfile := `FROM alpine:latest

# Install snowflake-deploy
RUN apk add --no-cache curl bash git
RUN curl -L https://github.com/your-org/snowflake-deploy/releases/latest/download/snowflake-deploy-linux-amd64 -o /usr/local/bin/flakedrop && \
    chmod +x /usr/local/bin/snowflake-deploy

# Set working directory
WORKDIR /project

# Copy project files
COPY . .

# Default command
CMD ["flakedrop", "--help"]
`
	
	dockerfilePath := filepath.Join(projectDir, "Dockerfile")
	if err := os.WriteFile(dockerfilePath, []byte(dockerfile), 0600); err != nil {
		return err
	}
	
	// docker-compose.yml
	dockerCompose := `version: '3.8'

services:
  snowflake-deploy:
    build: .
    volumes:
      - .:/project
    environment:
      - SNOWFLAKE_DEV_PASSWORD=${SNOWFLAKE_DEV_PASSWORD}
      - SNOWFLAKE_STAGING_PASSWORD=${SNOWFLAKE_STAGING_PASSWORD}
      - SNOWFLAKE_PROD_PASSWORD=${SNOWFLAKE_PROD_PASSWORD}
    command: flakedrop validate --config configs/config.yaml

  deploy-dev:
    build: .
    volumes:
      - .:/project
    environment:
      - SNOWFLAKE_DEV_PASSWORD=${SNOWFLAKE_DEV_PASSWORD}
    command: flakedrop deploy main --env dev --auto-approve

  deploy-prod:
    build: .
    volumes:
      - .:/project
    environment:
      - SNOWFLAKE_PROD_PASSWORD=${SNOWFLAKE_PROD_PASSWORD}
    command: flakedrop deploy main --env prod
`
	
	dockerComposePath := filepath.Join(projectDir, "docker-compose.yml")
	if err := os.WriteFile(dockerComposePath, []byte(dockerCompose), 0600); err != nil {
		return err
	}
	
	return nil
}

func showNextSteps(projectName string, platforms []string) {
	fmt.Println("\n📚 Next Steps")
	fmt.Println("=============")
	fmt.Printf("1. Navigate to your project:\n   cd %s\n\n", projectName)
	fmt.Println("2. Set up environment variables:")
	fmt.Println("   export SNOWFLAKE_DEV_PASSWORD='your-password'")
	fmt.Println("   export SNOWFLAKE_PROD_PASSWORD='your-password'")
	fmt.Println()
	fmt.Println("3. Review and customize configurations:")
	fmt.Println("   - configs/config.yaml")
	fmt.Println("   - configs/environments/*.yaml")
	fmt.Println()
	fmt.Println("4. Add your SQL migrations:")
	fmt.Println("   flakedrop scaffold migration \"create users table\"")
	fmt.Println()
	fmt.Println("5. Validate your setup:")
	fmt.Println("   flakedrop validate")
	fmt.Println()
	fmt.Println("6. Deploy to development:")
	fmt.Println("   flakedrop deploy main --env dev")
	
	if len(platforms) > 0 {
		fmt.Println("\n🔧 CI/CD Setup")
		fmt.Println("==============")
		
		for _, platform := range platforms {
			switch platform {
			case "github":
				fmt.Println("\nGitHub Actions:")
				fmt.Println("- Push your code to GitHub")
				fmt.Println("- Add secrets in repository settings")
				fmt.Println("- Workflows will run automatically")
			case "gitlab":
				fmt.Println("\nGitLab CI:")
				fmt.Println("- Push your code to GitLab")
				fmt.Println("- Configure CI/CD variables in project settings")
			case "jenkins":
				fmt.Println("\nJenkins:")
				fmt.Println("- Create a new Pipeline job")
				fmt.Println("- Configure credentials in Jenkins")
				fmt.Println("- Point to your Jenkinsfile")
			case "azure":
				fmt.Println("\nAzure DevOps:")
				fmt.Println("- Create a new pipeline")
				fmt.Println("- Link to your repository")
				fmt.Println("- Configure variables")
			}
		}
	}
	
	fmt.Println("\n📖 Documentation")
	fmt.Println("===============")
	fmt.Println("- README.md - Project overview")
	fmt.Println("- docs/MIGRATION_GUIDE.md - Creating migrations")
	fmt.Println("- docs/TROUBLESHOOTING.md - Common issues")
	fmt.Println("- docs/BEST_PRACTICES.md - Recommendations")
	fmt.Println()
	fmt.Println("Happy deploying! 🚀")
}

// Types

type ProjectInfo struct {
	Name        string
	Company     string
	Author      string
	Description string
}

type AdditionalFeatures struct {
	InitGit        bool
	CreateExamples bool
	VSCodeConfig   bool
	DockerSupport  bool
}