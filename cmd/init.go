package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"flakedrop/internal/common"
	"flakedrop/internal/config"
	"flakedrop/pkg/models"
	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cobra"
)

var (
	initFlags struct {
		template     string
		environment  string
		skipGit      bool
		skipExamples bool
		advanced     bool
	}
)

var initCmd = &cobra.Command{
	Use:   "init [project-name]",
	Short: "Initialize a new FlakeDrop project",
	Long: `Initialize a new FlakeDrop project with best practices and templates.

This command creates a complete project structure including:
- Configuration files for multiple environments
- SQL migration templates
- CI/CD pipeline configurations
- Documentation and examples
- Security and observability settings`,
	Args: cobra.MaximumNArgs(1),
	Run:  runInit,
}

func init() {
	initCmd.Flags().StringVarP(&initFlags.template, "template", "t", "standard", "Project template (standard, enterprise, minimal)")
	initCmd.Flags().StringVarP(&initFlags.environment, "env", "e", "dev,staging,prod", "Comma-separated list of environments")
	initCmd.Flags().BoolVar(&initFlags.skipGit, "skip-git", false, "Skip git repository initialization")
	initCmd.Flags().BoolVar(&initFlags.skipExamples, "skip-examples", false, "Skip creating example files")
	initCmd.Flags().BoolVarP(&initFlags.advanced, "advanced", "a", false, "Enable advanced configuration options")
	
	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) {
	projectName := "snowflake-project"
	if len(args) > 0 {
		projectName = args[0]
	}

	fmt.Printf("🚀 Initializing FlakeDrop project: %s\n", projectName)
	fmt.Println()

	// Create project directory
	projectDir, err := filepath.Abs(projectName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Check if directory exists
	if _, err := os.Stat(projectDir); err == nil {
		var overwrite bool
		prompt := &survey.Confirm{
			Message: fmt.Sprintf("Directory %s already exists. Continue anyway?", projectName),
			Default: false,
		}
		survey.AskOne(prompt, &overwrite)
		if !overwrite {
			fmt.Println("Initialization cancelled.")
			return
		}
	}

	// Create project structure
	if err := createProjectStructure(projectDir); err != nil {
		fmt.Printf("Error creating project structure: %v\n", err)
		os.Exit(1)
	}

	// Initialize configuration
	cfg, err := initializeConfiguration(projectDir)
	if err != nil {
		fmt.Printf("Error initializing configuration: %v\n", err)
		os.Exit(1)
	}

	// Save configuration
	if err := config.Save(cfg); err != nil {
		fmt.Printf("Error saving configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize git repository
	if !initFlags.skipGit {
		if err := initializeGitRepo(projectDir); err != nil {
			fmt.Printf("Warning: Failed to initialize git repository: %v\n", err)
		}
	}

	// Create example files
	if !initFlags.skipExamples {
		if err := createExampleFiles(projectDir, cfg); err != nil {
			fmt.Printf("Warning: Failed to create example files: %v\n", err)
		}
	}

	// Generate documentation
	if err := generateProjectDocumentation(projectDir, projectName); err != nil {
		fmt.Printf("Warning: Failed to generate documentation: %v\n", err)
	}

	fmt.Println()
	fmt.Println("✅ Project initialization complete!")
	fmt.Println()
	fmt.Printf("📁 Project created at: %s\n", projectDir)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("1. cd %s\n", projectName)
	fmt.Println("2. Review and update configs/config.yaml")
	fmt.Println("3. Add your SQL migration files to migrations/")
	fmt.Println("4. Run 'flakedrop deploy <repo-name>' to deploy")
	fmt.Println()
	fmt.Println("For more information, see docs/README.md")
}

func createProjectStructure(projectDir string) error {
	// Define directory structure based on template
	var dirs []string
	
	switch initFlags.template {
	case "minimal":
		dirs = []string{
			"configs",
			"migrations",
			"scripts",
		}
	case "enterprise":
		dirs = []string{
			"configs",
			"configs/environments",
			"migrations",
			"migrations/schema",
			"migrations/data",
			"migrations/procedures",
			"migrations/views",
			"scripts",
			"scripts/pre-deploy",
			"scripts/post-deploy",
			"scripts/validation",
			"tests",
			"tests/unit",
			"tests/integration",
			"docs",
			"ci",
			"monitoring",
			"security",
			"backups",
		}
	default: // standard
		dirs = []string{
			"configs",
			"configs/environments",
			"migrations",
			"migrations/schema",
			"migrations/data",
			"scripts",
			"scripts/pre-deploy",
			"scripts/post-deploy",
			"tests",
			"docs",
			"ci",
		}
	}

	// Create directories
	for _, dir := range dirs {
		fullPath := filepath.Join(projectDir, dir)
		if err := os.MkdirAll(fullPath, common.DirPermissionNormal); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create .gitignore
	gitignoreContent := `# FlakeDrop
*.log
*.tmp
.env
.env.*
!.env.example

# Credentials
configs/credentials/
*.key
*.pem

# Backups
backups/
*.bak
*.backup

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Test output
coverage.out
coverage.html
test-results/
`

	if err := os.WriteFile(filepath.Join(projectDir, ".gitignore"), []byte(gitignoreContent), common.FilePermissionNormal); err != nil {
		return fmt.Errorf("failed to create .gitignore: %w", err)
	}

	return nil
}

func initializeConfiguration(projectDir string) (*models.Config, error) {
	cfg := &models.Config{
		Repositories: []models.Repository{},
		Environments: []models.Environment{},
	}

	// Collect Snowflake credentials
	fmt.Println("📄 Snowflake Configuration")
	fmt.Println("-------------------------")
	
	// Ask for default environment
	var defaultEnv string
	envPrompt := &survey.Select{
		Message: "Select default environment:",
		Options: strings.Split(initFlags.environment, ","),
		Default: "dev",
	}
	survey.AskOne(envPrompt, &defaultEnv)

	// Configure each environment
	environments := strings.Split(initFlags.environment, ",")
	for _, env := range environments {
		env = strings.TrimSpace(env)
		fmt.Printf("\n🔧 Configuring %s environment:\n", env)
		
		envConfig := models.Environment{
			Name: env,
		}

		// For production, suggest different credentials
		isProduction := env == "prod" || env == "production"
		
		questions := []*survey.Question{
			{
				Name: "account",
				Prompt: &survey.Input{
					Message: "Snowflake Account:",
					Help:    "Format: account.region (e.g., xy12345.us-east-1)",
				},
				Validate: survey.Required,
			},
			{
				Name: "username",
				Prompt: &survey.Input{
					Message: "Username:",
					Default: func() string {
						if isProduction {
							return "PROD_DEPLOY_USER"
						}
						return "DEV_USER"
					}(),
				},
				Validate: survey.Required,
			},
			{
				Name: "password",
				Prompt: &survey.Password{
					Message: "Password:",
				},
				Validate: survey.Required,
			},
			{
				Name: "database",
				Prompt: &survey.Input{
					Message: "Database:",
					Default: func() string {
						if isProduction {
							return "PROD_DB"
						}
						return fmt.Sprintf("%s_DB", strings.ToUpper(env))
					}(),
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
						if isProduction {
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
					Default: func() string {
						if isProduction {
							return "PROD_DEPLOY_ROLE"
						}
						return "DEVELOPER"
					}(),
				},
				Validate: survey.Required,
			},
		}

		err := survey.Ask(questions, &envConfig)
		if err != nil {
			return nil, err
		}

		cfg.Environments = append(cfg.Environments, envConfig)

		// Set default Snowflake config from selected environment
		if env == defaultEnv {
			cfg.Snowflake = models.Snowflake{
				Account:   envConfig.Account,
				Username:  envConfig.Username,
				Password:  envConfig.Password,
				Database:  envConfig.Database,
				Schema:    envConfig.Schema,
				Warehouse: envConfig.Warehouse,
				Role:      envConfig.Role,
			}
		}
	}

	// Configure deployment settings
	if initFlags.advanced {
		fmt.Println("\n⚙️  Advanced Deployment Configuration")
		fmt.Println("------------------------------------")
		
		cfg.Deployment = models.Deployment{
			Timeout:    "30m",
			MaxRetries: 3,
			BatchSize:  10,
			Rollback: models.RollbackConfig{
				Enabled:         true,
				OnFailure:       true,
				BackupRetention: 30,
				Strategy:        "immediate",
			},
			Validation: models.ValidationConfig{
				Enabled:     true,
				SyntaxCheck: true,
				DryRun:      true,
			},
		}

		// Ask for custom deployment settings
		deployQuestions := []*survey.Question{
			{
				Name: "parallel",
				Prompt: &survey.Confirm{
					Message: "Enable parallel deployment?",
					Default: false,
				},
			},
			{
				Name: "batch_size",
				Prompt: &survey.Input{
					Message: "Batch size for parallel deployment:",
					Default: "10",
				},
			},
			{
				Name: "timeout",
				Prompt: &survey.Input{
					Message: "Deployment timeout:",
					Default: "30m",
					Help:    "Format: 30s, 5m, 1h",
				},
			},
		}

		deployAnswers := struct {
			Parallel  bool   `survey:"parallel"`
			BatchSize string `survey:"batch_size"`
			Timeout   string `survey:"timeout"`
		}{}

		if err := survey.Ask(deployQuestions, &deployAnswers); err != nil {
			return nil, err
		}

		cfg.Deployment.Parallel = deployAnswers.Parallel
		if size, err := fmt.Sscanf(deployAnswers.BatchSize, "%d", &cfg.Deployment.BatchSize); err == nil && size > 0 {
			// BatchSize already set
		}
		cfg.Deployment.Timeout = deployAnswers.Timeout
	} else {
		// Use default deployment settings
		cfg.Deployment = models.Deployment{
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
		}
	}

	// Configure plugin system
	cfg.Plugins = models.PluginConfig{
		Enabled:     true,
		Directories: []string{"plugins"},
		Plugins: []models.PluginConfigItem{
			{
				Name:    "slack-notifier",
				Enabled: false,
				Settings: map[string]interface{}{
					"webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
					"channel":     "#deployments",
				},
			},
			{
				Name:    "email-notifier",
				Enabled: false,
				Settings: map[string]interface{}{
					"smtp_host": "smtp.example.com",
					"smtp_port": 587,
					"from":      "snowflake-deploy@example.com",
					"to":        []string{"team@example.com"},
				},
			},
		},
		Security: models.PluginSecurityConfig{
			AllowUnsigned:    false,
			SandboxEnabled:   true,
			MaxMemoryMB:      512,
			MaxExecutionTime: "5m",
		},
	}

	// Ask if user wants to add a repository
	fmt.Println("\n📦 Repository Configuration")
	fmt.Println("-------------------------")
	
	var addRepo bool
	prompt := &survey.Confirm{
		Message: "Do you want to configure a repository now?",
		Default: true,
	}
	if err := survey.AskOne(prompt, &addRepo); err != nil {
		return nil, err
	}

	if addRepo {
		repo := models.Repository{}
		repoQs := []*survey.Question{
			{
				Name: "name",
				Prompt: &survey.Input{
					Message: "Repository name (identifier):",
					Help:    "A short name to identify this repository in CLI commands",
				},
				Validate: survey.Required,
			},
			{
				Name: "giturl",
				Prompt: &survey.Input{
					Message: "Git URL (or 'local' for local development):",
					Default: "local",
					Help:    "Git repository URL or 'local' to use the current project",
				},
				Validate: survey.Required,
			},
			{
				Name: "branch",
				Prompt: &survey.Input{
					Message: "Branch:",
					Default: "main",
				},
				Validate: survey.Required,
			},
			{
				Name: "path",
				Prompt: &survey.Input{
					Message: "Migrations path:",
					Default: "migrations",
					Help:    "Relative path to SQL migration files within the repository",
				},
				Validate: survey.Required,
			},
		}

		answers := struct {
			Name   string
			GitURL string `survey:"giturl"`
			Branch string
			Path   string
		}{}

		err := survey.Ask(repoQs, &answers)
		if err != nil {
			return nil, err
		}

		repo.Name = answers.Name
		repo.GitURL = answers.GitURL
		repo.Branch = answers.Branch
		repo.Path = answers.Path
		
		// Use environment-specific database/schema
		for _, env := range cfg.Environments {
			if env.Name == defaultEnv {
				repo.Database = env.Database
				repo.Schema = env.Schema
				break
			}
		}

		cfg.Repositories = append(cfg.Repositories, repo)
	}

	return cfg, nil
}

func initializeGitRepo(projectDir string) error {
	// Check if git is available
	if err := runCommand(projectDir, "git", "--version"); err != nil {
		return fmt.Errorf("git not found")
	}

	// Initialize repository
	if err := runCommand(projectDir, "git", "init"); err != nil {
		return err
	}

	// Create initial commit
	if err := runCommand(projectDir, "git", "add", "."); err != nil {
		return err
	}

	if err := runCommand(projectDir, "git", "commit", "-m", "Initial commit: FlakeDrop project"); err != nil {
		return err
	}

	return nil
}

func createExampleFiles(projectDir string, cfg *models.Config) error {
	// Create example migration files
	exampleMigrations := map[string]string{
		"migrations/schema/001_create_tables.sql": `-- Migration: Create initial tables
-- Author: System
-- Date: {{.Date}}

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER AUTOINCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS order_items (
    item_id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
`,

		"migrations/schema/002_create_indexes.sql": `-- Migration: Create indexes for performance
-- Author: System
-- Date: {{.Date}}

-- Index on customer email for faster lookups
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);

-- Index on order date for reporting
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);

-- Composite index for order items
CREATE INDEX IF NOT EXISTS idx_order_items_order_product 
ON order_items(order_id, product_name);
`,

		"migrations/data/001_seed_test_data.sql": `-- Migration: Seed test data
-- Author: System
-- Date: {{.Date}}
-- Environment: dev, staging

-- Only run in non-production environments
-- @env:exclude:prod

-- Insert test customers
INSERT INTO customers (first_name, last_name, email) VALUES
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@example.com'),
    ('Bob', 'Johnson', 'bob.johnson@example.com');

-- Insert test orders
INSERT INTO orders (customer_id, total_amount, status) 
SELECT 
    customer_id,
    UNIFORM(10, 1000, RANDOM())::DECIMAL(10,2),
    CASE WHEN RANDOM() > 0.5 THEN 'completed' ELSE 'pending' END
FROM customers
WHERE email LIKE '%@example.com';
`,

		"scripts/pre-deploy/validate_environment.sql": `-- Pre-deployment validation script
-- Validates that the target environment is ready for deployment

-- Check if required warehouse exists
SHOW WAREHOUSES LIKE '{{.Warehouse}}';

-- Check current role permissions
SHOW GRANTS TO ROLE {{.Role}};

-- Verify database exists
USE DATABASE {{.Database}};

-- Check if schema exists
USE SCHEMA {{.Schema}};

-- Return success
SELECT 'Environment validation passed' as status;
`,

		"scripts/post-deploy/analyze_tables.sql": `-- Post-deployment optimization script
-- Updates table statistics for query optimization

-- Analyze all tables in the schema
SHOW TABLES IN SCHEMA {{.Database}}.{{.Schema}};

-- Gather statistics (example for specific tables)
-- ANALYZE TABLE customers COMPUTE STATISTICS;
-- ANALYZE TABLE orders COMPUTE STATISTICS;
-- ANALYZE TABLE order_items COMPUTE STATISTICS;

SELECT 'Post-deployment tasks completed' as status;
`,
	}

	// Create example files
	for path, content := range exampleMigrations {
		fullPath := filepath.Join(projectDir, path)
		
		// Process template
		processedContent := strings.ReplaceAll(content, "{{.Date}}", "2024-01-01")
		if len(cfg.Environments) > 0 {
			processedContent = strings.ReplaceAll(processedContent, "{{.Database}}", cfg.Environments[0].Database)
			processedContent = strings.ReplaceAll(processedContent, "{{.Schema}}", cfg.Environments[0].Schema)
			processedContent = strings.ReplaceAll(processedContent, "{{.Warehouse}}", cfg.Environments[0].Warehouse)
			processedContent = strings.ReplaceAll(processedContent, "{{.Role}}", cfg.Environments[0].Role)
		}
		
		if err := os.WriteFile(fullPath, []byte(processedContent), 0600); err != nil {
			return fmt.Errorf("failed to create example file %s: %w", path, err)
		}
	}

	// Create CI/CD configurations
	ciConfigs := map[string]string{
		"ci/github-actions.yml": `name: FlakeDrop

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Snowflake Deploy
      run: |
        curl -L https://github.com/your-org/snowflake-deploy/releases/latest/download/snowflake-deploy-linux-amd64 -o snowflake-deploy
        chmod +x snowflake-deploy
        
    - name: Validate SQL
      run: ./flakedrop validate --config configs/config.yaml
      
  deploy-dev:
    needs: validate
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    steps:
    - uses: actions/checkout@v3
    
    - name: Deploy to Dev
      env:
        SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_DEV_PASSWORD }}
      run: |
        ./flakedrop deploy main --env dev --auto-approve
        
  deploy-prod:
    needs: validate
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
    - uses: actions/checkout@v3
    
    - name: Deploy to Production
      env:
        SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PROD_PASSWORD }}
      run: |
        ./flakedrop deploy main --env prod --dry-run
        ./flakedrop deploy main --env prod --auto-approve
`,

		"ci/gitlab-ci.yml": `.stages:
  - validate
  - deploy

variables:
  SNOWFLAKE_DEPLOY_VERSION: "latest"

before_script:
  - curl -L https://github.com/your-org/snowflake-deploy/releases/${SNOWFLAKE_DEPLOY_VERSION}/download/snowflake-deploy-linux-amd64 -o snowflake-deploy
  - chmod +x snowflake-deploy

validate:
  stage: validate
  script:
    - ./flakedrop validate --config configs/config.yaml

deploy:dev:
  stage: deploy
  script:
    - ./flakedrop deploy main --env dev --auto-approve
  environment:
    name: development
  only:
    - develop

deploy:prod:
  stage: deploy
  script:
    - ./flakedrop deploy main --env prod --dry-run
    - ./flakedrop deploy main --env prod --auto-approve
  environment:
    name: production
  only:
    - main
  when: manual
`,
	}

	for path, content := range ciConfigs {
		fullPath := filepath.Join(projectDir, path)
		if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("failed to create CI config %s: %w", path, err)
		}
	}

	return nil
}

func generateProjectDocumentation(projectDir, projectName string) error {
	readmeContent := fmt.Sprintf(`# %s

A FlakeDrop project managed by snowflake-deploy.

## Project Structure

- **configs/** - Configuration files for different environments
- **migrations/** - SQL migration files organized by type
  - **schema/** - DDL changes (tables, views, procedures)
  - **data/** - DML changes and data migrations
- **scripts/** - Utility scripts for deployment lifecycle
  - **pre-deploy/** - Scripts run before deployment
  - **post-deploy/** - Scripts run after deployment
- **tests/** - Test files for migrations
- **docs/** - Additional documentation
- **ci/** - CI/CD pipeline configurations

## Getting Started

1. **Configure Environments**
   
   Review and update the environment configurations in ` + "`configs/config.yaml`" + `:
   
   ` + "```bash" + `
   vim configs/config.yaml
   ` + "```" + `

2. **Add Migrations**
   
   Place your SQL migration files in the appropriate directories:
   
   ` + "```bash" + `
   # For schema changes
   cp your-migration.sql migrations/schema/
   
   # For data migrations
   cp your-data-migration.sql migrations/data/
   ` + "```" + `

3. **Test Locally**
   
   Run a dry-run deployment to validate your changes:
   
   ` + "```bash" + `
   flakedrop deploy <repo-name> --dry-run
   ` + "```" + `

4. **Deploy**
   
   Deploy to your target environment:
   
   ` + "```bash" + `
   # Deploy to development
   flakedrop deploy <repo-name> --env dev
   
   # Deploy to production with approval
   flakedrop deploy <repo-name> --env prod
   ` + "```" + `

## Environment Management

This project supports multiple environments. Each environment has its own:
- Snowflake account credentials
- Database and schema targets
- Warehouse and role configurations

To deploy to a specific environment:

` + "```bash" + `
flakedrop deploy <repo-name> --env <environment-name>
` + "```" + `

## CI/CD Integration

Example CI/CD configurations are provided in the ` + "`ci/`" + ` directory:
- GitHub Actions: ` + "`ci/github-actions.yml`" + `
- GitLab CI: ` + "`ci/gitlab-ci.yml`" + `

Configure your CI/CD secrets for automated deployments:
- ` + "`SNOWFLAKE_DEV_PASSWORD`" + ` - Development environment password
- ` + "`SNOWFLAKE_PROD_PASSWORD`" + ` - Production environment password

## Best Practices

1. **Version Control**
   - Always commit your migrations before deploying
   - Use meaningful commit messages
   - Tag releases for production deployments

2. **Migration Naming**
   - Use sequential numbering: ` + "`001_`, `002_`, etc." + `
   - Include descriptive names: ` + "`001_create_user_tables.sql`" + `
   - Date-based naming is also supported: ` + "`2024_01_15_create_indexes.sql`" + `

3. **Testing**
   - Always run dry-run deployments first
   - Test migrations in development before staging/production
   - Include rollback scripts for risky changes

4. **Security**
   - Never commit passwords or sensitive data
   - Use environment variables for credentials in CI/CD
   - Regularly rotate deployment credentials
   - Use role-based access control (RBAC)

## Rollback Procedures

If a deployment fails or needs to be reverted:

` + "```bash" + `
# View deployment history
flakedrop rollback list

# Rollback to a specific version
flakedrop rollback <deployment-id>

# Rollback the last deployment
flakedrop rollback last
` + "```" + `

## Monitoring and Observability

Monitor your deployments:

` + "```bash" + `
# View deployment logs
flakedrop logs <deployment-id>

# Check deployment status
flakedrop status

# Compare schema versions
flakedrop compare <source-env> <target-env>
` + "```" + `

## Troubleshooting

Common issues and solutions:

1. **Authentication Failures**
   - Verify credentials in ` + "`configs/config.yaml`" + `
   - Check network connectivity to Snowflake
   - Ensure your IP is whitelisted

2. **Permission Errors**
   - Verify the deployment role has necessary privileges
   - Check database and schema access rights
   - Ensure warehouse usage permissions

3. **Migration Failures**
   - Review SQL syntax with ` + "`--dry-run`" + `
   - Check for dependency order in migrations
   - Verify object existence before alterations

## Additional Resources

- [Snowflake Deploy Documentation](https://github.com/your-org/snowflake-deploy)
- [Snowflake SQL Reference](https://docs.snowflake.com/en/sql-reference.html)
- [Best Practices Guide](docs/BEST_PRACTICES.md)

## Support

For issues or questions:
- Create an issue in the project repository
- Contact your DevOps team
- Check the troubleshooting guide in ` + "`docs/TROUBLESHOOTING.md`" + `
`, projectName)

	readmePath := filepath.Join(projectDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(readmeContent), 0600); err != nil {
		return fmt.Errorf("failed to create README: %w", err)
	}

	// Create additional documentation files
	docs := map[string]string{
		"docs/MIGRATION_GUIDE.md": `# Migration Guide

## Creating Migrations

### File Naming Convention

Use one of these naming patterns:
- Sequential: ` + "`001_description.sql`, `002_description.sql`" + `
- Date-based: ` + "`2024_01_15_description.sql`" + `
- Version-based: ` + "`v1.0.0_description.sql`" + `

### Migration Structure

` + "```sql" + `
-- Migration: Brief description
-- Author: Your Name
-- Date: YYYY-MM-DD
-- Ticket: JIRA-123 (optional)

-- Forward migration
CREATE TABLE IF NOT EXISTS ...

-- Rollback section (optional)
-- @ROLLBACK
-- DROP TABLE IF EXISTS ...
` + "```" + `

### Best Practices

1. **Idempotency**
   - Use ` + "`IF NOT EXISTS`" + ` for CREATE statements
   - Use ` + "`IF EXISTS`" + ` for DROP statements
   - Make migrations re-runnable

2. **Transactions**
   - Group related changes in transactions
   - Ensure atomic operations

3. **Dependencies**
   - Order migrations by dependencies
   - Verify prerequisites in pre-deploy scripts

4. **Performance**
   - Consider impact on large tables
   - Use COPY for bulk data operations
   - Add indexes after data loads

## Environment-Specific Migrations

Control which environments run specific migrations:

` + "```sql" + `
-- Only run in development
-- @env:include:dev

-- Skip in production
-- @env:exclude:prod

-- Run in staging and prod only
-- @env:include:staging,prod
` + "```" + `
`,

		"docs/TROUBLESHOOTING.md": `# Troubleshooting Guide

## Common Issues

### Authentication Errors

**Error**: "Authentication failed"

**Solutions**:
1. Verify credentials in config.yaml
2. Check password for special characters that need escaping
3. Ensure MFA is properly configured
4. Verify network policies allow your IP

### Connection Timeouts

**Error**: "Connection timeout"

**Solutions**:
1. Check Snowflake account URL format
2. Verify network connectivity
3. Check firewall rules
4. Increase timeout in config

### Permission Denied

**Error**: "Insufficient privileges"

**Solutions**:
1. Verify role has CREATE/ALTER permissions
2. Check warehouse usage grants
3. Ensure database/schema access
4. Review role hierarchy

### Migration Failures

**Error**: "SQL compilation error"

**Solutions**:
1. Validate SQL syntax
2. Check object dependencies
3. Verify data types compatibility
4. Review Snowflake SQL differences

## Debugging Tips

1. **Enable Verbose Logging**
   ` + "```bash" + `
   flakedrop deploy <repo> --verbose
   ` + "```" + `

2. **Use Dry Run**
   ` + "```bash" + `
   flakedrop deploy <repo> --dry-run
   ` + "```" + `

3. **Check Logs**
   ` + "```bash" + `
   tail -f ~/.flakedrop/logs/deploy.log
   ` + "```" + `

4. **Validate Configuration**
   ` + "```bash" + `
   flakedrop validate --config configs/config.yaml
   ` + "```" + `
`,

		"docs/BEST_PRACTICES.md": `# Best Practices

## Development Workflow

1. **Branch Strategy**
   - Use feature branches for new migrations
   - Merge to develop for testing
   - Merge to main for production

2. **Code Review**
   - Review all SQL changes
   - Check for performance impact
   - Verify rollback procedures

3. **Testing**
   - Test in development first
   - Validate in staging
   - Use dry-run in production

## Security

1. **Credential Management**
   - Use environment variables
   - Rotate passwords regularly
   - Implement least-privilege access
   - Use service accounts for CI/CD

2. **Data Protection**
   - Mask sensitive data in dev/staging
   - Audit data access
   - Encrypt data at rest
   - Use row-level security where needed

## Performance

1. **Query Optimization**
   - Use appropriate warehouse sizes
   - Implement clustering keys
   - Partition large tables
   - Monitor query performance

2. **Deployment Optimization**
   - Deploy during off-peak hours
   - Use batch operations
   - Implement incremental changes
   - Monitor resource usage

## Monitoring

1. **Deployment Monitoring**
   - Track deployment duration
   - Monitor failure rates
   - Set up alerts
   - Review logs regularly

2. **Performance Monitoring**
   - Track query performance
   - Monitor warehouse utilization
   - Check storage growth
   - Review credit usage
`,
	}

	for path, content := range docs {
		fullPath := filepath.Join(projectDir, path)
		if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("failed to create doc %s: %w", path, err)
		}
	}

	return nil
}

func runCommand(dir, command string, args ...string) error {
	cmd := exec.Command(command, args...)
	cmd.Dir = dir
	return cmd.Run()
}