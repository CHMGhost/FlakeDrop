package template

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"flakedrop/internal/common"
)


// ProjectTemplate represents a project template
type ProjectTemplate struct {
	Name        string
	Description string
	Type        string // standard, enterprise, minimal
	Files       map[string]string
	Directories []string
	Variables   map[string]interface{}
}

// TemplateManager manages project templates
type TemplateManager struct {
	templates map[string]*ProjectTemplate
}

// NewTemplateManager creates a new template manager
func NewTemplateManager() *TemplateManager {
	tm := &TemplateManager{
		templates: make(map[string]*ProjectTemplate),
	}
	tm.loadBuiltInTemplates()
	return tm
}

// GetTemplate returns a template by name
func (tm *TemplateManager) GetTemplate(name string) (*ProjectTemplate, error) {
	tmpl, exists := tm.templates[name]
	if !exists {
		return nil, fmt.Errorf("template %s not found", name)
	}
	return tmpl, nil
}

// ListTemplates returns all available templates
func (tm *TemplateManager) ListTemplates() []*ProjectTemplate {
	templates := make([]*ProjectTemplate, 0, len(tm.templates))
	for _, tmpl := range tm.templates {
		templates = append(templates, tmpl)
	}
	return templates
}

// ApplyTemplate applies a template to create a project
func (tm *TemplateManager) ApplyTemplate(templateName, projectDir string, vars map[string]interface{}) error {
	tmpl, err := tm.GetTemplate(templateName)
	if err != nil {
		return err
	}

	// Merge variables
	variables := make(map[string]interface{})
	for k, v := range tmpl.Variables {
		variables[k] = v
	}
	for k, v := range vars {
		variables[k] = v
	}

	// Add default variables
	variables["ProjectName"] = filepath.Base(projectDir)
	variables["Date"] = time.Now().Format("2006-01-02")
	variables["Year"] = time.Now().Year()

	// Create directories
	for _, dir := range tmpl.Directories {
		fullPath := filepath.Join(projectDir, dir)
		if err := os.MkdirAll(fullPath, common.DirPermissionNormal); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create files
	for path, content := range tmpl.Files {
		fullPath := filepath.Join(projectDir, path)
		
		// Ensure directory exists
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", path, err)
		}

		// Process template
		processedContent, err := tm.processTemplate(path, content, variables)
		if err != nil {
			return fmt.Errorf("failed to process template %s: %w", path, err)
		}

		// Write file
		if err := os.WriteFile(fullPath, []byte(processedContent), 0600); err != nil {
			return fmt.Errorf("failed to write file %s: %w", path, err)
		}
	}

	return nil
}

// processTemplate processes a template string with variables
func (tm *TemplateManager) processTemplate(name, content string, vars map[string]interface{}) (string, error) {
	// Define template functions
	funcMap := template.FuncMap{
		"upper":   strings.ToUpper,
		"lower":   strings.ToLower,
		"title":   strings.Title,
		"env":     func(key string) string { return "${" + key + "}" }, // Environment variable placeholder
		"secrets": func(key string) string { return "${{ secrets." + key + " }}" }, // GitHub secrets placeholder
		"vars":    func(key string) string { return "${{ vars." + key + " }}" }, // GitHub vars placeholder
		"github":  func(key string) string { return "${{ github." + key + " }}" }, // GitHub context placeholder
	}
	
	tmpl, err := template.New(name).Funcs(funcMap).Parse(content)
	if err != nil {
		return "", err
	}

	var buf strings.Builder
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// loadBuiltInTemplates loads the built-in templates
func (tm *TemplateManager) loadBuiltInTemplates() {
	// Standard template
	tm.templates["standard"] = &ProjectTemplate{
		Name:        "standard",
		Description: "Standard FlakeDrop project with common features",
		Type:        "standard",
		Directories: []string{
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
			"plugins",
		},
		Files: map[string]string{
			".gitignore": gitignoreTemplate,
			"README.md":  readmeTemplate,
			"Makefile":   makefileTemplate,
			"configs/config.example.yaml": configExampleTemplate,
			"migrations/README.md":        migrationsReadmeTemplate,
			"scripts/README.md":           scriptsReadmeTemplate,
			"docs/CONTRIBUTING.md":        contributingTemplate,
			"docs/DEVELOPMENT.md":         developmentTemplate,
			// ".github/workflows/deploy.yml": githubWorkflowTemplate, // Disabled due to template conflicts
			".gitlab-ci.yml":              gitlabCITemplate,
			"scripts/setup.sh":            setupScriptTemplate,
			"scripts/validate.sh":         validateScriptTemplate,
		},
		Variables: map[string]interface{}{
			"DefaultEnvironments": []string{"dev", "staging", "prod"},
			"DefaultWarehouse":    "COMPUTE_WH",
			"DefaultRole":         "DEVELOPER",
		},
	}

	// Enterprise template
	tm.templates["enterprise"] = &ProjectTemplate{
		Name:        "enterprise",
		Description: "Enterprise-grade project with advanced features and compliance",
		Type:        "enterprise",
		Directories: []string{
			"configs",
			"configs/environments",
			"configs/credentials",
			"migrations",
			"migrations/schema",
			"migrations/data",
			"migrations/procedures",
			"migrations/views",
			"migrations/functions",
			"migrations/stages",
			"migrations/pipes",
			"migrations/tasks",
			"migrations/streams",
			"scripts",
			"scripts/pre-deploy",
			"scripts/post-deploy",
			"scripts/validation",
			"scripts/rollback",
			"scripts/backup",
			"tests",
			"tests/unit",
			"tests/integration",
			"tests/performance",
			"tests/security",
			"docs",
			"docs/architecture",
			"docs/runbooks",
			"docs/compliance",
			"ci",
			"ci/jenkins",
			"ci/azure-devops",
			"monitoring",
			"monitoring/dashboards",
			"monitoring/alerts",
			"security",
			"security/policies",
			"security/scans",
			"compliance",
			"compliance/reports",
			"backups",
			"plugins",
			"plugins/custom",
		},
		Files: map[string]string{
			".gitignore":                    gitignoreEnterpriseTemplate,
			"README.md":                     readmeEnterpriseTemplate,
			"Makefile":                      makefileEnterpriseTemplate,
			"configs/config.example.yaml":   configEnterpriseExampleTemplate,
			"security/policies/rbac.yaml":   rbacPolicyTemplate,
			"security/policies/data.yaml":   dataPolicyTemplate,
			"monitoring/dashboards/deployment.json": deploymentDashboardTemplate,
			"compliance/checklist.md":       complianceChecklistTemplate,
			"docs/architecture/README.md":   architectureDocTemplate,
			"docs/runbooks/deployment.md":   deploymentRunbookTemplate,
			"docs/compliance/gdpr.md":       gdprComplianceTemplate,
			"scripts/backup/backup.sh":      backupScriptTemplate,
			"scripts/security/scan.sh":      securityScanTemplate,
			".github/workflows/security.yml": securityWorkflowTemplate,
			"Jenkinsfile":                   jenkinsfileTemplate,
			"azure-pipelines.yml":           azurePipelinesTemplate,
		},
		Variables: map[string]interface{}{
			"DefaultEnvironments": []string{"dev", "test", "staging", "prod", "dr"},
			"DefaultWarehouse":    "DEPLOYMENT_WH",
			"DefaultRole":         "DEPLOYMENT_ROLE",
			"EnableCompliance":    true,
			"EnableMonitoring":    true,
			"EnableSecurity":      true,
		},
	}

	// Minimal template
	tm.templates["minimal"] = &ProjectTemplate{
		Name:        "minimal",
		Description: "Minimal project structure for simple deployments",
		Type:        "minimal",
		Directories: []string{
			"configs",
			"migrations",
			"scripts",
		},
		Files: map[string]string{
			".gitignore":                  gitignoreMinimalTemplate,
			"README.md":                   readmeMinimalTemplate,
			"configs/config.example.yaml": configMinimalExampleTemplate,
			"migrations/001_init.sql":     initMigrationTemplate,
			"scripts/deploy.sh":           deployScriptTemplate,
		},
		Variables: map[string]interface{}{
			"DefaultEnvironments": []string{"dev", "prod"},
			"DefaultWarehouse":    "COMPUTE_WH",
			"DefaultRole":         "ACCOUNTADMIN",
		},
	}
}

// Template content constants
const gitignoreTemplate = `# Snowflake Deploy
*.log
*.tmp
.env
.env.*
!.env.example

# Credentials
configs/credentials/
*.key
*.pem
*.p12

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

# Build artifacts
dist/
build/
*.exe
`

const readmeTemplate = `# {{.ProjectName}}

A FlakeDrop project managed by FlakeDrop.

## Quick Start

1. Copy the example configuration:
   ` + "```bash" + `
   cp configs/config.example.yaml configs/config.yaml
   ` + "```" + `

2. Update the configuration with your Snowflake credentials

3. Add your SQL migrations to the ` + "`migrations/`" + ` directory

4. Deploy to development:
   ` + "```bash" + `
   flakedrop deploy main --env dev
   ` + "```" + `

## Project Structure

- ` + "`configs/`" + ` - Configuration files
- ` + "`migrations/`" + ` - SQL migration files
- ` + "`scripts/`" + ` - Utility scripts
- ` + "`tests/`" + ` - Test files
- ` + "`docs/`" + ` - Documentation
- ` + "`ci/`" + ` - CI/CD configurations

## Documentation

- [Contributing Guide](docs/CONTRIBUTING.md)
- [Development Guide](docs/DEVELOPMENT.md)
- [Deployment Guide](docs/deployment.md)

## License

Copyright {{.Year}} - All rights reserved.
`

const makefileTemplate = `.PHONY: help install test lint deploy validate clean

# Default target
help:
	@echo "Available targets:"
	@echo "  install    - Install dependencies"
	@echo "  test       - Run tests"
	@echo "  lint       - Run linters"
	@echo "  validate   - Validate SQL migrations"
	@echo "  deploy     - Deploy to environment (use ENV=dev/staging/prod)"
	@echo "  clean      - Clean build artifacts"

install:
	@echo "Installing snowflake-deploy..."
	@curl -L https://github.com/your-org/snowflake-deploy/releases/latest/download/snowflake-deploy-$(shell uname -s)-$(shell uname -m) -o /usr/local/bin/snowflake-deploy
	@chmod +x /usr/local/bin/snowflake-deploy

test:
	@echo "Running tests..."
	@flakedrop test --config configs/config.yaml

lint:
	@echo "Running SQL linter..."
	@flakedrop lint migrations/

validate:
	@echo "Validating migrations..."
	@flakedrop validate --config configs/config.yaml

deploy:
	@if [ -z "$(ENV)" ]; then echo "Please specify ENV=dev/staging/prod"; exit 1; fi
	@echo "Deploying to $(ENV)..."
	@flakedrop deploy main --env $(ENV) --config configs/config.yaml

clean:
	@echo "Cleaning up..."
	@rm -rf dist/ build/ *.log
`

const configExampleTemplate = `# Snowflake Deploy Configuration
# Copy this file to config.yaml and update with your values

# Repository configurations
repositories:
  - name: main
    git_url: local  # Use 'local' for local development or provide Git URL
    branch: main
    path: migrations
    database: "{{.Database}}"
    schema: "{{.Schema}}"

# Environment configurations
environments:
{{range .DefaultEnvironments}}  - name: {{.}}
    account: "your-account.region"
    username: "{{.}}_deploy_user"
    password: "${SNOWFLAKE_{{. | upper}}_PASSWORD}"  # Use environment variable
    database: "{{. | upper}}_DB"
    schema: "PUBLIC"
    warehouse: "{{. | upper}}_WH"
    role: "{{. | upper}}_DEPLOY_ROLE"
{{end}}

# Default Snowflake configuration (fallback)
snowflake:
  account: "your-account.region"
  username: "deploy_user"
  password: "${SNOWFLAKE_PASSWORD}"
  database: "DEV_DB"
  schema: "PUBLIC"
  warehouse: "{{.DefaultWarehouse}}"
  role: "{{.DefaultRole}}"

# Deployment configuration
deployment:
  timeout: "30m"
  max_retries: 3
  batch_size: 10
  parallel: false
  dry_run: true  # Always dry-run by default for safety
  
  # Rollback configuration
  rollback:
    enabled: true
    on_failure: true
    backup_retention: 7  # days
    strategy: "immediate"
  
  # Validation configuration
  validation:
    enabled: true
    syntax_check: true
    dry_run: true

# Plugin configuration
plugins:
  enabled: true
  directories:
    - plugins
  
  # Example plugin configurations
  plugins:
    - name: slack-notifier
      enabled: false
      settings:
        webhook_url: "${SLACK_WEBHOOK_URL}"
        channel: "#deployments"
        
    - name: email-notifier
      enabled: false
      settings:
        smtp_host: "smtp.example.com"
        smtp_port: 587
        from: "snowflake-deploy@example.com"
        to:
          - "team@example.com"
`

const migrationsReadmeTemplate = `# Migrations

This directory contains SQL migration files for your Snowflake deployment.

## Directory Structure

- ` + "`schema/`" + ` - DDL changes (CREATE, ALTER, DROP)
- ` + "`data/`" + ` - DML changes (INSERT, UPDATE, DELETE)
- ` + "`procedures/`" + ` - Stored procedures
- ` + "`views/`" + ` - Views and materialized views

## Naming Convention

Use one of these patterns:
- Sequential: ` + "`001_description.sql`" + `
- Date-based: ` + "`2024_01_15_description.sql`" + `
- Version-based: ` + "`v1.0.0_description.sql`" + `

## Migration Format

` + "```sql" + `
-- Migration: Brief description
-- Author: Your Name
-- Date: {{.Date}}
-- Ticket: JIRA-123 (optional)

-- Your SQL here
CREATE TABLE IF NOT EXISTS ...

-- Optional rollback section
-- @ROLLBACK
DROP TABLE IF EXISTS ...
` + "```" + `

## Environment-Specific Migrations

Control which environments run specific migrations:

` + "```sql" + `
-- Only run in development
-- @env:include:dev

-- Skip in production
-- @env:exclude:prod
` + "```" + `
`

const scriptsReadmeTemplate = `# Scripts

This directory contains utility scripts for the deployment lifecycle.

## Directory Structure

- ` + "`pre-deploy/`" + ` - Scripts run before deployment
- ` + "`post-deploy/`" + ` - Scripts run after deployment
- ` + "`validation/`" + ` - Validation and testing scripts

## Pre-Deploy Scripts

Run before migrations to:
- Validate environment
- Check prerequisites
- Create backups
- Set up temporary objects

## Post-Deploy Scripts

Run after migrations to:
- Validate deployment
- Update statistics
- Clean up temporary objects
- Send notifications

## Usage

Scripts are automatically executed by flakedrop in the correct order.

To run manually:
` + "```bash" + `
flakedrop run-script scripts/pre-deploy/validate.sql
` + "```" + `
`

const contributingTemplate = `# Contributing Guide

Thank you for contributing to {{.ProjectName}}!

## Development Setup

1. Clone the repository
2. Install snowflake-deploy
3. Copy ` + "`configs/config.example.yaml`" + ` to ` + "`configs/config.yaml`" + `
4. Update configuration with your development credentials

## Making Changes

1. Create a feature branch
2. Add your migrations to the appropriate directory
3. Test locally with ` + "`make validate`" + `
4. Commit with clear messages
5. Push and create a pull request

## Testing

Run tests before submitting:
` + "```bash" + `
make test
make lint
make validate
` + "```" + `

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add migration rollback scripts
4. Request review from team lead
`

const developmentTemplate = `# Development Guide

## Local Development

### Prerequisites

- Snowflake account with development database
- flakedrop CLI installed
- Git for version control

### Setup

1. Clone the repository:
   ` + "```bash" + `
   git clone <repository-url>
   cd {{.ProjectName}}
   ` + "```" + `

2. Install dependencies:
   ` + "```bash" + `
   make install
   ` + "```" + `

3. Configure environment:
   ` + "```bash" + `
   cp configs/config.example.yaml configs/config.yaml
   # Edit config.yaml with your credentials
   ` + "```" + `

### Development Workflow

1. Create feature branch:
   ` + "```bash" + `
   git checkout -b feature/your-feature
   ` + "```" + `

2. Add migrations:
   ` + "```bash" + `
   # Schema changes
   vi migrations/schema/001_your_change.sql
   
   # Data changes
   vi migrations/data/001_your_data.sql
   ` + "```" + `

3. Validate changes:
   ` + "```bash" + `
   make validate
   ` + "```" + `

4. Test deployment:
   ` + "```bash" + `
   make deploy ENV=dev
   ` + "```" + `

5. Commit and push:
   ` + "```bash" + `
   git add .
   git commit -m "Add your feature"
   git push origin feature/your-feature
   ` + "```" + `

## Testing

### Unit Tests

Test individual migrations:
` + "```bash" + `
flakedrop test migrations/schema/001_create_tables.sql
` + "```" + `

### Integration Tests

Test full deployment:
` + "```bash" + `
flakedrop deploy main --env dev --dry-run
` + "```" + `

## Debugging

Enable verbose logging:
` + "```bash" + `
export SNOWFLAKE_DEPLOY_LOG_LEVEL=debug
flakedrop deploy main --verbose
` + "```" + `
`

const githubWorkflowTemplate = `name: Snowflake Deployment

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment to deploy to'
        required: true
        default: 'dev'
        type: choice
        options:
          - dev
          - staging
          - prod

env:
  SNOWFLAKE_DEPLOY_VERSION: latest

jobs:
  validate:
    name: Validate Migrations
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Snowflake Deploy
        run: |
          curl -L https://github.com/your-org/snowflake-deploy/releases/download/latest/snowflake-deploy-linux-amd64 -o snowflake-deploy
          chmod +x snowflake-deploy
          sudo mv flakedrop /usr/local/bin/
          
      - name: Validate SQL
        run: |
          flakedrop validate --config configs/config.yaml

  deploy-dev:
    name: Deploy to Development
    needs: validate
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop' && github.event_name == 'push'
    environment: development
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Snowflake Deploy
        run: |
          curl -L https://github.com/your-org/snowflake-deploy/releases/download/latest/snowflake-deploy-linux-amd64 -o snowflake-deploy
          chmod +x snowflake-deploy
          sudo mv flakedrop /usr/local/bin/
          
      - name: Deploy to Dev
        env:
          SNOWFLAKE_DEV_PASSWORD: "# Set this in GitHub secrets"
        run: |
          flakedrop deploy main --env dev --config configs/config.yaml --auto-approve

  deploy-staging:
    name: Deploy to Staging
    needs: validate
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    environment: staging
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Snowflake Deploy
        run: |
          curl -L https://github.com/your-org/snowflake-deploy/releases/download/latest/snowflake-deploy-linux-amd64 -o snowflake-deploy
          chmod +x snowflake-deploy
          sudo mv flakedrop /usr/local/bin/
          
      - name: Deploy to Staging
        env:
          SNOWFLAKE_STAGING_PASSWORD: "# Set this in GitHub secrets"
        run: |
          flakedrop deploy main --env staging --config configs/config.yaml --auto-approve

  deploy-prod:
    name: Deploy to Production
    needs: [validate, deploy-staging]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Snowflake Deploy
        run: |
          curl -L https://github.com/your-org/snowflake-deploy/releases/download/latest/snowflake-deploy-linux-amd64 -o snowflake-deploy
          chmod +x snowflake-deploy
          sudo mv flakedrop /usr/local/bin/
          
      - name: Dry Run Production Deployment
        env:
          SNOWFLAKE_PROD_PASSWORD: "# Set this in GitHub secrets"
        run: |
          flakedrop deploy main --env prod --config configs/config.yaml --dry-run
          
      - name: Deploy to Production
        env:
          SNOWFLAKE_PROD_PASSWORD: "# Set this in GitHub secrets"
        run: |
          flakedrop deploy main --env prod --config configs/config.yaml --auto-approve

  manual-deploy:
    name: Manual Deployment
    needs: validate
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch'
    environment: manual-deploy
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Snowflake Deploy
        run: |
          curl -L https://github.com/your-org/snowflake-deploy/releases/download/latest/snowflake-deploy-linux-amd64 -o snowflake-deploy
          chmod +x snowflake-deploy
          sudo mv flakedrop /usr/local/bin/
          
      - name: Deploy to Environment
        env:
          SNOWFLAKE_PASSWORD: "# Set appropriate password for target environment"
        run: |
          echo "Manual deployment step - set environment-specific secrets"
          # flakedrop deploy main --env ${{ github.event.inputs.environment }} --config configs/config.yaml --auto-approve
`

const gitlabCITemplate = `.stages:
  - validate
  - test
  - deploy

variables:
  SNOWFLAKE_DEPLOY_VERSION: "latest"

before_script:
  - curl -L https://github.com/your-org/snowflake-deploy/releases/download/${SNOWFLAKE_DEPLOY_VERSION}/snowflake-deploy-linux-amd64 -o snowflake-deploy
  - chmod +x snowflake-deploy

validate:migrations:
  stage: validate
  script:
    - ./flakedrop validate --config configs/config.yaml
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH == "develop"'
    - if: '$CI_COMMIT_BRANCH == "main"'

test:dry-run:
  stage: test
  script:
    - ./flakedrop deploy main --env dev --config configs/config.yaml --dry-run
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH == "develop"'

deploy:dev:
  stage: deploy
  script:
    - export SNOWFLAKE_DEV_PASSWORD=$SNOWFLAKE_DEV_PASSWORD
    - ./flakedrop deploy main --env dev --config configs/config.yaml --auto-approve
  environment:
    name: development
    url: https://app.snowflake.com
  only:
    - develop

deploy:staging:
  stage: deploy
  script:
    - export SNOWFLAKE_STAGING_PASSWORD=$SNOWFLAKE_STAGING_PASSWORD
    - ./flakedrop deploy main --env staging --config configs/config.yaml --auto-approve
  environment:
    name: staging
    url: https://app.snowflake.com
  only:
    - main

deploy:prod:
  stage: deploy
  script:
    - export SNOWFLAKE_PROD_PASSWORD=$SNOWFLAKE_PROD_PASSWORD
    - ./flakedrop deploy main --env prod --config configs/config.yaml --dry-run
    - ./flakedrop deploy main --env prod --config configs/config.yaml --auto-approve
  environment:
    name: production
    url: https://app.snowflake.com
  only:
    - main
  when: manual
`

const setupScriptTemplate = `#!/bin/bash
# Setup script for {{.ProjectName}}

set -e

echo "Setting up {{.ProjectName}}..."

# Check prerequisites
command -v flakedrop >/dev/null 2>&1 || {
    echo "flakedrop is not installed. Installing..."
    make install
}

# Setup configuration
if [ ! -f configs/config.yaml ]; then
    echo "Creating configuration..."
    cp configs/config.example.yaml configs/config.yaml
    echo "Please update configs/config.yaml with your Snowflake credentials"
fi

# Create required directories
mkdir -p backups logs

# Validate setup
echo "Validating setup..."
flakedrop validate --config configs/config.yaml || {
    echo "Validation failed. Please check your configuration."
    exit 1
}

echo "Setup complete!"
`

const validateScriptTemplate = `#!/bin/bash
# Validation script for migrations

set -e

echo "Validating migrations..."

# Check for SQL syntax
flakedrop validate --config configs/config.yaml

# Check for naming conventions
for file in migrations/**/*.sql; do
    if [[ ! "$file" =~ ^migrations/.*/[0-9]{3}_.*\.sql$ ]] && \
       [[ ! "$file" =~ ^migrations/.*/[0-9]{4}_[0-9]{2}_[0-9]{2}_.*\.sql$ ]]; then
        echo "Warning: $file does not follow naming convention"
    fi
done

# Check for rollback sections in schema changes
for file in migrations/schema/*.sql; do
    if ! grep -q "@ROLLBACK" "$file"; then
        echo "Warning: $file missing rollback section"
    fi
done

echo "Validation complete!"
`

// Enterprise template content
const gitignoreEnterpriseTemplate = `# Snowflake Deploy - Enterprise
*.log
*.tmp
.env
.env.*
!.env.example

# Credentials and Security
configs/credentials/
security/keys/
*.key
*.pem
*.p12
*.jks
*.pfx

# Compliance and Audit
compliance/reports/*.pdf
compliance/reports/*.xlsx
audit/logs/

# Backups
backups/
*.bak
*.backup
*.dump

# Monitoring
monitoring/data/
monitoring/logs/
*.metrics

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
performance-results/

# Build artifacts
dist/
build/
*.exe
target/

# Temporary files
temp/
tmp/
cache/
`

const readmeEnterpriseTemplate = `# {{.ProjectName}} - Enterprise

An enterprise-grade FlakeDrop project with advanced governance, compliance, and monitoring.

## Features

- Multi-environment deployment (dev, test, staging, prod, dr)
- Advanced security and compliance features
- Comprehensive monitoring and alerting
- Automated backup and recovery
- Role-based access control (RBAC)
- Audit logging and compliance reporting

## Quick Start

1. Setup credentials:
   ` + "```bash" + `
   ./scripts/setup-enterprise.sh
   ` + "```" + `

2. Configure environments:
   ` + "```bash" + `
   cp configs/config.example.yaml configs/config.yaml
   # Update with your environment-specific settings
   ` + "```" + `

3. Run security scan:
   ` + "```bash" + `
   ./scripts/security/scan.sh
   ` + "```" + `

4. Deploy to development:
   ` + "```bash" + `
   make deploy-dev
   ` + "```" + `

## Enterprise Features

### Security
- Encrypted credential storage
- Multi-factor authentication support
- Network policy enforcement
- Data masking and tokenization

### Compliance
- GDPR compliance tools
- SOC2 audit reports
- PCI-DSS checklist
- HIPAA compliance options

### Monitoring
- Real-time deployment monitoring
- Performance metrics collection
- Custom alerting rules
- Deployment dashboards

### Governance
- Change approval workflows
- Deployment audit trails
- Rollback procedures
- Disaster recovery plans

## Documentation

- [Architecture Guide](docs/architecture/README.md)
- [Security Policies](security/policies/README.md)
- [Compliance Guide](compliance/README.md)
- [Runbooks](docs/runbooks/README.md)
- [Monitoring Setup](monitoring/README.md)

## Support

- Internal Wiki: https://wiki.company.com/snowflake-deploy
- Slack: #snowflake-deploy
- Email: snowflake-team@company.com

## License

Proprietary - {{.Year}} {{.Company}} - All rights reserved.
`

const makefileEnterpriseTemplate = `.PHONY: all help install test lint validate security-scan compliance-check deploy-dev deploy-test deploy-staging deploy-prod deploy-dr backup restore monitor clean

# Default target
all: help

help:
	@echo "Enterprise Snowflake Deployment Makefile"
	@echo ""
	@echo "Setup targets:"
	@echo "  install          - Install all dependencies"
	@echo "  setup-enterprise - Run enterprise setup wizard"
	@echo ""
	@echo "Quality targets:"
	@echo "  test             - Run all tests"
	@echo "  lint             - Run linters"
	@echo "  validate         - Validate migrations"
	@echo "  security-scan    - Run security scans"
	@echo "  compliance-check - Check compliance requirements"
	@echo ""
	@echo "Deployment targets:"
	@echo "  deploy-dev      - Deploy to development"
	@echo "  deploy-test     - Deploy to test"
	@echo "  deploy-staging  - Deploy to staging"
	@echo "  deploy-prod     - Deploy to production (requires approval)"
	@echo "  deploy-dr       - Deploy to disaster recovery"
	@echo ""
	@echo "Operations targets:"
	@echo "  backup          - Create backup"
	@echo "  restore         - Restore from backup"
	@echo "  monitor         - View monitoring dashboard"
	@echo "  clean           - Clean build artifacts"

install:
	@echo "Installing enterprise dependencies..."
	@./scripts/install-enterprise.sh

setup-enterprise:
	@echo "Running enterprise setup..."
	@./scripts/setup-enterprise.sh

test:
	@echo "Running test suite..."
	@./scripts/test-all.sh

lint:
	@echo "Running linters..."
	@flakedrop lint migrations/
	@./scripts/lint-enterprise.sh

validate:
	@echo "Validating migrations..."
	@flakedrop validate --config configs/config.yaml --strict

security-scan:
	@echo "Running security scans..."
	@./scripts/security/scan.sh
	@./scripts/security/vulnerability-check.sh

compliance-check:
	@echo "Checking compliance..."
	@./scripts/compliance/check-all.sh

deploy-dev:
	@echo "Deploying to development..."
	@flakedrop deploy main --env dev --config configs/config.yaml --auto-approve

deploy-test:
	@echo "Deploying to test..."
	@flakedrop deploy main --env test --config configs/config.yaml --auto-approve

deploy-staging:
	@echo "Deploying to staging..."
	@flakedrop deploy main --env staging --config configs/config.yaml
	@./scripts/post-deploy/staging-tests.sh

deploy-prod:
	@echo "Deploying to production..."
	@./scripts/pre-deploy/prod-checklist.sh
	@flakedrop deploy main --env prod --config configs/config.yaml --dry-run
	@echo "Dry run complete. Proceed with deployment? [y/N]"
	@read -r response; if [ "$$response" = "y" ]; then \
		flakedrop deploy main --env prod --config configs/config.yaml; \
		./scripts/post-deploy/prod-validation.sh; \
	fi

deploy-dr:
	@echo "Deploying to disaster recovery..."
	@flakedrop deploy main --env dr --config configs/config.yaml

backup:
	@echo "Creating backup..."
	@./scripts/backup/backup.sh

restore:
	@echo "Restoring from backup..."
	@./scripts/backup/restore.sh

monitor:
	@echo "Opening monitoring dashboard..."
	@open http://localhost:3000/dashboard

clean:
	@echo "Cleaning up..."
	@rm -rf dist/ build/ *.log temp/ cache/
	@find . -name "*.pyc" -delete
	@find . -name "__pycache__" -delete
`

const configEnterpriseExampleTemplate = `# Enterprise Snowflake Deploy Configuration
# This is a comprehensive example for enterprise deployments

# Repository configurations
repositories:
  - name: main
    git_url: "git@github.com:company/snowflake-schemas.git"
    branch: main
    path: migrations
    database: "{{.Database}}"
    schema: "{{.Schema}}"
    
  - name: data-pipelines
    git_url: "git@github.com:company/data-pipelines.git"
    branch: main
    path: sql
    database: "{{.Database}}"
    schema: "PIPELINES"

# Environment configurations
environments:
  - name: dev
    account: "company-dev.us-east-1"
    username: "DEV_DEPLOY_SVC"
    password: "${SNOWFLAKE_DEV_PASSWORD}"
    database: "DEV_DB"
    schema: "PUBLIC"
    warehouse: "DEV_DEPLOY_WH"
    role: "DEV_DEPLOY_ROLE"
    
  - name: test
    account: "company-test.us-east-1"
    username: "TEST_DEPLOY_SVC"
    password: "${SNOWFLAKE_TEST_PASSWORD}"
    database: "TEST_DB"
    schema: "PUBLIC"
    warehouse: "TEST_DEPLOY_WH"
    role: "TEST_DEPLOY_ROLE"
    
  - name: staging
    account: "company-staging.us-east-1"
    username: "STG_DEPLOY_SVC"
    password: "${SNOWFLAKE_STAGING_PASSWORD}"
    database: "STAGING_DB"
    schema: "PUBLIC"
    warehouse: "STG_DEPLOY_WH"
    role: "STG_DEPLOY_ROLE"
    
  - name: prod
    account: "company-prod.us-east-1"
    username: "PRD_DEPLOY_SVC"
    password: "${SNOWFLAKE_PROD_PASSWORD}"
    database: "PROD_DB"
    schema: "PUBLIC"
    warehouse: "PRD_DEPLOY_WH"
    role: "PRD_DEPLOY_ROLE"
    
  - name: dr
    account: "company-dr.us-west-2"
    username: "DR_DEPLOY_SVC"
    password: "${SNOWFLAKE_DR_PASSWORD}"
    database: "DR_DB"
    schema: "PUBLIC"
    warehouse: "DR_DEPLOY_WH"
    role: "DR_DEPLOY_ROLE"

# Deployment configuration
deployment:
  timeout: "60m"
  max_retries: 3
  batch_size: 20
  parallel: true
  dry_run: true
  
  # Pre-deployment hooks
  pre_hooks:
    - "scripts/pre-deploy/backup.sh"
    - "scripts/pre-deploy/validate-env.sh"
    - "scripts/pre-deploy/check-dependencies.sh"
  
  # Post-deployment hooks
  post_hooks:
    - "scripts/post-deploy/validate.sh"
    - "scripts/post-deploy/update-stats.sh"
    - "scripts/post-deploy/notify.sh"
  
  # Rollback configuration
  rollback:
    enabled: true
    on_failure: true
    backup_retention: 30
    strategy: "checkpoint"
    max_rollback_duration: "30m"
    
  # Validation configuration
  validation:
    enabled: true
    syntax_check: true
    dry_run: true
    semantic_check: true
    dependency_check: true
    
  # Change control
  change_control:
    enabled: true
    approval_required: true
    approvers:
      - "snowflake-admins@company.com"
    ticket_system: "jira"
    ticket_regex: "^[A-Z]+-[0-9]+$"

# Security configuration
security:
  encryption:
    enabled: true
    key_vault: "aws-kms"
    key_id: "${KMS_KEY_ID}"
    
  authentication:
    mfa_required: true
    key_pair_auth: true
    private_key_path: "/secure/keys/snowflake_rsa_key.pem"
    
  network:
    allowed_ips:
      - "10.0.0.0/8"
      - "172.16.0.0/12"
    require_ssl: true
    
  audit:
    enabled: true
    log_retention: 90
    alert_on_failure: true

# Monitoring configuration
monitoring:
  enabled: true
  
  metrics:
    provider: "datadog"
    api_key: "${DATADOG_API_KEY}"
    custom_tags:
      - "team:data-platform"
      - "service:snowflake-deploy"
      
  alerts:
    - name: "deployment-failure"
      condition: "deployment.status == 'failed'"
      severity: "critical"
      channels: ["pagerduty", "slack"]
      
    - name: "long-running-deployment"
      condition: "deployment.duration > 30m"
      severity: "warning"
      channels: ["slack"]
      
  dashboards:
    - "deployment-metrics"
    - "resource-usage"
    - "error-rates"

# Compliance configuration
compliance:
  frameworks:
    - "gdpr"
    - "soc2"
    - "pci-dss"
    
  data_classification:
    enabled: true
    scan_frequency: "daily"
    
  retention_policies:
    default: "7 years"
    pii_data: "3 years"
    
  encryption_at_rest:
    enabled: true
    algorithm: "AES-256"

# Plugin configuration
plugins:
  enabled: true
  directories:
    - "plugins"
    - "plugins/custom"
    
  plugins:
    - name: "jira-integration"
      enabled: true
      settings:
        url: "https://company.atlassian.net"
        project: "SNOW"
        
    - name: "slack-notifier"
      enabled: true
      settings:
        webhook_url: "${SLACK_WEBHOOK_URL}"
        channels:
          success: "#snowflake-success"
          failure: "#snowflake-alerts"
          
    - name: "datadog-metrics"
      enabled: true
      settings:
        api_key: "${DATADOG_API_KEY}"
        
    - name: "vault-secrets"
      enabled: true
      settings:
        url: "https://vault.company.com"
        auth_method: "kubernetes"

# Backup configuration
backup:
  enabled: true
  schedule: "0 2 * * *"  # 2 AM daily
  retention:
    daily: 7
    weekly: 4
    monthly: 12
  storage:
    provider: "s3"
    bucket: "company-snowflake-backups"
    region: "us-east-1"
    encryption: true
`

// Additional enterprise templates
const rbacPolicyTemplate = `# Role-Based Access Control Policy

roles:
  - name: DEPLOYMENT_ADMIN
    description: "Full deployment administration"
    privileges:
      - CREATE DATABASE
      - CREATE SCHEMA
      - CREATE TABLE
      - CREATE VIEW
      - CREATE PROCEDURE
      - CREATE FUNCTION
      - EXECUTE TASK
    inherit_from:
      - SYSADMIN
      
  - name: DEPLOYMENT_OPERATOR
    description: "Deploy and monitor"
    privileges:
      - USE DATABASE
      - USE SCHEMA
      - CREATE TABLE
      - CREATE VIEW
      - MONITOR EXECUTION
    inherit_from:
      - PUBLIC
      
  - name: DEPLOYMENT_VIEWER
    description: "Read-only access to deployment info"
    privileges:
      - USE DATABASE
      - USE SCHEMA
      - SELECT
    inherit_from:
      - PUBLIC

users:
  - name: prod_deploy_svc
    roles:
      - DEPLOYMENT_ADMIN
    default_role: DEPLOYMENT_ADMIN
    
  - name: dev_deploy_svc
    roles:
      - DEPLOYMENT_OPERATOR
    default_role: DEPLOYMENT_OPERATOR

grants:
  databases:
    - database: PROD_DB
      role: DEPLOYMENT_ADMIN
      privileges: [ALL]
      
    - database: DEV_DB
      role: DEPLOYMENT_OPERATOR
      privileges: [CREATE SCHEMA, USAGE]
      
  schemas:
    - schema: PROD_DB.PUBLIC
      role: DEPLOYMENT_ADMIN
      privileges: [ALL]
      
  warehouses:
    - warehouse: DEPLOYMENT_WH
      role: DEPLOYMENT_ADMIN
      privileges: [USAGE, OPERATE, MONITOR]
`

const dataPolicyTemplate = `# Data Classification and Protection Policy

classification_levels:
  - name: PUBLIC
    description: "Public information"
    retention: "1 year"
    encryption: optional
    
  - name: INTERNAL
    description: "Internal use only"
    retention: "3 years"
    encryption: required
    masking: optional
    
  - name: CONFIDENTIAL
    description: "Confidential business data"
    retention: "5 years"
    encryption: required
    masking: required
    access_logging: true
    
  - name: RESTRICTED
    description: "Highly sensitive data (PII, PCI, etc)"
    retention: "7 years"
    encryption: required
    masking: required
    tokenization: true
    access_logging: true
    audit_frequency: "daily"

data_types:
  - pattern: "SSN|SOCIAL_SECURITY"
    classification: RESTRICTED
    masking_function: "MASK_SSN"
    
  - pattern: "CREDIT_CARD|CC_"
    classification: RESTRICTED
    masking_function: "MASK_CC"
    tokenization: true
    
  - pattern: "EMAIL|E_MAIL"
    classification: CONFIDENTIAL
    masking_function: "MASK_EMAIL"
    
  - pattern: "PHONE|MOBILE"
    classification: CONFIDENTIAL
    masking_function: "MASK_PHONE"
    
  - pattern: "ADDRESS|ADDR"
    classification: CONFIDENTIAL
    masking_function: "MASK_ADDRESS"

policies:
  - name: "PII Protection"
    applies_to: [RESTRICTED, CONFIDENTIAL]
    rules:
      - "Must use dynamic data masking"
      - "Access must be logged"
      - "Requires approval for production access"
      
  - name: "Data Retention"
    applies_to: [ALL]
    rules:
      - "Follow retention schedule"
      - "Automated purge after retention period"
      - "Backup before purge"
`

const deploymentDashboardTemplate = `{
  "dashboard": {
    "title": "Snowflake Deployment Dashboard",
    "panels": [
      {
        "title": "Deployment Status",
        "type": "stat",
        "targets": [
          {
            "metric": "snowflake.deployment.success.rate",
            "aggregation": "avg"
          }
        ]
      },
      {
        "title": "Deployment Duration",
        "type": "graph",
        "targets": [
          {
            "metric": "snowflake.deployment.duration",
            "aggregation": "avg",
            "groupBy": ["environment"]
          }
        ]
      },
      {
        "title": "Failed Deployments",
        "type": "table",
        "targets": [
          {
            "metric": "snowflake.deployment.failed",
            "aggregation": "count",
            "groupBy": ["environment", "error"]
          }
        ]
      },
      {
        "title": "Resource Usage",
        "type": "graph",
        "targets": [
          {
            "metric": "snowflake.warehouse.credits.used",
            "aggregation": "sum",
            "groupBy": ["warehouse"]
          }
        ]
      }
    ],
    "refresh": "30s",
    "timeRange": {
      "from": "now-6h",
      "to": "now"
    }
  }
}`

const complianceChecklistTemplate = `# Compliance Checklist

## GDPR Compliance
- [ ] Personal data identified and classified
- [ ] Data retention policies implemented
- [ ] Right to erasure procedures in place
- [ ] Data portability features available
- [ ] Privacy by design implemented
- [ ] DPO notified of changes

## SOC2 Compliance
- [ ] Access controls implemented
- [ ] Audit logging enabled
- [ ] Change management process followed
- [ ] Security monitoring active
- [ ] Incident response plan tested
- [ ] Vendor assessments completed

## PCI-DSS Compliance
- [ ] Cardholder data encrypted
- [ ] Network segmentation implemented
- [ ] Access controls enforced
- [ ] Regular security testing
- [ ] Audit trails maintained
- [ ] Security policies documented

## HIPAA Compliance
- [ ] PHI data encrypted at rest
- [ ] PHI data encrypted in transit
- [ ] Access controls implemented
- [ ] Audit controls enabled
- [ ] Integrity controls in place
- [ ] Transmission security ensured

## General Security
- [ ] Multi-factor authentication enabled
- [ ] Role-based access control configured
- [ ] Network policies defined
- [ ] Encryption keys rotated
- [ ] Security patches applied
- [ ] Penetration testing completed

## Documentation
- [ ] Data flow diagrams updated
- [ ] Security policies documented
- [ ] Incident response procedures
- [ ] Business continuity plan
- [ ] Disaster recovery procedures
- [ ] Training materials current
`

const architectureDocTemplate = `# Architecture Overview

## System Architecture

{{.ProjectName}} follows a multi-tier architecture designed for enterprise-scale deployments.

### Components

1. **Deployment Engine**
   - Git integration for version control
   - SQL parser and validator
   - Deployment orchestrator
   - Rollback manager

2. **Security Layer**
   - Credential management (Vault/KMS)
   - Network policy enforcement
   - Audit logging
   - Encryption services

3. **Monitoring Stack**
   - Metrics collection
   - Log aggregation
   - Alerting system
   - Dashboards

4. **Compliance Framework**
   - Policy engine
   - Data classification
   - Retention management
   - Audit reports

### Data Flow

1. Developer commits SQL changes to Git
2. CI/CD pipeline triggers validation
3. Deployment engine fetches changes
4. Security layer validates access
5. Changes deployed to Snowflake
6. Monitoring captures metrics
7. Compliance logs activities

### High Availability

- Multi-region deployment support
- Automated failover procedures
- Backup and restore capabilities
- Disaster recovery planning

### Scalability

- Parallel deployment processing
- Warehouse auto-scaling
- Batch optimization
- Resource pooling

## Security Architecture

### Authentication
- Service account management
- Key-pair authentication
- MFA enforcement
- Token rotation

### Authorization
- RBAC implementation
- Least privilege principle
- Dynamic permission grants
- Access reviews

### Encryption
- TLS 1.2+ for transit
- AES-256 for storage
- Key management (KMS/HSM)
- Certificate management

### Network Security
- IP whitelisting
- Private endpoints
- Network isolation
- DDoS protection
`

const deploymentRunbookTemplate = `# Deployment Runbook

## Pre-Deployment Checklist

1. **Environment Verification**
   - [ ] Target environment confirmed
   - [ ] Credentials validated
   - [ ] Network connectivity tested
   - [ ] Warehouse availability checked

2. **Change Validation**
   - [ ] SQL syntax validated
   - [ ] Dependencies verified
   - [ ] Rollback scripts prepared
   - [ ] Impact analysis completed

3. **Approvals**
   - [ ] Change ticket created
   - [ ] Technical review completed
   - [ ] Business approval obtained
   - [ ] Deployment window confirmed

## Deployment Process

### Step 1: Preparation
` + "```bash" + `
# Set environment
export ENV=prod

# Verify configuration
flakedrop validate --env $ENV

# Create backup
make backup ENV=$ENV
` + "```" + `

### Step 2: Dry Run
` + "```bash" + `
# Execute dry run
flakedrop deploy main --env $ENV --dry-run

# Review output
# Check for errors or warnings
` + "```" + `

### Step 3: Deployment
` + "```bash" + `
# Start deployment
flakedrop deploy main --env $ENV --auto-approve

# Monitor progress
tail -f logs/deployment.log
` + "```" + `

### Step 4: Validation
` + "```bash" + `
# Run validation scripts
./scripts/post-deploy/validate.sh

# Check application health
./scripts/health-check.sh
` + "```" + `

## Rollback Procedures

### Automatic Rollback
If deployment fails, automatic rollback will trigger.

### Manual Rollback
` + "```bash" + `
# List recent deployments
flakedrop rollback list

# Rollback to specific version
flakedrop rollback <deployment-id>

# Verify rollback
./scripts/validate-rollback.sh
` + "```" + `

## Troubleshooting

### Common Issues

1. **Authentication Failure**
   - Verify credentials
   - Check network policies
   - Validate key-pair setup

2. **Permission Denied**
   - Check role grants
   - Verify warehouse access
   - Review object ownership

3. **Timeout Errors**
   - Increase warehouse size
   - Check query complexity
   - Review network latency

### Emergency Contacts

- On-call Engineer: +1-xxx-xxx-xxxx
- Platform Team: platform@company.com
- Snowflake Support: support-ticket@snowflake.com
`

const gdprComplianceTemplate = `# GDPR Compliance Guide

## Data Protection Principles

### Lawfulness, Fairness, and Transparency
- All data processing has legal basis documented
- Data subjects informed of processing activities
- Privacy notices updated and accessible

### Purpose Limitation
- Data collected for specified, explicit purposes
- Further processing compatible with original purpose
- Purpose documentation maintained

### Data Minimization
- Only necessary data collected
- Regular reviews of data requirements
- Excess data purged

### Accuracy
- Processes for data correction
- Regular data quality checks
- Update procedures documented

### Storage Limitation
- Retention policies enforced
- Automated deletion processes
- Retention schedule documented

### Integrity and Confidentiality
- Encryption implemented
- Access controls enforced
- Security measures documented

## Technical Measures

### Pseudonymization
` + "```sql" + `
-- Example: Replace identifiable data with pseudonyms
CREATE OR REPLACE FUNCTION pseudonymize_email(email VARCHAR)
RETURNS VARCHAR
AS $$
  SELECT CONCAT(
    LEFT(MD5(email), 8),
    '@',
    SPLIT_PART(email, '@', 2)
  )
$$;
` + "```" + `

### Data Masking
` + "```sql" + `
-- Example: Mask sensitive data
CREATE OR REPLACE MASKING POLICY email_mask AS (val string) 
RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST') THEN '***@***.***'
    WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
    ELSE '***@***.***'
  END;
` + "```" + `

### Right to Erasure
` + "```sql" + `
-- Example: Delete personal data
CREATE OR REPLACE PROCEDURE delete_user_data(user_id INT)
RETURNS VARCHAR
AS $$
BEGIN
  -- Delete from main tables
  DELETE FROM users WHERE id = :user_id;
  DELETE FROM user_activities WHERE user_id = :user_id;
  DELETE FROM user_preferences WHERE user_id = :user_id;
  
  -- Log deletion
  INSERT INTO gdpr_deletions (user_id, deleted_at)
  VALUES (:user_id, CURRENT_TIMESTAMP());
  
  RETURN 'User data deleted successfully';
END;
$$;
` + "```" + `

### Data Portability
` + "```sql" + `
-- Example: Export user data
CREATE OR REPLACE PROCEDURE export_user_data(user_id INT)
RETURNS TABLE (data VARIANT)
AS $$
BEGIN
  RETURN TABLE(
    SELECT OBJECT_CONSTRUCT(
      'user_info', (SELECT * FROM users WHERE id = :user_id),
      'activities', (SELECT * FROM user_activities WHERE user_id = :user_id),
      'preferences', (SELECT * FROM user_preferences WHERE user_id = :user_id)
    ) as data
  );
END;
$$;
` + "```" + `

## Compliance Checklist

- [ ] Privacy Impact Assessment completed
- [ ] Data Processing Agreements signed
- [ ] Consent mechanisms implemented
- [ ] Data breach procedures tested
- [ ] Subject rights procedures documented
- [ ] Privacy by Design implemented
- [ ] DPO consulted on changes
- [ ] Training completed
`

const backupScriptTemplate = `#!/bin/bash
# Enterprise backup script

set -e

# Configuration
BACKUP_DIR="/backups/snowflake"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ENVIRONMENT=${1:-prod}

echo "Starting backup for environment: $ENVIRONMENT"

# Create backup directory
mkdir -p "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP"

# Export schema definitions
echo "Exporting schema definitions..."
flakedrop export-schema \
  --env "$ENVIRONMENT" \
  --output "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP/schema.sql"

# Export data samples (for testing)
if [ "$ENVIRONMENT" != "prod" ]; then
  echo "Exporting data samples..."
  flakedrop export-data \
    --env "$ENVIRONMENT" \
    --tables "critical_tables.txt" \
    --sample-size 1000 \
    --output "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP/data"
fi

# Export configurations
echo "Backing up configurations..."
cp -r configs "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP/"

# Export security policies
echo "Backing up security policies..."
flakedrop export-policies \
  --env "$ENVIRONMENT" \
  --output "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP/policies.sql"

# Create metadata file
cat > "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP/metadata.json" <<EOF
{
  "environment": "$ENVIRONMENT",
  "timestamp": "$TIMESTAMP",
  "version": "$(flakedrop version)",
  "created_by": "$(whoami)",
  "host": "$(hostname)"
}
EOF

# Compress backup
echo "Compressing backup..."
tar -czf "$BACKUP_DIR/$ENVIRONMENT/backup_$TIMESTAMP.tar.gz" \
  -C "$BACKUP_DIR/$ENVIRONMENT" "$TIMESTAMP"

# Upload to S3
if command -v aws &> /dev/null; then
  echo "Uploading to S3..."
  aws s3 cp "$BACKUP_DIR/$ENVIRONMENT/backup_$TIMESTAMP.tar.gz" \
    "s3://company-snowflake-backups/$ENVIRONMENT/"
fi

# Clean up old backups
echo "Cleaning up old backups..."
find "$BACKUP_DIR/$ENVIRONMENT" -name "backup_*.tar.gz" \
  -mtime +$RETENTION_DAYS -delete

# Clean up temporary directory
rm -rf "$BACKUP_DIR/$ENVIRONMENT/$TIMESTAMP"

echo "Backup completed successfully!"
echo "Backup file: $BACKUP_DIR/$ENVIRONMENT/backup_$TIMESTAMP.tar.gz"
`

const securityScanTemplate = `#!/bin/bash
# Security scanning script

set -e

echo "Running security scan..."

# Check for hardcoded credentials
echo "Checking for hardcoded credentials..."
if grep -r "password\s*=\s*['\"][^'\"]\+['\"]" migrations/ scripts/; then
  echo "ERROR: Hardcoded passwords found!"
  exit 1
fi

# Check for sensitive data exposure
echo "Checking for sensitive data exposure..."
patterns=(
  "SSN"
  "SOCIAL_SECURITY"
  "CREDIT_CARD"
  "PASSWORD"
  "API_KEY"
  "SECRET"
)

for pattern in "${patterns[@]}"; do
  if grep -ri "$pattern" migrations/ --include="*.sql" | grep -v "MASK"; then
    echo "WARNING: Potential sensitive data exposure for pattern: $pattern"
  fi
done

# Validate SQL injection vulnerabilities
echo "Checking for SQL injection vulnerabilities..."
if grep -r "EXECUTE IMMEDIATE" migrations/ | grep -v "USING"; then
  echo "WARNING: Dynamic SQL without parameters detected"
fi

# Check encryption settings
echo "Validating encryption settings..."
flakedrop validate-encryption --config configs/config.yaml

# Run dependency check
echo "Checking dependencies..."
if command -v safety &> /dev/null; then
  safety check
fi

# Check file permissions
echo "Checking file permissions..."
find . -type f -name "*.pem" -o -name "*.key" | while read -r file; do
  perms=$(stat -c %a "$file" 2>/dev/null || stat -f %p "$file")
  if [ "${perms: -3}" != "600" ]; then
    echo "WARNING: Insecure permissions on $file"
  fi
done

echo "Security scan completed!"
`

const securityWorkflowTemplate = `name: Security Scan

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday

jobs:
  security-scan:
    name: Security Scanning
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Run Secret Scanner
        uses: trufflesecurity/trufflehog@main
        with:
          path: ./
          
      - name: SQL Injection Check
        run: |
          ./scripts/security/sql-injection-check.sh
          
      - name: Dependency Check
        uses: dependency-check/Dependency-Check_Action@main
        with:
          project: '{{.ProjectName}}'
          path: '.'
          format: 'HTML'
          
      - name: Container Scan
        if: contains(github.event_name, 'push')
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: 'snowflake-deploy:latest'
          format: 'sarif'
          
      - name: SAST Scan
        uses: github/super-linter@v4
        env:
          DEFAULT_BRANCH: main
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          
      - name: Upload Security Reports
        uses: actions/upload-artifact@v3
        with:
          name: security-reports
          path: |
            dependency-check-report.html
            trivy-results.sarif
`

const jenkinsfileTemplate = `pipeline {
    agent any
    
    environment {
        SNOWFLAKE_DEPLOY_VERSION = 'latest'
        SLACK_CHANNEL = '#snowflake-deployments'
    }
    
    stages {
        stage('Setup') {
            steps {
                sh '''
                    curl -L https://github.com/your-org/snowflake-deploy/releases/download/${SNOWFLAKE_DEPLOY_VERSION}/snowflake-deploy-linux-amd64 -o snowflake-deploy
                    chmod +x snowflake-deploy
                '''
            }
        }
        
        stage('Validate') {
            steps {
                sh './flakedrop validate --config configs/config.yaml'
            }
        }
        
        stage('Security Scan') {
            steps {
                sh './scripts/security/scan.sh'
            }
        }
        
        stage('Deploy to Dev') {
            when {
                branch 'develop'
            }
            steps {
                withCredentials([string(credentialsId: 'snowflake-dev-password', variable: 'SNOWFLAKE_DEV_PASSWORD')]) {
                    sh './flakedrop deploy main --env dev --auto-approve'
                }
            }
        }
        
        stage('Deploy to Staging') {
            when {
                branch 'main'
            }
            steps {
                withCredentials([string(credentialsId: 'snowflake-staging-password', variable: 'SNOWFLAKE_STAGING_PASSWORD')]) {
                    sh './flakedrop deploy main --env staging --auto-approve'
                }
            }
        }
        
        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            input {
                message "Deploy to production?"
                ok "Deploy"
                parameters {
                    string(name: 'TICKET', defaultValue: '', description: 'Change ticket number')
                }
            }
            steps {
                withCredentials([string(credentialsId: 'snowflake-prod-password', variable: 'SNOWFLAKE_PROD_PASSWORD')]) {
                    sh """
                        ./flakedrop deploy main --env prod --dry-run
                        ./flakedrop deploy main --env prod --auto-approve --ticket ${params.TICKET}
                    """
                }
            }
        }
    }
    
    post {
        success {
            slackSend(
                channel: env.SLACK_CHANNEL,
                color: 'good',
                message: "Deployment successful: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
            )
        }
        failure {
            slackSend(
                channel: env.SLACK_CHANNEL,
                color: 'danger',
                message: "Deployment failed: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
            )
        }
    }
}
`

const azurePipelinesTemplate = `trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - migrations/*
      - configs/*

pool:
  vmImage: 'ubuntu-latest'

variables:
  snowflakeDeployVersion: 'latest'

stages:
  - stage: Validate
    jobs:
      - job: ValidateSQL
        steps:
          - task: Bash@3
            displayName: 'Setup Snowflake Deploy'
            inputs:
              targetType: 'inline'
              script: |
                curl -L https://github.com/your-org/snowflake-deploy/releases/download/$(snowflakeDeployVersion)/snowflake-deploy-linux-amd64 -o snowflake-deploy
                chmod +x snowflake-deploy
                
          - task: Bash@3
            displayName: 'Validate Migrations'
            inputs:
              targetType: 'inline'
              script: ./flakedrop validate --config configs/config.yaml

  - stage: SecurityScan
    jobs:
      - job: Security
        steps:
          - task: Bash@3
            displayName: 'Run Security Scan'
            inputs:
              targetType: 'inline'
              script: ./scripts/security/scan.sh

  - stage: DeployDev
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/develop'))
    jobs:
      - deployment: DeployToDev
        environment: 'development'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: Bash@3
                  displayName: 'Deploy to Development'
                  inputs:
                    targetType: 'inline'
                    script: ./flakedrop deploy main --env dev --auto-approve
                  env:
                    SNOWFLAKE_DEV_PASSWORD: $(SnowflakeDevPassword)

  - stage: DeployProd
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployToProd
        environment: 'production'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: Bash@3
                  displayName: 'Dry Run Production'
                  inputs:
                    targetType: 'inline'
                    script: ./flakedrop deploy main --env prod --dry-run
                  env:
                    SNOWFLAKE_PROD_PASSWORD: $(SnowflakeProdPassword)
                    
                - task: Bash@3
                  displayName: 'Deploy to Production'
                  inputs:
                    targetType: 'inline'
                    script: ./flakedrop deploy main --env prod --auto-approve
                  env:
                    SNOWFLAKE_PROD_PASSWORD: $(SnowflakeProdPassword)
`

// Minimal template content
const gitignoreMinimalTemplate = `# Snowflake Deploy
*.log
.env
configs/config.yaml

# OS
.DS_Store
Thumbs.db
`

const readmeMinimalTemplate = `# {{.ProjectName}}

A minimal FlakeDrop project.

## Setup

1. Configure Snowflake credentials:
   ` + "```bash" + `
   cp configs/config.example.yaml configs/config.yaml
   # Edit config.yaml with your credentials
   ` + "```" + `

2. Add SQL migrations to ` + "`migrations/`" + `

3. Deploy:
   ` + "```bash" + `
   ./scripts/deploy.sh
   ` + "```" + `
`

const configMinimalExampleTemplate = `# Minimal Snowflake Deploy Configuration

repositories:
  - name: main
    git_url: local
    branch: main
    path: migrations
    database: "{{.Database}}"
    schema: "{{.Schema}}"

environments:
  - name: dev
    account: "your-account.region"
    username: "your_username"
    password: "${SNOWFLAKE_PASSWORD}"
    database: "DEV_DB"
    schema: "PUBLIC"
    warehouse: "COMPUTE_WH"
    role: "DEVELOPER"
    
  - name: prod
    account: "your-account.region"
    username: "your_username"
    password: "${SNOWFLAKE_PASSWORD}"
    database: "PROD_DB"
    schema: "PUBLIC"
    warehouse: "COMPUTE_WH"
    role: "ACCOUNTADMIN"

deployment:
  timeout: "30m"
  dry_run: true
`

const initMigrationTemplate = `-- Initial database setup
-- Author: {{.Author}}
-- Date: {{.Date}}

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS {{.Schema}};

-- Use schema
USE SCHEMA {{.Schema}};

-- Create example table
CREATE TABLE IF NOT EXISTS example_table (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data
INSERT INTO example_table (name) VALUES ('Example 1'), ('Example 2');
`

const deployScriptTemplate = `#!/bin/bash
# Simple deployment script

set -e

ENVIRONMENT=${1:-dev}

echo "Deploying to $ENVIRONMENT..."

# Run deployment
flakedrop deploy main --env "$ENVIRONMENT" --config configs/config.yaml

echo "Deployment complete!"
`

// LoadCustomTemplate loads a custom template from a directory
func (tm *TemplateManager) LoadCustomTemplate(dir string) error {
	// Read template metadata
	_ = filepath.Join(dir, "template.yaml")
	// Implementation would read and parse template metadata
	// and load all template files from the directory
	return nil
}

// SaveTemplate saves a template to a directory
func (tm *TemplateManager) SaveTemplate(tmpl *ProjectTemplate, dir string) error {
	// Implementation would save template files and metadata
	return nil
}