package scaffold

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"flakedrop/internal/common"
)

// Generator handles project scaffolding
type Generator struct {
	projectDir string
	config     *Config
}

// Config holds scaffolding configuration
type Config struct {
	ProjectName  string
	Author       string
	Company      string
	Environments []string
	Features     []string
	Database     string
	Schema       string
}

// NewGenerator creates a new scaffold generator
func NewGenerator(projectDir string, config *Config) *Generator {
	return &Generator{
		projectDir: projectDir,
		config:     config,
	}
}

// GenerateMigration creates a new migration file
func (g *Generator) GenerateMigration(name, category string) (string, error) {
	// Generate timestamp-based filename
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.sql", timestamp, strings.ReplaceAll(name, " ", "_"))
	
	// Determine directory based on category
	dir := filepath.Join(g.projectDir, "migrations", category)
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Generate migration content
	content := g.generateMigrationContent(name, category)
	
	// Write file
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write migration file: %w", err)
	}
	
	return fullPath, nil
}

// generateMigrationContent creates migration file content
func (g *Generator) generateMigrationContent(name, category string) string {
	vars := map[string]interface{}{
		"Name":     name,
		"Category": category,
		"Author":   g.config.Author,
		"Date":     time.Now().Format("2006-01-02"),
		"Database": g.config.Database,
		"Schema":   g.config.Schema,
	}
	
	var tmpl string
	switch category {
	case "schema":
		tmpl = schemaMigrationTemplate
	case "data":
		tmpl = dataMigrationTemplate
	case "procedures":
		tmpl = procedureMigrationTemplate
	case "views":
		tmpl = viewMigrationTemplate
	default:
		tmpl = genericMigrationTemplate
	}
	
	return g.processTemplate("migration", tmpl, vars)
}

// GenerateScript creates a new script file
func (g *Generator) GenerateScript(name, scriptType string) (string, error) {
	// Determine directory
	dir := filepath.Join(g.projectDir, "scripts", scriptType)
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Generate filename
	filename := fmt.Sprintf("%s.sql", strings.ReplaceAll(name, " ", "_"))
	
	// Generate content
	content := g.generateScriptContent(name, scriptType)
	
	// Write file
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write script file: %w", err)
	}
	
	return fullPath, nil
}

// generateScriptContent creates script file content
func (g *Generator) generateScriptContent(name, scriptType string) string {
	vars := map[string]interface{}{
		"Name":     name,
		"Type":     scriptType,
		"Author":   g.config.Author,
		"Date":     time.Now().Format("2006-01-02"),
		"Database": g.config.Database,
		"Schema":   g.config.Schema,
	}
	
	var tmpl string
	switch scriptType {
	case "pre-deploy":
		tmpl = preDeployScriptTemplate
	case "post-deploy":
		tmpl = postDeployScriptTemplate
	case "validation":
		tmpl = validationScriptTemplate
	case "rollback":
		tmpl = rollbackScriptTemplate
	default:
		tmpl = genericScriptTemplate
	}
	
	return g.processTemplate("script", tmpl, vars)
}

// GenerateTest creates a new test file
func (g *Generator) GenerateTest(name, testType string) (string, error) {
	// Determine directory
	dir := filepath.Join(g.projectDir, "tests", testType)
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Generate filename
	filename := fmt.Sprintf("%s_test.sql", strings.ReplaceAll(name, " ", "_"))
	
	// Generate content
	content := g.generateTestContent(name, testType)
	
	// Write file
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write test file: %w", err)
	}
	
	return fullPath, nil
}

// generateTestContent creates test file content
func (g *Generator) generateTestContent(name, testType string) string {
	vars := map[string]interface{}{
		"Name":     name,
		"Type":     testType,
		"Author":   g.config.Author,
		"Date":     time.Now().Format("2006-01-02"),
		"Database": g.config.Database,
		"Schema":   g.config.Schema,
	}
	
	var tmpl string
	switch testType {
	case "unit":
		tmpl = unitTestTemplate
	case "integration":
		tmpl = integrationTestTemplate
	case "performance":
		tmpl = performanceTestTemplate
	default:
		tmpl = genericTestTemplate
	}
	
	return g.processTemplate("test", tmpl, vars)
}

// GenerateEnvironmentConfig creates environment-specific configuration
func (g *Generator) GenerateEnvironmentConfig(env string) (string, error) {
	// Create directory
	dir := filepath.Join(g.projectDir, "configs", "environments")
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Generate content
	content := g.generateEnvironmentConfig(env)
	
	// Write file
	filename := fmt.Sprintf("%s.yaml", env)
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write environment config: %w", err)
	}
	
	return fullPath, nil
}

// generateEnvironmentConfig creates environment configuration content
func (g *Generator) generateEnvironmentConfig(env string) string {
	isProduction := env == "prod" || env == "production"
	
	vars := map[string]interface{}{
		"Environment": env,
		"Account":     "your-account.region",
		"Username": func() string {
			if isProduction {
				return fmt.Sprintf("%s_PROD_SVC", strings.ToUpper(g.config.Company))
			}
			return fmt.Sprintf("%s_%s_SVC", strings.ToUpper(g.config.Company), strings.ToUpper(env))
		}(),
		"Database": fmt.Sprintf("%s_DB", strings.ToUpper(env)),
		"Schema":   "PUBLIC",
		"Warehouse": func() string {
			if isProduction {
				return "PROD_WH"
			}
			return fmt.Sprintf("%s_WH", strings.ToUpper(env))
		}(),
		"Role": func() string {
			if isProduction {
				return "PROD_DEPLOY_ROLE"
			}
			return fmt.Sprintf("%s_DEPLOY_ROLE", strings.ToUpper(env))
		}(),
	}
	
	return g.processTemplate("env-config", environmentConfigTemplate, vars)
}

// GeneratePipeline creates CI/CD pipeline configuration
func (g *Generator) GeneratePipeline(platform string) (string, error) {
	var dir, filename, content string
	
	switch platform {
	case "github":
		dir = filepath.Join(g.projectDir, ".github", "workflows")
		filename = "deploy.yml"
		content = g.generateGitHubWorkflow()
	case "gitlab":
		dir = g.projectDir
		filename = ".gitlab-ci.yml"
		content = g.generateGitLabCI()
	case "jenkins":
		dir = g.projectDir
		filename = "Jenkinsfile"
		content = g.generateJenkinsfile()
	case "azure":
		dir = g.projectDir
		filename = "azure-pipelines.yml"
		content = g.generateAzurePipeline()
	default:
		return "", fmt.Errorf("unsupported platform: %s", platform)
	}
	
	// Create directory
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	// Write file
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write pipeline file: %w", err)
	}
	
	return fullPath, nil
}

// generateGitHubWorkflow creates GitHub Actions workflow
func (g *Generator) generateGitHubWorkflow() string {
	vars := map[string]interface{}{
		"ProjectName":  g.config.ProjectName,
		"Environments": g.config.Environments,
	}
	
	return g.processTemplate("github-workflow", githubWorkflowScaffoldTemplate, vars)
}

// generateGitLabCI creates GitLab CI configuration
func (g *Generator) generateGitLabCI() string {
	vars := map[string]interface{}{
		"ProjectName":  g.config.ProjectName,
		"Environments": g.config.Environments,
	}
	
	return g.processTemplate("gitlab-ci", gitlabCIScaffoldTemplate, vars)
}

// generateJenkinsfile creates Jenkins pipeline
func (g *Generator) generateJenkinsfile() string {
	vars := map[string]interface{}{
		"ProjectName":  g.config.ProjectName,
		"Environments": g.config.Environments,
	}
	
	return g.processTemplate("jenkinsfile", jenkinsfileScaffoldTemplate, vars)
}

// generateAzurePipeline creates Azure DevOps pipeline
func (g *Generator) generateAzurePipeline() string {
	vars := map[string]interface{}{
		"ProjectName":  g.config.ProjectName,
		"Environments": g.config.Environments,
	}
	
	return g.processTemplate("azure-pipeline", azurePipelineScaffoldTemplate, vars)
}

// GenerateDocumentation creates documentation files
func (g *Generator) GenerateDocumentation(docType string) (string, error) {
	dir := filepath.Join(g.projectDir, "docs")
	if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	
	var filename, content string
	
	switch docType {
	case "deployment":
		filename = "DEPLOYMENT.md"
		content = g.generateDeploymentDoc()
	case "migration":
		filename = "MIGRATION_GUIDE.md"
		content = g.generateMigrationDoc()
	case "troubleshooting":
		filename = "TROUBLESHOOTING.md"
		content = g.generateTroubleshootingDoc()
	case "architecture":
		filename = "ARCHITECTURE.md"
		content = g.generateArchitectureDoc()
	default:
		return "", fmt.Errorf("unsupported documentation type: %s", docType)
	}
	
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, []byte(content), 0600); err != nil {
		return "", fmt.Errorf("failed to write documentation: %w", err)
	}
	
	return fullPath, nil
}

// processTemplate applies template with variables
func (g *Generator) processTemplate(name, tmplStr string, vars map[string]interface{}) string {
	tmpl, err := template.New(name).Parse(tmplStr)
	if err != nil {
		return fmt.Sprintf("Error processing template: %v", err)
	}
	
	var buf strings.Builder
	if err := tmpl.Execute(&buf, vars); err != nil {
		return fmt.Sprintf("Error executing template: %v", err)
	}
	
	return buf.String()
}

// Template constants
const schemaMigrationTemplate = `-- Migration: {{.Name}}
-- Category: {{.Category}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- DDL Changes
-- ============================================

-- Add your schema changes here
-- Example: CREATE TABLE, ALTER TABLE, CREATE VIEW, etc.

-- CREATE TABLE IF NOT EXISTS table_name (
--     id INTEGER AUTOINCREMENT PRIMARY KEY,
--     column1 VARCHAR(255) NOT NULL,
--     column2 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
-- );

-- ============================================
-- Rollback Section
-- ============================================
-- @ROLLBACK

-- Add rollback statements here
-- Example: DROP TABLE IF EXISTS table_name;
`

const dataMigrationTemplate = `-- Migration: {{.Name}}
-- Category: {{.Category}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Data Migration
-- ============================================

-- Add your data migration statements here
-- Consider using transactions for data integrity

BEGIN TRANSACTION;

-- Example: INSERT, UPDATE, DELETE, MERGE statements

-- INSERT INTO target_table (column1, column2)
-- SELECT column1, column2
-- FROM source_table
-- WHERE condition;

COMMIT;

-- ============================================
-- Validation
-- ============================================

-- Add validation queries to ensure migration success
-- SELECT COUNT(*) as migrated_rows FROM target_table;
`

const procedureMigrationTemplate = `-- Migration: {{.Name}}
-- Category: {{.Category}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Stored Procedure
-- ============================================

CREATE OR REPLACE PROCEDURE procedure_name(
    param1 VARCHAR,
    param2 NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    result VARCHAR;
BEGIN
    -- Procedure logic here
    
    -- Example:
    -- SELECT column INTO result
    -- FROM table_name
    -- WHERE condition = :param1
    -- LIMIT 1;
    
    RETURN result;
END;
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE procedure_name(VARCHAR, NUMBER) TO ROLE your_role;

-- ============================================
-- Rollback Section
-- ============================================
-- @ROLLBACK

-- DROP PROCEDURE IF EXISTS procedure_name(VARCHAR, NUMBER);
`

const viewMigrationTemplate = `-- Migration: {{.Name}}
-- Category: {{.Category}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- View Definition
-- ============================================

CREATE OR REPLACE VIEW view_name AS
SELECT 
    -- Add columns here
    column1,
    column2,
    column3
FROM 
    table_name
WHERE 
    -- Add conditions here
    1=1
;

-- Grant permissions
GRANT SELECT ON VIEW view_name TO ROLE your_role;

-- ============================================
-- Rollback Section
-- ============================================
-- @ROLLBACK

-- DROP VIEW IF EXISTS view_name;
`

const genericMigrationTemplate = `-- Migration: {{.Name}}
-- Category: {{.Category}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Migration Content
-- ============================================

-- Add your SQL statements here

-- ============================================
-- Rollback Section
-- ============================================
-- @ROLLBACK

-- Add rollback statements here
`

const preDeployScriptTemplate = `-- Pre-Deploy Script: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

-- This script runs before the main deployment

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Environment Validation
-- ============================================

-- Verify prerequisites
SHOW WAREHOUSES;
SHOW ROLES;

-- Check required permissions
SHOW GRANTS TO ROLE CURRENT_ROLE();

-- ============================================
-- Backup Critical Data (if needed)
-- ============================================

-- Example: Create backup table
-- CREATE TABLE IF NOT EXISTS backup_table_{{.Date | replace "-" "_"}} AS
-- SELECT * FROM critical_table;

-- ============================================
-- Pre-deployment Tasks
-- ============================================

-- Add any setup tasks here
-- Example: Create temporary tables, disable constraints, etc.

SELECT 'Pre-deployment checks completed successfully' as status;
`

const postDeployScriptTemplate = `-- Post-Deploy Script: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

-- This script runs after the main deployment

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Post-deployment Validation
-- ============================================

-- Verify deployment success
-- Example: Check if new objects were created

-- SELECT COUNT(*) as table_count
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = '{{.Schema}}'
-- AND CREATED > DATEADD(hour, -1, CURRENT_TIMESTAMP());

-- ============================================
-- Update Statistics
-- ============================================

-- Analyze tables for query optimization
-- ANALYZE TABLE table_name COMPUTE STATISTICS;

-- ============================================
-- Cleanup Tasks
-- ============================================

-- Remove temporary objects
-- DROP TABLE IF EXISTS temp_migration_table;

-- ============================================
-- Notifications
-- ============================================

SELECT 
    'Post-deployment tasks completed' as status,
    CURRENT_TIMESTAMP() as completed_at,
    '{{.Author}}' as deployed_by;
`

const validationScriptTemplate = `-- Validation Script: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Data Validation
-- ============================================

-- Row count validations
WITH row_counts AS (
    SELECT 
        'table1' as table_name,
        COUNT(*) as row_count
    FROM table1
    UNION ALL
    SELECT 
        'table2' as table_name,
        COUNT(*) as row_count
    FROM table2
)
SELECT * FROM row_counts;

-- ============================================
-- Schema Validation
-- ============================================

-- Check table structures
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{{.Schema}}'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- ============================================
-- Constraint Validation
-- ============================================

-- Check constraints
SHOW PRIMARY KEYS IN SCHEMA {{.Schema}};
SHOW UNIQUE KEYS IN SCHEMA {{.Schema}};

-- ============================================
-- Business Rule Validation
-- ============================================

-- Add custom validation queries here
-- Example: Check data integrity

-- SELECT 
--     CASE 
--         WHEN COUNT(*) = 0 THEN 'PASS'
--         ELSE 'FAIL - Found ' || COUNT(*) || ' invalid records'
--     END as validation_result
-- FROM table_name
-- WHERE business_rule_condition IS FALSE;

SELECT 'All validations passed' as final_status;
`

const rollbackScriptTemplate = `-- Rollback Script: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

-- This script reverses deployment changes

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Rollback Preparation
-- ============================================

-- Verify we can rollback
SELECT 
    'Starting rollback' as status,
    CURRENT_TIMESTAMP() as started_at;

-- ============================================
-- Rollback DDL Changes
-- ============================================

-- Drop new objects in reverse order
-- DROP TABLE IF EXISTS new_table CASCADE;
-- DROP VIEW IF EXISTS new_view;
-- DROP PROCEDURE IF EXISTS new_procedure(VARCHAR, NUMBER);

-- ============================================
-- Restore Data
-- ============================================

-- Restore from backup if available
-- CREATE TABLE IF NOT EXISTS restored_table AS
-- SELECT * FROM backup_table_{{.Date | replace "-" "_"}};

-- ============================================
-- Rollback Validation
-- ============================================

-- Verify rollback success
SELECT 
    'Rollback completed' as status,
    CURRENT_TIMESTAMP() as completed_at;
`

const genericScriptTemplate = `-- Script: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA {{.Schema}};

-- ============================================
-- Script Content
-- ============================================

-- Add your SQL statements here

SELECT 'Script completed' as status;
`

const unitTestTemplate = `-- Unit Test: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}}_TEST;
USE SCHEMA {{.Schema}};

-- ============================================
-- Test Setup
-- ============================================

-- Create test data
CREATE OR REPLACE TEMPORARY TABLE test_data AS
SELECT 
    -- Add test data here
    1 as id,
    'test' as name
;

-- ============================================
-- Test Cases
-- ============================================

-- Test Case 1: Description
DECLARE
    expected_result VARCHAR DEFAULT 'expected';
    actual_result VARCHAR;
BEGIN
    -- Execute test
    SELECT name INTO actual_result
    FROM test_data
    WHERE id = 1;
    
    -- Assert
    IF (actual_result != expected_result) THEN
        RAISE EXCEPTION 'Test failed: Expected %, got %', expected_result, actual_result;
    END IF;
    
    RETURN 'Test Case 1: PASSED';
END;

-- Test Case 2: Description
-- Add more test cases here

-- ============================================
-- Test Cleanup
-- ============================================

DROP TABLE IF EXISTS test_data;

SELECT 'All unit tests passed' as status;
`

const integrationTestTemplate = `-- Integration Test: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}}_TEST;
USE SCHEMA {{.Schema}};

-- ============================================
-- Integration Test Setup
-- ============================================

-- Setup test environment
-- This might include creating multiple tables,
-- loading test data, setting up dependencies

-- ============================================
-- Integration Test Scenarios
-- ============================================

-- Scenario 1: End-to-end data flow
BEGIN TRANSACTION;

-- Step 1: Insert test data
-- INSERT INTO source_table ...

-- Step 2: Execute transformation
-- CALL transformation_procedure();

-- Step 3: Verify results
-- SELECT COUNT(*) FROM target_table ...

ROLLBACK; -- Clean up test data

-- ============================================
-- Integration Test Results
-- ============================================

SELECT 
    'Integration Test' as test_name,
    'PASSED' as status,
    CURRENT_TIMESTAMP() as executed_at;
`

const performanceTestTemplate = `-- Performance Test: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}}_TEST;
USE SCHEMA {{.Schema}};

-- ============================================
-- Performance Test Setup
-- ============================================

-- Create large dataset for testing
CREATE OR REPLACE TABLE perf_test_data AS
SELECT 
    SEQ4() as id,
    RANDSTR(100, RANDOM()) as data
FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

-- ============================================
-- Performance Tests
-- ============================================

-- Test 1: Query performance
SET query_start = CURRENT_TIMESTAMP();

-- Your query here
SELECT COUNT(*), AVG(LENGTH(data))
FROM perf_test_data;

SET query_end = CURRENT_TIMESTAMP();

SELECT 
    'Query Performance' as test_name,
    TIMESTAMPDIFF(MILLISECOND, $query_start, $query_end) as duration_ms;

-- Test 2: Insert performance
SET insert_start = CURRENT_TIMESTAMP();

INSERT INTO perf_test_data_copy
SELECT * FROM perf_test_data;

SET insert_end = CURRENT_TIMESTAMP();

SELECT 
    'Insert Performance' as test_name,
    TIMESTAMPDIFF(SECOND, $insert_start, $insert_end) as duration_seconds;

-- ============================================
-- Performance Test Cleanup
-- ============================================

DROP TABLE IF EXISTS perf_test_data;
DROP TABLE IF EXISTS perf_test_data_copy;

SELECT 'Performance tests completed' as status;
`

const genericTestTemplate = `-- Test: {{.Name}}
-- Type: {{.Type}}
-- Author: {{.Author}}
-- Date: {{.Date}}

USE DATABASE {{.Database}}_TEST;
USE SCHEMA {{.Schema}};

-- ============================================
-- Test Setup
-- ============================================

-- Add test setup here

-- ============================================
-- Test Execution
-- ============================================

-- Add test cases here

-- ============================================
-- Test Cleanup
-- ============================================

-- Clean up test data

SELECT 'Test completed' as status;
`

const environmentConfigTemplate = `# Environment: {{.Environment}}
# Snowflake configuration for {{.Environment}} environment

snowflake:
  account: "{{.Account}}"
  username: "{{.Username}}"
  password: "${SNOWFLAKE_{{.Environment | upper}}_PASSWORD}"
  database: "{{.Database}}"
  schema: "{{.Schema}}"
  warehouse: "{{.Warehouse}}"
  role: "{{.Role}}"
  
# Environment-specific settings
settings:
  timeout: "30m"
  max_retries: 3
  
# Security settings
security:
  require_mfa: {{ if eq .Environment "prod" }}true{{ else }}false{{ end }}
  ip_whitelist:
    - "10.0.0.0/8"
    {{ if eq .Environment "prod" }}- "production-vpc-cidr"{{ end }}
    
# Monitoring
monitoring:
  enabled: true
  alert_channel: "#snowflake-{{.Environment}}"
`

const githubWorkflowScaffoldTemplate = `name: Deploy to {{.ProjectName}}

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
      - name: Validate SQL
        run: |
          flakedrop validate --config configs/config.yaml

{{range .Environments}}
  deploy-{{.}}:
    needs: validate
    runs-on: ubuntu-latest
    environment: {{.}}
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to {{.}}
        env:
          SNOWFLAKE_{{. | upper}}_PASSWORD: ${{ secrets.SNOWFLAKE_{{. | upper}}_PASSWORD }}
        run: |
          flakedrop deploy main --env {{.}} --auto-approve
{{end}}
`

const gitlabCIScaffoldTemplate = `.stages:
  - validate
  - deploy

validate:
  stage: validate
  script:
    - flakedrop validate --config configs/config.yaml

{{range .Environments}}
deploy:{{.}}:
  stage: deploy
  script:
    - flakedrop deploy main --env {{.}} --auto-approve
  environment:
    name: {{.}}
  only:
    - {{ if eq . "prod" }}main{{ else }}develop{{ end }}
{{end}}
`

const jenkinsfileScaffoldTemplate = `pipeline {
    agent any
    
    stages {
        stage('Validate') {
            steps {
                sh 'flakedrop validate --config configs/config.yaml'
            }
        }
        
{{range .Environments}}
        stage('Deploy to {{.}}') {
            when {
                branch '{{ if eq . "prod" }}main{{ else }}develop{{ end }}'
            }
            steps {
                withCredentials([string(credentialsId: 'snowflake-{{.}}-password', variable: 'SNOWFLAKE_{{. | upper}}_PASSWORD')]) {
                    sh 'flakedrop deploy main --env {{.}} --auto-approve'
                }
            }
        }
{{end}}
    }
}
`

const azurePipelineScaffoldTemplate = `trigger:
  branches:
    include:
      - main
      - develop

stages:
  - stage: Validate
    jobs:
      - job: ValidateSQL
        steps:
          - script: flakedrop validate --config configs/config.yaml

{{range .Environments}}
  - stage: Deploy{{. | title}}
    jobs:
      - deployment: Deploy
        environment: {{.}}
        strategy:
          runOnce:
            deploy:
              steps:
                - script: flakedrop deploy main --env {{.}} --auto-approve
                  env:
                    SNOWFLAKE_{{. | upper}}_PASSWORD: $(Snowflake{{. | title}}Password)
{{end}}
`

// Document generation methods
func (g *Generator) generateDeploymentDoc() string {
	return g.processTemplate("deployment-doc", deploymentDocTemplate, map[string]interface{}{
		"ProjectName":  g.config.ProjectName,
		"Environments": g.config.Environments,
	})
}

func (g *Generator) generateMigrationDoc() string {
	return g.processTemplate("migration-doc", migrationDocTemplate, map[string]interface{}{
		"ProjectName": g.config.ProjectName,
	})
}

func (g *Generator) generateTroubleshootingDoc() string {
	return g.processTemplate("troubleshooting-doc", troubleshootingDocTemplate, map[string]interface{}{
		"ProjectName": g.config.ProjectName,
	})
}

func (g *Generator) generateArchitectureDoc() string {
	return g.processTemplate("architecture-doc", architectureDocTemplate, map[string]interface{}{
		"ProjectName": g.config.ProjectName,
		"Features":    g.config.Features,
	})
}

const deploymentDocTemplate = `# Deployment Guide

## Overview

This guide covers the deployment process for {{.ProjectName}}.

## Environments

{{range .Environments}}
- **{{.}}**: {{. | title}} environment
{{end}}

## Deployment Process

### Prerequisites

1. Snowflake account access
2. Appropriate role permissions
3. Network connectivity
4. flakedrop CLI installed

### Deployment Steps

1. **Validate Changes**
   ` + "```bash" + `
   flakedrop validate --config configs/config.yaml
   ` + "```" + `

2. **Dry Run**
   ` + "```bash" + `
   flakedrop deploy main --env <environment> --dry-run
   ` + "```" + `

3. **Deploy**
   ` + "```bash" + `
   flakedrop deploy main --env <environment>
   ` + "```" + `

## Rollback Procedures

If a deployment needs to be rolled back:

` + "```bash" + `
flakedrop rollback list
flakedrop rollback <deployment-id>
` + "```" + `
`

const migrationDocTemplate = `# Migration Guide

## Creating Migrations

### Naming Convention

- Sequential: ` + "`001_description.sql`" + `
- Date-based: ` + "`YYYYMMDD_HHMMSS_description.sql`" + `

### Migration Structure

` + "```sql" + `
-- Migration: Description
-- Author: Your Name
-- Date: YYYY-MM-DD

-- Forward migration
[SQL statements]

-- @ROLLBACK
-- Rollback statements
` + "```" + `

## Best Practices

1. Always include rollback statements
2. Test migrations in development first
3. Use transactions for data changes
4. Make migrations idempotent
5. Document complex logic
`

const troubleshootingDocTemplate = `# Troubleshooting Guide

## Common Issues

### Authentication Errors

**Problem**: Cannot authenticate to Snowflake

**Solutions**:
1. Verify credentials in config.yaml
2. Check environment variables
3. Ensure IP is whitelisted
4. Verify MFA settings

### Permission Errors

**Problem**: Insufficient privileges

**Solutions**:
1. Check role grants
2. Verify warehouse access
3. Ensure database/schema permissions
4. Review object ownership

### Connection Issues

**Problem**: Cannot connect to Snowflake

**Solutions**:
1. Check network connectivity
2. Verify account URL format
3. Check firewall settings
4. Test with Snowflake web UI

## Debug Commands

` + "```bash" + `
# Enable verbose logging
export SNOWFLAKE_DEPLOY_DEBUG=true

# Test connection
flakedrop test-connection --env <environment>

# Validate configuration
flakedrop validate --config configs/config.yaml --verbose
` + "```" + `
`

const architectureDocTemplate = `# Architecture

## Overview

{{.ProjectName}} is designed for reliable, scalable Snowflake deployments.

## Components

### Core Features
{{range .Features}}
- {{.}}
{{end}}

### Architecture Diagram

` + "```" + `
┌─────────────┐     ┌──────────────┐     ┌────────────┐
│   Git Repo  │────>│ CI/CD Pipeline│────>│  Snowflake │
└─────────────┘     └──────────────┘     └────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Deploy Tool  │
                    └──────────────┘
` + "```" + `

## Security

- Encrypted credentials
- Role-based access control
- Audit logging
- Network isolation

## Scalability

- Parallel deployments
- Batch processing
- Resource optimization
- Performance monitoring
`