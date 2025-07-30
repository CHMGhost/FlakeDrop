package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"flakedrop/internal/common"
	"flakedrop/internal/template"
	"flakedrop/internal/config"
	"flakedrop/pkg/models"
	"github.com/spf13/cobra"
)

var quickstartCmd = &cobra.Command{
	Use:   "quickstart [scenario]",
	Short: "Quick project setup for common scenarios",
	Long: `Create a new FlakeDrop project pre-configured for common scenarios:

Available scenarios:
  data-warehouse    - Traditional data warehouse with staging and marts
  data-lake         - Data lake with raw, refined, and curated layers
  analytics         - Analytics platform with reporting and BI
  microservices     - Microservices data architecture
  migration         - Database migration from another platform
  poc               - Proof of concept or demo project`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"data-warehouse", "data-lake", "analytics", "microservices", "migration", "poc"},
	Run:       runQuickstart,
}

var quickstartFlags struct {
	name         string
	skipExamples bool
	environments string
}

func init() {
	quickstartCmd.Flags().StringVarP(&quickstartFlags.name, "name", "n", "", "Project name (defaults to scenario name)")
	quickstartCmd.Flags().BoolVar(&quickstartFlags.skipExamples, "skip-examples", false, "Skip creating example files")
	quickstartCmd.Flags().StringVarP(&quickstartFlags.environments, "environments", "e", "dev,staging,prod", "Comma-separated list of environments")
	
	rootCmd.AddCommand(quickstartCmd)
}

func runQuickstart(cmd *cobra.Command, args []string) {
	scenario := args[0]
	
	// Determine project name
	projectName := quickstartFlags.name
	if projectName == "" {
		projectName = fmt.Sprintf("%s-project", scenario)
	}
	
	fmt.Printf("🚀 Creating %s project: %s\n", scenario, projectName)
	fmt.Println()
	
	// Create project directory
	projectDir, err := filepath.Abs(projectName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	
	// Check if directory exists
	if _, err := os.Stat(projectDir); err == nil {
		fmt.Printf("Error: Directory %s already exists\n", projectName)
		os.Exit(1)
	}
	
	// Get scenario configuration
	scenarioConfig := getScenarioConfig(scenario)
	
	// Create project structure
	if err := createQuickstartProject(projectDir, scenario, scenarioConfig); err != nil {
		fmt.Printf("Error creating project: %v\n", err)
		os.Exit(1)
	}
	
	// Show completion message and next steps
	showQuickstartComplete(projectName, scenario, scenarioConfig)
}

func getScenarioConfig(scenario string) ScenarioConfig {
	configs := map[string]ScenarioConfig{
		"data-warehouse": {
			Template:    "standard",
			Description: "Traditional data warehouse with staging and marts",
			Features:    []string{"testing", "documentation", "monitoring"},
			Structure: ProjectStructure{
				Schemas: []string{"STAGING", "DW_CORE", "DW_MARTS", "DW_REPORTS"},
				Folders: map[string][]string{
					"migrations/schema": {
						"001_staging_layer.sql",
						"002_core_layer.sql", 
						"003_marts_layer.sql",
						"004_reports_layer.sql",
					},
					"migrations/data": {
						"001_reference_data.sql",
						"002_dimension_loads.sql",
					},
					"scripts/etl": {
						"extract_source_data.sql",
						"transform_staging.sql",
						"load_dimensions.sql",
						"load_facts.sql",
					},
				},
			},
			Examples: map[string]string{
				"migrations/schema/001_staging_layer.sql": dataWarehouseStagingSchema,
				"migrations/schema/002_core_layer.sql":    dataWarehouseCoreSchema,
				"migrations/schema/003_marts_layer.sql":   dataWarehouseMartsSchema,
			},
		},
		"data-lake": {
			Template:    "enterprise",
			Description: "Data lake with raw, refined, and curated layers",
			Features:    []string{"monitoring", "security", "compliance"},
			Structure: ProjectStructure{
				Schemas: []string{"RAW", "REFINED", "CURATED", "SANDBOX"},
				Folders: map[string][]string{
					"migrations/schema": {
						"001_raw_layer.sql",
						"002_refined_layer.sql",
						"003_curated_layer.sql",
						"004_sandbox_layer.sql",
					},
					"migrations/stages": {
						"001_external_stages.sql",
						"002_file_formats.sql",
					},
					"migrations/pipes": {
						"001_ingestion_pipes.sql",
					},
				},
			},
			Examples: map[string]string{
				"migrations/schema/001_raw_layer.sql":       dataLakeRawSchema,
				"migrations/stages/001_external_stages.sql": dataLakeStages,
				"migrations/pipes/001_ingestion_pipes.sql":  dataLakePipes,
			},
		},
		"analytics": {
			Template:    "standard",
			Description: "Analytics platform with reporting and BI",
			Features:    []string{"testing", "documentation"},
			Structure: ProjectStructure{
				Schemas: []string{"ANALYTICS", "REPORTING", "METRICS", "ML_FEATURES"},
				Folders: map[string][]string{
					"migrations/schema": {
						"001_analytics_schema.sql",
						"002_reporting_schema.sql",
						"003_metrics_schema.sql",
						"004_ml_features_schema.sql",
					},
					"migrations/views": {
						"001_business_metrics.sql",
						"002_kpi_dashboards.sql",
						"003_executive_reports.sql",
					},
				},
			},
			Examples: map[string]string{
				"migrations/schema/001_analytics_schema.sql": analyticsSchema,
				"migrations/views/001_business_metrics.sql":  analyticsViews,
			},
		},
		"microservices": {
			Template:    "minimal",
			Description: "Microservices data architecture",
			Features:    []string{},
			Structure: ProjectStructure{
				Schemas: []string{"SERVICE_USER", "SERVICE_ORDER", "SERVICE_INVENTORY", "SERVICE_COMMON"},
				Folders: map[string][]string{
					"migrations/services": {
						"user_service/",
						"order_service/",
						"inventory_service/",
						"common/",
					},
				},
			},
			Examples: map[string]string{
				"migrations/services/user_service/001_schema.sql":  microservicesUserSchema,
				"migrations/services/order_service/001_schema.sql": microservicesOrderSchema,
				"migrations/services/common/001_shared.sql":        microservicesCommonSchema,
			},
		},
		"migration": {
			Template:    "standard",
			Description: "Database migration from another platform",
			Features:    []string{"testing", "validation", "rollback"},
			Structure: ProjectStructure{
				Schemas: []string{"MIGRATION_STAGING", "MIGRATION_TARGET", "MIGRATION_ARCHIVE"},
				Folders: map[string][]string{
					"migrations/schema": {
						"001_staging_tables.sql",
						"002_target_schema.sql",
						"003_archive_schema.sql",
					},
					"scripts/migration": {
						"validate_source.sql",
						"data_quality_checks.sql",
						"reconciliation.sql",
					},
				},
			},
			Examples: map[string]string{
				"migrations/schema/001_staging_tables.sql":     migrationStagingSchema,
				"scripts/migration/data_quality_checks.sql":    migrationQualityChecks,
				"scripts/migration/reconciliation.sql":         migrationReconciliation,
			},
		},
		"poc": {
			Template:    "minimal",
			Description: "Proof of concept or demo project",
			Features:    []string{},
			Structure: ProjectStructure{
				Schemas: []string{"POC_DEMO"},
				Folders: map[string][]string{
					"migrations": {
						"001_demo_schema.sql",
						"002_sample_data.sql",
					},
					"scripts": {
						"demo_queries.sql",
					},
				},
			},
			Examples: map[string]string{
				"migrations/001_demo_schema.sql":  pocDemoSchema,
				"migrations/002_sample_data.sql":  pocSampleData,
				"scripts/demo_queries.sql":        pocDemoQueries,
			},
		},
	}
	
	return configs[scenario]
}

func createQuickstartProject(projectDir, scenario string, config ScenarioConfig) error {
	// Use template manager to create base project
	tm := template.NewTemplateManager()
	
	// Get environments
	environments := strings.Split(quickstartFlags.environments, ",")
	for i := range environments {
		environments[i] = strings.TrimSpace(environments[i])
	}
	
	// Prepare template variables
	vars := map[string]interface{}{
		"ProjectName":       filepath.Base(projectDir),
		"Scenario":          scenario,
		"Description":       config.Description,
		"Environments":      environments,
		"Features":          config.Features,
		"Schemas":           config.Structure.Schemas,
		"DefaultWarehouse":  "COMPUTE_WH",
		"DefaultRole":       "DEVELOPER",
		"Database":          fmt.Sprintf("%s_DB", strings.ToUpper(scenario)),
		"Schema":            config.Structure.Schemas[0],
	}
	
	// Apply template
	if err := tm.ApplyTemplate(config.Template, projectDir, vars); err != nil {
		return err
	}
	
	// Create scenario-specific structure
	for folder, files := range config.Structure.Folders {
		folderPath := filepath.Join(projectDir, folder)
		if err := os.MkdirAll(folderPath, common.DirPermissionNormal); err != nil {
			return err
		}
		
		// Create placeholder files or subdirectories
		for _, file := range files {
			if strings.HasSuffix(file, "/") {
				// It's a directory
				subdir := filepath.Join(folderPath, file)
				if err := os.MkdirAll(subdir, common.DirPermissionNormal); err != nil {
					return err
				}
			}
		}
	}
	
	// Create example files unless skipped
	if !quickstartFlags.skipExamples {
		for path, content := range config.Examples {
			fullPath := filepath.Join(projectDir, path)
			
			// Ensure directory exists
			dir := filepath.Dir(fullPath)
			if err := os.MkdirAll(dir, common.DirPermissionNormal); err != nil {
				return err
			}
			
			// Process template variables in content
			processedContent := processTemplateVars(content, vars)
			
			if err := os.WriteFile(fullPath, []byte(processedContent), 0600); err != nil {
				return err
			}
		}
	}
	
	// Create scenario-specific configuration
	if err := createScenarioConfig(projectDir, scenario, config, environments); err != nil {
		return err
	}
	
	// Create scenario-specific documentation
	if err := createScenarioDocs(projectDir, scenario, config); err != nil {
		return err
	}
	
	// Initialize git repository
	if err := initializeGitRepo(projectDir); err != nil {
		// Git initialization is optional, just log the error
		fmt.Printf("Note: Git repository initialization failed: %v\n", err)
	}
	
	return nil
}

func createScenarioConfig(projectDir, scenario string, scenarioConfig ScenarioConfig, environments []string) error {
	// Create main configuration
	cfg := &models.Config{
		Repositories: []models.Repository{
			{
				Name:     "main",
				GitURL:   "local",
				Branch:   "main",
				Path:     "migrations",
				Database: fmt.Sprintf("%s_DB", strings.ToUpper(scenario)),
				Schema:   scenarioConfig.Structure.Schemas[0],
			},
		},
		Environments: []models.Environment{},
	}
	
	// Add environments
	for _, env := range environments {
		isProduction := env == "prod" || env == "production"
		
		envConfig := models.Environment{
			Name:     env,
			Account:  "your-account.region",
			Username: fmt.Sprintf("%s_DEPLOY_USER", strings.ToUpper(env)),
			Password: fmt.Sprintf("${SNOWFLAKE_%s_PASSWORD}", strings.ToUpper(env)),
			Database: fmt.Sprintf("%s_%s_DB", strings.ToUpper(scenario), strings.ToUpper(env)),
			Schema:   scenarioConfig.Structure.Schemas[0],
			Warehouse: func() string {
				if isProduction {
					return "PROD_WH"
				}
				return "DEV_WH"
			}(),
			Role: fmt.Sprintf("%s_DEPLOY_ROLE", strings.ToUpper(env)),
		}
		
		cfg.Environments = append(cfg.Environments, envConfig)
	}
	
	// Set deployment configuration based on scenario
	cfg.Deployment = models.Deployment{
		Timeout:    "30m",
		MaxRetries: 3,
		BatchSize:  10,
		Parallel:   scenario == "data-lake" || scenario == "data-warehouse",
		DryRun:     true,
		Rollback: models.RollbackConfig{
			Enabled:         true,
			OnFailure:       true,
			BackupRetention: 30,
			Strategy:        "checkpoint",
		},
		Validation: models.ValidationConfig{
			Enabled:     true,
			SyntaxCheck: true,
			DryRun:      true,
		},
	}
	
	// Save configuration
	return config.Save(cfg)
}

func createScenarioDocs(projectDir, scenario string, config ScenarioConfig) error {
	// Create scenario-specific README
	readmePath := filepath.Join(projectDir, "README.md")
	readmeContent := fmt.Sprintf(`# %s Project

%s

## Architecture Overview

This project follows the %s pattern with the following schemas:
%s

## Quick Start

1. Set up your Snowflake credentials:
   %s

2. Deploy to development:
   %s

3. Run sample queries:
   %s

## Project Structure

%s

## Next Steps

- Review the example migrations in the migrations/ directory
- Customize the schemas for your specific needs
- Set up CI/CD pipelines for automated deployments
- Configure monitoring and alerting

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md)
- [Migration Guide](docs/MIGRATION_GUIDE.md)
- [Best Practices](docs/BEST_PRACTICES.md)
`, 
		strings.Title(strings.ReplaceAll(scenario, "-", " ")),
		config.Description,
		scenario,
		formatSchemaList(config.Structure.Schemas),
		getCredentialSetup(scenario),
		getDeployCommand(scenario),
		getSampleCommand(scenario),
		getProjectStructure(config),
	)
	
	return os.WriteFile(readmePath, []byte(readmeContent), 0600)
}

func processTemplateVars(content string, vars map[string]interface{}) string {
	result := content
	
	// Simple variable replacement
	result = strings.ReplaceAll(result, "{{.Database}}", vars["Database"].(string))
	result = strings.ReplaceAll(result, "{{.Schema}}", vars["Schema"].(string))
	result = strings.ReplaceAll(result, "{{.ProjectName}}", vars["ProjectName"].(string))
	result = strings.ReplaceAll(result, "{{.Date}}", time.Now().Format("2006-01-02"))
	
	return result
}

func formatSchemaList(schemas []string) string {
	var lines []string
	for _, schema := range schemas {
		lines = append(lines, fmt.Sprintf("- **%s**", schema))
	}
	return strings.Join(lines, "\n")
}

func getCredentialSetup(scenario string) string {
	return "```bash\nexport SNOWFLAKE_DEV_PASSWORD='your-password'\nexport SNOWFLAKE_STAGING_PASSWORD='your-password'\nexport SNOWFLAKE_PROD_PASSWORD='your-password'\n```"
}

func getDeployCommand(scenario string) string {
	return "```bash\nflakedrop deploy main --env dev\n```"
}

func getSampleCommand(scenario string) string {
	switch scenario {
	case "poc":
		return "```bash\nflakedrop run-script scripts/demo_queries.sql\n```"
	default:
		return "```bash\nflakedrop validate\n```"
	}
}

func getProjectStructure(config ScenarioConfig) string {
	var lines []string
	lines = append(lines, "```")
	lines = append(lines, ".")
	lines = append(lines, "├── configs/          # Configuration files")
	lines = append(lines, "├── migrations/       # SQL migration files")
	
	for folder := range config.Structure.Folders {
		lines = append(lines, fmt.Sprintf("│   ├── %s/", filepath.Base(folder)))
	}
	
	lines = append(lines, "├── scripts/          # Utility scripts")
	lines = append(lines, "├── tests/            # Test files")
	lines = append(lines, "└── docs/             # Documentation")
	lines = append(lines, "```")
	
	return strings.Join(lines, "\n")
}

func showQuickstartComplete(projectName, scenario string, config ScenarioConfig) {
	fmt.Println()
	fmt.Println("✅ Quickstart project created successfully!")
	fmt.Println()
	fmt.Printf("📁 Project: %s\n", projectName)
	fmt.Printf("🎯 Scenario: %s\n", scenario)
	fmt.Printf("📝 Description: %s\n", config.Description)
	fmt.Println()
	fmt.Println("🔧 Created schemas:")
	for _, schema := range config.Structure.Schemas {
		fmt.Printf("   - %s\n", schema)
	}
	fmt.Println()
	fmt.Println("📚 Next Steps:")
	fmt.Printf("1. cd %s\n", projectName)
	fmt.Println("2. Update configs/config.yaml with your Snowflake account details")
	fmt.Println("3. Set environment variables for passwords")
	fmt.Println("4. Review and customize the example migrations")
	fmt.Println("5. Deploy to development: flakedrop deploy main --env dev")
	fmt.Println()
	
	// Scenario-specific tips
	switch scenario {
	case "data-warehouse":
		fmt.Println("💡 Data Warehouse Tips:")
		fmt.Println("   - Start with staging layer transformations")
		fmt.Println("   - Build dimensions before facts")
		fmt.Println("   - Implement slowly changing dimensions (SCD)")
		fmt.Println("   - Set up regular ETL schedules")
	case "data-lake":
		fmt.Println("💡 Data Lake Tips:")
		fmt.Println("   - Configure external stages for data ingestion")
		fmt.Println("   - Set up Snowpipe for continuous loading")
		fmt.Println("   - Implement data quality checks in refined layer")
		fmt.Println("   - Use tasks for orchestration")
	case "analytics":
		fmt.Println("💡 Analytics Platform Tips:")
		fmt.Println("   - Create materialized views for performance")
		fmt.Println("   - Build KPI definitions as views")
		fmt.Println("   - Set up row-level security for multi-tenancy")
		fmt.Println("   - Document metric calculations")
	case "microservices":
		fmt.Println("💡 Microservices Tips:")
		fmt.Println("   - Keep service schemas isolated")
		fmt.Println("   - Use common schema for shared data")
		fmt.Println("   - Implement event sourcing patterns")
		fmt.Println("   - Set up service-specific roles")
	case "migration":
		fmt.Println("💡 Migration Tips:")
		fmt.Println("   - Run data quality checks before migration")
		fmt.Println("   - Use staging tables for validation")
		fmt.Println("   - Implement reconciliation reports")
		fmt.Println("   - Plan for incremental migrations")
	case "poc":
		fmt.Println("💡 POC Tips:")
		fmt.Println("   - Focus on core functionality")
		fmt.Println("   - Document assumptions and limitations")
		fmt.Println("   - Create demo scripts for stakeholders")
		fmt.Println("   - Plan for production transition")
	}
}

// Types

type ScenarioConfig struct {
	Template    string
	Description string
	Features    []string
	Structure   ProjectStructure
	Examples    map[string]string
}

type ProjectStructure struct {
	Schemas []string
	Folders map[string][]string
}

// Example content templates

const dataWarehouseStagingSchema = `-- Data Warehouse: Staging Layer
-- This layer contains raw data from source systems
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Example: Customer staging table
CREATE TABLE IF NOT EXISTS STG_CUSTOMERS (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    created_date VARCHAR(50),
    modified_date VARCHAR(50),
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Example: Orders staging table
CREATE TABLE IF NOT EXISTS STG_ORDERS (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date VARCHAR(50),
    ship_date VARCHAR(50),
    order_status VARCHAR(50),
    total_amount VARCHAR(50),
    currency_code VARCHAR(10),
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create tasks for regular data loads
CREATE TASK IF NOT EXISTS LOAD_STAGING_DATA
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
AS
  -- Add your data loading logic here
  SELECT 'Staging data load completed' as status;
`

const dataWarehouseCoreSchema = `-- Data Warehouse: Core Layer
-- This layer contains cleansed and conformed dimensions and facts
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS DW_CORE;
USE SCHEMA DW_CORE;

-- Dimension: Customer (Type 2 SCD)
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    customer_key INTEGER AUTOINCREMENT,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_key)
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key INTEGER,
    date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    PRIMARY KEY (date_key)
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    order_key INTEGER AUTOINCREMENT,
    order_id VARCHAR(50) NOT NULL,
    customer_key INTEGER,
    order_date_key INTEGER,
    ship_date_key INTEGER,
    order_status VARCHAR(50),
    total_amount DECIMAL(18,2),
    currency_code VARCHAR(10),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_key),
    FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMER(customer_key),
    FOREIGN KEY (order_date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (ship_date_key) REFERENCES DIM_DATE(date_key)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON FACT_ORDERS(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON FACT_ORDERS(order_date_key);
`

const dataWarehouseMartsSchema = `-- Data Warehouse: Data Marts Layer
-- This layer contains business-specific data marts
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS DW_MARTS;
USE SCHEMA DW_MARTS;

-- Sales Mart: Customer Order Summary
CREATE OR REPLACE VIEW CUSTOMER_ORDER_SUMMARY AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_amount) as lifetime_value,
    AVG(f.total_amount) as avg_order_value,
    MIN(d.date) as first_order_date,
    MAX(d.date) as last_order_date,
    DATEDIFF('day', MAX(d.date), CURRENT_DATE()) as days_since_last_order
FROM {{.Database}}.DW_CORE.FACT_ORDERS f
JOIN {{.Database}}.DW_CORE.DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN {{.Database}}.DW_CORE.DIM_DATE d ON f.order_date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY 1,2,3,4;

-- Sales Mart: Monthly Sales Trends
CREATE OR REPLACE VIEW MONTHLY_SALES_TRENDS AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) as order_count,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month) as prev_month_revenue,
    (SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month)) / 
        NULLIF(LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month), 0) * 100 as month_over_month_growth
FROM {{.Database}}.DW_CORE.FACT_ORDERS f
JOIN {{.Database}}.DW_CORE.DIM_DATE d ON f.order_date_key = d.date_key
GROUP BY 1,2,3
ORDER BY 1,2;
`

const dataLakeRawSchema = `-- Data Lake: Raw Layer
-- This layer stores data in its original format
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- External table for JSON data
CREATE OR REPLACE EXTERNAL TABLE RAW_EVENTS (
    event_data VARIANT,
    filename VARCHAR(255) AS (METADATA$FILENAME),
    file_row_number INTEGER AS (METADATA$FILE_ROW_NUMBER),
    load_timestamp TIMESTAMP_NTZ AS (CURRENT_TIMESTAMP())
)
WITH LOCATION = @{{.Database}}.PUBLIC.S3_STAGE/events/
FILE_FORMAT = (TYPE = JSON);

-- External table for CSV data
CREATE OR REPLACE EXTERNAL TABLE RAW_TRANSACTIONS (
    transaction_data VARIANT,
    filename VARCHAR(255) AS (METADATA$FILENAME),
    file_row_number INTEGER AS (METADATA$FILE_ROW_NUMBER),
    load_timestamp TIMESTAMP_NTZ AS (CURRENT_TIMESTAMP())
)
WITH LOCATION = @{{.Database}}.PUBLIC.S3_STAGE/transactions/
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Table for streaming data
CREATE TABLE IF NOT EXISTS RAW_CLICKSTREAM (
    event_id VARCHAR(100),
    user_id VARCHAR(100),
    session_id VARCHAR(100),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP_NTZ,
    page_url VARCHAR(1000),
    referrer_url VARCHAR(1000),
    user_agent VARCHAR(500),
    ip_address VARCHAR(50),
    event_properties VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create stream on raw data for CDC
CREATE STREAM IF NOT EXISTS RAW_CLICKSTREAM_STREAM ON TABLE RAW_CLICKSTREAM;
`

const dataLakeStages = `-- Data Lake: External Stages
-- Configure external stages for data ingestion
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA PUBLIC;

-- S3 Stage
CREATE OR REPLACE STAGE S3_STAGE
    URL = 's3://your-bucket-name/data/'
    CREDENTIALS = (AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}')
    FILE_FORMAT = (TYPE = JSON);

-- Azure Stage
CREATE OR REPLACE STAGE AZURE_STAGE
    URL = 'azure://your-account.blob.core.windows.net/your-container/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '${AZURE_SAS_TOKEN}')
    FILE_FORMAT = (TYPE = CSV);

-- File Formats
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = JSON
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    COMPRESSION = GZIP;

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY;
`

const dataLakePipes = `-- Data Lake: Snowpipe for Continuous Ingestion
-- Configure pipes for automatic data loading
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA RAW;

-- Pipe for JSON events
CREATE OR REPLACE PIPE EVENTS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_EVENTS
FROM @{{.Database}}.PUBLIC.S3_STAGE/events/
FILE_FORMAT = (FORMAT_NAME = {{.Database}}.PUBLIC.JSON_FORMAT)
ON_ERROR = 'CONTINUE';

-- Pipe for clickstream data
CREATE OR REPLACE PIPE CLICKSTREAM_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_CLICKSTREAM (
    event_id,
    user_id,
    session_id,
    event_type,
    event_timestamp,
    page_url,
    referrer_url,
    user_agent,
    ip_address,
    event_properties
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:user_id::VARCHAR,
        $1:session_id::VARCHAR,
        $1:event_type::VARCHAR,
        $1:event_timestamp::TIMESTAMP_NTZ,
        $1:page_url::VARCHAR,
        $1:referrer_url::VARCHAR,
        $1:user_agent::VARCHAR,
        $1:ip_address::VARCHAR,
        $1:properties::VARIANT
    FROM @{{.Database}}.PUBLIC.S3_STAGE/clickstream/
)
FILE_FORMAT = (FORMAT_NAME = {{.Database}}.PUBLIC.JSON_FORMAT)
ON_ERROR = 'CONTINUE';

-- Get pipe status
SELECT SYSTEM$PIPE_STATUS('EVENTS_PIPE');
SELECT SYSTEM$PIPE_STATUS('CLICKSTREAM_PIPE');
`

const analyticsSchema = `-- Analytics Platform: Core Schema
-- Foundation for analytics and reporting
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- User behavior analysis
CREATE TABLE IF NOT EXISTS USER_SESSIONS (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100),
    session_start TIMESTAMP_NTZ,
    session_end TIMESTAMP_NTZ,
    duration_seconds INTEGER,
    page_views INTEGER,
    events_count INTEGER,
    bounce BOOLEAN,
    conversion BOOLEAN,
    device_type VARCHAR(50),
    browser VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product analytics
CREATE TABLE IF NOT EXISTS PRODUCT_METRICS (
    date DATE,
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    views INTEGER,
    add_to_carts INTEGER,
    purchases INTEGER,
    revenue DECIMAL(18,2),
    conversion_rate DECIMAL(5,4),
    PRIMARY KEY (date, product_id)
);

-- Customer segments
CREATE TABLE IF NOT EXISTS CUSTOMER_SEGMENTS (
    customer_id VARCHAR(100) PRIMARY KEY,
    segment_name VARCHAR(100),
    segment_value VARCHAR(100),
    lifetime_value DECIMAL(18,2),
    churn_risk_score DECIMAL(3,2),
    last_activity_date DATE,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- A/B test results
CREATE TABLE IF NOT EXISTS AB_TEST_RESULTS (
    test_id VARCHAR(100),
    variant VARCHAR(50),
    user_id VARCHAR(100),
    exposure_timestamp TIMESTAMP_NTZ,
    conversion BOOLEAN,
    revenue DECIMAL(18,2),
    PRIMARY KEY (test_id, user_id)
);
`

const analyticsViews = `-- Analytics Platform: Business Metrics Views
-- Key business metrics and KPIs
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA ANALYTICS;

-- Daily Active Users (DAU)
CREATE OR REPLACE VIEW DAILY_ACTIVE_USERS AS
SELECT 
    DATE(session_start) as date,
    COUNT(DISTINCT user_id) as dau,
    COUNT(DISTINCT session_id) as total_sessions,
    AVG(duration_seconds) as avg_session_duration,
    SUM(CASE WHEN bounce = TRUE THEN 1 ELSE 0 END) / COUNT(*) as bounce_rate
FROM USER_SESSIONS
WHERE session_start >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

-- Product Performance Dashboard
CREATE OR REPLACE VIEW PRODUCT_PERFORMANCE AS
SELECT 
    p.date,
    p.product_id,
    p.product_name,
    p.views,
    p.add_to_carts,
    p.purchases,
    p.revenue,
    p.conversion_rate,
    SUM(p.revenue) OVER (
        PARTITION BY p.product_id 
        ORDER BY p.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as revenue_7day_rolling,
    RANK() OVER (
        PARTITION BY p.date 
        ORDER BY p.revenue DESC
    ) as daily_revenue_rank
FROM PRODUCT_METRICS p
WHERE p.date >= DATEADD('day', -30, CURRENT_DATE());

-- Customer Lifetime Value Cohorts
CREATE OR REPLACE VIEW CUSTOMER_LTV_COHORTS AS
SELECT 
    DATE_TRUNC('month', first_purchase_date) as cohort_month,
    COUNT(DISTINCT customer_id) as customers,
    AVG(lifetime_value) as avg_ltv,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) as median_ltv,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lifetime_value) as p90_ltv,
    SUM(lifetime_value) as total_ltv
FROM (
    SELECT 
        customer_id,
        MIN(purchase_date) as first_purchase_date,
        SUM(revenue) as lifetime_value
    FROM orders
    GROUP BY 1
) cohorts
GROUP BY 1
ORDER BY 1;

-- A/B Test Summary
CREATE OR REPLACE VIEW AB_TEST_SUMMARY AS
SELECT 
    test_id,
    variant,
    COUNT(DISTINCT user_id) as users,
    SUM(CASE WHEN conversion = TRUE THEN 1 ELSE 0 END) as conversions,
    SUM(CASE WHEN conversion = TRUE THEN 1 ELSE 0 END) / COUNT(*) as conversion_rate,
    AVG(revenue) as avg_revenue_per_user,
    SUM(revenue) as total_revenue,
    -- Statistical significance (simplified)
    CASE 
        WHEN COUNT(*) > 100 AND 
             ABS(SUM(CASE WHEN conversion = TRUE THEN 1 ELSE 0 END) / COUNT(*) - 
                 LAG(SUM(CASE WHEN conversion = TRUE THEN 1 ELSE 0 END) / COUNT(*)) 
                 OVER (PARTITION BY test_id ORDER BY variant)) > 0.05
        THEN 'Significant'
        ELSE 'Not Significant'
    END as significance
FROM AB_TEST_RESULTS
GROUP BY 1, 2;
`

const microservicesUserSchema = `-- Microservices: User Service Schema
-- Schema for user service data
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS SERVICE_USER;
USE SCHEMA SERVICE_USER;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    version INTEGER DEFAULT 1
);

-- User profiles
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id VARCHAR(100) PRIMARY KEY REFERENCES users(user_id),
    bio TEXT,
    avatar_url VARCHAR(500),
    preferences VARIANT,
    metadata VARIANT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Authentication tokens
CREATE TABLE IF NOT EXISTS auth_tokens (
    token_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) REFERENCES users(user_id),
    token_hash VARCHAR(255) NOT NULL,
    token_type VARCHAR(20),
    expires_at TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Audit log
CREATE TABLE IF NOT EXISTS user_audit_log (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    user_id VARCHAR(100),
    action VARCHAR(50),
    details VARIANT,
    ip_address VARCHAR(50),
    user_agent VARCHAR(500),
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create service-specific role
CREATE ROLE IF NOT EXISTS USER_SERVICE_ROLE;
GRANT USAGE ON SCHEMA SERVICE_USER TO ROLE USER_SERVICE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SERVICE_USER TO ROLE USER_SERVICE_ROLE;
`

const microservicesOrderSchema = `-- Microservices: Order Service Schema
-- Schema for order service data
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS SERVICE_ORDER;
USE SCHEMA SERVICE_ORDER;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(18,2),
    currency VARCHAR(3) DEFAULT 'USD',
    shipping_address VARIANT,
    billing_address VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    version INTEGER DEFAULT 1
);

-- Order items
CREATE TABLE IF NOT EXISTS order_items (
    item_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES orders(order_id),
    product_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(18,2),
    discount_amount DECIMAL(18,2) DEFAULT 0,
    tax_amount DECIMAL(18,2) DEFAULT 0,
    total_amount DECIMAL(18,2)
);

-- Order events (Event Sourcing)
CREATE TABLE IF NOT EXISTS order_events (
    event_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data VARIANT NOT NULL,
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    INDEX idx_order_events_order (order_id),
    INDEX idx_order_events_timestamp (event_timestamp)
);

-- Payment information
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) REFERENCES orders(order_id),
    payment_method VARCHAR(50),
    amount DECIMAL(18,2),
    status VARCHAR(20),
    transaction_id VARCHAR(100),
    processed_at TIMESTAMP_NTZ
);

-- Create service-specific role
CREATE ROLE IF NOT EXISTS ORDER_SERVICE_ROLE;
GRANT USAGE ON SCHEMA SERVICE_ORDER TO ROLE ORDER_SERVICE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SERVICE_ORDER TO ROLE ORDER_SERVICE_ROLE;
`

const microservicesCommonSchema = `-- Microservices: Common/Shared Schema
-- Shared data and reference tables
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS SERVICE_COMMON;
USE SCHEMA SERVICE_COMMON;

-- Products catalog (read-only for services)
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(100) PRIMARY KEY,
    sku VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(18,2),
    currency VARCHAR(3) DEFAULT 'USD',
    inventory_count INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Service registry
CREATE TABLE IF NOT EXISTS service_registry (
    service_name VARCHAR(100) PRIMARY KEY,
    service_version VARCHAR(20),
    endpoint_url VARCHAR(500),
    health_check_url VARCHAR(500),
    status VARCHAR(20) DEFAULT 'active',
    last_heartbeat TIMESTAMP_NTZ,
    metadata VARIANT
);

-- Event bus (for inter-service communication)
CREATE TABLE IF NOT EXISTS event_bus (
    event_id VARCHAR(100) PRIMARY KEY,
    source_service VARCHAR(100),
    target_service VARCHAR(100),
    event_type VARCHAR(100),
    payload VARIANT,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP_NTZ,
    INDEX idx_event_bus_status_created (status, created_at)
);

-- Configuration store
CREATE TABLE IF NOT EXISTS service_config (
    service_name VARCHAR(100),
    config_key VARCHAR(255),
    config_value VARIANT,
    environment VARCHAR(20),
    version INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (service_name, config_key, environment)
);

-- Grant read access to all services
CREATE ROLE IF NOT EXISTS SERVICE_READER_ROLE;
GRANT USAGE ON SCHEMA SERVICE_COMMON TO ROLE SERVICE_READER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SERVICE_COMMON TO ROLE SERVICE_READER_ROLE;

-- Grant write access for event bus
GRANT INSERT ON TABLE event_bus TO ROLE USER_SERVICE_ROLE;
GRANT INSERT ON TABLE event_bus TO ROLE ORDER_SERVICE_ROLE;
`

const migrationStagingSchema = `-- Database Migration: Staging Schema
-- Temporary staging area for migration
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS MIGRATION_STAGING;
USE SCHEMA MIGRATION_STAGING;

-- Source system metadata
CREATE TABLE IF NOT EXISTS source_tables (
    table_name VARCHAR(255) PRIMARY KEY,
    source_system VARCHAR(100),
    row_count INTEGER,
    last_modified TIMESTAMP_NTZ,
    migration_status VARCHAR(20) DEFAULT 'pending',
    migration_started TIMESTAMP_NTZ,
    migration_completed TIMESTAMP_NTZ
);

-- Migration control table
CREATE TABLE IF NOT EXISTS migration_control (
    batch_id INTEGER AUTOINCREMENT PRIMARY KEY,
    table_name VARCHAR(255),
    start_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    end_timestamp TIMESTAMP_NTZ,
    rows_migrated INTEGER,
    status VARCHAR(20),
    error_message VARCHAR(1000)
);

-- Data quality issues
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id INTEGER AUTOINCREMENT PRIMARY KEY,
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    issue_type VARCHAR(100),
    issue_description TEXT,
    row_count INTEGER,
    sample_data VARIANT,
    discovered_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create dynamic staging tables based on source
CREATE OR REPLACE PROCEDURE create_staging_table(source_table_name VARCHAR, columns_definition VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    LET sql_stmt := 'CREATE TABLE IF NOT EXISTS STG_' || source_table_name || ' (' || columns_definition || 
                    ', _migration_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())';
    EXECUTE IMMEDIATE :sql_stmt;
    RETURN 'Staging table created: STG_' || source_table_name;
END;
$$;
`

const migrationQualityChecks = `-- Database Migration: Data Quality Checks
-- Validate data before and after migration
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA MIGRATION_STAGING;

-- Check for NULL values in required fields
CREATE OR REPLACE PROCEDURE check_required_fields(table_name VARCHAR, required_columns ARRAY)
RETURNS TABLE (column_name VARCHAR, null_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    col_name VARCHAR;
    result_set RESULTSET;
BEGIN
    LET query := '';
    FOR i IN 0 TO ARRAY_SIZE(required_columns)-1 DO
        col_name := required_columns[i];
        IF (i > 0) THEN
            query := query || ' UNION ALL ';
        END IF;
        query := query || 'SELECT ''' || col_name || ''' as column_name, COUNT(*) as null_count FROM ' || 
                 table_name || ' WHERE ' || col_name || ' IS NULL';
    END FOR;
    
    result_set := (EXECUTE IMMEDIATE :query);
    RETURN TABLE(result_set);
END;
$$;

-- Check for duplicate records
CREATE OR REPLACE PROCEDURE check_duplicates(table_name VARCHAR, key_columns ARRAY)
RETURNS TABLE (duplicate_keys VARCHAR, record_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    key_list VARCHAR;
BEGIN
    key_list := ARRAY_TO_STRING(key_columns, ', ');
    
    LET query := 'SELECT ' || key_list || ' as duplicate_keys, COUNT(*) as record_count ' ||
                 'FROM ' || table_name || ' ' ||
                 'GROUP BY ' || key_list || ' ' ||
                 'HAVING COUNT(*) > 1';
    
    RETURN TABLE(EXECUTE IMMEDIATE :query);
END;
$$;

-- Data type validation
CREATE OR REPLACE PROCEDURE validate_data_types(staging_table VARCHAR, target_table VARCHAR)
RETURNS TABLE (column_name VARCHAR, data_type_mismatch BOOLEAN)
LANGUAGE SQL
AS
$$
BEGIN
    RETURN TABLE(
        SELECT 
            s.column_name,
            CASE WHEN s.data_type != t.data_type THEN TRUE ELSE FALSE END as data_type_mismatch
        FROM INFORMATION_SCHEMA.COLUMNS s
        JOIN INFORMATION_SCHEMA.COLUMNS t
            ON s.column_name = t.column_name
        WHERE s.table_schema = 'MIGRATION_STAGING'
            AND s.table_name = :staging_table
            AND t.table_schema = 'MIGRATION_TARGET'
            AND t.table_name = :target_table
    );
END;
$$;

-- Row count reconciliation
CREATE OR REPLACE PROCEDURE reconcile_row_counts()
RETURNS TABLE (table_name VARCHAR, source_count INTEGER, target_count INTEGER, difference INTEGER, match_percentage DECIMAL(5,2))
LANGUAGE SQL
AS
$$
BEGIN
    RETURN TABLE(
        SELECT 
            s.table_name,
            s.row_count as source_count,
            t.row_count as target_count,
            s.row_count - t.row_count as difference,
            CASE 
                WHEN s.row_count = 0 THEN 0
                ELSE (t.row_count::DECIMAL / s.row_count::DECIMAL) * 100
            END as match_percentage
        FROM (
            SELECT table_name, row_count 
            FROM source_tables
        ) s
        LEFT JOIN (
            SELECT 
                REPLACE(table_name, 'STG_', '') as table_name,
                ROW_COUNT as row_count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'MIGRATION_TARGET'
        ) t ON s.table_name = t.table_name
    );
END;
$$;
`

const migrationReconciliation = `-- Database Migration: Reconciliation Reports
-- Compare source and target data
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA MIGRATION_STAGING;

-- Create reconciliation summary view
CREATE OR REPLACE VIEW MIGRATION_RECONCILIATION_SUMMARY AS
SELECT 
    mc.table_name,
    mc.status,
    mc.rows_migrated,
    st.row_count as source_rows,
    mc.rows_migrated - st.row_count as row_difference,
    CASE 
        WHEN st.row_count = 0 THEN 100
        ELSE ROUND((mc.rows_migrated::DECIMAL / st.row_count) * 100, 2)
    END as completion_percentage,
    mc.start_timestamp,
    mc.end_timestamp,
    TIMESTAMPDIFF('minute', mc.start_timestamp, mc.end_timestamp) as duration_minutes
FROM migration_control mc
JOIN source_tables st ON mc.table_name = st.table_name
WHERE mc.batch_id = (
    SELECT MAX(batch_id) 
    FROM migration_control mc2 
    WHERE mc2.table_name = mc.table_name
);

-- Data quality summary
CREATE OR REPLACE VIEW DATA_QUALITY_SUMMARY AS
SELECT 
    table_name,
    COUNT(DISTINCT issue_type) as issue_types,
    COUNT(*) as total_issues,
    SUM(row_count) as affected_rows,
    MAX(discovered_at) as last_discovered
FROM data_quality_issues
GROUP BY table_name;

-- Migration progress dashboard
CREATE OR REPLACE VIEW MIGRATION_PROGRESS AS
SELECT 
    COUNT(DISTINCT table_name) as total_tables,
    COUNT(DISTINCT CASE WHEN migration_status = 'completed' THEN table_name END) as completed_tables,
    COUNT(DISTINCT CASE WHEN migration_status = 'in_progress' THEN table_name END) as in_progress_tables,
    COUNT(DISTINCT CASE WHEN migration_status = 'pending' THEN table_name END) as pending_tables,
    SUM(CASE WHEN migration_status = 'completed' THEN row_count ELSE 0 END) as total_rows_migrated,
    MIN(migration_started) as migration_start_time,
    MAX(migration_completed) as last_completion_time
FROM source_tables;

-- Generate reconciliation report
CREATE OR REPLACE PROCEDURE generate_reconciliation_report()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    report_date VARCHAR;
BEGIN
    report_date := TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD');
    
    -- Create report table
    CREATE TABLE IF NOT EXISTS reconciliation_reports AS
    SELECT 
        :report_date as report_date,
        mrs.*,
        dqs.total_issues,
        dqs.affected_rows as quality_affected_rows
    FROM MIGRATION_RECONCILIATION_SUMMARY mrs
    LEFT JOIN DATA_QUALITY_SUMMARY dqs ON mrs.table_name = dqs.table_name;
    
    RETURN 'Reconciliation report generated for ' || report_date;
END;
$$;
`

const pocDemoSchema = `-- POC Demo: Initial Schema
-- Quick setup for proof of concept
-- Date: {{.Date}}

USE DATABASE {{.Database}};
CREATE SCHEMA IF NOT EXISTS POC_DEMO;
USE SCHEMA POC_DEMO;

-- Simple customer table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Simple product table
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER AUTOINCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100)
);

-- Simple orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    total_amount DECIMAL(10,2)
);

-- Order details
CREATE TABLE IF NOT EXISTS order_items (
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Simple view for reporting
CREATE OR REPLACE VIEW order_summary AS
SELECT 
    o.order_id,
    c.name as customer_name,
    c.email,
    o.order_date,
    o.total_amount,
    COUNT(oi.product_id) as item_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY 1,2,3,4,5;
`

const pocSampleData = `-- POC Demo: Sample Data
-- Load sample data for demonstration
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA POC_DEMO;

-- Insert sample customers
INSERT INTO customers (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('Diana Prince', 'diana@example.com'),
    ('Eve Davis', 'eve@example.com');

-- Insert sample products  
INSERT INTO products (name, price, category) VALUES
    ('Laptop', 999.99, 'Electronics'),
    ('Mouse', 29.99, 'Electronics'),
    ('Keyboard', 79.99, 'Electronics'),
    ('Monitor', 299.99, 'Electronics'),
    ('Desk Chair', 199.99, 'Furniture'),
    ('Standing Desk', 499.99, 'Furniture'),
    ('Notebook', 9.99, 'Office Supplies'),
    ('Pen Set', 19.99, 'Office Supplies');

-- Insert sample orders
INSERT INTO orders (customer_id, total_amount) VALUES
    (1, 1029.98),
    (2, 579.98),
    (3, 309.98),
    (1, 219.98),
    (4, 1299.97);

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 999.99),
    (1, 2, 1, 29.99),
    (2, 5, 1, 199.99),
    (2, 6, 1, 499.99),
    (2, 7, 8, 9.99),
    (3, 4, 1, 299.99),
    (3, 8, 1, 19.99),
    (4, 5, 1, 199.99),
    (4, 8, 1, 19.99),
    (5, 1, 1, 999.99),
    (5, 4, 1, 299.99);

-- Verify data load
SELECT 'Customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items;
`

const pocDemoQueries = `-- POC Demo: Sample Queries
-- Demonstrate key functionality
-- Date: {{.Date}}

USE DATABASE {{.Database}};
USE SCHEMA POC_DEMO;

-- 1. Top customers by revenue
SELECT 
    c.name,
    c.email,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(o.total_amount) as total_revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
ORDER BY 4 DESC;

-- 2. Product sales analysis
SELECT 
    p.name as product_name,
    p.category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY 1, 2
ORDER BY 4 DESC;

-- 3. Recent orders with details
SELECT 
    o.order_id,
    c.name as customer,
    o.order_date,
    o.total_amount,
    LISTAGG(p.name, ', ') WITHIN GROUP (ORDER BY p.name) as products
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY 1, 2, 3, 4
ORDER BY o.order_date DESC
LIMIT 10;

-- 4. Category performance
SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) as product_count,
    COUNT(DISTINCT o.order_id) as orders_with_category,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.quantity * oi.unit_price) as category_revenue
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id
GROUP BY 1
ORDER BY 5 DESC;

-- 5. Customer order patterns
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.name,
        COUNT(DISTINCT o.order_id) as order_count,
        AVG(o.total_amount) as avg_order_value,
        DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) as customer_lifetime_days
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 1, 2
)
SELECT 
    name,
    order_count,
    ROUND(avg_order_value, 2) as avg_order_value,
    customer_lifetime_days,
    CASE 
        WHEN order_count >= 2 THEN 'Repeat Customer'
        WHEN order_count = 1 THEN 'Single Purchase'
        ELSE 'No Purchase'
    END as customer_type
FROM customer_metrics
ORDER BY order_count DESC, avg_order_value DESC;
`