package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"
)

// TemplateRegistry manages migration templates
type TemplateRegistry struct {
	templates map[string]*MigrationTemplate
	categories map[string][]string
}

// NewTemplateRegistry creates a new template registry
func NewTemplateRegistry() *TemplateRegistry {
	r := &TemplateRegistry{
		templates:  make(map[string]*MigrationTemplate),
		categories: make(map[string][]string),
	}

	// Register built-in templates
	r.registerBuiltInTemplates()

	return r
}

// RegisterTemplate registers a new migration template
func (tr *TemplateRegistry) RegisterTemplate(template *MigrationTemplate) error {
	if template.ID == "" {
		return fmt.Errorf("template ID cannot be empty")
	}
	if template.Name == "" {
		return fmt.Errorf("template name cannot be empty")
	}

	tr.templates[template.ID] = template

	// Add to category
	if template.Category != "" {
		if _, exists := tr.categories[template.Category]; !exists {
			tr.categories[template.Category] = []string{}
		}
		tr.categories[template.Category] = append(tr.categories[template.Category], template.ID)
	}

	return nil
}

// GetTemplate retrieves a template by ID
func (tr *TemplateRegistry) GetTemplate(templateID string) (*MigrationTemplate, error) {
	template, exists := tr.templates[templateID]
	if !exists {
		return nil, fmt.Errorf("template %s not found", templateID)
	}
	return template, nil
}

// ListTemplates returns all registered templates
func (tr *TemplateRegistry) ListTemplates() []*MigrationTemplate {
	templates := make([]*MigrationTemplate, 0, len(tr.templates))
	for _, template := range tr.templates {
		templates = append(templates, template)
	}
	return templates
}

// ListTemplatesByCategory returns templates in a specific category
func (tr *TemplateRegistry) ListTemplatesByCategory(category string) []*MigrationTemplate {
	templateIDs, exists := tr.categories[category]
	if !exists {
		return []*MigrationTemplate{}
	}

	templates := make([]*MigrationTemplate, 0, len(templateIDs))
	for _, id := range templateIDs {
		if template, exists := tr.templates[id]; exists {
			templates = append(templates, template)
		}
	}
	return templates
}

// GetCategories returns all template categories
func (tr *TemplateRegistry) GetCategories() []string {
	categories := make([]string, 0, len(tr.categories))
	for category := range tr.categories {
		categories = append(categories, category)
	}
	return categories
}

// ApplyTemplate applies a template with parameters
func (tr *TemplateRegistry) ApplyTemplate(ctx context.Context, templateID string, parameters map[string]interface{}) (*MigrationPlan, error) {
	migrationTemplate, err := tr.GetTemplate(templateID)
	if err != nil {
		return nil, err
	}

	// Validate parameters
	if err := tr.validateParameters(migrationTemplate, parameters); err != nil {
		return nil, fmt.Errorf("parameter validation failed: %w", err)
	}

	// Apply template
	plan, err := tr.renderTemplate(migrationTemplate, parameters)
	if err != nil {
		return nil, fmt.Errorf("template rendering failed: %w", err)
	}

	return plan, nil
}

// CreateTemplate creates a new template from a migration plan
func (tr *TemplateRegistry) CreateTemplate(plan *MigrationPlan, templateInfo TemplateInfo) (*MigrationTemplate, error) {
	template := &MigrationTemplate{
		ID:          generateID("template"),
		Name:        templateInfo.Name,
		Description: templateInfo.Description,
		Category:    templateInfo.Category,
		Version:     "1.0",
		Type:        plan.Type,
		Strategy:    plan.Strategy,
		Parameters:  []TemplateParameter{},
		Validations: plan.Validations,
		Tags:        templateInfo.Tags,
		CreatedAt:   time.Now(),
		CreatedBy:   templateInfo.CreatedBy,
	}

	// Extract parameters from plan
	parameters, err := tr.extractParameters(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to extract parameters: %w", err)
	}
	template.Parameters = parameters

	// Create plan template
	planTemplate, err := tr.createPlanTemplate(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to create plan template: %w", err)
	}
	template.PlanTemplate = planTemplate

	// Create script templates
	scriptTemplates, err := tr.createScriptTemplates(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to create script templates: %w", err)
	}
	template.ScriptTemplates = scriptTemplates

	// Register the template
	if err := tr.RegisterTemplate(template); err != nil {
		return nil, fmt.Errorf("failed to register template: %w", err)
	}

	return template, nil
}

// Private methods

func (tr *TemplateRegistry) registerBuiltInTemplates() {
	// Register common migration templates

	// Full table copy template
	tr.RegisterTemplate(&MigrationTemplate{
		ID:          "full-table-copy",
		Name:        "Full Table Copy",
		Description: "Copy all data from source table to target table",
		Category:    "basic",
		Version:     "1.0",
		Type:        MigrationTypeData,
		Strategy:    MigrationStrategyBatch,
		Parameters: []TemplateParameter{
			{
				Name:         "source_table",
				Type:         "string",
				Description:  "Source table name",
				Required:     true,
			},
			{
				Name:         "target_table",
				Type:         "string",
				Description:  "Target table name",
				Required:     true,
			},
			{
				Name:         "batch_size",
				Type:         "integer",
				Description:  "Batch size for processing",
				Required:     false,
				DefaultValue: 10000,
			},
		},
		PlanTemplate: `{
			"name": "{{.name}}",
			"description": "Full copy of {{.source_table}} to {{.target_table}}",
			"type": "DATA",
			"strategy": "BATCH",
			"tables": [{
				"sourceTable": "{{.source_table}}",
				"targetTable": "{{.target_table}}",
				"migrationMode": "INSERT",
				"batchSize": {{.batch_size}}
			}]
		}`,
		Validations: []ValidationRule{
			{
				ID:       "row-count-validation",
				Name:     "Row Count Validation",
				Type:     ValidationTypeRowCount,
				Severity: ValidationSeverityError,
				Enabled:  true,
			},
		},
		Tags:      []string{"basic", "copy", "data"},
		CreatedAt: time.Now(),
		CreatedBy: "system",
	})

	// Incremental sync template
	tr.RegisterTemplate(&MigrationTemplate{
		ID:          "incremental-sync",
		Name:        "Incremental Data Sync",
		Description: "Sync only new or modified records based on timestamp",
		Category:    "sync",
		Version:     "1.0",
		Type:        MigrationTypeData,
		Strategy:    MigrationStrategyIncremental,
		Parameters: []TemplateParameter{
			{
				Name:        "source_table",
				Type:        "string",
				Description: "Source table name",
				Required:    true,
			},
			{
				Name:        "target_table",
				Type:        "string",
				Description: "Target table name",
				Required:    true,
			},
			{
				Name:        "timestamp_column",
				Type:        "string",
				Description: "Column used for incremental sync",
				Required:    true,
			},
			{
				Name:         "last_sync_time",
				Type:         "string",
				Description:  "Last synchronization timestamp",
				Required:     false,
				DefaultValue: "1970-01-01 00:00:00",
			},
		},
		PlanTemplate: `{
			"name": "{{.name}}",
			"description": "Incremental sync of {{.source_table}} to {{.target_table}}",
			"type": "DATA",
			"strategy": "INCREMENTAL",
			"tables": [{
				"sourceTable": "{{.source_table}}",
				"targetTable": "{{.target_table}}",
				"migrationMode": "UPSERT",
				"filters": [{
					"name": "incremental_filter",
					"expression": "{{.timestamp_column}} > '{{.last_sync_time}}'",
					"enabled": true
				}]
			}]
		}`,
		Tags:      []string{"sync", "incremental", "data"},
		CreatedAt: time.Now(),
		CreatedBy: "system",
	})

	// Data transformation template
	tr.RegisterTemplate(&MigrationTemplate{
		ID:          "data-transformation",
		Name:        "Data Transformation",
		Description: "Migrate data with transformations and validation",
		Category:    "transform",
		Version:     "1.0",
		Type:        MigrationTypeTransform,
		Strategy:    MigrationStrategyBatch,
		Parameters: []TemplateParameter{
			{
				Name:        "source_table",
				Type:        "string",
				Description: "Source table name",
				Required:    true,
			},
			{
				Name:        "target_table",
				Type:        "string",
				Description: "Target table name",
				Required:    true,
			},
			{
				Name:        "transformations",
				Type:        "array",
				Description: "List of transformations to apply",
				Required:    false,
				DefaultValue: []interface{}{},
			},
		},
		Validations: []ValidationRule{
			{
				ID:       "data-quality-validation",
				Name:     "Data Quality Validation",
				Type:     ValidationTypeDataQuality,
				Severity: ValidationSeverityWarning,
				Enabled:  true,
			},
		},
		Tags:      []string{"transform", "validation", "data"},
		CreatedAt: time.Now(),
		CreatedBy: "system",
	})

	// Large table streaming template
	tr.RegisterTemplate(&MigrationTemplate{
		ID:          "large-table-streaming",
		Name:        "Large Table Streaming",
		Description: "Stream large tables with optimized performance",
		Category:    "performance",
		Version:     "1.0",
		Type:        MigrationTypeData,
		Strategy:    MigrationStrategyStream,
		Parameters: []TemplateParameter{
			{
				Name:        "source_table",
				Type:        "string",
				Description: "Source table name",
				Required:    true,
			},
			{
				Name:        "target_table",
				Type:        "string",
				Description: "Target table name",
				Required:    true,
			},
			{
				Name:         "chunk_size", 
				Type:         "integer",
				Description:  "Size of data chunks for streaming",
				Required:     false,
				DefaultValue: 50000,
			},
			{
				Name:         "parallelism",
				Type:         "integer",
				Description:  "Number of parallel streams",
				Required:     false,
				DefaultValue: 4,
			},
		},
		PlanTemplate: `{
			"name": "{{.name}}",
			"description": "Streaming migration of {{.source_table}} to {{.target_table}}",
			"type": "DATA",
			"strategy": "STREAM",
			"tables": [{
				"sourceTable": "{{.source_table}}",
				"targetTable": "{{.target_table}}",
				"migrationMode": "INSERT",
				"batchSize": {{.chunk_size}},
				"parallelism": {{.parallelism}}
			}],
			"configuration": {
				"enableCheckpoints": true,
				"checkpointInterval": "5m"
			}
		}`,
		Tags:      []string{"performance", "streaming", "large"},
		CreatedAt: time.Now(),
		CreatedBy: "system",
	})
}

func (tr *TemplateRegistry) validateParameters(template *MigrationTemplate, parameters map[string]interface{}) error {
	for _, param := range template.Parameters {
		value, exists := parameters[param.Name]

		// Check required parameters
		if param.Required && !exists {
			return fmt.Errorf("required parameter %s is missing", param.Name)
		}

		// Use default value if not provided
		if !exists && param.DefaultValue != nil {
			parameters[param.Name] = param.DefaultValue
			continue
		}

		// Validate parameter type
		if exists {
			if err := tr.validateParameterType(param, value); err != nil {
				return fmt.Errorf("parameter %s validation failed: %w", param.Name, err)
			}
		}

		// Validate parameter value
		if param.Validation != "" && exists {
			if err := tr.validateParameterValue(param, value); err != nil {
				return fmt.Errorf("parameter %s value validation failed: %w", param.Name, err)
			}
		}
	}

	return nil
}

func (tr *TemplateRegistry) validateParameterType(param TemplateParameter, value interface{}) error {
	switch param.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "integer":
		switch value.(type) {
		case int, int32, int64, float64:
			// OK
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case "array":
		if _, ok := value.([]interface{}); !ok {
			return fmt.Errorf("expected array, got %T", value)
		}
	case "object":
		if _, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("expected object, got %T", value)
		}
	}

	return nil
}

func (tr *TemplateRegistry) validateParameterValue(param TemplateParameter, value interface{}) error {
	// Simple validation based on validation string
	// In a real implementation, this would use a proper validation engine
	validation := param.Validation
	valueStr := fmt.Sprintf("%v", value)

	// Check for length constraints
	if strings.Contains(validation, "min_length") {
		// Extract and check minimum length
		if len(valueStr) < 3 { // Example constraint
			return fmt.Errorf("value too short")
		}
	}

	// Check for pattern matching
	if strings.Contains(validation, "pattern") {
		// Pattern validation would go here
	}

	return nil
}

func (tr *TemplateRegistry) renderTemplate(migrationTemplate *MigrationTemplate, parameters map[string]interface{}) (*MigrationPlan, error) {
	// Parse template
	tmpl, err := template.New("migration_plan").Parse(migrationTemplate.PlanTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	// Render template
	var rendered strings.Builder
	if err := tmpl.Execute(&rendered, parameters); err != nil {
		return nil, fmt.Errorf("failed to render template: %w", err)
	}

	// Parse rendered JSON into migration plan
	var plan MigrationPlan
	if err := json.Unmarshal([]byte(rendered.String()), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse rendered plan: %w", err)
	}

	// Set additional properties
	plan.ID = generateID("plan")
	plan.CreatedAt = time.Now()
	plan.Version = migrationTemplate.Version
	plan.Configuration = DefaultMigrationConfig()

	// Copy validations from template
	plan.Validations = append(plan.Validations, migrationTemplate.Validations...)

	return &plan, nil
}

func (tr *TemplateRegistry) extractParameters(plan *MigrationPlan) ([]TemplateParameter, error) {
	// Extract parameterizable values from the plan
	// This is a simplified implementation
	var parameters []TemplateParameter

	// Common parameters
	parameters = append(parameters,
		TemplateParameter{
			Name:        "name",
			Type:        "string",
			Description: "Migration plan name",
			Required:    true,
		},
		TemplateParameter{
			Name:        "description",
			Type:        "string",
			Description: "Migration plan description",
			Required:    false,
		},
	)

	// Extract table-specific parameters
	for _, table := range plan.Tables {
		parameters = append(parameters,
			TemplateParameter{
				Name:        fmt.Sprintf("%s_source_table", table.SourceTable),
				Type:        "string",
				Description: fmt.Sprintf("Source table for %s", table.SourceTable),
				Required:    true,
				DefaultValue: table.SourceTable,
			},
			TemplateParameter{
				Name:        fmt.Sprintf("%s_target_table", table.SourceTable),
				Type:        "string",
				Description: fmt.Sprintf("Target table for %s", table.SourceTable),
				Required:    true,
				DefaultValue: table.TargetTable,
			},
		)
	}

	return parameters, nil
}

func (tr *TemplateRegistry) createPlanTemplate(plan *MigrationPlan) (string, error) {
	// Convert plan to JSON template
	planBytes, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return "", err
	}

	// Replace specific values with template variables
	planTemplate := string(planBytes)
	planTemplate = strings.ReplaceAll(planTemplate, fmt.Sprintf(`"%s"`, plan.Name), `"{{.name}}"`)
	planTemplate = strings.ReplaceAll(planTemplate, fmt.Sprintf(`"%s"`, plan.Description), `"{{.description}}"`)

	// Replace table names with template variables
	for _, table := range plan.Tables {
		planTemplate = strings.ReplaceAll(planTemplate, 
			fmt.Sprintf(`"%s"`, table.SourceTable), 
			fmt.Sprintf(`"{{.%s_source_table}}"`, table.SourceTable))
		planTemplate = strings.ReplaceAll(planTemplate, 
			fmt.Sprintf(`"%s"`, table.TargetTable), 
			fmt.Sprintf(`"{{.%s_target_table}}"`, table.SourceTable))
	}

	return planTemplate, nil
}

func (tr *TemplateRegistry) createScriptTemplates(plan *MigrationPlan) (map[string]string, error) {
	scriptTemplates := make(map[string]string)

	// Create pre-migration script template
	scriptTemplates["pre_migration"] = `
-- Pre-migration script for {{.name}}
-- Validate prerequisites
SELECT 'Pre-migration checks completed' as status;
`

	// Create post-migration script template
	scriptTemplates["post_migration"] = `
-- Post-migration script for {{.name}}
-- Analyze tables and update statistics
{{range .tables}}
ANALYZE TABLE {{.target_table}};
{{end}}
SELECT 'Post-migration tasks completed' as status;
`

	// Create validation script template
	scriptTemplates["validation"] = `
-- Validation script for {{.name}}
{{range .tables}}
SELECT 
  '{{.source_table}}' as table_name,
  COUNT(*) as row_count,
  'Source' as source_type
FROM {{.source_table}}
UNION ALL
SELECT 
  '{{.target_table}}' as table_name,
  COUNT(*) as row_count,
  'Target' as source_type
FROM {{.target_table}};
{{end}}
`

	return scriptTemplates, nil
}

// TemplateInfo contains information for creating a template
type TemplateInfo struct {
	Name        string
	Description string
	Category    string
	Tags        []string
	CreatedBy   string
}

// TemplateManager provides high-level template management
type TemplateManager struct {
	registry   *TemplateRegistry
	repository MigrationRepository
}

// NewTemplateManager creates a new template manager
func NewTemplateManager(repository MigrationRepository) *TemplateManager {
	return &TemplateManager{
		registry:   NewTemplateRegistry(),
		repository: repository,
	}
}

// CreateTemplate creates a new template from a migration plan
func (tm *TemplateManager) CreateTemplate(ctx context.Context, plan *MigrationPlan, info TemplateInfo) (*MigrationTemplate, error) {
	template, err := tm.registry.CreateTemplate(plan, info)
	if err != nil {
		return nil, err
	}

	// Save to repository
	if err := tm.repository.SaveTemplate(ctx, template); err != nil {
		return nil, fmt.Errorf("failed to save template: %w", err)
	}

	return template, nil
}

// GetTemplate retrieves a template by ID
func (tm *TemplateManager) GetTemplate(ctx context.Context, templateID string) (*MigrationTemplate, error) {
	// Try registry first
	if template, err := tm.registry.GetTemplate(templateID); err == nil {
		return template, nil
	}

	// Try repository
	template, err := tm.repository.GetTemplate(ctx, templateID)
	if err != nil {
		return nil, err
	}

	// Register in registry for faster access
	tm.registry.RegisterTemplate(template)

	return template, nil
}

// ListTemplates returns all available templates
func (tm *TemplateManager) ListTemplates(ctx context.Context) ([]*MigrationTemplate, error) {
	// Get templates from repository
	repoTemplates, err := tm.repository.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}

	// Register in registry
	for _, template := range repoTemplates {
		tm.registry.RegisterTemplate(template)
	}

	// Return all templates from registry
	return tm.registry.ListTemplates(), nil
}

// ApplyTemplate applies a template with parameters
func (tm *TemplateManager) ApplyTemplate(ctx context.Context, templateID string, parameters map[string]interface{}) (*MigrationPlan, error) {
	template, err := tm.GetTemplate(ctx, templateID)
	if err != nil {
		return nil, err
	}

	return tm.registry.ApplyTemplate(ctx, template.ID, parameters)
}

// DeleteTemplate deletes a template
func (tm *TemplateManager) DeleteTemplate(ctx context.Context, templateID string) error {
	// Delete from repository
	if err := tm.repository.DeleteTemplate(ctx, templateID); err != nil {
		return err
	}

	// Remove from registry
	delete(tm.registry.templates, templateID)

	// Remove from categories
	for category, templateIDs := range tm.registry.categories {
		for i, id := range templateIDs {
			if id == templateID {
				tm.registry.categories[category] = append(templateIDs[:i], templateIDs[i+1:]...)
				break
			}
		}
	}

	return nil
}
