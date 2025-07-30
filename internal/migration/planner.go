package migration

import (
	"context"
	"fmt"
	"sort"
	"time"

	"flakedrop/internal/schema"
	"flakedrop/pkg/errors"
)

// Planner creates and optimizes migration plans
type Planner struct {
	schemaService    *schema.Service
	errorHandler     *errors.ErrorHandler
	templateRegistry *TemplateRegistry
}

// NewPlanner creates a new migration planner
func NewPlanner(schemaService *schema.Service) *Planner {
	return &Planner{
		schemaService:    schemaService,
		errorHandler:     errors.GetGlobalErrorHandler(),
		templateRegistry: NewTemplateRegistry(),
	}
}

// CreatePlan creates a comprehensive migration plan
func (p *Planner) CreatePlan(ctx context.Context, config PlanConfig) (*MigrationPlan, error) {
	plan := &MigrationPlan{
		ID:               generateID("plan"),
		Name:             config.Name,
		Description:      config.Description,
		Type:             config.Type,
		Strategy:         config.Strategy,
		SourceDatabase:   config.SourceConfig.Name,
		SourceSchema:     config.SourceConfig.Properties["schema"].(string),
		TargetDatabase:   config.TargetConfig.Name,
		TargetSchema:     config.TargetConfig.Properties["schema"].(string),
		Configuration:    config.MigrationConfig,
		CreatedAt:        time.Now(),
		CreatedBy:        config.Parameters["created_by"].(string),
		Version:          "1.0",
		Tags:             make(map[string]string),
	}

	// Apply template if specified
	if config.TemplateID != "" {
		if err := p.applyTemplate(ctx, plan, config.TemplateID, config.Parameters); err != nil {
			return nil, fmt.Errorf("failed to apply template: %w", err)
		}
	}

	// Analyze source and target schemas
	sourceObjects, err := p.analyzeSchema(ctx, config.SourceConfig, config.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze source schema: %w", err)
	}

	targetObjects, err := p.analyzeSchema(ctx, config.TargetConfig, config.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze target schema: %w", err)
	}

	// Create table migrations
	tableMigrations, err := p.createTableMigrations(ctx, sourceObjects, targetObjects, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create table migrations: %w", err)
	}
	plan.Tables = tableMigrations

	// Analyze dependencies
	if err := p.analyzeDependencies(ctx, plan); err != nil {
		return nil, fmt.Errorf("failed to analyze dependencies: %w", err)
	}

	// Create pre-conditions
	plan.PreConditions = p.createPreConditions(ctx, plan)

	// Create post-actions
	plan.PostActions = p.createPostActions(ctx, plan)

	// Create validations
	plan.Validations = p.createValidations(ctx, plan)

	// Estimate time and size
	if err := p.estimateExecution(ctx, plan); err != nil {
		return nil, fmt.Errorf("failed to estimate execution: %w", err)
	}

	return plan, nil
}

// ValidatePlan validates a migration plan
func (p *Planner) ValidatePlan(ctx context.Context, plan *MigrationPlan) error {
	// Validate basic structure
	if plan.ID == "" {
		return errors.ValidationError("id", plan.ID, "Plan ID cannot be empty")
	}
	if plan.Name == "" {
		return errors.ValidationError("name", plan.Name, "Plan name cannot be empty")
	}
	if len(plan.Tables) == 0 {
		return errors.ValidationError("tables", len(plan.Tables), "Plan must include at least one table")
	}

	// Validate dependencies
	if err := p.validateDependencies(ctx, plan); err != nil {
		return fmt.Errorf("dependency validation failed: %w", err)
	}

	// Validate table migrations
	for i, table := range plan.Tables {
		if err := p.validateTableMigration(ctx, &table); err != nil {
			return fmt.Errorf("table migration validation failed for %s: %w", table.SourceTable, err)
		}
		plan.Tables[i] = table
	}

	// Validate pre-conditions
	for _, precondition := range plan.PreConditions {
		if err := p.validatePreCondition(ctx, &precondition); err != nil {
			return fmt.Errorf("pre-condition validation failed: %w", err)
		}
	}

	// Validate post-actions
	for _, action := range plan.PostActions {
		if err := p.validatePostAction(ctx, &action); err != nil {
			return fmt.Errorf("post-action validation failed: %w", err)
		}
	}

	return nil
}

// OptimizePlan optimizes a migration plan for performance
func (p *Planner) OptimizePlan(ctx context.Context, plan *MigrationPlan) (*MigrationPlan, error) {
	optimizedPlan := *plan

	// Optimize table order based on dependencies and size
	if err := p.optimizeTableOrder(ctx, &optimizedPlan); err != nil {
		return nil, fmt.Errorf("failed to optimize table order: %w", err)
	}

	// Optimize batch sizes based on table characteristics
	if err := p.optimizeBatchSizes(ctx, &optimizedPlan); err != nil {
		return nil, fmt.Errorf("failed to optimize batch sizes: %w", err)
	}

	// Optimize parallelism
	if err := p.optimizeParallelism(ctx, &optimizedPlan); err != nil {
		return nil, fmt.Errorf("failed to optimize parallelism: %w", err)
	}

	// Update estimates
	if err := p.estimateExecution(ctx, &optimizedPlan); err != nil {
		return nil, fmt.Errorf("failed to update estimates: %w", err)
	}

	return &optimizedPlan, nil
}

// analyzeSchema analyzes schema objects for migration planning
func (p *Planner) analyzeSchema(ctx context.Context, config DataSource, tables []string) ([]schema.SchemaObject, error) {
	database := config.Properties["database"].(string)
	schemaName := config.Properties["schema"].(string)

	// Define object types to analyze
	objectTypes := []schema.ObjectType{
		schema.ObjectTypeTable,
		schema.ObjectTypeView,
	}

	// Get all objects if no specific tables requested
	if len(tables) == 0 {
		return p.schemaService.GetDatabaseObjects(ctx, database, schemaName, objectTypes)
	}

	// Get specific tables
	var objects []schema.SchemaObject
	for _, table := range tables {
		tableDetails, err := p.schemaService.GetTableDetails(ctx, database, schemaName, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get table details for %s: %w", table, err)
		}
		objects = append(objects, tableDetails.SchemaObject)
	}

	return objects, nil
}

// createTableMigrations creates table migration configurations
func (p *Planner) createTableMigrations(ctx context.Context, sourceObjects, targetObjects []schema.SchemaObject, config PlanConfig) ([]TableMigration, error) {
	var migrations []TableMigration

	// Create lookup map for target objects
	targetMap := make(map[string]schema.SchemaObject)
	for _, target := range targetObjects {
		targetMap[target.Name] = target
	}

	for i, source := range sourceObjects {
		// Skip non-table objects for data migration
		if source.Type != schema.ObjectTypeTable {
			continue
		}

		migration := TableMigration{
			SourceTable:     source.Name,
			TargetTable:     source.Name, // Default to same name
			MigrationMode:   p.determineMigrationMode(source, targetMap[source.Name]),
			BatchSize:       config.MigrationConfig.BatchSize,
			Parallelism:     1, // Default to sequential
			Transformations: []DataTransformation{},
			Filters:         []DataFilter{},
			Mappings:        []ColumnMapping{},
			Validations:     []ValidationRule{},
			DependsOn:       []string{},
			Priority:        i + 1,
			Checkpoints:     []Checkpoint{},
		}

		// Get table statistics for estimation
		if err := p.enrichTableMigration(ctx, &migration, source, config); err != nil {
			return nil, fmt.Errorf("failed to enrich table migration for %s: %w", source.Name, err)
		}

		// Create column mappings
		if err := p.createColumnMappings(ctx, &migration, source, targetMap[source.Name]); err != nil {
			return nil, fmt.Errorf("failed to create column mappings for %s: %w", source.Name, err)
		}

		migrations = append(migrations, migration)
	}

	return migrations, nil
}

// analyzeDependencies analyzes table dependencies for proper ordering
func (p *Planner) analyzeDependencies(ctx context.Context, plan *MigrationPlan) error {
	// Create dependency graph
	dependencyGraph := make(map[string][]string)
	tableSet := make(map[string]bool)

	// Initialize tables in set
	for _, table := range plan.Tables {
		tableSet[table.SourceTable] = true
		dependencyGraph[table.SourceTable] = []string{}
	}

	// Analyze foreign key dependencies
	for i := range plan.Tables {
		table := &plan.Tables[i]
		deps, err := p.getForeignKeyDependencies(ctx, plan.SourceDatabase, plan.SourceSchema, table.SourceTable)
		if err != nil {
			return fmt.Errorf("failed to get dependencies for %s: %w", table.SourceTable, err)
		}

		// Filter dependencies to only include tables in the migration
		var filteredDeps []string
		for _, dep := range deps {
			if tableSet[dep] {
				filteredDeps = append(filteredDeps, dep)
			}
		}

		table.DependsOn = filteredDeps
		dependencyGraph[table.SourceTable] = filteredDeps
	}

	// Topological sort to determine execution order
	executionOrder, err := p.topologicalSort(dependencyGraph)
	if err != nil {
		return fmt.Errorf("failed to sort dependencies: %w", err)
	}

	// Update table priorities based on execution order
	for i, tableName := range executionOrder {
		for j := range plan.Tables {
			if plan.Tables[j].SourceTable == tableName {
				plan.Tables[j].Priority = i + 1
				break
			}
		}
	}

	return nil
}

// createPreConditions creates pre-conditions for the migration
func (p *Planner) createPreConditions(ctx context.Context, plan *MigrationPlan) []PreCondition {
	var preConditions []PreCondition

	// Check target schema exists
	preConditions = append(preConditions, PreCondition{
		ID:          generateID("precond"),
		Name:        "Target Schema Exists",
		Type:        PreConditionTypeSchemaExists,
		Expression:  fmt.Sprintf("SCHEMA_EXISTS('%s', '%s')", plan.TargetDatabase, plan.TargetSchema),
		Required:    true,
		Description: "Ensure target schema exists before migration",
	})

	// Check permissions
	preConditions = append(preConditions, PreCondition{
		ID:          generateID("precond"),
		Name:        "Migration Permissions",
		Type:        PreConditionTypePermission,
		Expression:  "HAS_PRIVILEGES('INSERT', 'UPDATE', 'DELETE')",
		Required:    true,
		Description: "Ensure user has necessary permissions for migration",
	})

	// Check available space
	preConditions = append(preConditions, PreCondition{
		ID:          generateID("precond"),
		Name:        "Available Space",
		Type:        PreConditionTypeSpace,
		Expression:  fmt.Sprintf("AVAILABLE_SPACE() > %d", plan.EstimatedSize*2), // 2x for safety
		Required:    true,
		Description: "Ensure sufficient space for migration",
	})

	return preConditions
}

// createPostActions creates post-migration actions
func (p *Planner) createPostActions(ctx context.Context, plan *MigrationPlan) []PostAction {
	var postActions []PostAction

	// Analyze tables after migration
	for i, table := range plan.Tables {
		postActions = append(postActions, PostAction{
			ID:          generateID("postact"),
			Name:        fmt.Sprintf("Analyze %s", table.TargetTable),
			Type:        PostActionTypeAnalyze,
			SQL:         fmt.Sprintf("ANALYZE TABLE %s.%s.%s", plan.TargetDatabase, plan.TargetSchema, table.TargetTable),
			Order:       i + 1,
			Required:    false,
			Description: "Update table statistics after migration",
		})
	}

	return postActions
}

// createValidations creates validation rules for the migration
func (p *Planner) createValidations(ctx context.Context, plan *MigrationPlan) []ValidationRule {
	var validations []ValidationRule

	// Row count validation for each table
	for _, table := range plan.Tables {
		validations = append(validations, ValidationRule{
			ID:          generateID("valid"),
			Name:        fmt.Sprintf("Row Count - %s", table.SourceTable),
			Type:        ValidationTypeRowCount,
			Expression:  fmt.Sprintf("SOURCE_COUNT('%s') = TARGET_COUNT('%s')", table.SourceTable, table.TargetTable),
			Severity:    ValidationSeverityError,
			Enabled:     true,
			Description: "Validate row count matches between source and target",
		})

		// Data quality validation
		validations = append(validations, ValidationRule{
			ID:          generateID("valid"),
			Name:        fmt.Sprintf("Data Quality - %s", table.SourceTable),
			Type:        ValidationTypeDataQuality,
			Expression:  fmt.Sprintf("VALIDATE_DATA_QUALITY('%s')", table.TargetTable),
			Severity:    ValidationSeverityWarning,
			Enabled:     true,
			Description: "Validate data quality in target table",
		})
	}

	return validations
}

// Helper methods

func (p *Planner) determineMigrationMode(source schema.SchemaObject, target schema.SchemaObject) MigrationMode {
	if target.Name == "" {
		return MigrationModeInsert // Target doesn't exist, insert all
	}
	return MigrationModeUpsert // Default to upsert for existing tables
}

func (p *Planner) enrichTableMigration(ctx context.Context, migration *TableMigration, source schema.SchemaObject, config PlanConfig) error {
	// Get table details for estimation
	tableDetails, err := p.schemaService.GetTableDetails(ctx, config.SourceConfig.Properties["database"].(string), config.SourceConfig.Properties["schema"].(string), source.Name)
	if err != nil {
		return err
	}

	migration.EstimatedRows = tableDetails.RowCount
	migration.EstimatedSize = tableDetails.SizeBytes

	return nil
}

func (p *Planner) createColumnMappings(ctx context.Context, migration *TableMigration, source, target schema.SchemaObject) error {
	// Get source table columns
	sourceDetails, err := p.schemaService.GetTableDetails(ctx, "", "", source.Name)
	if err != nil {
		return err
	}

	// Create default 1:1 mappings
	for _, col := range sourceDetails.Columns {
		mapping := ColumnMapping{
			SourceColumn: col.Name,
			TargetColumn: col.Name,
			DataType:     col.Type,
			Required:     !col.Nullable,
		}

		if col.Default != nil {
			mapping.DefaultValue = *col.Default
		}

		migration.Mappings = append(migration.Mappings, mapping)
	}

	return nil
}

func (p *Planner) getForeignKeyDependencies(ctx context.Context, database, schemaName, table string) ([]string, error) {
	// This would query the information schema for foreign key dependencies
	// For now, return empty dependencies
	return []string{}, nil
}

func (p *Planner) topologicalSort(graph map[string][]string) ([]string, error) {
	// Kahn's algorithm for topological sorting
	inDegree := make(map[string]int)
	order := []string{}
	queue := []string{}

	// Initialize in-degrees
	for node := range graph {
		inDegree[node] = 0
	}

	// Calculate in-degrees
	for _, neighbors := range graph {
		for _, neighbor := range neighbors {
			inDegree[neighbor]++
		}
	}

	// Find nodes with no incoming edges
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	// Process queue
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		order = append(order, current)

		// Update neighbors
		for _, neighbor := range graph[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// Check for cycles
	if len(order) != len(graph) {
		return nil, fmt.Errorf("circular dependency detected")
	}

	return order, nil
}

func (p *Planner) validateDependencies(ctx context.Context, plan *MigrationPlan) error {
	// Check that all dependencies exist in the plan
	tableSet := make(map[string]bool)
	for _, table := range plan.Tables {
		tableSet[table.SourceTable] = true
	}

	for _, table := range plan.Tables {
		for _, dep := range table.DependsOn {
			if !tableSet[dep] {
				return fmt.Errorf("table %s depends on %s which is not included in migration", table.SourceTable, dep)
			}
		}
	}

	return nil
}

func (p *Planner) validateTableMigration(ctx context.Context, migration *TableMigration) error {
	if migration.SourceTable == "" {
		return errors.ValidationError("source_table", "", "Source table cannot be empty")
	}
	if migration.TargetTable == "" {
		return errors.ValidationError("target_table", "", "Target table cannot be empty")
	}
	if migration.BatchSize <= 0 {
		return errors.ValidationError("batch_size", 0, "Batch size must be positive")
	}
	if migration.Parallelism < 1 {
		return errors.ValidationError("parallelism", 0, "Parallelism must be at least 1")
	}

	return nil
}

func (p *Planner) validatePreCondition(ctx context.Context, condition *PreCondition) error {
	if condition.Name == "" {
		return errors.ValidationError("pre_condition_name", condition.Name, "Pre-condition name cannot be empty")
	}
	if condition.Expression == "" {
		return errors.ValidationError("pre_condition_expression", condition.Expression, "Pre-condition expression cannot be empty")
	}

	return nil
}

func (p *Planner) validatePostAction(ctx context.Context, action *PostAction) error {
	if action.Name == "" {
		return errors.ValidationError("post_action_name", action.Name, "Post-action name cannot be empty")
	}
	if action.SQL == "" {
		return errors.ValidationError("post_action_sql", action.SQL, "Post-action SQL cannot be empty")
	}

	return nil
}

func (p *Planner) optimizeTableOrder(ctx context.Context, plan *MigrationPlan) error {
	// Sort tables by priority (which includes dependency order)
	sort.Slice(plan.Tables, func(i, j int) bool {
		return plan.Tables[i].Priority < plan.Tables[j].Priority
	})

	return nil
}

func (p *Planner) optimizeBatchSizes(ctx context.Context, plan *MigrationPlan) error {
	for i, table := range plan.Tables {
		// Adjust batch size based on estimated row count
		if table.EstimatedRows > 1000000 {
			// Large tables: smaller batches
			plan.Tables[i].BatchSize = 5000
		} else if table.EstimatedRows > 100000 {
			// Medium tables: default batches
			plan.Tables[i].BatchSize = 10000
		} else {
			// Small tables: larger batches
			plan.Tables[i].BatchSize = 25000
		}
	}

	return nil
}

func (p *Planner) optimizeParallelism(ctx context.Context, plan *MigrationPlan) error {
	for i, table := range plan.Tables {
		// Enable parallelism for larger tables without dependencies
		if table.EstimatedRows > 100000 && len(table.DependsOn) == 0 {
			plan.Tables[i].Parallelism = 2
		} else {
			plan.Tables[i].Parallelism = 1
		}
	}

	return nil
}

func (p *Planner) estimateExecution(ctx context.Context, plan *MigrationPlan) error {
	var totalRows int64
	var totalSize int64
	var totalTime time.Duration

	for _, table := range plan.Tables {
		totalRows += table.EstimatedRows
		totalSize += table.EstimatedSize

		// Rough time estimation based on row count and complexity
		// Assume 10,000 rows per second processing rate
		processingTime := time.Duration(table.EstimatedRows/10000) * time.Second
		if processingTime < time.Minute {
			processingTime = time.Minute // Minimum 1 minute per table
		}
		totalTime += processingTime
	}

	plan.EstimatedSize = totalSize
	plan.EstimatedTime = totalTime

	return nil
}

func (p *Planner) applyTemplate(ctx context.Context, plan *MigrationPlan, templateID string, parameters map[string]interface{}) error {
	template, err := p.templateRegistry.GetTemplate(templateID)
	if err != nil {
		return fmt.Errorf("template not found: %w", err)
	}

	// Apply template settings
	plan.Type = template.Type
	plan.Strategy = template.Strategy

	// Apply template validations
	plan.Validations = append(plan.Validations, template.Validations...)

	return nil
}

// generateID generates a unique ID with prefix
func generateID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
