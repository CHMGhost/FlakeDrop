package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// Service provides schema-related operations
type Service struct {
	snowflake *snowflake.Service
	errorHandler *errors.ErrorHandler
}

// NewService creates a new schema service
func NewService(sf *snowflake.Service) *Service {
	return &Service{
		snowflake: sf,
		errorHandler: errors.GetGlobalErrorHandler(),
	}
}

// GetDatabaseObjects retrieves all objects in a database
func (s *Service) GetDatabaseObjects(ctx context.Context, database, schema string, objectTypes []ObjectType) ([]SchemaObject, error) {
	var objects []SchemaObject

	// Get tables
	if s.shouldIncludeType(ObjectTypeTable, objectTypes) {
		tables, err := s.getTables(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get tables: %w", err)
		}
		objects = append(objects, tables...)
	}

	// Get views
	if s.shouldIncludeType(ObjectTypeView, objectTypes) {
		views, err := s.getViews(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get views: %w", err)
		}
		objects = append(objects, views...)
	}

	// Get procedures
	if s.shouldIncludeType(ObjectTypeProcedure, objectTypes) {
		procedures, err := s.getProcedures(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get procedures: %w", err)
		}
		objects = append(objects, procedures...)
	}

	// Get functions
	if s.shouldIncludeType(ObjectTypeFunction, objectTypes) {
		functions, err := s.getFunctions(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get functions: %w", err)
		}
		objects = append(objects, functions...)
	}

	// Get sequences
	if s.shouldIncludeType(ObjectTypeSequence, objectTypes) {
		sequences, err := s.getSequences(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get sequences: %w", err)
		}
		objects = append(objects, sequences...)
	}

	// Get stages
	if s.shouldIncludeType(ObjectTypeStage, objectTypes) {
		stages, err := s.getStages(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get stages: %w", err)
		}
		objects = append(objects, stages...)
	}

	// Get file formats
	if s.shouldIncludeType(ObjectTypeFileFormat, objectTypes) {
		fileFormats, err := s.getFileFormats(ctx, database, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get file formats: %w", err)
		}
		objects = append(objects, fileFormats...)
	}

	return objects, nil
}

// GetTableDetails retrieves detailed information about a table
func (s *Service) GetTableDetails(ctx context.Context, database, schema, table string) (*TableDetails, error) {
	// Get basic table info
	tableInfo, err := s.getTableInfo(ctx, database, schema, table)
	if err != nil {
		return nil, err
	}

	// Get columns
	columns, err := s.getTableColumns(ctx, database, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get constraints
	constraints, err := s.getTableConstraints(ctx, database, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get constraints: %w", err)
	}

	// Get indexes
	indexes, err := s.getTableIndexes(ctx, database, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}

	details := &TableDetails{
		SchemaObject: *tableInfo,
		Columns:      columns,
		Constraints:  constraints,
		Indexes:      indexes,
	}

	return details, nil
}

// GetViewDetails retrieves detailed information about a view
func (s *Service) GetViewDetails(ctx context.Context, database, schema, view string) (*ViewDetails, error) {
	query := `
		SELECT 
			view_name,
			view_owner,
			created,
			last_altered,
			comment,
			is_secure,
			is_materialized,
			view_definition
		FROM information_schema.views
		WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
	`

	var details ViewDetails
	var createdStr, lastAlteredStr sql.NullString
	var comment sql.NullString

	err := s.snowflake.GetDB().QueryRowContext(ctx, query, database, schema, view).Scan(
		&details.Name,
		&details.Owner,
		&createdStr,
		&lastAlteredStr,
		&comment,
		&details.IsSecure,
		&details.IsMaterialized,
		&details.ViewDefinition,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get view details: %w", err)
	}

	details.Type = ObjectTypeView
	details.Database = database
	details.Schema = schema
	if comment.Valid {
		details.Comment = comment.String
	}

	// Parse timestamps
	if createdStr.Valid {
		details.CreatedOn, _ = time.Parse(time.RFC3339, createdStr.String)
	}
	if lastAlteredStr.Valid {
		details.LastModified, _ = time.Parse(time.RFC3339, lastAlteredStr.String)
	}

	return &details, nil
}

// GetProcedureDetails retrieves detailed information about a procedure
func (s *Service) GetProcedureDetails(ctx context.Context, database, schema, procedure string) (*ProcedureDetails, error) {
	// Get procedure definition using SHOW PROCEDURES
	query := fmt.Sprintf("SHOW PROCEDURES LIKE '%s' IN SCHEMA %s.%s", procedure, database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get procedure details: %w", err)
	}
	defer rows.Close()

	var details ProcedureDetails
	if rows.Next() {
		// Parse procedure information from SHOW PROCEDURES result
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Map column values (column positions may vary)
		for i, col := range cols {
			switch strings.ToUpper(col) {
			case "NAME":
				if v, ok := values[i].(string); ok {
					details.Name = v
				}
			case "LANGUAGE":
				if v, ok := values[i].(string); ok {
					details.Language = v
				}
			case "ARGUMENTS":
				if v, ok := values[i].(string); ok {
					details.Arguments = s.parseProcedureArguments(v)
				}
			case "RETURN_TYPE":
				if v, ok := values[i].(string); ok {
					details.ReturnType = v
				}
			}
		}
	}

	details.Type = ObjectTypeProcedure
	details.Database = database
	details.Schema = schema

	// Get procedure body using GET_DDL
	ddlQuery := fmt.Sprintf("SELECT GET_DDL('PROCEDURE', '%s.%s.%s')", database, schema, procedure)
	var ddl string
	err = s.snowflake.GetDB().QueryRowContext(ctx, ddlQuery).Scan(&ddl)
	if err == nil {
		details.Body = ddl
		details.Definition = ddl
	}

	return &details, nil
}

// Helper methods

func (s *Service) shouldIncludeType(objType ObjectType, types []ObjectType) bool {
	if len(types) == 0 {
		return true
	}
	for _, t := range types {
		if t == objType {
			return true
		}
	}
	return false
}

func (s *Service) getTables(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW TABLES IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeTable,
			Database: database,
			Schema:   schema,
		}

		// Parse table information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Map column values
		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					obj.Comment = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getViews(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW VIEWS IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeView,
			Database: database,
			Schema:   schema,
		}

		// Similar parsing logic as getTables
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					obj.Comment = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getProcedures(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW PROCEDURES IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeProcedure,
			Database: database,
			Schema:   schema,
		}

		// Parse procedure information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getFunctions(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW FUNCTIONS IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeFunction,
			Database: database,
			Schema:   schema,
		}

		// Parse function information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getSequences(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW SEQUENCES IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeSequence,
			Database: database,
			Schema:   schema,
		}

		// Parse sequence information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					obj.Comment = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getStages(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW STAGES IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeStage,
			Database: database,
			Schema:   schema,
		}

		// Parse stage information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					obj.Comment = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getFileFormats(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf("SHOW FILE FORMATS IN SCHEMA %s.%s", database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		obj := SchemaObject{
			Type:     ObjectTypeFileFormat,
			Database: database,
			Schema:   schema,
		}

		// Parse file format information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			switch strings.ToLower(col) {
			case "name":
				if v, ok := values[i].(string); ok {
					obj.Name = v
				}
			case "owner":
				if v, ok := values[i].(string); ok {
					obj.Owner = v
				}
			case "created_on":
				if v, ok := values[i].(time.Time); ok {
					obj.CreatedOn = v
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					obj.Comment = v
				}
			}
		}

		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (s *Service) getTableInfo(ctx context.Context, database, schema, table string) (*SchemaObject, error) {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s' IN SCHEMA %s.%s", table, database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("table %s not found", table)
	}

	obj := &SchemaObject{
		Type:     ObjectTypeTable,
		Database: database,
		Schema:   schema,
		Name:     table,
	}

	// Parse table information
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	for i, col := range cols {
		switch strings.ToLower(col) {
		case "owner":
			if v, ok := values[i].(string); ok {
				obj.Owner = v
			}
		case "created_on":
			if v, ok := values[i].(time.Time); ok {
				obj.CreatedOn = v
			}
		case "comment":
			if v, ok := values[i].(string); ok {
				obj.Comment = v
			}
		}
	}

	return obj, nil
}

func (s *Service) getTableColumns(ctx context.Context, database, schema, table string) ([]Column, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s.%s.%s", database, schema, table)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	position := 0
	
	for rows.Next() {
		var col Column
		col.Position = position
		position++

		// Parse column information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Map column values based on DESCRIBE TABLE output
		for i, colName := range cols {
			switch strings.ToLower(colName) {
			case "name":
				if v, ok := values[i].(string); ok {
					col.Name = v
				}
			case "type":
				if v, ok := values[i].(string); ok {
					col.Type = v
				}
			case "kind":
				if v, ok := values[i].(string); ok && v == "COLUMN" {
					// This is a regular column
				}
			case "null?":
				if v, ok := values[i].(string); ok {
					col.Nullable = v == "Y"
				}
			case "default":
				if v, ok := values[i].(string); ok && v != "" {
					col.Default = &v
				}
			case "primary key":
				if v, ok := values[i].(string); ok {
					col.PrimaryKey = v == "Y"
				}
			case "unique key":
				if v, ok := values[i].(string); ok {
					col.UniqueKey = v == "Y"
				}
			case "comment":
				if v, ok := values[i].(string); ok {
					col.Comment = v
				}
			}
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (s *Service) getTableConstraints(ctx context.Context, database, schema, table string) ([]Constraint, error) {
	query := fmt.Sprintf("SHOW PRIMARY KEYS IN TABLE %s.%s.%s", database, schema, table)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		// If error, return empty constraints list
		return []Constraint{}, nil
	}
	defer rows.Close()

	constraintMap := make(map[string]*Constraint)
	
	for rows.Next() {
		// Parse constraint information
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		var constraintName, columnName string
		for i, col := range cols {
			switch strings.ToLower(col) {
			case "constraint_name":
				if v, ok := values[i].(string); ok {
					constraintName = v
				}
			case "column_name":
				if v, ok := values[i].(string); ok {
					columnName = v
				}
			}
		}

		if constraintName != "" {
			if _, exists := constraintMap[constraintName]; !exists {
				constraintMap[constraintName] = &Constraint{
					Name:    constraintName,
					Type:    "PRIMARY KEY",
					Enabled: true,
					Columns: []string{},
				}
			}
			if columnName != "" {
				constraintMap[constraintName].Columns = append(constraintMap[constraintName].Columns, columnName)
			}
		}
	}

	// Convert map to slice
	var constraints []Constraint
	for _, c := range constraintMap {
		constraints = append(constraints, *c)
	}

	return constraints, nil
}

func (s *Service) getTableIndexes(ctx context.Context, database, schema, table string) ([]Index, error) {
	// Snowflake doesn't have traditional indexes, but we can get clustering keys
	query := fmt.Sprintf("SHOW TABLES LIKE '%s' IN SCHEMA %s.%s", table, database, schema)
	
	rows, err := s.snowflake.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []Index
	
	if rows.Next() {
		// Parse table information for clustering keys
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			if strings.ToLower(col) == "cluster_by" {
				if v, ok := values[i].(string); ok && v != "" {
					// Parse clustering key
					keys := strings.Split(v, ",")
					for i, k := range keys {
						keys[i] = strings.TrimSpace(k)
					}
					
					indexes = append(indexes, Index{
						Name:      "CLUSTERING_KEY",
						Columns:   keys,
						Type:      "CLUSTERED",
						Clustered: true,
					})
				}
			}
		}
	}

	return indexes, nil
}

func (s *Service) parseProcedureArguments(argsStr string) []ProcedureArgument {
	var args []ProcedureArgument
	
	// Simple parsing of procedure arguments
	// Format: "arg1 TYPE1, arg2 TYPE2, ..."
	if argsStr == "" {
		return args
	}

	parts := strings.Split(argsStr, ",")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by space to get name and type
		tokens := strings.Fields(part)
		if len(tokens) >= 2 {
			arg := ProcedureArgument{
				Name:     tokens[0],
				Type:     strings.Join(tokens[1:], " "),
				Mode:     "IN", // Default mode
				Position: i,
			}
			args = append(args, arg)
		}
	}

	return args
}

// GetDB returns the underlying Snowflake database connection
func (s *Service) GetDB() *sql.DB {
	return s.snowflake.GetDB()
}