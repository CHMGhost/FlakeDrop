package schema

import (
	"fmt"
	"strings"
	"time"
)

// SyncGenerator generates synchronization scripts
type SyncGenerator struct {
	options SyncOptions
}

// NewSyncGenerator creates a new sync generator
func NewSyncGenerator(options SyncOptions) *SyncGenerator {
	return &SyncGenerator{
		options: options,
	}
}

// GenerateSyncScript generates SQL script to synchronize schemas
func (g *SyncGenerator) GenerateSyncScript(result *ComparisonResult) string {
	var script strings.Builder

	// Add header
	g.writeHeader(&script, result)

	// Add transaction wrapper if requested
	if g.options.UseTransactions {
		script.WriteString("BEGIN TRANSACTION;\n\n")
	}

	// Group differences by object type for proper ordering
	diffsByType := g.groupDifferencesByType(result.Differences)

	// Process object types in dependency order
	objectOrder := g.getObjectOrder()
	
	for _, objType := range objectOrder {
		if diffs, exists := diffsByType[objType]; exists {
			g.processObjectTypeDiffs(&script, objType, diffs)
		}
	}

	// Add transaction commit if requested
	if g.options.UseTransactions {
		script.WriteString("\nCOMMIT;\n")
	}

	// Store the script in the result
	syncScript := script.String()
	result.SyncScript = syncScript

	return syncScript
}

// GenerateObjectSync generates sync SQL for a specific difference
func (g *SyncGenerator) GenerateObjectSync(diff Difference) string {
	switch diff.DiffType {
	case DiffTypeAdded:
		if g.options.IncludeDrops && !g.options.SafeMode {
			return g.generateDropStatement(diff)
		}
	case DiffTypeRemoved:
		if g.options.IncludeCreates {
			return g.generateCreateStatement(diff)
		}
	case DiffTypeModified:
		if g.options.IncludeAlters {
			return g.generateAlterStatement(diff)
		}
	}
	return ""
}

// writeHeader writes the script header
func (g *SyncGenerator) writeHeader(script *strings.Builder, result *ComparisonResult) {
	script.WriteString("-- Schema Synchronization Script\n")
	script.WriteString(fmt.Sprintf("-- Generated: %s\n", time.Now().Format(time.RFC3339)))
	script.WriteString(fmt.Sprintf("-- Source: %s\n", result.SourceEnvironment))
	script.WriteString(fmt.Sprintf("-- Target: %s\n", result.TargetEnvironment))
	script.WriteString(fmt.Sprintf("-- Total Differences: %d\n", len(result.Differences)))
	script.WriteString("-- \n")
	script.WriteString("-- WARNING: Review this script carefully before execution!\n")
	script.WriteString("-- \n\n")

	if g.options.SafeMode {
		script.WriteString("-- SAFE MODE: Drop statements for objects only in target are excluded\n\n")
	}
}

// groupDifferencesByType groups differences by object type
func (g *SyncGenerator) groupDifferencesByType(differences []Difference) map[ObjectType][]Difference {
	grouped := make(map[ObjectType][]Difference)
	
	for _, diff := range differences {
		grouped[diff.ObjectType] = append(grouped[diff.ObjectType], diff)
	}
	
	return grouped
}

// getObjectOrder returns the order in which object types should be processed
func (g *SyncGenerator) getObjectOrder() []ObjectType {
	if len(g.options.ObjectOrder) > 0 {
		return g.options.ObjectOrder
	}
	
	// Default order considering dependencies
	return []ObjectType{
		ObjectTypeSequence,
		ObjectTypeFileFormat,
		ObjectTypeStage,
		ObjectTypeTable,
		ObjectTypeView,
		ObjectTypeFunction,
		ObjectTypeProcedure,
		ObjectTypeStream,
		ObjectTypePipe,
		ObjectTypeTask,
	}
}

// processObjectTypeDiffs processes differences for a specific object type
func (g *SyncGenerator) processObjectTypeDiffs(script *strings.Builder, objType ObjectType, diffs []Difference) {
	if len(diffs) == 0 {
		return
	}

	// Add section header
	script.WriteString(fmt.Sprintf("\n-- %s Changes (%d)\n", objType, len(diffs)))
	script.WriteString(strings.Repeat("-", 60) + "\n\n")

	// Process removals first (drops)
	for _, diff := range diffs {
		if diff.DiffType == DiffTypeAdded && g.options.IncludeDrops && !g.options.SafeMode {
			g.writeDifference(script, diff)
		}
	}

	// Process modifications
	for _, diff := range diffs {
		if diff.DiffType == DiffTypeModified && g.options.IncludeAlters {
			g.writeDifference(script, diff)
		}
	}

	// Process additions last (creates)
	for _, diff := range diffs {
		if diff.DiffType == DiffTypeRemoved && g.options.IncludeCreates {
			g.writeDifference(script, diff)
		}
	}
}

// writeDifference writes a single difference to the script
func (g *SyncGenerator) writeDifference(script *strings.Builder, diff Difference) {
	if g.options.AddComments {
		script.WriteString(fmt.Sprintf("-- %s\n", diff.Description))
		if len(diff.Details) > 0 {
			for _, detail := range diff.Details {
				script.WriteString(fmt.Sprintf("--   %s\n", detail.Description))
			}
		}
	}

	sql := g.GenerateObjectSync(diff)
	if sql != "" {
		script.WriteString(sql)
		script.WriteString("\n\n")
		
		// Update the difference with the generated SQL
		diff.SyncSQL = sql
	}
}

// generateDropStatement generates a DROP statement
func (g *SyncGenerator) generateDropStatement(diff Difference) string {
	if diff.TargetObject == nil {
		return ""
	}

	fullName := g.getFullObjectName(diff.TargetObject)
	
	switch diff.ObjectType {
	case ObjectTypeTable:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", fullName)
	case ObjectTypeView:
		return fmt.Sprintf("DROP VIEW IF EXISTS %s;", fullName)
	case ObjectTypeProcedure:
		return fmt.Sprintf("DROP PROCEDURE IF EXISTS %s;", fullName)
	case ObjectTypeFunction:
		return fmt.Sprintf("DROP FUNCTION IF EXISTS %s;", fullName)
	case ObjectTypeSequence:
		return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s;", fullName)
	case ObjectTypeStage:
		return fmt.Sprintf("DROP STAGE IF EXISTS %s;", fullName)
	case ObjectTypeFileFormat:
		return fmt.Sprintf("DROP FILE FORMAT IF EXISTS %s;", fullName)
	case ObjectTypeStream:
		return fmt.Sprintf("DROP STREAM IF EXISTS %s;", fullName)
	case ObjectTypePipe:
		return fmt.Sprintf("DROP PIPE IF EXISTS %s;", fullName)
	case ObjectTypeTask:
		return fmt.Sprintf("DROP TASK IF EXISTS %s;", fullName)
	default:
		return fmt.Sprintf("-- DROP %s %s (type not supported)", diff.ObjectType, fullName)
	}
}

// generateCreateStatement generates a CREATE statement
func (g *SyncGenerator) generateCreateStatement(diff Difference) string {
	if diff.SourceObject == nil {
		return ""
	}

	// If we have the full definition, use it
	if diff.SourceObject.Definition != "" {
		if g.options.GenerateBackup {
			backup := g.generateBackupStatement(diff)
			if backup != "" {
				return backup + "\n\n" + diff.SourceObject.Definition
			}
		}
		return diff.SourceObject.Definition
	}

	// Otherwise, generate a placeholder
	fullName := g.getFullObjectName(diff.SourceObject)
	return fmt.Sprintf("-- CREATE %s %s\n-- Definition not available. Use GET_DDL to retrieve full definition.", 
		diff.ObjectType, fullName)
}

// generateAlterStatement generates ALTER statements for modifications
func (g *SyncGenerator) generateAlterStatement(diff Difference) string {
	if diff.SourceObject == nil || diff.TargetObject == nil {
		return ""
	}

	var statements []string
	fullName := g.getFullObjectName(diff.SourceObject)

	// Generate ALTER statements based on differences
	for _, detail := range diff.Details {
		switch detail.Property {
		case "Comment":
			stmt := g.generateCommentStatement(diff.ObjectType, fullName, detail.TargetValue)
			if stmt != "" {
				statements = append(statements, stmt)
			}
		case "Owner":
			stmt := g.generateOwnerStatement(diff.ObjectType, fullName, detail.TargetValue)
			if stmt != "" {
				statements = append(statements, stmt)
			}
		case "Definition":
			// For views and procedures, we need to recreate
			if diff.ObjectType == ObjectTypeView || 
			   diff.ObjectType == ObjectTypeProcedure || 
			   diff.ObjectType == ObjectTypeFunction {
				drop := g.generateDropStatement(diff)
				create := g.generateCreateStatement(diff)
				if drop != "" && create != "" {
					statements = append(statements, drop, create)
				}
			}
		default:
			// Generate generic ALTER statement
			stmt := fmt.Sprintf("-- ALTER %s %s\n-- Property '%s' changed from '%v' to '%v'",
				diff.ObjectType, fullName, detail.Property, detail.SourceValue, detail.TargetValue)
			statements = append(statements, stmt)
		}
	}

	return strings.Join(statements, "\n\n")
}

// generateBackupStatement generates a backup statement
func (g *SyncGenerator) generateBackupStatement(diff Difference) string {
	if diff.TargetObject == nil {
		return ""
	}

	fullName := g.getFullObjectName(diff.TargetObject)
	backupName := fmt.Sprintf("%s_BACKUP_%s", diff.TargetObject.Name, time.Now().Format("20060102_150405"))
	
	switch diff.ObjectType {
	case ObjectTypeTable:
		return fmt.Sprintf("-- Backup existing table\nCREATE TABLE %s.%s.%s AS SELECT * FROM %s;",
			diff.Database, diff.Schema, backupName, fullName)
	case ObjectTypeView:
		return fmt.Sprintf("-- Backup existing view\nCREATE VIEW %s.%s.%s AS SELECT * FROM %s;",
			diff.Database, diff.Schema, backupName, fullName)
	default:
		return fmt.Sprintf("-- Backup of %s %s recommended before modification", diff.ObjectType, fullName)
	}
}

// generateCommentStatement generates a COMMENT statement
func (g *SyncGenerator) generateCommentStatement(objType ObjectType, fullName string, newComment interface{}) string {
	comment := ""
	if c, ok := newComment.(string); ok {
		comment = c
	}

	switch objType {
	case ObjectTypeTable:
		return fmt.Sprintf("COMMENT ON TABLE %s IS '%s';", fullName, comment)
	case ObjectTypeView:
		return fmt.Sprintf("COMMENT ON VIEW %s IS '%s';", fullName, comment)
	case ObjectTypeProcedure:
		return fmt.Sprintf("COMMENT ON PROCEDURE %s IS '%s';", fullName, comment)
	case ObjectTypeFunction:
		return fmt.Sprintf("COMMENT ON FUNCTION %s IS '%s';", fullName, comment)
	default:
		return fmt.Sprintf("COMMENT ON %s %s IS '%s';", objType, fullName, comment)
	}
}

// generateOwnerStatement generates an ownership change statement
func (g *SyncGenerator) generateOwnerStatement(objType ObjectType, fullName string, newOwner interface{}) string {
	owner := ""
	if o, ok := newOwner.(string); ok {
		owner = o
	}

	if owner == "" {
		return ""
	}

	return fmt.Sprintf("GRANT OWNERSHIP ON %s %s TO ROLE %s;", objType, fullName, owner)
}

// getFullObjectName returns the fully qualified object name
func (g *SyncGenerator) getFullObjectName(obj *SchemaObject) string {
	return fmt.Sprintf("%s.%s.%s", obj.Database, obj.Schema, obj.Name)
}

// GenerateSelectiveSyncScript generates a sync script for selected differences
func (g *SyncGenerator) GenerateSelectiveSyncScript(result *ComparisonResult, selectedIndices []int) string {
	// Create a new result with only selected differences
	selectedResult := &ComparisonResult{
		SourceEnvironment: result.SourceEnvironment,
		TargetEnvironment: result.TargetEnvironment,
		ComparedAt:        result.ComparedAt,
		Differences:       []Difference{},
		Summary:           result.Summary,
	}

	// Add selected differences
	for _, idx := range selectedIndices {
		if idx >= 0 && idx < len(result.Differences) {
			selectedResult.Differences = append(selectedResult.Differences, result.Differences[idx])
		}
	}

	return g.GenerateSyncScript(selectedResult)
}