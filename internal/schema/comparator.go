package schema

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Comparator provides schema comparison functionality
type Comparator struct {
	service      *Service
	options      ComparisonOptions
	ignoreRules  []IgnoreRule
}

// NewComparator creates a new schema comparator
func NewComparator(service *Service, options ComparisonOptions) *Comparator {
	return &Comparator{
		service:      service,
		options:      options,
		ignoreRules:  []IgnoreRule{},
	}
}

// AddIgnoreRule adds a rule for ignoring certain differences
func (c *Comparator) AddIgnoreRule(rule IgnoreRule) {
	c.ignoreRules = append(c.ignoreRules, rule)
}

// CompareSchemas compares schemas between two environments
func (c *Comparator) CompareSchemas(ctx context.Context, source, target SchemaSnapshot) (*ComparisonResult, error) {
	result := &ComparisonResult{
		SourceEnvironment: source.Environment,
		TargetEnvironment: target.Environment,
		ComparedAt:        time.Now(),
		Differences:       []Difference{},
		Summary:           ComparisonSummary{
			ByObjectType: make(map[ObjectType]ObjectTypeSummary),
		},
	}

	// Create maps for easy lookup
	sourceMap := c.createObjectMap(source.Objects)
	targetMap := c.createObjectMap(target.Objects)

	// Find added and modified objects
	for key, sourceObj := range sourceMap {
		targetObj, exists := targetMap[key]
		
		if !exists {
			// Object exists in source but not in target (removed)
			diff := Difference{
				ObjectName:   sourceObj.Name,
				ObjectType:   sourceObj.Type,
				Database:     sourceObj.Database,
				Schema:       sourceObj.Schema,
				DiffType:     DiffTypeRemoved,
				Description:  fmt.Sprintf("%s %s exists in %s but not in %s", sourceObj.Type, sourceObj.Name, source.Environment, target.Environment),
				SourceObject: &sourceObj,
				TargetObject: nil,
			}
			
			if !c.shouldIgnoreDifference(diff) {
				result.Differences = append(result.Differences, diff)
			}
		} else {
			// Object exists in both - check for modifications
			diffs := c.compareObjects(sourceObj, targetObj)
			if len(diffs) > 0 {
				diff := Difference{
					ObjectName:   sourceObj.Name,
					ObjectType:   sourceObj.Type,
					Database:     sourceObj.Database,
					Schema:       sourceObj.Schema,
					DiffType:     DiffTypeModified,
					Description:  fmt.Sprintf("%s %s has been modified", sourceObj.Type, sourceObj.Name),
					SourceObject: &sourceObj,
					TargetObject: &targetObj,
					Details:      diffs,
				}
				
				if !c.shouldIgnoreDifference(diff) {
					result.Differences = append(result.Differences, diff)
				}
			}
		}
	}

	// Find added objects (exist in target but not in source)
	for key, targetObj := range targetMap {
		if _, exists := sourceMap[key]; !exists {
			diff := Difference{
				ObjectName:   targetObj.Name,
				ObjectType:   targetObj.Type,
				Database:     targetObj.Database,
				Schema:       targetObj.Schema,
				DiffType:     DiffTypeAdded,
				Description:  fmt.Sprintf("%s %s exists in %s but not in %s", targetObj.Type, targetObj.Name, target.Environment, source.Environment),
				SourceObject: nil,
				TargetObject: &targetObj,
			}
			
			if !c.shouldIgnoreDifference(diff) {
				result.Differences = append(result.Differences, diff)
			}
		}
	}

	// Calculate summary
	c.calculateSummary(result, source.Objects, target.Objects)

	// Sort differences for consistent output
	c.sortDifferences(result.Differences)

	return result, nil
}

// CompareObjects compares two schema objects in detail
func (c *Comparator) compareObjects(source, target SchemaObject) []DifferenceDetail {
	var details []DifferenceDetail

	// Compare basic properties
	if !c.options.IgnoreOwner && source.Owner != target.Owner {
		details = append(details, DifferenceDetail{
			Property:    "Owner",
			SourceValue: source.Owner,
			TargetValue: target.Owner,
			Description: fmt.Sprintf("Owner changed from %s to %s", source.Owner, target.Owner),
		})
	}

	if !c.options.IgnoreComments && source.Comment != target.Comment {
		details = append(details, DifferenceDetail{
			Property:    "Comment",
			SourceValue: source.Comment,
			TargetValue: target.Comment,
			Description: "Comment has been modified",
		})
	}

	// Compare definitions if available
	if source.Definition != "" && target.Definition != "" {
		if !c.compareDefinitions(source.Definition, target.Definition) {
			details = append(details, DifferenceDetail{
				Property:    "Definition",
				SourceValue: source.Definition,
				TargetValue: target.Definition,
				Description: "Object definition has changed",
			})
		}
	}

	// Type-specific comparisons
	if c.options.DeepCompare {
		switch source.Type {
		case ObjectTypeTable:
			details = append(details, c.compareTableDetails(source, target)...)
		case ObjectTypeView:
			details = append(details, c.compareViewDetails(source, target)...)
		case ObjectTypeProcedure:
			details = append(details, c.compareProcedureDetails(source, target)...)
		case ObjectTypeFunction:
			details = append(details, c.compareFunctionDetails(source, target)...)
		}
	}

	return details
}

// compareDefinitions compares two SQL definitions
func (c *Comparator) compareDefinitions(source, target string) bool {
	// Normalize definitions for comparison
	sourceDef := c.normalizeSQL(source)
	targetDef := c.normalizeSQL(target)

	return sourceDef == targetDef
}

// normalizeSQL normalizes SQL for comparison
func (c *Comparator) normalizeSQL(sql string) string {
	normalized := sql

	if c.options.IgnoreWhitespace {
		// Replace multiple whitespaces with single space
		normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")
		normalized = strings.TrimSpace(normalized)
	}

	if c.options.IgnoreComments {
		// Remove SQL comments
		// Remove single-line comments
		normalized = regexp.MustCompile(`--[^\n]*`).ReplaceAllString(normalized, "")
		// Remove multi-line comments
		normalized = regexp.MustCompile(`/\*[\s\S]*?\*/`).ReplaceAllString(normalized, "")
	}

	if !c.options.CaseSensitive {
		normalized = strings.ToUpper(normalized)
	}

	return normalized
}

// compareTableDetails compares table-specific details
func (c *Comparator) compareTableDetails(source, target SchemaObject) []DifferenceDetail {
	var details []DifferenceDetail

	// This would be enhanced with actual table metadata comparison
	// For now, we'll use the metadata if available
	if source.Metadata != nil && target.Metadata != nil {
		// Compare column counts
		sourceColumns, _ := source.Metadata["column_count"].(int)
		targetColumns, _ := target.Metadata["column_count"].(int)
		
		if sourceColumns != targetColumns {
			details = append(details, DifferenceDetail{
				Property:    "ColumnCount",
				SourceValue: sourceColumns,
				TargetValue: targetColumns,
				Description: fmt.Sprintf("Number of columns changed from %d to %d", sourceColumns, targetColumns),
			})
		}
	}

	return details
}

// compareViewDetails compares view-specific details
func (c *Comparator) compareViewDetails(source, target SchemaObject) []DifferenceDetail {
	var details []DifferenceDetail

	// Compare view-specific properties from metadata
	if source.Metadata != nil && target.Metadata != nil {
		sourceSecure, _ := source.Metadata["is_secure"].(bool)
		targetSecure, _ := target.Metadata["is_secure"].(bool)
		
		if sourceSecure != targetSecure {
			details = append(details, DifferenceDetail{
				Property:    "IsSecure",
				SourceValue: sourceSecure,
				TargetValue: targetSecure,
				Description: fmt.Sprintf("Secure view setting changed from %v to %v", sourceSecure, targetSecure),
			})
		}

		sourceMaterialized, _ := source.Metadata["is_materialized"].(bool)
		targetMaterialized, _ := target.Metadata["is_materialized"].(bool)
		
		if sourceMaterialized != targetMaterialized {
			details = append(details, DifferenceDetail{
				Property:    "IsMaterialized",
				SourceValue: sourceMaterialized,
				TargetValue: targetMaterialized,
				Description: fmt.Sprintf("Materialized view setting changed from %v to %v", sourceMaterialized, targetMaterialized),
			})
		}
	}

	return details
}

// compareProcedureDetails compares procedure-specific details
func (c *Comparator) compareProcedureDetails(source, target SchemaObject) []DifferenceDetail {
	var details []DifferenceDetail

	// Compare procedure-specific properties from metadata
	if source.Metadata != nil && target.Metadata != nil {
		sourceLanguage, _ := source.Metadata["language"].(string)
		targetLanguage, _ := target.Metadata["language"].(string)
		
		if sourceLanguage != targetLanguage {
			details = append(details, DifferenceDetail{
				Property:    "Language",
				SourceValue: sourceLanguage,
				TargetValue: targetLanguage,
				Description: fmt.Sprintf("Procedure language changed from %s to %s", sourceLanguage, targetLanguage),
			})
		}
	}

	return details
}

// compareFunctionDetails compares function-specific details
func (c *Comparator) compareFunctionDetails(source, target SchemaObject) []DifferenceDetail {
	var details []DifferenceDetail

	// Compare function-specific properties from metadata
	if source.Metadata != nil && target.Metadata != nil {
		sourceLanguage, _ := source.Metadata["language"].(string)
		targetLanguage, _ := target.Metadata["language"].(string)
		
		if sourceLanguage != targetLanguage {
			details = append(details, DifferenceDetail{
				Property:    "Language",
				SourceValue: sourceLanguage,
				TargetValue: targetLanguage,
				Description: fmt.Sprintf("Function language changed from %s to %s", sourceLanguage, targetLanguage),
			})
		}

		sourceReturnType, _ := source.Metadata["return_type"].(string)
		targetReturnType, _ := target.Metadata["return_type"].(string)
		
		if sourceReturnType != targetReturnType {
			details = append(details, DifferenceDetail{
				Property:    "ReturnType",
				SourceValue: sourceReturnType,
				TargetValue: targetReturnType,
				Description: fmt.Sprintf("Return type changed from %s to %s", sourceReturnType, targetReturnType),
			})
		}
	}

	return details
}

// createObjectMap creates a map of objects for easy lookup
func (c *Comparator) createObjectMap(objects []SchemaObject) map[string]SchemaObject {
	objectMap := make(map[string]SchemaObject)
	
	for _, obj := range objects {
		key := c.getObjectKey(obj)
		objectMap[key] = obj
	}
	
	return objectMap
}

// getObjectKey returns a unique key for an object
func (c *Comparator) getObjectKey(obj SchemaObject) string {
	return fmt.Sprintf("%s.%s.%s.%s", obj.Database, obj.Schema, obj.Type, obj.Name)
}

// shouldIgnoreDifference checks if a difference should be ignored
func (c *Comparator) shouldIgnoreDifference(diff Difference) bool {
	// Check exclude patterns
	for _, pattern := range c.options.ExcludePatterns {
		matched, _ := regexp.MatchString(pattern, diff.ObjectName)
		if matched {
			return true
		}
	}

	// Check include patterns (if specified, only include matching)
	if len(c.options.IncludePatterns) > 0 {
		included := false
		for _, pattern := range c.options.IncludePatterns {
			matched, _ := regexp.MatchString(pattern, diff.ObjectName)
			if matched {
				included = true
				break
			}
		}
		if !included {
			return true
		}
	}

	// Check object type filters
	if len(c.options.ExcludeTypes) > 0 {
		for _, t := range c.options.ExcludeTypes {
			if t == diff.ObjectType {
				return true
			}
		}
	}

	if len(c.options.IncludeTypes) > 0 {
		included := false
		for _, t := range c.options.IncludeTypes {
			if t == diff.ObjectType {
				included = true
				break
			}
		}
		if !included {
			return true
		}
	}

	// Check ignore rules
	for _, rule := range c.ignoreRules {
		if c.matchesIgnoreRule(diff, rule) {
			return true
		}
	}

	return false
}

// matchesIgnoreRule checks if a difference matches an ignore rule
func (c *Comparator) matchesIgnoreRule(diff Difference, rule IgnoreRule) bool {
	// Check object type
	if rule.ObjectType != "" && rule.ObjectType != diff.ObjectType {
		return false
	}

	// Check pattern
	if rule.Pattern != "" {
		matched, _ := regexp.MatchString(rule.Pattern, diff.ObjectName)
		if !matched {
			return false
		}
	}

	// Check property (for modified objects)
	if rule.Property != "" && diff.DiffType == DiffTypeModified {
		propertyMatched := false
		for _, detail := range diff.Details {
			if detail.Property == rule.Property {
				propertyMatched = true
				break
			}
		}
		if !propertyMatched {
			return false
		}
	}

	return true
}

// calculateSummary calculates the comparison summary
func (c *Comparator) calculateSummary(result *ComparisonResult, sourceObjects, targetObjects []SchemaObject) {
	summary := &result.Summary
	
	// Count objects by type
	sourceByType := c.countObjectsByType(sourceObjects)
	targetByType := c.countObjectsByType(targetObjects)
	
	// Calculate totals
	for objType := range sourceByType {
		if _, exists := targetByType[objType]; !exists {
			targetByType[objType] = 0
		}
	}
	for objType := range targetByType {
		if _, exists := sourceByType[objType]; !exists {
			sourceByType[objType] = 0
		}
	}

	// Process differences
	for _, diff := range result.Differences {
		switch diff.DiffType {
		case DiffTypeAdded:
			summary.AddedObjects++
			if typeSummary, exists := summary.ByObjectType[diff.ObjectType]; exists {
				typeSummary.Added++
				summary.ByObjectType[diff.ObjectType] = typeSummary
			} else {
				summary.ByObjectType[diff.ObjectType] = ObjectTypeSummary{Added: 1}
			}
		case DiffTypeRemoved:
			summary.RemovedObjects++
			if typeSummary, exists := summary.ByObjectType[diff.ObjectType]; exists {
				typeSummary.Removed++
				summary.ByObjectType[diff.ObjectType] = typeSummary
			} else {
				summary.ByObjectType[diff.ObjectType] = ObjectTypeSummary{Removed: 1}
			}
		case DiffTypeModified:
			summary.ModifiedObjects++
			if typeSummary, exists := summary.ByObjectType[diff.ObjectType]; exists {
				typeSummary.Modified++
				summary.ByObjectType[diff.ObjectType] = typeSummary
			} else {
				summary.ByObjectType[diff.ObjectType] = ObjectTypeSummary{Modified: 1}
			}
		}
	}

	// Calculate totals and matching
	allTypes := make(map[ObjectType]bool)
	for objType := range sourceByType {
		allTypes[objType] = true
	}
	for objType := range targetByType {
		allTypes[objType] = true
	}

	for objType := range allTypes {
		sourceCount := sourceByType[objType]
		targetCount := targetByType[objType]
		
		typeSummary := summary.ByObjectType[objType]
		typeSummary.Total = max(sourceCount, targetCount)
		
		// Calculate matching objects
		typeSummary.Different = typeSummary.Added + typeSummary.Removed + typeSummary.Modified
		typeSummary.Matching = typeSummary.Total - typeSummary.Different
		
		summary.ByObjectType[objType] = typeSummary
		
		summary.TotalObjects += typeSummary.Total
		summary.MatchingObjects += typeSummary.Matching
		summary.DifferentObjects += typeSummary.Different
	}
}

// countObjectsByType counts objects by type
func (c *Comparator) countObjectsByType(objects []SchemaObject) map[ObjectType]int {
	counts := make(map[ObjectType]int)
	
	for _, obj := range objects {
		counts[obj.Type]++
	}
	
	return counts
}

// sortDifferences sorts differences for consistent output
func (c *Comparator) sortDifferences(differences []Difference) {
	sort.Slice(differences, func(i, j int) bool {
		// Sort by type, then by database, schema, and name
		if differences[i].ObjectType != differences[j].ObjectType {
			return differences[i].ObjectType < differences[j].ObjectType
		}
		if differences[i].Database != differences[j].Database {
			return differences[i].Database < differences[j].Database
		}
		if differences[i].Schema != differences[j].Schema {
			return differences[i].Schema < differences[j].Schema
		}
		return differences[i].ObjectName < differences[j].ObjectName
	})
}

// Helper function for max
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SchemaSnapshot represents a snapshot of schema objects
type SchemaSnapshot struct {
	Environment string
	Objects     []SchemaObject
	CapturedAt  time.Time
}