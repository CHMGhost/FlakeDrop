package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
	
	"flakedrop/internal/common"
)

// VersionStore manages schema versions
type VersionStore struct {
	basePath string
	service  *Service
}

// NewVersionStore creates a new version store
func NewVersionStore(basePath string, service *Service) *VersionStore {
	return &VersionStore{
		basePath: basePath,
		service:  service,
	}
}

// CaptureVersion captures the current schema state
func (v *VersionStore) CaptureVersion(ctx context.Context, environment, database, schema string, description string) (*SchemaVersion, error) {
	// Create version
	version := &SchemaVersion{
		Version:     v.generateVersionID(),
		Environment: environment,
		CapturedAt:  time.Now(),
		CapturedBy:  v.getCurrentUser(),
		Description: description,
		Objects:     []SchemaObject{},
		Metadata:    make(map[string]interface{}),
	}

	// Capture all objects
	objects, err := v.service.GetDatabaseObjects(ctx, database, schema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to capture objects: %w", err)
	}

	// Capture detailed information for each object
	for i, obj := range objects {
		// Get object DDL
		ddl, err := v.getObjectDDL(ctx, obj)
		if err == nil {
			objects[i].Definition = ddl
		}

		// Get additional metadata based on object type
		switch obj.Type {
		case ObjectTypeTable:
			if details, err := v.service.GetTableDetails(ctx, database, schema, obj.Name); err == nil {
				objects[i].Metadata = v.tableDetailsToMetadata(details)
			}
		case ObjectTypeView:
			if details, err := v.service.GetViewDetails(ctx, database, schema, obj.Name); err == nil {
				objects[i].Metadata = v.viewDetailsToMetadata(details)
			}
		case ObjectTypeProcedure:
			if details, err := v.service.GetProcedureDetails(ctx, database, schema, obj.Name); err == nil {
				objects[i].Metadata = v.procedureDetailsToMetadata(details)
			}
		}
	}

	version.Objects = objects

	// Add metadata
	version.Metadata["database"] = database
	version.Metadata["schema"] = schema
	version.Metadata["object_count"] = len(objects)
	version.Metadata["capture_duration"] = time.Since(version.CapturedAt).Seconds()

	// Save version
	if err := v.SaveVersion(version); err != nil {
		return nil, fmt.Errorf("failed to save version: %w", err)
	}

	return version, nil
}

// SaveVersion saves a schema version to disk
func (v *VersionStore) SaveVersion(version *SchemaVersion) error {
	// Create directory structure
	envPath := filepath.Join(v.basePath, "versions", version.Environment)
	if err := os.MkdirAll(envPath, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Generate filename
	filename := fmt.Sprintf("%s_%s.json", 
		version.CapturedAt.Format("20060102_150405"), 
		version.Version)
	fullPath := filepath.Join(envPath, filename)

	// Marshal version
	data, err := json.MarshalIndent(version, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal version: %w", err)
	}

	// Write file
	if err := os.WriteFile(fullPath, data, common.FilePermissionSecure); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Create latest symlink
	latestPath := filepath.Join(envPath, "latest.json")
	os.Remove(latestPath) // Remove existing symlink
	if err := os.Symlink(filename, latestPath); err != nil {
		// Not critical if symlink fails
		fmt.Printf("Warning: failed to create latest symlink: %v\n", err)
	}

	return nil
}

// LoadVersion loads a schema version from disk
func (v *VersionStore) LoadVersion(environment, versionID string) (*SchemaVersion, error) {
	// Find version file
	envPath := filepath.Join(v.basePath, "versions", environment)
	pattern := filepath.Join(envPath, fmt.Sprintf("*_%s.json", versionID))
	
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to find version: %w", err)
	}
	
	if len(matches) == 0 {
		return nil, fmt.Errorf("version %s not found for environment %s", versionID, environment)
	}

	// Read file
	data, err := os.ReadFile(matches[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read version file: %w", err)
	}

	// Unmarshal version
	var version SchemaVersion
	if err := json.Unmarshal(data, &version); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version: %w", err)
	}

	return &version, nil
}

// LoadLatestVersion loads the latest version for an environment
func (v *VersionStore) LoadLatestVersion(environment string) (*SchemaVersion, error) {
	latestPath := filepath.Join(v.basePath, "versions", environment, "latest.json")
	
	// Validate the path
	validatedPath, err := common.ValidatePath(latestPath, v.basePath)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	
	// Try to read symlink first
	data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err == nil {
		var version SchemaVersion
		if err := json.Unmarshal(data, &version); err == nil {
			return &version, nil
		}
	}

	// Fall back to finding the newest file
	envPath := filepath.Join(v.basePath, "versions", environment)
	pattern := filepath.Join(envPath, "*.json")
	
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}
	
	if len(matches) == 0 {
		return nil, fmt.Errorf("no versions found for environment %s", environment)
	}

	// Find the newest file
	var newestFile string
	var newestTime time.Time
	
	for _, match := range matches {
		if filepath.Base(match) == "latest.json" {
			continue
		}
		
		info, err := os.Stat(match)
		if err != nil {
			continue
		}
		
		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newestFile = match
		}
	}

	if newestFile == "" {
		return nil, fmt.Errorf("no valid versions found for environment %s", environment)
	}

	// Validate and read file
	validatedFile, err := common.ValidatePath(newestFile, v.basePath)
	if err != nil {
		return nil, fmt.Errorf("invalid file path: %w", err)
	}
	
	// Read file
	data, err = os.ReadFile(validatedFile) // #nosec G304 - path is validated
	if err != nil {
		return nil, fmt.Errorf("failed to read version file: %w", err)
	}

	// Unmarshal version
	var version SchemaVersion
	if err := json.Unmarshal(data, &version); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version: %w", err)
	}

	return &version, nil
}

// ListVersions lists all versions for an environment
func (v *VersionStore) ListVersions(environment string) ([]SchemaVersion, error) {
	envPath := filepath.Join(v.basePath, "versions", environment)
	pattern := filepath.Join(envPath, "*.json")
	
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	var versions []SchemaVersion
	
	for _, match := range matches {
		if filepath.Base(match) == "latest.json" {
			continue
		}
		
		// Validate the file path
		validatedMatch, err := common.ValidatePath(match, v.basePath)
		if err != nil {
			continue
		}
		
		data, err := os.ReadFile(validatedMatch) // #nosec G304 - path is validated
		if err != nil {
			continue
		}
		
		var version SchemaVersion
		if err := json.Unmarshal(data, &version); err != nil {
			continue
		}
		
		versions = append(versions, version)
	}

	return versions, nil
}

// CompareVersions compares two stored versions
func (v *VersionStore) CompareVersions(environment1, version1, environment2, version2 string) (*ComparisonResult, error) {
	// Load versions
	v1, err := v.LoadVersion(environment1, version1)
	if err != nil {
		return nil, fmt.Errorf("failed to load version 1: %w", err)
	}

	v2, err := v.LoadVersion(environment2, version2)
	if err != nil {
		return nil, fmt.Errorf("failed to load version 2: %w", err)
	}

	// Create snapshots
	snapshot1 := SchemaSnapshot{
		Environment: v1.Environment,
		Objects:     v1.Objects,
		CapturedAt:  v1.CapturedAt,
	}

	snapshot2 := SchemaSnapshot{
		Environment: v2.Environment,
		Objects:     v2.Objects,
		CapturedAt:  v2.CapturedAt,
	}

	// Compare
	comparator := NewComparator(v.service, ComparisonOptions{})
	return comparator.CompareSchemas(context.Background(), snapshot1, snapshot2)
}

// Helper methods

func (v *VersionStore) generateVersionID() string {
	return fmt.Sprintf("v%d", time.Now().Unix())
}

func (v *VersionStore) getCurrentUser() string {
	if user := os.Getenv("USER"); user != "" {
		return user
	}
	if user := os.Getenv("USERNAME"); user != "" {
		return user
	}
	return "unknown"
}

func (v *VersionStore) getObjectDDL(ctx context.Context, obj SchemaObject) (string, error) {
	query := fmt.Sprintf("SELECT GET_DDL('%s', '%s.%s.%s')", 
		obj.Type, obj.Database, obj.Schema, obj.Name)
	
	var ddl string
	err := v.service.GetDB().QueryRowContext(ctx, query).Scan(&ddl)
	if err != nil {
		return "", err
	}
	
	return ddl, nil
}

func (v *VersionStore) tableDetailsToMetadata(details *TableDetails) map[string]interface{} {
	metadata := make(map[string]interface{})
	
	metadata["column_count"] = len(details.Columns)
	metadata["constraint_count"] = len(details.Constraints)
	metadata["index_count"] = len(details.Indexes)
	metadata["row_count"] = details.RowCount
	metadata["size_bytes"] = details.SizeBytes
	
	// Column names
	columnNames := make([]string, len(details.Columns))
	for i, col := range details.Columns {
		columnNames[i] = col.Name
	}
	metadata["columns"] = columnNames
	
	return metadata
}

func (v *VersionStore) viewDetailsToMetadata(details *ViewDetails) map[string]interface{} {
	metadata := make(map[string]interface{})
	
	metadata["is_secure"] = details.IsSecure
	metadata["is_materialized"] = details.IsMaterialized
	
	return metadata
}

func (v *VersionStore) procedureDetailsToMetadata(details *ProcedureDetails) map[string]interface{} {
	metadata := make(map[string]interface{})
	
	metadata["language"] = details.Language
	metadata["argument_count"] = len(details.Arguments)
	metadata["return_type"] = details.ReturnType
	metadata["is_secure"] = details.IsSecure
	
	return metadata
}

// CleanupOldVersions removes versions older than the specified duration
func (v *VersionStore) CleanupOldVersions(environment string, maxAge time.Duration) error {
	versions, err := v.ListVersions(environment)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for _, version := range versions {
		if version.CapturedAt.Before(cutoff) {
			// Find and remove the file
			envPath := filepath.Join(v.basePath, "versions", environment)
			pattern := filepath.Join(envPath, fmt.Sprintf("*_%s.json", version.Version))
			
			matches, err := filepath.Glob(pattern)
			if err != nil {
				continue
			}
			
			for _, match := range matches {
				if err := os.Remove(match); err != nil {
					fmt.Printf("Warning: failed to remove old version %s: %v\n", match, err)
				} else {
					removed++
				}
			}
		}
	}

	if removed > 0 {
		fmt.Printf("Removed %d old versions from %s\n", removed, environment)
	}

	return nil
}

// GetBasePath returns the base path for version storage
func (v *VersionStore) GetBasePath() string {
	return v.basePath
}