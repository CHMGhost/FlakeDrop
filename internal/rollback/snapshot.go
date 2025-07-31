package rollback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// SnapshotManager manages schema snapshots
type SnapshotManager struct {
	snowflakeService *snowflake.Service
	storageDir       string
	mu               sync.RWMutex
	snapshots        map[string]*SchemaSnapshot
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(snowflakeService *snowflake.Service, storageDir string) (*SnapshotManager, error) {
	snapshotDir := filepath.Join(storageDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, common.DirPermissionNormal); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	sm := &SnapshotManager{
		snowflakeService: snowflakeService,
		storageDir:       snapshotDir,
		snapshots:        make(map[string]*SchemaSnapshot),
	}

	// Load existing snapshots
	if err := sm.loadSnapshots(); err != nil {
		return nil, fmt.Errorf("failed to load snapshots: %w", err)
	}

	return sm, nil
}

// CreateSnapshot creates a snapshot of a schema
func (sm *SnapshotManager) CreateSnapshot(ctx context.Context, deploymentID, database, schema string) (*SchemaSnapshot, error) {
	snapshot := &SchemaSnapshot{
		ID:           sm.generateSnapshotID(),
		DeploymentID: deploymentID,
		Database:     database,
		Schema:       schema,
		Timestamp:    time.Now(),
		Objects:      []SchemaObject{},
		Metadata: map[string]interface{}{
			"created_by": "rollback_manager",
			"version":    "1.0",
		},
	}

	// Get schema objects
	objects, err := sm.captureSchemaObjects(ctx, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture schema objects: %w", err)
	}
	snapshot.Objects = objects

	// Store snapshot
	sm.mu.Lock()
	sm.snapshots[snapshot.ID] = snapshot
	sm.mu.Unlock()

	// Persist to disk
	if err := sm.saveSnapshot(snapshot); err != nil {
		return nil, fmt.Errorf("failed to save snapshot: %w", err)
	}

	return snapshot, nil
}

// GetSnapshot retrieves a snapshot by ID
func (sm *SnapshotManager) GetSnapshot(id string) (*SchemaSnapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snapshot, exists := sm.snapshots[id]
	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "snapshot not found").
			WithContext("snapshot_id", id)
	}

	return snapshot, nil
}

// ListSnapshots lists all snapshots for a database/schema
func (sm *SnapshotManager) ListSnapshots(database, schema string) ([]*SchemaSnapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var snapshots []*SchemaSnapshot
	for _, snapshot := range sm.snapshots {
		if snapshot.Database == database && snapshot.Schema == schema {
			snapshots = append(snapshots, snapshot)
		}
	}

	return snapshots, nil
}

// CompareSnapshots compares two snapshots and returns differences
func (sm *SnapshotManager) CompareSnapshots(snapshot1ID, snapshot2ID string) (*SnapshotDiff, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snapshot1, exists1 := sm.snapshots[snapshot1ID]
	snapshot2, exists2 := sm.snapshots[snapshot2ID]

	if !exists1 || !exists2 {
		return nil, errors.New(errors.ErrCodeNotFound, "one or both snapshots not found")
	}

	diff := &SnapshotDiff{
		Snapshot1ID: snapshot1ID,
		Snapshot2ID: snapshot2ID,
		Timestamp:   time.Now(),
		Added:       []SchemaObject{},
		Modified:    []ObjectModification{},
		Removed:     []SchemaObject{},
	}

	// Create maps for efficient comparison
	objects1 := make(map[string]*SchemaObject)
	objects2 := make(map[string]*SchemaObject)

	for i := range snapshot1.Objects {
		key := fmt.Sprintf("%s:%s", snapshot1.Objects[i].Type, snapshot1.Objects[i].Name)
		objects1[key] = &snapshot1.Objects[i]
	}

	for i := range snapshot2.Objects {
		key := fmt.Sprintf("%s:%s", snapshot2.Objects[i].Type, snapshot2.Objects[i].Name)
		objects2[key] = &snapshot2.Objects[i]
	}

	// Find added and modified objects
	for key, obj2 := range objects2 {
		if obj1, exists := objects1[key]; exists {
			// Object exists in both - check if modified
			if obj1.Checksum != obj2.Checksum {
				diff.Modified = append(diff.Modified, ObjectModification{
					Object:     *obj2,
					OldChecksum: obj1.Checksum,
					NewChecksum: obj2.Checksum,
				})
			}
		} else {
			// Object only exists in snapshot2 - added
			diff.Added = append(diff.Added, *obj2)
		}
	}

	// Find removed objects
	for key, obj1 := range objects1 {
		if _, exists := objects2[key]; !exists {
			// Object only exists in snapshot1 - removed
			diff.Removed = append(diff.Removed, *obj1)
		}
	}

	return diff, nil
}

// RestoreSnapshot restores a schema to a snapshot state
func (sm *SnapshotManager) RestoreSnapshot(ctx context.Context, snapshotID string, dryRun bool) (*RestoreResult, error) {
	snapshot, err := sm.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	result := &RestoreResult{
		SnapshotID: snapshotID,
		StartTime:  time.Now(),
		DryRun:     dryRun,
		Operations: []RestoreOperation{},
		Success:    true,
	}

	// Get current state
	currentObjects, err := sm.captureSchemaObjects(ctx, snapshot.Database, snapshot.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture current state: %w", err)
	}

	// Generate restore operations
	operations := sm.generateRestoreOperations(snapshot.Objects, currentObjects)
	result.Operations = operations

	if !dryRun {
		// Execute restore operations
		for i := range operations {
			op := &operations[i]
			op.StartTime = time.Now()

			if err := sm.executeRestoreOperation(ctx, snapshot.Database, snapshot.Schema, op); err != nil {
				op.Success = false
				op.Error = err.Error()
				result.Success = false
				break
			}

			op.Success = true
			op.EndTime = timePtr(time.Now())
		}
	}

	result.EndTime = timePtr(time.Now())
	return result, nil
}

// Private methods

func (sm *SnapshotManager) captureSchemaObjects(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	var objects []SchemaObject

	// Capture tables
	tables, err := sm.captureTables(ctx, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture tables: %w", err)
	}
	objects = append(objects, tables...)

	// Capture views
	views, err := sm.captureViews(ctx, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture views: %w", err)
	}
	objects = append(objects, views...)

	// Capture procedures
	procedures, err := sm.captureProcedures(ctx, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture procedures: %w", err)
	}
	objects = append(objects, procedures...)

	// Capture functions
	functions, err := sm.captureFunctions(ctx, database, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to capture functions: %w", err)
	}
	objects = append(objects, functions...)

	return objects, nil
}

func (sm *SnapshotManager) captureTables(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf(`
		SELECT table_name, created, last_altered
		FROM %s.information_schema.tables
		WHERE table_schema = '%s'
		AND table_type = 'BASE TABLE'
	`, database, schema)

	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		var name string
		var created, modified time.Time
		if err := rows.Scan(&name, &created, &modified); err != nil {
			return nil, err
		}

		// Get table DDL
		ddl, err := sm.getTableDDL(ctx, database, schema, name)
		if err != nil {
			continue // Skip tables we can't get DDL for
		}

		objects = append(objects, SchemaObject{
			Type:       "TABLE",
			Name:       name,
			Definition: ddl,
			Checksum:   sm.calculateChecksum(ddl),
			CreatedAt:  created,
			ModifiedAt: modified,
		})
	}

	return objects, rows.Err()
}

func (sm *SnapshotManager) captureViews(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf(`
		SELECT table_name, created, last_altered
		FROM %s.information_schema.views
		WHERE table_schema = '%s'
	`, database, schema)

	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	for rows.Next() {
		var name string
		var created, modified time.Time
		if err := rows.Scan(&name, &created, &modified); err != nil {
			return nil, err
		}

		// Get view DDL
		ddl, err := sm.getViewDDL(ctx, database, schema, name)
		if err != nil {
			continue
		}

		objects = append(objects, SchemaObject{
			Type:       "VIEW",
			Name:       name,
			Definition: ddl,
			Checksum:   sm.calculateChecksum(ddl),
			CreatedAt:  created,
			ModifiedAt: modified,
		})
	}

	return objects, rows.Err()
}

func (sm *SnapshotManager) captureProcedures(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf(`
		SHOW PROCEDURES IN SCHEMA %s.%s
	`, database, schema)

	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	// Process procedures (implementation depends on Snowflake response format)
	
	return objects, nil
}

func (sm *SnapshotManager) captureFunctions(ctx context.Context, database, schema string) ([]SchemaObject, error) {
	query := fmt.Sprintf(`
		SHOW FUNCTIONS IN SCHEMA %s.%s
	`, database, schema)

	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []SchemaObject
	// Process functions (implementation depends on Snowflake response format)
	
	return objects, nil
}

func (sm *SnapshotManager) getTableDDL(ctx context.Context, database, schema, table string) (string, error) {
	query := fmt.Sprintf("SELECT GET_DDL('TABLE', '%s.%s.%s')", database, schema, table)
	
	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var ddl string
		if err := rows.Scan(&ddl); err != nil {
			return "", err
		}
		return ddl, nil
	}

	return "", fmt.Errorf("no DDL found for table %s", table)
}

func (sm *SnapshotManager) getViewDDL(ctx context.Context, database, schema, view string) (string, error) {
	query := fmt.Sprintf("SELECT GET_DDL('VIEW', '%s.%s.%s')", database, schema, view)
	
	rows, err := sm.snowflakeService.ExecuteQueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var ddl string
		if err := rows.Scan(&ddl); err != nil {
			return "", err
		}
		return ddl, nil
	}

	return "", fmt.Errorf("no DDL found for view %s", view)
}

func (sm *SnapshotManager) calculateChecksum(content string) string {
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

func (sm *SnapshotManager) generateSnapshotID() string {
	return fmt.Sprintf("snapshot-%d-%s", time.Now().Unix(), generateRandomID(8))
}

func (sm *SnapshotManager) generateRestoreOperations(targetObjects, currentObjects []SchemaObject) []RestoreOperation {
	var operations []RestoreOperation

	// Create maps for efficient lookup
	targetMap := make(map[string]*SchemaObject)
	currentMap := make(map[string]*SchemaObject)

	for i := range targetObjects {
		key := fmt.Sprintf("%s:%s", targetObjects[i].Type, targetObjects[i].Name)
		targetMap[key] = &targetObjects[i]
	}

	for i := range currentObjects {
		key := fmt.Sprintf("%s:%s", currentObjects[i].Type, currentObjects[i].Name)
		currentMap[key] = &currentObjects[i]
	}

	// Drop objects that don't exist in target
	for key, current := range currentMap {
		if _, exists := targetMap[key]; !exists {
			operations = append(operations, RestoreOperation{
				Type:        "DROP",
				ObjectType:  current.Type,
				ObjectName:  current.Name,
				SQL:         fmt.Sprintf("DROP %s IF EXISTS %s", current.Type, current.Name),
				Description: fmt.Sprintf("Drop %s %s", strings.ToLower(current.Type), current.Name),
			})
		}
	}

	// Create or replace objects from target
	for key, target := range targetMap {
		if current, exists := currentMap[key]; exists {
			// Object exists - check if it needs to be updated
			if current.Checksum != target.Checksum {
				operations = append(operations, RestoreOperation{
					Type:        "REPLACE",
					ObjectType:  target.Type,
					ObjectName:  target.Name,
					SQL:         target.Definition,
					Description: fmt.Sprintf("Replace %s %s", strings.ToLower(target.Type), target.Name),
				})
			}
		} else {
			// Object doesn't exist - create it
			operations = append(operations, RestoreOperation{
				Type:        "CREATE",
				ObjectType:  target.Type,
				ObjectName:  target.Name,
				SQL:         target.Definition,
				Description: fmt.Sprintf("Create %s %s", strings.ToLower(target.Type), target.Name),
			})
		}
	}

	return operations
}

func (sm *SnapshotManager) executeRestoreOperation(ctx context.Context, database, schema string, op *RestoreOperation) error {
	return sm.snowflakeService.ExecuteSQL(op.SQL, database, schema)
}

func (sm *SnapshotManager) saveSnapshot(snapshot *SchemaSnapshot) error {
	filename := fmt.Sprintf("%s.json", snapshot.ID)
	filepath := filepath.Join(sm.storageDir, filename)

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath, data, 0600)
}

func (sm *SnapshotManager) loadSnapshots() error {
	pattern := filepath.Join(sm.storageDir, "snapshot-*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, file := range files {
		// Validate the file path
		validatedPath, err := common.ValidatePath(file, sm.storageDir)
		if err != nil {
			continue
		}
		
		data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
		if err != nil {
			continue
		}

		var snapshot SchemaSnapshot
		if err := json.Unmarshal(data, &snapshot); err != nil {
			continue
		}

		sm.snapshots[snapshot.ID] = &snapshot
	}

	return nil
}

// Helper types

// SnapshotDiff represents differences between two snapshots
type SnapshotDiff struct {
	Snapshot1ID string                `json:"snapshot1_id"`
	Snapshot2ID string                `json:"snapshot2_id"`
	Timestamp   time.Time             `json:"timestamp"`
	Added       []SchemaObject        `json:"added"`
	Modified    []ObjectModification  `json:"modified"`
	Removed     []SchemaObject        `json:"removed"`
}

// ObjectModification represents a modified object
type ObjectModification struct {
	Object      SchemaObject `json:"object"`
	OldChecksum string       `json:"old_checksum"`
	NewChecksum string       `json:"new_checksum"`
}

// RestoreResult represents the result of a restore operation
type RestoreResult struct {
	SnapshotID string             `json:"snapshot_id"`
	StartTime  time.Time          `json:"start_time"`
	EndTime    *time.Time         `json:"end_time,omitempty"`
	DryRun     bool               `json:"dry_run"`
	Operations []RestoreOperation `json:"operations"`
	Success    bool               `json:"success"`
}

// RestoreOperation represents a single restore operation
type RestoreOperation struct {
	Type        string     `json:"type"` // CREATE, DROP, REPLACE
	ObjectType  string     `json:"object_type"`
	ObjectName  string     `json:"object_name"`
	SQL         string     `json:"sql"`
	Description string     `json:"description"`
	StartTime   time.Time  `json:"start_time,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	Success     bool       `json:"success"`
	Error       string     `json:"error,omitempty"`
}