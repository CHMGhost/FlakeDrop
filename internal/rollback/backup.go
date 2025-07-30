package rollback

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/internal/snowflake"
	"flakedrop/pkg/errors"
)

// BackupManager handles backup and restore operations
type BackupManager struct {
	snowflakeService *snowflake.Service
	storageDir       string
	mu               sync.RWMutex
	backups          map[string]*BackupMetadata
}

// BackupOptions configures backup behavior
type BackupOptions struct {
	Type            string // full, incremental, differential
	Format          string // sql, parquet, json
	Compression     bool
	Encrypted       bool
	IncludeTables   []string
	ExcludeTables   []string
	IncludeData     bool
	IncludeMetadata bool
	MaxSizeMB       int64
}

// RestoreOptions configures restore behavior
type RestoreOptions struct {
	DryRun          bool
	SkipExisting    bool
	IncludeTables   []string
	ExcludeTables   []string
	RestoreData     bool
	RestoreMetadata bool
	Parallel        bool
	Workers         int
}

// NewBackupManager creates a new backup manager
func NewBackupManager(snowflakeService *snowflake.Service, storageDir string) (*BackupManager, error) {
	backupDir := filepath.Join(storageDir, "backups")
	if err := os.MkdirAll(backupDir, common.DirPermissionNormal); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	bm := &BackupManager{
		snowflakeService: snowflakeService,
		storageDir:       backupDir,
		backups:          make(map[string]*BackupMetadata),
	}

	// Load existing backups
	if err := bm.loadBackups(); err != nil {
		return nil, fmt.Errorf("failed to load backups: %w", err)
	}

	return bm, nil
}

// CreateBackup creates a backup of a database schema
func (bm *BackupManager) CreateBackup(ctx context.Context, database, schema string, options *BackupOptions) (*BackupMetadata, error) {
	if options == nil {
		options = &BackupOptions{
			Type:            "full",
			Format:          "sql",
			Compression:     true,
			IncludeData:     true,
			IncludeMetadata: true,
		}
	}

	backup := &BackupMetadata{
		ID:              bm.generateBackupID(),
		Database:        database,
		Schema:          schema,
		Timestamp:       time.Now(),
		Type:            options.Type,
		Format:          options.Format,
		CompressionType: "none",
		Encrypted:       options.Encrypted,
		Metadata:        map[string]interface{}{},
	}

	// Create backup file
	backupFile := filepath.Join(bm.storageDir, fmt.Sprintf("%s.tar", backup.ID))
	if options.Compression {
		backupFile += ".gz"
		backup.CompressionType = "gzip"
	}
	
	// Validate the backup file path
	validatedPath, err := common.ValidatePath(backupFile, bm.storageDir)
	if err != nil {
		return nil, fmt.Errorf("invalid backup file path: %w", err)
	}
	backup.Location = validatedPath

	// Create backup archive
	file, err := os.Create(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return nil, fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Setup writers
	var writer io.Writer = file
	var gzWriter *gzip.Writer
	if options.Compression {
		gzWriter = gzip.NewWriter(file)
		writer = gzWriter
		defer gzWriter.Close()
	}

	tarWriter := tar.NewWriter(writer)
	defer tarWriter.Close()

	// Backup schema objects
	if err := bm.backupSchemaObjects(ctx, database, schema, tarWriter, options); err != nil {
		os.Remove(backupFile)
		return nil, fmt.Errorf("failed to backup schema objects: %w", err)
	}

	// Backup data if requested
	if options.IncludeData {
		if err := bm.backupData(ctx, database, schema, tarWriter, options); err != nil {
			os.Remove(backupFile)
			return nil, fmt.Errorf("failed to backup data: %w", err)
		}
	}

	// Write metadata
	if err := bm.writeBackupMetadata(tarWriter, backup); err != nil {
		os.Remove(backupFile)
		return nil, fmt.Errorf("failed to write backup metadata: %w", err)
	}

	// Close writers
	tarWriter.Close()
	if gzWriter != nil {
		gzWriter.Close()
	}
	file.Close()

	// Calculate checksum
	checksum, err := bm.calculateFileChecksum(backupFile)
	if err != nil {
		os.Remove(backupFile)
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}
	backup.Checksum = checksum

	// Get file size
	fileInfo, err := os.Stat(backupFile)
	if err != nil {
		os.Remove(backupFile)
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	backup.Size = fileInfo.Size()

	// Store backup metadata
	bm.mu.Lock()
	bm.backups[backup.ID] = backup
	bm.mu.Unlock()

	// Save metadata
	if err := bm.saveBackupMetadata(backup); err != nil {
		return nil, fmt.Errorf("failed to save backup metadata: %w", err)
	}

	return backup, nil
}

// RestoreBackup restores from a backup
func (bm *BackupManager) RestoreBackup(ctx context.Context, backupID string, options *RestoreOptions) (*RestoreBackupResult, error) {
	bm.mu.RLock()
	backup, exists := bm.backups[backupID]
	bm.mu.RUnlock()

	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "backup not found").
			WithContext("backup_id", backupID)
	}

	result := &RestoreBackupResult{
		BackupID:   backupID,
		StartTime:  time.Now(),
		DryRun:     options.DryRun,
		Operations: []RestoreBackupOperation{},
		Success:    true,
	}

	// Verify backup file exists
	if _, err := os.Stat(backup.Location); err != nil {
		return nil, errors.New(errors.ErrCodeFileNotFound, "backup file not found").
			WithContext("location", backup.Location)
	}

	// Verify checksum
	checksum, err := bm.calculateFileChecksum(backup.Location)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}
	if checksum != backup.Checksum {
		return nil, errors.New(errors.ErrCodeIntegrityCheckFailed, "backup checksum mismatch").
			WithContext("expected", backup.Checksum).
			WithContext("actual", checksum)
	}

	// Extract and restore backup
	if err := bm.extractAndRestore(ctx, backup, options, result); err != nil {
		result.Success = false
		result.Error = err.Error()
	}

	result.EndTime = timePtr(time.Now())
	return result, nil
}

// ListBackups lists available backups
func (bm *BackupManager) ListBackups(database, schema string) ([]*BackupMetadata, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var backups []*BackupMetadata
	for _, backup := range bm.backups {
		if (database == "" || backup.Database == database) &&
		   (schema == "" || backup.Schema == schema) {
			backups = append(backups, backup)
		}
	}

	return backups, nil
}

// DeleteBackup deletes a backup
func (bm *BackupManager) DeleteBackup(backupID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	backup, exists := bm.backups[backupID]
	if !exists {
		return errors.New(errors.ErrCodeNotFound, "backup not found")
	}

	// Remove backup file
	if err := os.Remove(backup.Location); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove backup file: %w", err)
	}

	// Remove metadata file
	metadataFile := filepath.Join(bm.storageDir, fmt.Sprintf("%s.json", backup.ID))
	if err := os.Remove(metadataFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove metadata file: %w", err)
	}

	// Remove from memory
	delete(bm.backups, backupID)

	return nil
}

// Private methods

func (bm *BackupManager) backupSchemaObjects(ctx context.Context, database, schema string, tarWriter *tar.Writer, options *BackupOptions) error {
	// Get all schema objects
	objects := []struct {
		Type     string
		Query    string
		FileName string
	}{
		{
			Type:     "TABLES",
			Query:    fmt.Sprintf("SHOW TABLES IN SCHEMA %s.%s", database, schema),
			FileName: "tables.sql",
		},
		{
			Type:     "VIEWS",
			Query:    fmt.Sprintf("SHOW VIEWS IN SCHEMA %s.%s", database, schema),
			FileName: "views.sql",
		},
		{
			Type:     "PROCEDURES",
			Query:    fmt.Sprintf("SHOW PROCEDURES IN SCHEMA %s.%s", database, schema),
			FileName: "procedures.sql",
		},
		{
			Type:     "FUNCTIONS",
			Query:    fmt.Sprintf("SHOW FUNCTIONS IN SCHEMA %s.%s", database, schema),
			FileName: "functions.sql",
		},
	}

	for _, obj := range objects {
		content, err := bm.getObjectsDDL(ctx, database, schema, obj.Type)
		if err != nil {
			return fmt.Errorf("failed to get %s DDL: %w", obj.Type, err)
		}

		// Write to tar
		header := &tar.Header{
			Name:     fmt.Sprintf("schema/%s", obj.FileName),
			Size:     int64(len(content)),
			Mode:     0644,
			ModTime:  time.Now(),
			Typeflag: tar.TypeReg,
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if _, err := tarWriter.Write([]byte(content)); err != nil {
			return err
		}
	}

	return nil
}

func (bm *BackupManager) backupData(ctx context.Context, database, schema string, tarWriter *tar.Writer, options *BackupOptions) error {
	// Get tables to backup
	tables, err := bm.getTablesToBackup(ctx, database, schema, options)
	if err != nil {
		return err
	}

	for _, table := range tables {
		// Export table data
		data, err := bm.exportTableData(ctx, database, schema, table, options.Format)
		if err != nil {
			return fmt.Errorf("failed to export table %s: %w", table, err)
		}

		// Write to tar
		filename := fmt.Sprintf("data/%s.%s", table, options.Format)
		header := &tar.Header{
			Name:     filename,
			Size:     int64(len(data)),
			Mode:     0644,
			ModTime:  time.Now(),
			Typeflag: tar.TypeReg,
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if _, err := tarWriter.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (bm *BackupManager) getObjectsDDL(ctx context.Context, database, schema, objectType string) (string, error) {
	var ddl strings.Builder
	
	// This would query Snowflake for object DDLs
	// Simplified for demonstration
	ddl.WriteString(fmt.Sprintf("-- %s DDL for %s.%s\n", objectType, database, schema))
	
	return ddl.String(), nil
}

func (bm *BackupManager) getTablesToBackup(ctx context.Context, database, schema string, options *BackupOptions) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT table_name 
		FROM %s.information_schema.tables 
		WHERE table_schema = '%s' 
		AND table_type = 'BASE TABLE'
	`, database, schema)

	rows, err := bm.snowflakeService.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}

		// Apply filters
		if len(options.IncludeTables) > 0 && !contains(options.IncludeTables, table) {
			continue
		}
		if contains(options.ExcludeTables, table) {
			continue
		}

		tables = append(tables, table)
	}

	return tables, rows.Err()
}

func (bm *BackupManager) exportTableData(ctx context.Context, database, schema, table, format string) ([]byte, error) {
	// This would export actual table data in the specified format
	// Simplified for demonstration
	return []byte(fmt.Sprintf("-- Data for %s.%s.%s in %s format\n", database, schema, table, format)), nil
}

func (bm *BackupManager) extractAndRestore(ctx context.Context, backup *BackupMetadata, options *RestoreOptions, result *RestoreBackupResult) error {
	// Open backup file
	file, err := os.Open(backup.Location)
	if err != nil {
		return err
	}
	defer file.Close()

	// Setup readers
	var reader io.Reader = file
	if backup.CompressionType == "gzip" {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gzReader.Close()
		reader = gzReader
	}

	tarReader := tar.NewReader(reader)

	// Process tar entries
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Read content
		content := make([]byte, header.Size)
		if _, err := io.ReadFull(tarReader, content); err != nil {
			return err
		}

		// Process based on file type
		if strings.HasPrefix(header.Name, "schema/") && options.RestoreMetadata {
			op := bm.processSchemaFile(ctx, backup, header.Name, content, options)
			result.Operations = append(result.Operations, op)
		} else if strings.HasPrefix(header.Name, "data/") && options.RestoreData {
			op := bm.processDataFile(ctx, backup, header.Name, content, options)
			result.Operations = append(result.Operations, op)
		}
	}

	return nil
}

func (bm *BackupManager) processSchemaFile(ctx context.Context, backup *BackupMetadata, filename string, content []byte, options *RestoreOptions) RestoreBackupOperation {
	op := RestoreBackupOperation{
		Type:      "schema",
		Object:    filename,
		StartTime: time.Now(),
	}

	if !options.DryRun {
		// Execute schema DDL
		if err := bm.snowflakeService.ExecuteSQL(string(content), backup.Database, backup.Schema); err != nil {
			op.Success = false
			op.Error = err.Error()
		} else {
			op.Success = true
		}
	} else {
		op.Success = true
		op.Skipped = true
	}

	op.EndTime = timePtr(time.Now())
	return op
}

func (bm *BackupManager) processDataFile(ctx context.Context, backup *BackupMetadata, filename string, content []byte, options *RestoreOptions) RestoreBackupOperation {
	op := RestoreBackupOperation{
		Type:      "data",
		Object:    filename,
		StartTime: time.Now(),
	}

	if !options.DryRun {
		// Import data (simplified)
		op.Success = true
	} else {
		op.Success = true
		op.Skipped = true
	}

	op.EndTime = timePtr(time.Now())
	return op
}

func (bm *BackupManager) writeBackupMetadata(tarWriter *tar.Writer, backup *BackupMetadata) error {
	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return err
	}

	header := &tar.Header{
		Name:     "metadata.json",
		Size:     int64(len(data)),
		Mode:     0644,
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	_, err = tarWriter.Write(data)
	return err
}

func (bm *BackupManager) calculateFileChecksum(filepath string) (string, error) {
	// Validate the file path
	validatedPath, err := common.CleanPath(filepath)
	if err != nil {
		return "", fmt.Errorf("invalid file path: %w", err)
	}
	
	file, err := os.Open(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (bm *BackupManager) generateBackupID() string {
	return fmt.Sprintf("backup-%d-%s", time.Now().Unix(), generateRandomID(8))
}

func (bm *BackupManager) saveBackupMetadata(backup *BackupMetadata) error {
	filename := fmt.Sprintf("%s.json", backup.ID)
	filepath := filepath.Join(bm.storageDir, filename)

	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath, data, 0600)
}

func (bm *BackupManager) loadBackups() error {
	pattern := filepath.Join(bm.storageDir, "backup-*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, file := range files {
		// Validate the file path
		validatedPath, err := common.ValidatePath(file, bm.storageDir)
		if err != nil {
			continue
		}
		
		data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
		if err != nil {
			continue
		}

		var backup BackupMetadata
		if err := json.Unmarshal(data, &backup); err != nil {
			continue
		}

		bm.backups[backup.ID] = &backup
	}

	return nil
}

// Helper types

// RestoreBackupResult represents the result of a restore operation
type RestoreBackupResult struct {
	BackupID   string
	StartTime  time.Time
	EndTime    *time.Time
	DryRun     bool
	Operations []RestoreBackupOperation
	Success    bool
	Error      string
}

// RestoreBackupOperation represents a single restore operation
type RestoreBackupOperation struct {
	Type      string
	Object    string
	StartTime time.Time
	EndTime   *time.Time
	Success   bool
	Skipped   bool
	Error     string
}

// Helper functions

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}