package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// BulkOperations provides optimized bulk data operations for Snowflake
type BulkOperations struct {
	service          *Service
	connManager      *ConnectionManager
	stagingManager   *StagingManager
	copyOptimizer    *CopyOptimizer
	streamProcessor  *StreamProcessor
	metricsCollector *BulkMetricsCollector
}

// BulkInsertOptions configures bulk insert operations
type BulkInsertOptions struct {
	// Target table information
	Database string
	Schema   string
	Table    string
	
	// File options
	FileFormat      string // CSV, JSON, PARQUET, AVRO, ORC
	Compression     string // GZIP, BZIP2, DEFLATE, RAW_DEFLATE, ZSTD
	FieldDelimiter  string
	RecordDelimiter string
	SkipHeader      int
	
	// Performance options
	BatchSize       int
	Parallelism     int
	UsePut          bool // Use PUT command for file staging
	PurgeAfterLoad  bool
	ContinueOnError bool
	
	// Validation options
	ValidateData    bool
	MaxErrors       int
	
	// Stage options
	StageName       string
	CreateStage     bool
}

// StagingManager manages Snowflake stages for bulk operations
type StagingManager struct {
	stages map[string]*Stage
	mu     sync.RWMutex
}

// Stage represents a Snowflake stage
type Stage struct {
	Name         string
	Type         string // INTERNAL, EXTERNAL
	URL          string
	Credentials  map[string]string
	FileFormat   string
	CopyOptions  map[string]string
	CreatedAt    time.Time
}

// CopyOptimizer optimizes COPY INTO operations
type CopyOptimizer struct {
	analyzer *CopyAnalyzer
	cache    *CopyPlanCache
}

// StreamProcessor handles streaming data loads
type StreamProcessor struct {
	bufferSize int
	workers    int
	mu         sync.Mutex
}

// BulkMetricsCollector collects bulk operation metrics
type BulkMetricsCollector struct {
	operations []BulkOperationMetric
	mu         sync.RWMutex
}

// BulkOperationMetric tracks bulk operation performance
type BulkOperationMetric struct {
	OperationID     string
	Type            string
	RowsLoaded      int64
	RowsRejected    int64
	BytesProcessed  int64
	Duration        time.Duration
	CreditsUsed     float64
	ErrorCount      int
	WarehouseUsed   string
	Timestamp       time.Time
}

// NewBulkOperations creates a new bulk operations handler
func NewBulkOperations(service *Service, connManager *ConnectionManager) *BulkOperations {
	return &BulkOperations{
		service:          service,
		connManager:      connManager,
		stagingManager:   NewStagingManager(),
		copyOptimizer:    NewCopyOptimizer(),
		streamProcessor:  NewStreamProcessor(),
		metricsCollector: NewBulkMetricsCollector(),
	}
}

// BulkInsertFromFile performs optimized bulk insert from file(s)
func (bo *BulkOperations) BulkInsertFromFile(ctx context.Context, files []string, options BulkInsertOptions) (*BulkOperationResult, error) {
	startTime := time.Now()
	operationID := fmt.Sprintf("bulk_%d", time.Now().UnixNano())
	
	result := &BulkOperationResult{
		OperationID: operationID,
		StartTime:   startTime,
		Files:       files,
	}

	// Validate options
	if err := bo.validateOptions(options); err != nil {
		return nil, err
	}

	// Get optimized connection
	conn, err := bo.connManager.GetConnection(ctx, "")
	if err != nil {
		return nil, err
	}
	defer bo.connManager.ReleaseConnection(conn)

	// Create or get stage
	stage, err := bo.stagingManager.GetOrCreateStage(conn, options)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeStagingFailed, "Failed to setup stage")
	}

	// Process files based on size and options
	if options.UsePut {
		err = bo.bulkLoadWithPut(ctx, conn, files, stage, options, result)
	} else {
		err = bo.bulkLoadWithCopy(ctx, conn, files, stage, options, result)
	}

	if err != nil {
		result.Status = "FAILED"
		result.Error = err
	} else {
		result.Status = "SUCCESS"
	}

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(startTime)

	// Record metrics
	bo.recordMetrics(result, options)

	return result, err
}

// BulkInsertFromReader performs streaming bulk insert from io.Reader
func (bo *BulkOperations) BulkInsertFromReader(ctx context.Context, reader io.Reader, options BulkInsertOptions) (*BulkOperationResult, error) {
	startTime := time.Now()
	operationID := fmt.Sprintf("stream_%d", time.Now().UnixNano())

	result := &BulkOperationResult{
		OperationID: operationID,
		StartTime:   startTime,
	}

	// Stream data to temporary file
	tempFile, err := bo.streamToTempFile(reader, options)
	if err != nil {
		result.Status = "FAILED"
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(startTime)
		return result, err
	}
	defer os.Remove(tempFile)

	// Use file-based bulk insert
	return bo.BulkInsertFromFile(ctx, []string{tempFile}, options)
}

// bulkLoadWithPut uses PUT command for staging files
func (bo *BulkOperations) bulkLoadWithPut(ctx context.Context, conn *ManagedConnection, files []string, stage *Stage, options BulkInsertOptions, result *BulkOperationResult) error {
	// Upload files to stage using PUT
	for _, file := range files {
		putSQL := fmt.Sprintf("PUT file://%s @%s AUTO_COMPRESS=%v",
			file,
			stage.Name,
			options.Compression != "",
		)

		if _, err := conn.Execute(ctx, putSQL); err != nil {
			return errors.Wrap(err, errors.ErrCodeStagingFailed, "Failed to PUT file").
				WithContext("file", file).
				WithContext("stage", stage.Name)
		}

		result.FilesStaged++
	}

	// Execute COPY INTO
	return bo.executeCopyInto(ctx, conn, stage, options, result)
}

// bulkLoadWithCopy uses direct COPY INTO for local files
func (bo *BulkOperations) bulkLoadWithCopy(ctx context.Context, conn *ManagedConnection, files []string, stage *Stage, options BulkInsertOptions, result *BulkOperationResult) error {
	// For each file, execute COPY INTO directly
	for _, file := range files {
		copySQL := bo.buildCopyIntoSQL(file, stage, options)
		
		rows, err := conn.Query(ctx, copySQL)
		if err != nil {
			if !options.ContinueOnError {
				return errors.SQLError("Failed to execute COPY INTO", copySQL, err)
			}
			result.Errors = append(result.Errors, err)
			continue
		}
		defer rows.Close()

		// Parse COPY results
		if err := bo.parseCopyResults(rows, result); err != nil {
			return err
		}
	}

	return nil
}

// executeCopyInto executes COPY INTO from stage
func (bo *BulkOperations) executeCopyInto(ctx context.Context, conn *ManagedConnection, stage *Stage, options BulkInsertOptions, result *BulkOperationResult) error {
	copySQL := fmt.Sprintf(`
		COPY INTO %s.%s.%s
		FROM @%s
		FILE_FORMAT = (TYPE = %s %s)
		ON_ERROR = %s
		PURGE = %v
		RETURN_FAILED_ONLY = FALSE
	`,
		options.Database,
		options.Schema,
		options.Table,
		stage.Name,
		options.FileFormat,
		bo.buildFileFormatOptions(options),
		bo.getOnErrorOption(options),
		options.PurgeAfterLoad,
	)

	rows, err := conn.Query(ctx, copySQL)
	if err != nil {
		return errors.SQLError("Failed to execute COPY INTO", copySQL, err)
	}
	defer rows.Close()

	return bo.parseCopyResults(rows, result)
}

// buildCopyIntoSQL builds COPY INTO SQL statement
func (bo *BulkOperations) buildCopyIntoSQL(file string, stage *Stage, options BulkInsertOptions) string {
	return fmt.Sprintf(`
		COPY INTO %s.%s.%s
		FROM 'file://%s'
		FILE_FORMAT = (TYPE = %s %s)
		ON_ERROR = %s
		RETURN_FAILED_ONLY = FALSE
	`,
		options.Database,
		options.Schema,
		options.Table,
		file,
		options.FileFormat,
		bo.buildFileFormatOptions(options),
		bo.getOnErrorOption(options),
	)
}

// buildFileFormatOptions builds file format options string
func (bo *BulkOperations) buildFileFormatOptions(options BulkInsertOptions) string {
	var formatOptions []string

	if options.Compression != "" {
		formatOptions = append(formatOptions, fmt.Sprintf("COMPRESSION = %s", options.Compression))
	}

	if options.FieldDelimiter != "" {
		formatOptions = append(formatOptions, fmt.Sprintf("FIELD_DELIMITER = '%s'", options.FieldDelimiter))
	}

	if options.RecordDelimiter != "" {
		formatOptions = append(formatOptions, fmt.Sprintf("RECORD_DELIMITER = '%s'", options.RecordDelimiter))
	}

	if options.SkipHeader > 0 {
		formatOptions = append(formatOptions, fmt.Sprintf("SKIP_HEADER = %d", options.SkipHeader))
	}

	// Add format-specific options
	switch strings.ToUpper(options.FileFormat) {
	case "CSV":
		formatOptions = append(formatOptions, "FIELD_OPTIONALLY_ENCLOSED_BY = '\"'")
		formatOptions = append(formatOptions, "ESCAPE_UNENCLOSED_FIELD = '\\\\'")
	case "JSON":
		formatOptions = append(formatOptions, "STRIP_OUTER_ARRAY = TRUE")
	}

	return strings.Join(formatOptions, " ")
}

// getOnErrorOption returns the ON_ERROR option based on settings
func (bo *BulkOperations) getOnErrorOption(options BulkInsertOptions) string {
	if options.ContinueOnError {
		if options.MaxErrors > 0 {
			return fmt.Sprintf("CONTINUE %d", options.MaxErrors)
		}
		return "CONTINUE"
	}
	return "ABORT_STATEMENT"
}

// parseCopyResults parses COPY INTO command results
func (bo *BulkOperations) parseCopyResults(rows *sql.Rows, result *BulkOperationResult) error {
	for rows.Next() {
		var file string
		var status string
		var rowsParsed, rowsLoaded int64
		var errorsSeen int
		var firstError sql.NullString

		err := rows.Scan(&file, &status, &rowsParsed, &rowsLoaded, &errorsSeen, &firstError)
		if err != nil {
			// Try alternate result format
			err = rows.Scan(&file, &rowsParsed, &rowsLoaded)
			if err != nil {
				return errors.Wrap(err, errors.ErrCodeResultParsing, "Failed to parse COPY results")
			}
			status = "LOADED"
			errorsSeen = 0
		}

		result.RowsParsed += rowsParsed
		result.RowsLoaded += rowsLoaded
		result.RowsRejected += int64(errorsSeen)

		if status != "LOADED" && firstError.Valid {
			result.Errors = append(result.Errors, fmt.Errorf("file %s: %s", file, firstError.String))
		}
	}

	return rows.Err()
}

// streamToTempFile streams reader content to a temporary file
func (bo *BulkOperations) streamToTempFile(reader io.Reader, options BulkInsertOptions) (string, error) {
	// Create temp file
	tempFile, err := os.CreateTemp("", fmt.Sprintf("bulk_*.%s", strings.ToLower(options.FileFormat)))
	if err != nil {
		return "", errors.Wrap(err, errors.ErrCodeFileOperation, "Failed to create temp file")
	}
	defer tempFile.Close()

	// Copy data
	_, err = io.Copy(tempFile, reader)
	if err != nil {
		os.Remove(tempFile.Name())
		return "", errors.Wrap(err, errors.ErrCodeFileOperation, "Failed to write temp file")
	}

	return tempFile.Name(), nil
}

// validateOptions validates bulk insert options
func (bo *BulkOperations) validateOptions(options BulkInsertOptions) error {
	if options.Database == "" || options.Schema == "" || options.Table == "" {
		return errors.New(errors.ErrCodeInvalidInput, "Database, schema, and table are required")
	}

	if options.FileFormat == "" {
		return errors.New(errors.ErrCodeInvalidInput, "File format is required")
	}

	validFormats := map[string]bool{
		"CSV": true, "JSON": true, "PARQUET": true, "AVRO": true, "ORC": true,
	}
	if !validFormats[strings.ToUpper(options.FileFormat)] {
		return errors.New(errors.ErrCodeInvalidInput, "Invalid file format").
			WithContext("format", options.FileFormat).
			WithSuggestions("Use one of: CSV, JSON, PARQUET, AVRO, ORC")
	}

	return nil
}

// recordMetrics records bulk operation metrics
func (bo *BulkOperations) recordMetrics(result *BulkOperationResult, options BulkInsertOptions) {
	metric := BulkOperationMetric{
		OperationID:    result.OperationID,
		Type:           "BULK_INSERT",
		RowsLoaded:     result.RowsLoaded,
		RowsRejected:   result.RowsRejected,
		BytesProcessed: result.BytesProcessed,
		Duration:       result.Duration,
		ErrorCount:     len(result.Errors),
		Timestamp:      time.Now(),
	}

	bo.metricsCollector.Record(metric)
}

// BulkOperationResult contains results of a bulk operation
type BulkOperationResult struct {
	OperationID    string
	Status         string
	StartTime      time.Time
	EndTime        time.Time
	Duration       time.Duration
	Files          []string
	FilesStaged    int
	RowsParsed     int64
	RowsLoaded     int64
	RowsRejected   int64
	BytesProcessed int64
	Errors         []error
	Error          error
}

// StagingManager methods

func NewStagingManager() *StagingManager {
	return &StagingManager{
		stages: make(map[string]*Stage),
	}
}

// GetOrCreateStage gets an existing stage or creates a new one
func (sm *StagingManager) GetOrCreateStage(conn *ManagedConnection, options BulkInsertOptions) (*Stage, error) {
	stageName := options.StageName
	if stageName == "" {
		stageName = fmt.Sprintf("BULK_STAGE_%s", strings.ToUpper(options.Table))
	}

	sm.mu.RLock()
	stage, exists := sm.stages[stageName]
	sm.mu.RUnlock()

	if exists {
		return stage, nil
	}

	// Create stage
	if options.CreateStage {
		createSQL := fmt.Sprintf("CREATE STAGE IF NOT EXISTS %s", stageName)
		if _, err := conn.Execute(context.Background(), createSQL); err != nil {
			return nil, errors.SQLError("Failed to create stage", createSQL, err)
		}
	}

	stage = &Stage{
		Name:        stageName,
		Type:        "INTERNAL",
		FileFormat:  options.FileFormat,
		CopyOptions: make(map[string]string),
		CreatedAt:   time.Now(),
	}

	sm.mu.Lock()
	sm.stages[stageName] = stage
	sm.mu.Unlock()

	return stage, nil
}

// CopyOptimizer methods

func NewCopyOptimizer() *CopyOptimizer {
	return &CopyOptimizer{
		analyzer: NewCopyAnalyzer(),
		cache:    NewCopyPlanCache(),
	}
}

// CopyAnalyzer analyzes COPY operations
type CopyAnalyzer struct{}

func NewCopyAnalyzer() *CopyAnalyzer {
	return &CopyAnalyzer{}
}

// CopyPlanCache caches COPY operation plans
type CopyPlanCache struct {
	plans map[string]*CopyPlan
	mu    sync.RWMutex
}

type CopyPlan struct {
	Options      BulkInsertOptions
	OptimalBatch int
	Parallelism  int
	CreatedAt    time.Time
}

func NewCopyPlanCache() *CopyPlanCache {
	return &CopyPlanCache{
		plans: make(map[string]*CopyPlan),
	}
}

// StreamProcessor methods

func NewStreamProcessor() *StreamProcessor {
	return &StreamProcessor{
		bufferSize: 1024 * 1024, // 1MB
		workers:    4,
	}
}

// BulkMetricsCollector methods

func NewBulkMetricsCollector() *BulkMetricsCollector {
	return &BulkMetricsCollector{
		operations: make([]BulkOperationMetric, 0),
	}
}

func (bmc *BulkMetricsCollector) Record(metric BulkOperationMetric) {
	bmc.mu.Lock()
	defer bmc.mu.Unlock()
	
	bmc.operations = append(bmc.operations, metric)
	
	// Keep only last 1000 operations
	if len(bmc.operations) > 1000 {
		bmc.operations = bmc.operations[len(bmc.operations)-1000:]
	}
}

func (bmc *BulkMetricsCollector) GetMetrics() []BulkOperationMetric {
	bmc.mu.RLock()
	defer bmc.mu.RUnlock()
	
	result := make([]BulkOperationMetric, len(bmc.operations))
	copy(result, bmc.operations)
	return result
}

// GetAverageLoadRate returns average rows per second
func (bmc *BulkMetricsCollector) GetAverageLoadRate() float64 {
	bmc.mu.RLock()
	defer bmc.mu.RUnlock()
	
	if len(bmc.operations) == 0 {
		return 0
	}
	
	var totalRows int64
	var totalDuration time.Duration
	
	for _, op := range bmc.operations {
		totalRows += op.RowsLoaded
		totalDuration += op.Duration
	}
	
	if totalDuration == 0 {
		return 0
	}
	
	return float64(totalRows) / totalDuration.Seconds()
}

// BulkExport provides optimized data export from Snowflake
func (bo *BulkOperations) BulkExport(ctx context.Context, query string, outputPath string, options ExportOptions) (*ExportResult, error) {
	startTime := time.Now()
	
	conn, err := bo.connManager.GetConnection(ctx, "")
	if err != nil {
		return nil, err
	}
	defer bo.connManager.ReleaseConnection(conn)

	// Create output file
	// Note: outputPath should be validated by the caller
	file, err := os.Create(outputPath) // #nosec G304 - path should be validated by caller
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileOperation, "Failed to create output file")
	}
	defer file.Close()

	// Execute query with COPY INTO
	copySQL := fmt.Sprintf(`
		COPY INTO 'file://%s'
		FROM (%s)
		FILE_FORMAT = (TYPE = %s %s)
		SINGLE = TRUE
		OVERWRITE = TRUE
	`,
		outputPath,
		query,
		options.Format,
		bo.buildFileFormatOptions(BulkInsertOptions{
			FileFormat:      options.Format,
			Compression:     options.Compression,
			FieldDelimiter:  options.FieldDelimiter,
			RecordDelimiter: options.RecordDelimiter,
		}),
	)

	rows, err := conn.Query(ctx, copySQL)
	if err != nil {
		return nil, errors.SQLError("Failed to execute COPY export", copySQL, err)
	}
	defer rows.Close()

	result := &ExportResult{
		OutputPath: outputPath,
		StartTime:  startTime,
		EndTime:    time.Now(),
	}

	// Parse export results
	for rows.Next() {
		var rowsUnloaded int64
		var inputBytes int64
		var outputBytes int64
		
		if err := rows.Scan(&rowsUnloaded, &inputBytes, &outputBytes); err != nil {
			return nil, errors.Wrap(err, errors.ErrCodeResultParsing, "Failed to parse export results")
		}
		
		result.RowsExported = rowsUnloaded
		result.BytesWritten = outputBytes
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

// ExportOptions configures data export
type ExportOptions struct {
	Format          string
	Compression     string
	FieldDelimiter  string
	RecordDelimiter string
	Header          bool
	MaxFileSize     int64
}

// ExportResult contains export operation results
type ExportResult struct {
	OutputPath   string
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	RowsExported int64
	BytesWritten int64
	Files        []string
}