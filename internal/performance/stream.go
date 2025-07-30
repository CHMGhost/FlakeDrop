package performance

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/pkg/errors"
)

// StreamProcessor handles streaming execution of large SQL files
type StreamProcessor struct {
	pool          *ConnectionPool
	bufferSize    int
	maxChunkSize  int64
	parallel      int
	errorHandler  *errors.ErrorHandler
	metrics       *StreamMetrics
}

// StreamMetrics tracks streaming performance
type StreamMetrics struct {
	mu               sync.RWMutex
	FilesProcessed   int64
	BytesProcessed   int64
	StatementsCount  int64
	ChunksProcessed  int64
	Errors           int64
	ProcessingTime   time.Duration
	ThroughputMBps   float64
}

// StreamConfig contains streaming processor configuration
type StreamConfig struct {
	BufferSize   int
	MaxChunkSize int64
	Parallel     int
}

// DefaultStreamConfig returns sensible defaults
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		BufferSize:   64 * 1024,      // 64KB buffer
		MaxChunkSize: 5 * 1024 * 1024, // 5MB chunks
		Parallel:     4,
	}
}

// NewStreamProcessor creates a new streaming processor
func NewStreamProcessor(pool *ConnectionPool, config StreamConfig) *StreamProcessor {
	return &StreamProcessor{
		pool:         pool,
		bufferSize:   config.BufferSize,
		maxChunkSize: config.MaxChunkSize,
		parallel:     config.Parallel,
		errorHandler: errors.GetGlobalErrorHandler(),
		metrics:      &StreamMetrics{},
	}
}

// ProcessFile processes a large SQL file using streaming
func (sp *StreamProcessor) ProcessFile(ctx context.Context, filePath, database, schema string) error {
	startTime := time.Now()
	
	// Validate and clean the path
	cleanedPath, err := common.CleanPath(filePath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}
	
	// Open file
	file, err := os.Open(cleanedPath) // #nosec G304 - path is validated
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFileNotFound, "Failed to open file").
			WithContext("file", cleanedPath)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFilePermission, "Failed to get file info")
	}

	sp.metrics.recordFileStart(fileInfo.Size())

	// Create stream reader
	reader := &StreamReader{
		reader:       bufio.NewReaderSize(file, sp.bufferSize),
		maxChunkSize: sp.maxChunkSize,
		metrics:      sp.metrics,
	}

	// Process chunks in parallel
	chunkChan := make(chan *SQLChunk, sp.parallel*2)
	resultChan := make(chan error, sp.parallel)
	
	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < sp.parallel; i++ {
		wg.Add(1)
		go sp.chunkWorker(ctx, &wg, database, schema, chunkChan, resultChan)
	}

	// Start chunk producer
	go func() {
		defer close(chunkChan)
		
		for {
			chunk, err := reader.ReadChunk()
			if err == io.EOF {
				break
			}
			if err != nil {
				resultChan <- err
				return
			}
			
			select {
			case chunkChan <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var lastErr error
	for err := range resultChan {
		if err != nil {
			lastErr = err
			sp.metrics.recordError()
		}
	}

	duration := time.Since(startTime)
	sp.metrics.recordFileComplete(duration)

	if lastErr != nil {
		return fmt.Errorf("streaming failed: %w", lastErr)
	}

	return nil
}

// ProcessReader processes SQL from an io.Reader
func (sp *StreamProcessor) ProcessReader(ctx context.Context, reader io.Reader, database, schema string) error {
	streamReader := &StreamReader{
		reader:       bufio.NewReaderSize(reader, sp.bufferSize),
		maxChunkSize: sp.maxChunkSize,
		metrics:      sp.metrics,
	}

	// Get connection
	conn, err := sp.pool.Get(ctx)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeConnectionFailed, "Failed to get connection")
	}
	defer sp.pool.Put(conn)

	// Process chunks sequentially from reader
	for {
		chunk, err := streamReader.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := sp.executeChunk(ctx, conn, database, schema, chunk); err != nil {
			return err
		}
	}

	return nil
}

// chunkWorker processes chunks from the channel
func (sp *StreamProcessor) chunkWorker(ctx context.Context, wg *sync.WaitGroup, database, schema string, chunks <-chan *SQLChunk, results chan<- error) {
	defer wg.Done()

	// Get connection for this worker
	conn, err := sp.pool.Get(ctx)
	if err != nil {
		results <- err
		return
	}
	defer sp.pool.Put(conn)

	for chunk := range chunks {
		if err := sp.executeChunk(ctx, conn, database, schema, chunk); err != nil {
			results <- err
		}
		sp.metrics.recordChunkProcessed()
	}
}

// executeChunk executes a chunk of SQL statements
func (sp *StreamProcessor) executeChunk(ctx context.Context, conn *PooledConnection, database, schema string, chunk *SQLChunk) error {
	// Start transaction for chunk
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSQLTransaction, "Failed to begin transaction for chunk")
	}

	// Create transaction handler
	txHandler := sp.errorHandler.NewTransactionHandler(tx, tx.Rollback)

	return txHandler.Execute(func() error {
		// Set context
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", database)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE SCHEMA %s", schema)); err != nil {
			return err
		}

		// Execute statements in chunk
		for i, stmt := range chunk.Statements {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return errors.SQLError(
					fmt.Sprintf("Failed to execute statement %d in chunk %d", i+1, chunk.ID),
					stmt,
					err,
				)
			}
			sp.metrics.recordStatement()
		}

		return tx.Commit()
	})
}

// GetMetrics returns streaming metrics
func (sp *StreamProcessor) GetMetrics() StreamMetrics {
	sp.metrics.mu.RLock()
	defer sp.metrics.mu.RUnlock()
	return *sp.metrics
}

// StreamReader reads SQL statements from a stream
type StreamReader struct {
	reader         *bufio.Reader
	buffer         strings.Builder
	maxChunkSize   int64
	currentChunkID int
	inString       bool
	stringChar     rune
	metrics        *StreamMetrics
}

// SQLChunk represents a chunk of SQL statements
type SQLChunk struct {
	ID         int
	Statements []string
	Size       int64
}

// ReadChunk reads the next chunk of complete SQL statements
func (sr *StreamReader) ReadChunk() (*SQLChunk, error) {
	chunk := &SQLChunk{
		ID:         sr.currentChunkID,
		Statements: make([]string, 0),
	}
	sr.currentChunkID++

	var currentStatement strings.Builder
	var chunkSize int64

	for chunkSize < sr.maxChunkSize {
		line, err := sr.reader.ReadString('\n')
		if err == io.EOF {
			// Process remaining content
			if currentStatement.Len() > 0 {
				chunk.Statements = append(chunk.Statements, currentStatement.String())
				chunk.Size += int64(currentStatement.Len())
			}
			if len(chunk.Statements) > 0 {
				return chunk, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		// Track bytes read
		lineSize := int64(len(line))
		chunkSize += lineSize
		sr.metrics.recordBytesRead(lineSize)

		// Process line for statement boundaries
		for i, char := range line {
			if !sr.inString {
				if char == '\'' || char == '"' {
					sr.inString = true
					sr.stringChar = char
				} else if char == ';' {
					// Check if it's not escaped
					if i == 0 || line[i-1] != '\\' {
						// Complete statement
						currentStatement.WriteString(line[:i])
						stmt := strings.TrimSpace(currentStatement.String())
						if stmt != "" {
							chunk.Statements = append(chunk.Statements, stmt)
							chunk.Size += int64(len(stmt))
						}
						currentStatement.Reset()
						
						// Start new statement with remainder of line
						if i+1 < len(line) {
							currentStatement.WriteString(line[i+1:])
						}
						continue
					}
				}
			} else {
				if char == sr.stringChar && (i == 0 || line[i-1] != '\\') {
					sr.inString = false
				}
			}
		}

		// Add line to current statement if no semicolon found
		if currentStatement.Len() > 0 || !strings.HasSuffix(strings.TrimSpace(line), ";") {
			currentStatement.WriteString(line)
		}

		// Check if we should finish this chunk
		if len(chunk.Statements) > 0 && chunkSize >= sr.maxChunkSize {
			// Save incomplete statement for next chunk
			if currentStatement.Len() > 0 {
				sr.buffer = currentStatement
			}
			break
		}
	}

	return chunk, nil
}

// LazyFileReader provides lazy loading for SQL files
type LazyFileReader struct {
	filePath    string
	file        *os.File
	reader      *bufio.Reader
	isOpen      bool
	mu          sync.Mutex
}

// NewLazyFileReader creates a new lazy file reader
func NewLazyFileReader(filePath string) *LazyFileReader {
	return &LazyFileReader{
		filePath: filePath,
	}
}

// Read implements io.Reader interface with lazy loading
func (lfr *LazyFileReader) Read(p []byte) (n int, err error) {
	lfr.mu.Lock()
	defer lfr.mu.Unlock()

	if !lfr.isOpen {
		if err := lfr.open(); err != nil {
			return 0, err
		}
	}

	return lfr.reader.Read(p)
}

// Close closes the file if open
func (lfr *LazyFileReader) Close() error {
	lfr.mu.Lock()
	defer lfr.mu.Unlock()

	if lfr.isOpen && lfr.file != nil {
		err := lfr.file.Close()
		lfr.isOpen = false
		lfr.file = nil
		lfr.reader = nil
		return err
	}
	return nil
}

// open opens the file lazily
func (lfr *LazyFileReader) open() error {
	file, err := os.Open(lfr.filePath)
	if err != nil {
		return err
	}

	lfr.file = file
	lfr.reader = bufio.NewReader(file)
	lfr.isOpen = true
	return nil
}

// StreamMetrics methods

func (sm *StreamMetrics) recordFileStart(size int64) {
	sm.mu.Lock()
	sm.FilesProcessed++
	sm.mu.Unlock()
}

func (sm *StreamMetrics) recordBytesRead(bytes int64) {
	sm.mu.Lock()
	sm.BytesProcessed += bytes
	sm.mu.Unlock()
}

func (sm *StreamMetrics) recordStatement() {
	sm.mu.Lock()
	sm.StatementsCount++
	sm.mu.Unlock()
}

func (sm *StreamMetrics) recordChunkProcessed() {
	sm.mu.Lock()
	sm.ChunksProcessed++
	sm.mu.Unlock()
}

func (sm *StreamMetrics) recordError() {
	sm.mu.Lock()
	sm.Errors++
	sm.mu.Unlock()
}

func (sm *StreamMetrics) recordFileComplete(duration time.Duration) {
	sm.mu.Lock()
	sm.ProcessingTime += duration
	
	// Calculate throughput
	if duration.Seconds() > 0 {
		mbProcessed := float64(sm.BytesProcessed) / (1024 * 1024)
		sm.ThroughputMBps = mbProcessed / duration.Seconds()
	}
	sm.mu.Unlock()
}