package errors

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"flakedrop/internal/common"
)

// ErrorHandler provides centralized error handling and logging
type ErrorHandler struct {
	logFile      *os.File
	logWriter    io.Writer
	errorLog     []ErrorLogEntry
	mu           sync.Mutex
	config       ErrorHandlerConfig
	circuitBreakers *CircuitBreakerManager
}

// ErrorHandlerConfig configures the error handler
type ErrorHandlerConfig struct {
	LogToFile       bool
	LogFilePath     string
	MaxLogEntries   int
	EnableMetrics   bool
	MetricsInterval time.Duration
}

// ErrorLogEntry represents a logged error
type ErrorLogEntry struct {
	Timestamp   time.Time              `json:"timestamp"`
	Code        ErrorCode              `json:"code"`
	Severity    ErrorSeverity          `json:"severity"`
	Message     string                 `json:"message"`
	Context     map[string]interface{} `json:"context,omitempty"`
	Stack       string                 `json:"stack,omitempty"`
	Recoverable bool                   `json:"recoverable"`
}

// DefaultErrorHandlerConfig returns default configuration
func DefaultErrorHandlerConfig() ErrorHandlerConfig {
	homeDir, _ := os.UserHomeDir()
	return ErrorHandlerConfig{
		LogToFile:       true,
		LogFilePath:     filepath.Join(homeDir, ".flakedrop", "errors.log"),
		MaxLogEntries:   1000,
		EnableMetrics:   true,
		MetricsInterval: 5 * time.Minute,
	}
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(config ErrorHandlerConfig) (*ErrorHandler, error) {
	handler := &ErrorHandler{
		config:          config,
		errorLog:        make([]ErrorLogEntry, 0),
		circuitBreakers: NewCircuitBreakerManager(),
	}
	
	if config.LogToFile {
		// Ensure log directory exists
		logDir := filepath.Dir(config.LogFilePath)
		if err := os.MkdirAll(logDir, common.DirPermissionNormal); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}
		
		// Open log file
		file, err := os.OpenFile(config.LogFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		
		handler.logFile = file
		handler.logWriter = io.MultiWriter(os.Stderr, file)
	} else {
		handler.logWriter = os.Stderr
	}
	
	// Start metrics collection if enabled
	if config.EnableMetrics {
		go handler.collectMetrics()
	}
	
	return handler, nil
}

// Handle processes an error with full context
func (h *ErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Convert to AppError if needed
	appErr, ok := err.(*AppError)
	if !ok {
		appErr = Wrap(err, ErrCodeInternal, err.Error())
	}
	
	// Create log entry
	entry := ErrorLogEntry{
		Timestamp:   appErr.Timestamp,
		Code:        appErr.Code,
		Severity:    appErr.Severity,
		Message:     appErr.Message,
		Context:     appErr.Context,
		Stack:       appErr.Stack,
		Recoverable: appErr.Recoverable,
	}
	
	// Add to in-memory log
	h.errorLog = append(h.errorLog, entry)
	if len(h.errorLog) > h.config.MaxLogEntries {
		h.errorLog = h.errorLog[1:]
	}
	
	// Write to log
	h.writeLog(entry)
	
	// Display user-friendly error
	h.displayError(appErr)
}

// HandleWithRecovery handles an error and attempts recovery
func (h *ErrorHandler) HandleWithRecovery(err error, recoveryFn func() error) error {
	if err == nil {
		return nil
	}
	
	h.Handle(err)
	
	// Check if error is recoverable
	if IsRecoverable(err) {
		fmt.Fprintln(os.Stderr, "\nAttempting automatic recovery...")
		
		if recoveryErr := recoveryFn(); recoveryErr != nil {
			h.Handle(Wrap(recoveryErr, ErrCodeInternal, "Recovery failed"))
			return recoveryErr
		}
		
		fmt.Fprintln(os.Stderr, "Recovery successful!")
		return nil
	}
	
	return err
}

// writeLog writes an error entry to the log
func (h *ErrorHandler) writeLog(entry ErrorLogEntry) {
	// Format as JSON for structured logging
	jsonData, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(h.logWriter, "Failed to marshal error log: %v\n", err)
		return
	}
	
	fmt.Fprintln(h.logWriter, string(jsonData))
}

// displayError displays a user-friendly error message
func (h *ErrorHandler) displayError(err *AppError) {
	// Use color codes for terminal output
	var severityColor string
	switch err.Severity {
	case SeverityCritical:
		severityColor = "\033[31m" // Red
	case SeverityError:
		severityColor = "\033[91m" // Light Red
	case SeverityWarning:
		severityColor = "\033[33m" // Yellow
	case SeverityInfo:
		severityColor = "\033[36m" // Cyan
	default:
		severityColor = "\033[0m"  // Reset
	}
	
	resetColor := "\033[0m"
	
	// Print error header
	fmt.Fprintf(os.Stderr, "\n%s[%s] %s%s\n", severityColor, err.Code, err.Message, resetColor)
	
	// Print context if available
	if len(err.Context) > 0 {
		fmt.Fprintln(os.Stderr, "\nContext:")
		for key, value := range err.Context {
			fmt.Fprintf(os.Stderr, "  %s: %v\n", key, value)
		}
	}
	
	// Print suggestions
	if len(err.Suggestions) > 0 {
		fmt.Fprintln(os.Stderr, "\nSuggestions:")
		for i, suggestion := range err.Suggestions {
			fmt.Fprintf(os.Stderr, "  %d. %s\n", i+1, suggestion)
		}
	}
	
	// Print support information for critical errors
	if err.Severity == SeverityCritical {
		fmt.Fprintln(os.Stderr, "\nFor support, please include:")
		fmt.Fprintf(os.Stderr, "  - Error code: %s\n", err.Code)
		fmt.Fprintf(os.Stderr, "  - Log file: %s\n", h.config.LogFilePath)
		fmt.Fprintf(os.Stderr, "  - Timestamp: %s\n", err.Timestamp.Format(time.RFC3339))
	}
}

// GetErrorSummary returns a summary of recent errors
func (h *ErrorHandler) GetErrorSummary() map[string]interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	summary := map[string]interface{}{
		"total_errors": len(h.errorLog),
		"by_severity":  make(map[ErrorSeverity]int),
		"by_code":      make(map[ErrorCode]int),
		"recent_errors": []ErrorLogEntry{},
	}
	
	// Count by severity and code
	for _, entry := range h.errorLog {
		summary["by_severity"].(map[ErrorSeverity]int)[entry.Severity]++
		summary["by_code"].(map[ErrorCode]int)[entry.Code]++
	}
	
	// Get recent errors (last 10)
	start := len(h.errorLog) - 10
	if start < 0 {
		start = 0
	}
	summary["recent_errors"] = h.errorLog[start:]
	
	return summary
}

// collectMetrics periodically collects and logs error metrics
func (h *ErrorHandler) collectMetrics() {
	ticker := time.NewTicker(h.config.MetricsInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		summary := h.GetErrorSummary()
		
		// Log metrics
		metrics := map[string]interface{}{
			"timestamp": time.Now(),
			"type":      "error_metrics",
			"summary":   summary,
		}
		
		jsonData, _ := json.Marshal(metrics)
		fmt.Fprintln(h.logWriter, string(jsonData))
	}
}

// Close closes the error handler and releases resources
func (h *ErrorHandler) Close() error {
	if h.logFile != nil {
		return h.logFile.Close()
	}
	return nil
}

// TransactionHandler manages error handling for transactions
type TransactionHandler struct {
	handler      *ErrorHandler
	transaction  interface{}
	rollbackFunc func() error
	committed    bool
}

// NewTransactionHandler creates a new transaction handler
func (h *ErrorHandler) NewTransactionHandler(tx interface{}, rollbackFunc func() error) *TransactionHandler {
	return &TransactionHandler{
		handler:      h,
		transaction:  tx,
		rollbackFunc: rollbackFunc,
		committed:    false,
	}
}

// Execute executes a function within a transaction with automatic rollback on error
func (th *TransactionHandler) Execute(fn func() error) error {
	// Execute the function
	err := fn()
	
	if err != nil {
		// Handle the error
		th.handler.Handle(err)
		
		// Attempt rollback
		if th.rollbackFunc != nil && !th.committed {
			rollbackErr := th.rollbackFunc()
			if rollbackErr != nil {
				th.handler.Handle(Wrap(rollbackErr, ErrCodeSQLTransaction, "Failed to rollback transaction"))
			} else {
				fmt.Fprintln(os.Stderr, "Transaction rolled back successfully")
			}
		}
		
		return err
	}
	
	th.committed = true
	return nil
}

// GlobalErrorHandler is a singleton instance
var globalHandler *ErrorHandler
var globalHandlerOnce sync.Once

// GetGlobalErrorHandler returns the global error handler instance
func GetGlobalErrorHandler() *ErrorHandler {
	globalHandlerOnce.Do(func() {
		handler, err := NewErrorHandler(DefaultErrorHandlerConfig())
		if err != nil {
			// Fallback to basic handler
			handler = &ErrorHandler{
				logWriter:       os.Stderr,
				errorLog:        make([]ErrorLogEntry, 0),
				circuitBreakers: NewCircuitBreakerManager(),
			}
		}
		globalHandler = handler
	})
	return globalHandler
}