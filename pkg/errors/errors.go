package errors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// ErrorCode represents a unique error code for categorizing errors
type ErrorCode string

const (
	// Connection errors (1xxx)
	ErrCodeConnectionFailed    ErrorCode = "SFDE1001"
	ErrCodeConnectionTimeout   ErrorCode = "SFDE1002"
	ErrCodeAuthenticationFailed ErrorCode = "SFDE1003"
	ErrCodeNetworkUnavailable  ErrorCode = "SFDE1004"
	
	// Configuration errors (2xxx)
	ErrCodeConfigNotFound      ErrorCode = "SFDE2001"
	ErrCodeConfigInvalid       ErrorCode = "SFDE2002"
	ErrCodeConfigMissing       ErrorCode = "SFDE2003"
	ErrCodeConfigPermission    ErrorCode = "SFDE2004"
	
	// Repository errors (3xxx)
	ErrCodeRepoNotFound        ErrorCode = "SFDE3001"
	ErrCodeRepoAccessDenied    ErrorCode = "SFDE3002"
	ErrCodeRepoSyncFailed      ErrorCode = "SFDE3003"
	ErrCodeRepoInvalidFormat   ErrorCode = "SFDE3004"
	ErrCodeCommitNotFound      ErrorCode = "SFDE3005"
	
	// SQL execution errors (4xxx)
	ErrCodeSQLSyntax           ErrorCode = "SFDE4001"
	ErrCodeSQLPermission       ErrorCode = "SFDE4002"
	ErrCodeSQLTimeout          ErrorCode = "SFDE4003"
	ErrCodeSQLTransaction      ErrorCode = "SFDE4004"
	ErrCodeSQLObjectNotFound   ErrorCode = "SFDE4005"
	ErrCodeSQLExecution        ErrorCode = "SFDE4006"
	ErrCodeStagingFailed       ErrorCode = "SFDE4007"
	ErrCodeNoResults           ErrorCode = "SFDE4008"
	ErrCodeDuplicateEntry      ErrorCode = "SFDE4009"
	ErrCodeUnknown             ErrorCode = "SFDE4999"
	
	// File system errors (5xxx)
	ErrCodeFileNotFound        ErrorCode = "SFDE5001"
	ErrCodeFilePermission      ErrorCode = "SFDE5002"
	ErrCodeFileCorrupted       ErrorCode = "SFDE5003"
	ErrCodeDiskSpace           ErrorCode = "SFDE5004"
	ErrCodeFileOperation       ErrorCode = "SFDE5005"
	
	// Validation errors (6xxx)
	ErrCodeValidationFailed    ErrorCode = "SFDE6001"
	ErrCodeInvalidInput        ErrorCode = "SFDE6002"
	ErrCodeRequiredField       ErrorCode = "SFDE6003"
	
	// Security errors (7xxx)
	ErrCodeSecurityViolation   ErrorCode = "SFDE7001"
	ErrCodeEncryptionFailed    ErrorCode = "SFDE7002"
	ErrCodeCredentialsExpired  ErrorCode = "SFDE7003"
	
	// System errors (9xxx)
	ErrCodeInternal            ErrorCode = "SFDE9001"
	ErrCodeTimeout             ErrorCode = "SFDE9002"
	ErrCodeResourceExhausted   ErrorCode = "SFDE9003"
	ErrCodeServiceUnavailable  ErrorCode = "SFDE9004"
	ErrCodeResultParsing       ErrorCode = "SFDE9005"
	ErrCodeConfiguration       ErrorCode = "SFDE9006"
	ErrCodeMaxRetriesExceeded  ErrorCode = "SFDE9007"
	
	// Rollback errors (8xxx)
	ErrCodeRollbackFailed      ErrorCode = "SFDE8001"
	ErrCodeRecoveryFailed      ErrorCode = "SFDE8002"
	ErrCodeRestoreFailed       ErrorCode = "SFDE8003"
	ErrCodeNotFound            ErrorCode = "SFDE8004"
	ErrCodeInvalidState        ErrorCode = "SFDE8005"
	ErrCodeIntegrityCheckFailed ErrorCode = "SFDE8006"
	
	// Additional error codes
	ErrCodeInitialization      ErrorCode = "SFDE1005"
	ErrCodeGit                 ErrorCode = "SFDE3006"
	ErrCodeUserInput           ErrorCode = "SFDE6004"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity string

const (
	SeverityCritical ErrorSeverity = "CRITICAL" // System failure, requires immediate attention
	SeverityError    ErrorSeverity = "ERROR"    // Operation failed, but system continues
	SeverityWarning  ErrorSeverity = "WARNING"  // Operation succeeded with issues
	SeverityInfo     ErrorSeverity = "INFO"     // Informational, not an error
)

// AppError represents a structured application error with context
type AppError struct {
	Code        ErrorCode
	Message     string
	Severity    ErrorSeverity
	Context     map[string]interface{}
	Cause       error
	Stack       string
	Timestamp   time.Time
	Recoverable bool
	Suggestions []string
}

// Error implements the error interface
func (e *AppError) Error() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("[%s] %s: %s", e.Code, e.Severity, e.Message))
	
	if e.Cause != nil {
		b.WriteString(fmt.Sprintf("\nCaused by: %v", e.Cause))
	}
	
	if len(e.Suggestions) > 0 {
		b.WriteString("\nSuggestions:")
		for i, suggestion := range e.Suggestions {
			b.WriteString(fmt.Sprintf("\n  %d. %s", i+1, suggestion))
		}
	}
	
	return b.String()
}

// Unwrap returns the cause of the error
func (e *AppError) Unwrap() error {
	return e.Cause
}

// Is implements error comparison
func (e *AppError) Is(target error) bool {
	t, ok := target.(*AppError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// New creates a new AppError
func New(code ErrorCode, message string) *AppError {
	return &AppError{
		Code:        code,
		Message:     message,
		Severity:    SeverityError,
		Context:     make(map[string]interface{}),
		Stack:       captureStack(),
		Timestamp:   time.Now(),
		Recoverable: false,
	}
}

// Wrap wraps an existing error with AppError
func Wrap(err error, code ErrorCode, message string) *AppError {
	if err == nil {
		return nil
	}
	
	appErr := New(code, message)
	appErr.Cause = err
	
	// If wrapping another AppError, inherit some properties
	if ae, ok := err.(*AppError); ok {
		for k, v := range ae.Context {
			appErr.Context[k] = v
		}
	}
	
	return appErr
}

// WithContext adds context to the error
func (e *AppError) WithContext(key string, value interface{}) *AppError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// WithSeverity sets the error severity
func (e *AppError) WithSeverity(severity ErrorSeverity) *AppError {
	e.Severity = severity
	return e
}

// WithSuggestions adds recovery suggestions
func (e *AppError) WithSuggestions(suggestions ...string) *AppError {
	e.Suggestions = append(e.Suggestions, suggestions...)
	return e
}

// AsRecoverable marks the error as recoverable
func (e *AppError) AsRecoverable() *AppError {
	e.Recoverable = true
	return e
}

// captureStack captures the current stack trace
func captureStack() string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	
	var b strings.Builder
	frames := runtime.CallersFrames(pcs[:n])
	
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.File, "runtime/") {
			b.WriteString(fmt.Sprintf("%s:%d %s\n", frame.File, frame.Line, frame.Function))
		}
		if !more {
			break
		}
	}
	
	return b.String()
}

// Common error constructors

// ConnectionError creates a connection-related error
func ConnectionError(message string, cause error) *AppError {
	return Wrap(cause, ErrCodeConnectionFailed, message).
		WithSeverity(SeverityError).
		WithSuggestions(
			"Check your network connection",
			"Verify Snowflake endpoint is accessible",
			"Check firewall settings",
		)
}

// ConfigError creates a configuration-related error
func ConfigError(message string, field string) *AppError {
	return New(ErrCodeConfigInvalid, message).
		WithContext("field", field).
		WithSuggestions(
			fmt.Sprintf("Check the '%s' configuration value", field),
			"Run 'flakedrop setup' to reconfigure",
			"Refer to the configuration documentation",
		)
}

// SQLError creates an SQL execution error
func SQLError(message string, query string, cause error) *AppError {
	err := Wrap(cause, ErrCodeSQLSyntax, message).
		WithContext("query", truncateString(query, 200))
	
	if strings.Contains(message, "permission") || strings.Contains(message, "access denied") {
		err.Code = ErrCodeSQLPermission
		_ = err.WithSuggestions(
			"Check user permissions in Snowflake",
			"Verify the role has required privileges",
			"Contact your Snowflake administrator",
		)
	} else if strings.Contains(message, "timeout") {
		err.Code = ErrCodeSQLTimeout
		_ = err.WithSuggestions(
			"Optimize the query for better performance",
			"Increase the query timeout setting",
			"Check Snowflake warehouse size",
		)
	}
	
	return err
}

// ValidationError creates a validation error
func ValidationError(field string, value interface{}, reason string) *AppError {
	return New(ErrCodeValidationFailed, fmt.Sprintf("Validation failed for %s: %s", field, reason)).
		WithContext("field", field).
		WithContext("value", value).
		WithSeverity(SeverityWarning).
		AsRecoverable()
}

// IsRecoverable checks if an error is recoverable
func IsRecoverable(err error) bool {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Recoverable
	}
	return false
}

// GetErrorCode extracts the error code from an error
func GetErrorCode(err error) ErrorCode {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Code
	}
	return ErrCodeInternal
}

// truncateString truncates a string to maxLen characters
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}