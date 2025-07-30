package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

var levelNames = map[LogLevel]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
	FatalLevel: "FATAL",
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp   time.Time              `json:"timestamp"`
	Level       string                 `json:"level"`
	Message     string                 `json:"message"`
	Fields      map[string]interface{} `json:"fields,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
	SpanID      string                 `json:"span_id,omitempty"`
	Service     string                 `json:"service"`
	Version     string                 `json:"version"`
	Environment string                 `json:"environment,omitempty"`
	Host        string                 `json:"host,omitempty"`
	Caller      string                 `json:"caller,omitempty"`
	Stack       string                 `json:"stack,omitempty"`
}

// Logger provides structured logging capabilities
type Logger struct {
	mu          sync.RWMutex
	level       LogLevel
	output      io.Writer
	fields      map[string]interface{}
	service     string
	version     string
	environment string
	hostname    string
	hooks       []LogHook
	encoder     LogEncoder
}

// LogHook allows processing of log entries before output
type LogHook interface {
	Process(entry *LogEntry) error
}

// LogEncoder handles encoding of log entries
type LogEncoder interface {
	Encode(entry *LogEntry) ([]byte, error)
}

// JSONEncoder encodes log entries as JSON
type JSONEncoder struct {
	pretty bool
}

// NewJSONEncoder creates a new JSON encoder
func NewJSONEncoder(pretty bool) *JSONEncoder {
	return &JSONEncoder{pretty: pretty}
}

// Encode encodes a log entry to JSON
func (e *JSONEncoder) Encode(entry *LogEntry) ([]byte, error) {
	if e.pretty {
		return json.MarshalIndent(entry, "", "  ")
	}
	return json.Marshal(entry)
}

// LoggerConfig contains logger configuration
type LoggerConfig struct {
	Level       LogLevel
	Output      io.Writer
	Service     string
	Version     string
	Environment string
	Encoder     LogEncoder
}

// NewLogger creates a new logger instance
func NewLogger(config LoggerConfig) *Logger {
	hostname, _ := os.Hostname()
	
	if config.Output == nil {
		config.Output = os.Stdout
	}
	
	if config.Encoder == nil {
		config.Encoder = NewJSONEncoder(false)
	}
	
	return &Logger{
		level:       config.Level,
		output:      config.Output,
		fields:      make(map[string]interface{}),
		service:     config.Service,
		version:     config.Version,
		environment: config.Environment,
		hostname:    hostname,
		hooks:       make([]LogHook, 0),
		encoder:     config.Encoder,
	}
}

// AddHook adds a log hook
func (l *Logger) AddHook(hook LogHook) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hooks = append(l.hooks, hook)
}

// WithField returns a new logger with an additional field
func (l *Logger) WithField(key string, value interface{}) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	newFields[key] = value
	
	return &Logger{
		level:       l.level,
		output:      l.output,
		fields:      newFields,
		service:     l.service,
		version:     l.version,
		environment: l.environment,
		hostname:    l.hostname,
		hooks:       l.hooks,
		encoder:     l.encoder,
	}
}

// WithFields returns a new logger with additional fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}
	
	return &Logger{
		level:       l.level,
		output:      l.output,
		fields:      newFields,
		service:     l.service,
		version:     l.version,
		environment: l.environment,
		hostname:    l.hostname,
		hooks:       l.hooks,
		encoder:     l.encoder,
	}
}

// WithContext returns a new logger with trace context
func (l *Logger) WithContext(ctx context.Context) *Logger {
	traceID := GetTraceID(ctx)
	spanID := GetSpanID(ctx)
	
	fields := make(map[string]interface{})
	if traceID != "" {
		fields["trace_id"] = traceID
	}
	if spanID != "" {
		fields["span_id"] = spanID
	}
	
	return l.WithFields(fields)
}

// log writes a log entry
func (l *Logger) log(level LogLevel, msg string, fields map[string]interface{}) {
	if level < l.level {
		return
	}
	
	entry := &LogEntry{
		Timestamp:   time.Now(),
		Level:       levelNames[level],
		Message:     msg,
		Fields:      make(map[string]interface{}),
		Service:     l.service,
		Version:     l.version,
		Environment: l.environment,
		Host:        l.hostname,
	}
	
	// Add logger fields
	l.mu.RLock()
	for k, v := range l.fields {
		entry.Fields[k] = v
	}
	l.mu.RUnlock()
	
	// Add method fields
	for k, v := range fields {
		entry.Fields[k] = v
	}
	
	// Extract trace context from fields
	if traceID, ok := entry.Fields["trace_id"].(string); ok {
		entry.TraceID = traceID
		delete(entry.Fields, "trace_id")
	}
	if spanID, ok := entry.Fields["span_id"].(string); ok {
		entry.SpanID = spanID
		delete(entry.Fields, "span_id")
	}
	
	// Add caller information
	if pc, file, line, ok := runtime.Caller(2); ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			entry.Caller = fmt.Sprintf("%s:%d %s", file, line, fn.Name())
		}
	}
	
	// Add stack trace for errors
	if level >= ErrorLevel && entry.Stack == "" {
		entry.Stack = getStackTrace(3)
	}
	
	// Process hooks
	for _, hook := range l.hooks {
		if err := hook.Process(entry); err != nil {
			// Log hook error to stderr
			fmt.Fprintf(os.Stderr, "Log hook error: %v\n", err)
		}
	}
	
	// Encode and write
	data, err := l.encoder.Encode(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode log entry: %v\n", err)
		return
	}
	
	l.mu.Lock()
	defer l.mu.Unlock()
	
	_, _ = l.output.Write(data)
	_, _ = l.output.Write([]byte("\n"))
}

// Debug logs a debug message
func (l *Logger) Debug(msg string) {
	l.log(DebugLevel, msg, nil)
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log(DebugLevel, fmt.Sprintf(format, args...), nil)
}

// DebugWithFields logs a debug message with fields
func (l *Logger) DebugWithFields(msg string, fields map[string]interface{}) {
	l.log(DebugLevel, msg, fields)
}

// Info logs an info message
func (l *Logger) Info(msg string) {
	l.log(InfoLevel, msg, nil)
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log(InfoLevel, fmt.Sprintf(format, args...), nil)
}

// InfoWithFields logs an info message with fields
func (l *Logger) InfoWithFields(msg string, fields map[string]interface{}) {
	l.log(InfoLevel, msg, fields)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string) {
	l.log(WarnLevel, msg, nil)
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log(WarnLevel, fmt.Sprintf(format, args...), nil)
}

// WarnWithFields logs a warning message with fields
func (l *Logger) WarnWithFields(msg string, fields map[string]interface{}) {
	l.log(WarnLevel, msg, fields)
}

// Error logs an error message
func (l *Logger) Error(msg string) {
	l.log(ErrorLevel, msg, nil)
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log(ErrorLevel, fmt.Sprintf(format, args...), nil)
}

// ErrorWithFields logs an error message with fields
func (l *Logger) ErrorWithFields(msg string, fields map[string]interface{}) {
	l.log(ErrorLevel, msg, fields)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string) {
	l.log(FatalLevel, msg, nil)
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(FatalLevel, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

// FatalWithFields logs a fatal message with fields and exits
func (l *Logger) FatalWithFields(msg string, fields map[string]interface{}) {
	l.log(FatalLevel, msg, fields)
	os.Exit(1)
}

// SetLevel sets the minimum log level
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// getStackTrace returns the current stack trace
func getStackTrace(skip int) string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])
	
	// Skip the first few lines
	lines := strings.Split(stack, "\n")
	if len(lines) > skip*2 {
		lines = lines[skip*2:]
	}
	
	return strings.Join(lines, "\n")
}

// LogLevelFromString converts a string to LogLevel
func LogLevelFromString(level string) LogLevel {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return DebugLevel
	case "INFO":
		return InfoLevel
	case "WARN", "WARNING":
		return WarnLevel
	case "ERROR":
		return ErrorLevel
	case "FATAL":
		return FatalLevel
	default:
		return InfoLevel
	}
}

// Global logger instance
var defaultLogger = NewLogger(LoggerConfig{
	Level:   InfoLevel,
	Service: "flakedrop",
	Version: "1.0.0",
})

// SetDefaultLogger sets the global default logger
func SetDefaultLogger(logger *Logger) {
	defaultLogger = logger
}

// GetDefaultLogger returns the global default logger
func GetDefaultLogger() *Logger {
	return defaultLogger
}

// Package-level convenience functions

// Debug logs a debug message using the default logger
func Debug(msg string) {
	defaultLogger.Debug(msg)
}

// Debugf logs a formatted debug message using the default logger
func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

// Info logs an info message using the default logger
func Info(msg string) {
	defaultLogger.Info(msg)
}

// Infof logs a formatted info message using the default logger
func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

// Warn logs a warning message using the default logger
func Warn(msg string) {
	defaultLogger.Warn(msg)
}

// Warnf logs a formatted warning message using the default logger
func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

// Error logs an error message using the default logger
func Error(msg string) {
	defaultLogger.Error(msg)
}

// Errorf logs a formatted error message using the default logger
func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

// Fatal logs a fatal message using the default logger and exits
func Fatal(msg string) {
	defaultLogger.Fatal(msg)
}

// Fatalf logs a formatted fatal message using the default logger and exits
func Fatalf(format string, args ...interface{}) {
	defaultLogger.Fatalf(format, args...)
}