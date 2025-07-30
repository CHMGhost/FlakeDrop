package security

import (
	"crypto/rand"
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
)

// AuditEvent represents a single audit log entry
type AuditEvent struct {
	ID          string                 `json:"id"`
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"event_type"`
	Action      string                 `json:"action"`
	User        string                 `json:"user"`
	Resource    string                 `json:"resource"`
	Result      string                 `json:"result"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Source      AuditSource            `json:"source"`
	Hash        string                 `json:"hash"`
	PrevHash    string                 `json:"prev_hash,omitempty"`
}

// AuditSource contains information about where the event originated
type AuditSource struct {
	IP        string `json:"ip,omitempty"`
	Hostname  string `json:"hostname"`
	Process   string `json:"process"`
	PID       int    `json:"pid"`
	SessionID string `json:"session_id"`
}

// AuditLogger handles secure audit logging
type AuditLogger struct {
	mu          sync.Mutex
	logPath     string
	sessionID   string
	rotateSize  int64
	retainDays  int
	hashChain   bool
	lastHash    string
	eventBuffer []AuditEvent
	bufferSize  int
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(logPath string) (*AuditLogger, error) {
	if logPath == "" {
		home, _ := os.UserHomeDir()
		logPath = filepath.Join(home, ".flakedrop", "audit")
	}

	// Ensure audit directory exists with proper permissions
	if err := os.MkdirAll(logPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create audit directory: %w", err)
	}

	sessionID := generateSessionID()

	logger := &AuditLogger{
		logPath:    logPath,
		sessionID:  sessionID,
		rotateSize: 10 * 1024 * 1024, // 10MB
		retainDays: 90,
		hashChain:  true,
		bufferSize: 100,
	}

	// Load last hash for chain continuity
	logger.lastHash = logger.loadLastHash()

	// Start background workers
	go logger.rotateWorker()
	go logger.flushWorker()

	return logger, nil
}

// LogEvent logs an audit event
func (al *AuditLogger) LogEvent(eventType, action, resource, result string, details map[string]interface{}) error {
	event := AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now().UTC(),
		EventType: eventType,
		Action:    action,
		User:      al.getCurrentUser(),
		Resource:  resource,
		Result:    result,
		Details:   al.sanitizeDetails(details),
		Source:    al.getSource(),
	}

	al.mu.Lock()
	defer al.mu.Unlock()

	// Add hash chain
	if al.hashChain {
		event.PrevHash = al.lastHash
		event.Hash = al.calculateHash(event)
		al.lastHash = event.Hash
	}

	// Add to buffer
	al.eventBuffer = append(al.eventBuffer, event)

	// Flush if buffer is full
	if len(al.eventBuffer) >= al.bufferSize {
		return al.flushEvents()
	}

	return nil
}

// Common audit event types
const (
	EventTypeAuth       = "authentication"
	EventTypeAccess     = "access"
	EventTypeConfig     = "configuration"
	EventTypeDeployment = "deployment"
	EventTypeSecurity   = "security"
	EventTypeError      = "error"
)

// Common actions
const (
	ActionLogin        = "login"
	ActionLogout       = "logout"
	ActionCreate       = "create"
	ActionRead         = "read"
	ActionUpdate       = "update"
	ActionDelete       = "delete"
	ActionDeploy       = "deploy"
	ActionRollback     = "rollback"
	ActionGrant        = "grant"
	ActionRevoke       = "revoke"
)

// LogAuthentication logs authentication events
func (al *AuditLogger) LogAuthentication(action, result string, details map[string]interface{}) error {
	return al.LogEvent(EventTypeAuth, action, "authentication", result, details)
}

// LogAccess logs resource access events
func (al *AuditLogger) LogAccess(action, resource, result string, details map[string]interface{}) error {
	return al.LogEvent(EventTypeAccess, action, resource, result, details)
}

// LogDeployment logs deployment events
func (al *AuditLogger) LogDeployment(action, resource, result string, details map[string]interface{}) error {
	return al.LogEvent(EventTypeDeployment, action, resource, result, details)
}

// LogSecurityEvent logs security-related events
func (al *AuditLogger) LogSecurityEvent(action, resource, result string, details map[string]interface{}) error {
	return al.LogEvent(EventTypeSecurity, action, resource, result, details)
}

// Query searches audit logs
func (al *AuditLogger) Query(filter AuditFilter) ([]AuditEvent, error) {
	al.mu.Lock()
	defer al.mu.Unlock()

	// Flush current buffer first
	if err := al.flushEvents(); err != nil {
		return nil, err
	}

	var events []AuditEvent

	// Get list of log files to search
	files, err := al.getLogFiles(filter.StartTime, filter.EndTime)
	if err != nil {
		return nil, err
	}

	// Search each file
	for _, file := range files {
		fileEvents, err := al.searchLogFile(file, filter)
		if err != nil {
			return nil, err
		}
		events = append(events, fileEvents...)
	}

	return events, nil
}

// AuditFilter defines criteria for searching audit logs
type AuditFilter struct {
	StartTime *time.Time
	EndTime   *time.Time
	EventType string
	Action    string
	User      string
	Resource  string
	Result    string
	Limit     int
}

// VerifyIntegrity verifies the hash chain integrity of audit logs
func (al *AuditLogger) VerifyIntegrity(startTime, endTime time.Time) (*IntegrityReport, error) {
	al.mu.Lock()
	defer al.mu.Unlock()

	report := &IntegrityReport{
		StartTime:    startTime,
		EndTime:      endTime,
		CheckedFiles: 0,
		ValidEvents:  0,
		Issues:       []string{},
	}

	files, err := al.getLogFiles(&startTime, &endTime)
	if err != nil {
		return nil, err
	}

	var prevHash string
	for _, file := range files {
		report.CheckedFiles++

		events, err := al.loadEventsFromFile(file)
		if err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("Failed to read file %s: %v", file, err))
			continue
		}

		for i, event := range events {
			// Verify hash chain
			if al.hashChain {
				if i == 0 && prevHash != "" && event.PrevHash != prevHash {
					report.Issues = append(report.Issues, 
						fmt.Sprintf("Hash chain broken between files at event %s", event.ID))
				}

				calculatedHash := al.calculateHash(event)
				if calculatedHash != event.Hash {
					report.Issues = append(report.Issues,
						fmt.Sprintf("Invalid hash for event %s", event.ID))
				} else {
					report.ValidEvents++
				}

				prevHash = event.Hash
			} else {
				report.ValidEvents++
			}
		}
	}

	report.IsValid = len(report.Issues) == 0
	return report, nil
}

// IntegrityReport contains the results of an integrity check
type IntegrityReport struct {
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	CheckedFiles int       `json:"checked_files"`
	ValidEvents  int       `json:"valid_events"`
	IsValid      bool      `json:"is_valid"`
	Issues       []string  `json:"issues,omitempty"`
}

// Helper methods

func (al *AuditLogger) getCurrentUser() string {
	user := os.Getenv("USER")
	if user == "" {
		user = os.Getenv("USERNAME")
	}
	if user == "" {
		user = "unknown"
	}
	return user
}

func (al *AuditLogger) getSource() AuditSource {
	hostname, _ := os.Hostname()
	
	return AuditSource{
		Hostname:  hostname,
		Process:   os.Args[0],
		PID:       os.Getpid(),
		SessionID: al.sessionID,
	}
}

func (al *AuditLogger) sanitizeDetails(details map[string]interface{}) map[string]interface{} {
	if details == nil {
		return nil
	}

	// Remove sensitive fields
	sanitized := make(map[string]interface{})
	sensitiveFields := []string{"password", "token", "key", "secret", "credential"}

	for k, v := range details {
		isSensitive := false
		lowerKey := strings.ToLower(k)
		
		for _, sensitive := range sensitiveFields {
			if strings.Contains(lowerKey, sensitive) {
				isSensitive = true
				break
			}
		}

		if isSensitive {
			sanitized[k] = "[REDACTED]"
		} else {
			sanitized[k] = v
		}
	}

	return sanitized
}

func (al *AuditLogger) calculateHash(event AuditEvent) string {
	// Create a copy without the hash field
	eventCopy := event
	eventCopy.Hash = ""
	
	data, _ := json.Marshal(eventCopy)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (al *AuditLogger) flushEvents() error {
	if len(al.eventBuffer) == 0 {
		return nil
	}

	// Get current log file
	logFile := al.getCurrentLogFile()
	
	// Validate the log file path
	validatedLogFile, err := common.ValidatePath(logFile, al.logPath)
	if err != nil {
		return fmt.Errorf("invalid log file path: %w", err)
	}
	
	// Open file for appending
	file, err := os.OpenFile(validatedLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600) // #nosec G304 - path is validated
	if err != nil {
		return fmt.Errorf("failed to open audit log: %w", err)
	}
	defer file.Close()

	// Write events
	encoder := json.NewEncoder(file)
	for _, event := range al.eventBuffer {
		if err := encoder.Encode(event); err != nil {
			return fmt.Errorf("failed to write audit event: %w", err)
		}
	}

	// Clear buffer
	al.eventBuffer = al.eventBuffer[:0]

	// Check if rotation is needed
	info, _ := file.Stat()
	if info.Size() > al.rotateSize {
		go al.rotateLog()
	}

	return nil
}

func (al *AuditLogger) getCurrentLogFile() string {
	return filepath.Join(al.logPath, "audit.log")
}

func (al *AuditLogger) rotateLog() {
	al.mu.Lock()
	defer al.mu.Unlock()

	currentFile := al.getCurrentLogFile()
	timestamp := time.Now().Format("20060102-150405")
	rotatedFile := filepath.Join(al.logPath, fmt.Sprintf("audit-%s.log", timestamp))

	// Rename current file
	if err := os.Rename(currentFile, rotatedFile); err != nil {
		// Log rotation failed, but we continue
		return
	}

	// Compress rotated file
	go al.compressLog(rotatedFile)
}

func (al *AuditLogger) compressLog(logFile string) {
	// Implementation would compress the log file
	// For now, we'll leave it uncompressed
}

func (al *AuditLogger) flushWorker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		al.mu.Lock()
		_ = al.flushEvents()
		al.mu.Unlock()
	}
}

func (al *AuditLogger) rotateWorker() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		al.cleanOldLogs()
	}
}

func (al *AuditLogger) cleanOldLogs() {
	cutoff := time.Now().AddDate(0, 0, -al.retainDays)

	entries, err := os.ReadDir(al.logPath)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			os.Remove(filepath.Join(al.logPath, entry.Name()))
		}
	}
}

func (al *AuditLogger) loadLastHash() string {
	hashFile := filepath.Join(al.logPath, ".lasthash")
	
	// Validate the hash file path
	validatedHashFile, err := common.ValidatePath(hashFile, al.logPath)
	if err != nil {
		return ""
	}
	
	data, err := os.ReadFile(validatedHashFile) // #nosec G304 - path is validated
	if err != nil {
		return ""
	}
	return string(data)
}

func (al *AuditLogger) saveLastHash() {
	if al.lastHash == "" {
		return
	}
	
	hashFile := filepath.Join(al.logPath, ".lasthash")
	_ = os.WriteFile(hashFile, []byte(al.lastHash), 0600)
}

func (al *AuditLogger) getLogFiles(startTime, endTime *time.Time) ([]string, error) {
	entries, err := os.ReadDir(al.logPath)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		// Check if file is within time range
		info, err := entry.Info()
		if err != nil {
			continue
		}

		if startTime != nil && info.ModTime().Before(*startTime) {
			continue
		}

		if endTime != nil && info.ModTime().After(*endTime) {
			continue
		}

		files = append(files, filepath.Join(al.logPath, entry.Name()))
	}

	return files, nil
}

func (al *AuditLogger) searchLogFile(filename string, filter AuditFilter) ([]AuditEvent, error) {
	// Validate the file path
	validatedFile, err := common.ValidatePath(filename, al.logPath)
	if err != nil {
		return nil, fmt.Errorf("invalid log file path: %w", err)
	}
	
	file, err := os.Open(validatedFile) // #nosec G304 - path is validated
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []AuditEvent
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var event AuditEvent
		if err := decoder.Decode(&event); err != nil {
			continue
		}

		if al.matchesFilter(event, filter) {
			events = append(events, event)
			
			if filter.Limit > 0 && len(events) >= filter.Limit {
				break
			}
		}
	}

	return events, nil
}

func (al *AuditLogger) matchesFilter(event AuditEvent, filter AuditFilter) bool {
	if filter.StartTime != nil && event.Timestamp.Before(*filter.StartTime) {
		return false
	}

	if filter.EndTime != nil && event.Timestamp.After(*filter.EndTime) {
		return false
	}

	if filter.EventType != "" && event.EventType != filter.EventType {
		return false
	}

	if filter.Action != "" && event.Action != filter.Action {
		return false
	}

	if filter.User != "" && event.User != filter.User {
		return false
	}

	if filter.Resource != "" && !strings.Contains(event.Resource, filter.Resource) {
		return false
	}

	if filter.Result != "" && event.Result != filter.Result {
		return false
	}

	return true
}

func (al *AuditLogger) loadEventsFromFile(filename string) ([]AuditEvent, error) {
	// Validate the file path
	validatedPath, err := common.ValidatePath(filename, filepath.Dir(al.logPath))
	if err != nil {
		return nil, fmt.Errorf("invalid log file path: %w", err)
	}
	
	file, err := os.Open(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []AuditEvent
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var event AuditEvent
		if err := decoder.Decode(&event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// Close flushes remaining events and saves state
func (al *AuditLogger) Close() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	// Flush remaining events
	if err := al.flushEvents(); err != nil {
		return err
	}

	// Save last hash
	al.saveLastHash()

	return nil
}

// Utility functions

func generateEventID() string {
	timestamp := time.Now().UnixNano()
	random := make([]byte, 8)
	_, _ = rand.Read(random)
	
	data := fmt.Sprintf("%d-%s", timestamp, hex.EncodeToString(random))
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16])
}

func generateSessionID() string {
	data := make([]byte, 16)
	_, _ = rand.Read(data)
	return hex.EncodeToString(data)
}