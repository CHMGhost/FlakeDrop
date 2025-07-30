package plugin

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/pkg/models"
)

// SecurityManager manages plugin security and sandboxing
type SecurityManager struct {
	config        *models.PluginSecurityConfig
	trustedHashes map[string]bool
	permissions   map[string][]Permission
	sandboxes     map[string]*Sandbox
	mu            sync.RWMutex
}

// Sandbox represents a security sandbox for plugin execution
type Sandbox struct {
	pluginName    string
	maxMemoryMB   int
	maxExecTime   time.Duration
	allowedPaths  []string
	permissions   []Permission
	resourceUsage *ResourceUsage
	active        bool
	mu            sync.RWMutex
}

// ResourceUsage tracks resource usage for a plugin
type ResourceUsage struct {
	MemoryUsageMB  int
	CPUTime        time.Duration
	DiskReadMB     int
	DiskWriteMB    int
	NetworkReqCount int
	StartTime      time.Time
	LastActivity   time.Time
}

// SecurityContext contains security information for plugin execution
type SecurityContext struct {
	PluginName    string
	Permissions   []Permission
	TrustedSources []string
	Sandbox       *Sandbox
	StartTime     time.Time
}

// PermissionType represents different types of permissions
type PermissionType string

const (
	PermissionRead         PermissionType = "read"
	PermissionWrite        PermissionType = "write"
	PermissionNetwork      PermissionType = "network"
	PermissionExec         PermissionType = "exec" 
	PermissionEnv          PermissionType = "env"
	PermissionConfig       PermissionType = "config"
	PermissionNotify       PermissionType = "notify"
	PermissionTransform    PermissionType = "transform"
	PermissionFileSystem   PermissionType = "filesystem"
	PermissionDatabase     PermissionType = "database"
)

// NewSecurityManager creates a new security manager
func NewSecurityManager(config *models.PluginSecurityConfig) *SecurityManager {
	return &SecurityManager{
		config:        config,
		trustedHashes: make(map[string]bool),
		permissions:   make(map[string][]Permission),
		sandboxes:     make(map[string]*Sandbox),
	}
}

// ValidatePlugin validates a plugin before loading
func (sm *SecurityManager) ValidatePlugin(pluginPath string, manifest PluginManifest) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Check if plugin is from trusted source
	if !sm.isTrustedSource(manifest.Repository) && !sm.config.AllowUnsigned {
		return fmt.Errorf("plugin %s is not from a trusted source", manifest.Name)
	}

	// Validate plugin hash if available
	if err := sm.validatePluginHash(pluginPath, manifest); err != nil {
		return fmt.Errorf("plugin hash validation failed: %w", err)
	}

	// Check required permissions
	for _, permission := range manifest.Permissions {
		if !sm.isPermissionAllowed(permission.Name) {
			return fmt.Errorf("plugin requires disallowed permission: %s", permission.Name)
		}
	}

	// Validate plugin size and structure
	if err := sm.validatePluginStructure(pluginPath); err != nil {
		return fmt.Errorf("plugin structure validation failed: %w", err)
	}

	return nil
}

// CreateSandbox creates a security sandbox for a plugin
func (sm *SecurityManager) CreateSandbox(pluginName string, manifest PluginManifest) (*Sandbox, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.config.SandboxEnabled {
		return nil, nil // Sandboxing disabled
	}

	// Check if sandbox already exists
	if sandbox, exists := sm.sandboxes[pluginName]; exists {
		return sandbox, nil
	}

	// Create new sandbox
	sandbox := &Sandbox{
		pluginName:    pluginName,
		maxMemoryMB:   sm.config.MaxMemoryMB,
		permissions:   manifest.Permissions,
		resourceUsage: &ResourceUsage{
			StartTime:    time.Now(),
			LastActivity: time.Now(),
		},
		active: false,
	}

	// Parse max execution time
	if sm.config.MaxExecutionTime != "" {
		duration, err := time.ParseDuration(sm.config.MaxExecutionTime)
		if err != nil {
			return nil, fmt.Errorf("invalid max execution time: %w", err)
		}
		sandbox.maxExecTime = duration
	}

	// Set allowed paths based on permissions
	sandbox.allowedPaths = sm.getAllowedPaths(manifest.Permissions)

	sm.sandboxes[pluginName] = sandbox
	return sandbox, nil
}

// EnterSandbox enters a sandbox for plugin execution
func (s *Sandbox) EnterSandbox(ctx context.Context) (*SecurityContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		return nil, fmt.Errorf("sandbox already active for plugin %s", s.pluginName)
	}

	s.active = true
	s.resourceUsage.StartTime = time.Now()
	s.resourceUsage.LastActivity = time.Now()

	securityContext := &SecurityContext{
		PluginName:  s.pluginName,
		Permissions: s.permissions,
		Sandbox:     s,
		StartTime:   time.Now(),
	}

	// Start resource monitoring
	go s.monitorResources(ctx)

	return securityContext, nil
}

// ExitSandbox exits the sandbox
func (s *Sandbox) ExitSandbox() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return fmt.Errorf("sandbox not active for plugin %s", s.pluginName)
	}

	s.active = false
	return nil
}

// CheckPermission checks if a plugin has a specific permission
func (sc *SecurityContext) CheckPermission(permissionType PermissionType) error {
	for _, permission := range sc.Permissions {
		if permission.Name == string(permissionType) {
			return nil
		}
	}

	return fmt.Errorf("plugin %s does not have permission %s", sc.PluginName, permissionType)
}

// CheckResourceUsage checks if the plugin is within resource limits
func (s *Sandbox) CheckResourceUsage() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.active {
		return nil
	}

	// Check memory usage
	if s.resourceUsage.MemoryUsageMB > s.maxMemoryMB {
		return fmt.Errorf("plugin %s exceeded memory limit: %dMB > %dMB", 
			s.pluginName, s.resourceUsage.MemoryUsageMB, s.maxMemoryMB)
	}

	// Check execution time
	if s.maxExecTime > 0 {
		elapsed := time.Since(s.resourceUsage.StartTime)
		if elapsed > s.maxExecTime {
			return fmt.Errorf("plugin %s exceeded execution time limit: %s > %s", 
				s.pluginName, elapsed, s.maxExecTime)
		}
	}

	return nil
}

// GetResourceUsage returns current resource usage
func (s *Sandbox) GetResourceUsage() *ResourceUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to avoid concurrent access issues
	return &ResourceUsage{
		MemoryUsageMB:   s.resourceUsage.MemoryUsageMB,
		CPUTime:         s.resourceUsage.CPUTime,
		DiskReadMB:      s.resourceUsage.DiskReadMB,
		DiskWriteMB:     s.resourceUsage.DiskWriteMB,
		NetworkReqCount: s.resourceUsage.NetworkReqCount,
		StartTime:       s.resourceUsage.StartTime,
		LastActivity:    s.resourceUsage.LastActivity,
	}
}

// monitorResources monitors resource usage for the sandbox
func (s *Sandbox) monitorResources(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			if !s.active {
				s.mu.Unlock()
				return
			}

			// Update resource usage (simplified implementation)
			// In a real implementation, you would use proper OS APIs to get actual usage
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			// Safe conversion to avoid integer overflow
			memoryMB := m.Alloc / 1024 / 1024
			if memoryMB > math.MaxInt32 {
				s.resourceUsage.MemoryUsageMB = math.MaxInt32
			} else {
				s.resourceUsage.MemoryUsageMB = int(memoryMB)
			}
			s.resourceUsage.LastActivity = time.Now()

			s.mu.Unlock()

			// Check if resource limits are exceeded
			if err := s.CheckResourceUsage(); err != nil {
				// In a real implementation, you would terminate the plugin
				fmt.Printf("Resource limit exceeded: %v\n", err)
				return
			}
		}
	}
}

// isTrustedSource checks if a source is trusted
func (sm *SecurityManager) isTrustedSource(source string) bool {
	for _, trustedSource := range sm.config.TrustedSources {
		if strings.Contains(source, trustedSource) {
			return true
		}
	}
	return false
}

// validatePluginHash validates the plugin file hash
func (sm *SecurityManager) validatePluginHash(pluginPath string, manifest PluginManifest) error {
	// Calculate file hash
	pluginPath, err := common.ValidatePath(pluginPath, "")
	if err != nil {
		return fmt.Errorf("invalid plugin path: %w", err)
	}
	file, err := os.Open(pluginPath) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to open plugin file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("failed to calculate plugin hash: %w", err)
	}

	calculatedHash := fmt.Sprintf("%x", hash.Sum(nil))

	// Check against trusted hashes (in a real implementation, this would come from manifest or registry)
	if len(sm.trustedHashes) > 0 {
		if !sm.trustedHashes[calculatedHash] {
			return fmt.Errorf("plugin hash %s is not trusted", calculatedHash)
		}
	}

	return nil
}

// isPermissionAllowed checks if a permission is allowed
func (sm *SecurityManager) isPermissionAllowed(permission string) bool {
	for _, allowedPermission := range sm.config.AllowedPermissions {
		if permission == allowedPermission {
			return true
		}
	}
	return false
}

// validatePluginStructure validates the plugin file structure
func (sm *SecurityManager) validatePluginStructure(pluginPath string) error {
	// Check file size
	fileInfo, err := os.Stat(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to get plugin file info: %w", err)
	}

	// Limit plugin size to 100MB
	maxSize := int64(100 * 1024 * 1024)
	if fileInfo.Size() > maxSize {
		return fmt.Errorf("plugin file too large: %d bytes > %d bytes", fileInfo.Size(), maxSize)
	}

	// Validate file extension
	ext := filepath.Ext(pluginPath)
	allowedExtensions := []string{".so", ".dll", ".dylib", ".wasm"}
	
	isAllowed := false
	for _, allowedExt := range allowedExtensions {
		if ext == allowedExt {
			isAllowed = true
			break
		}
	}

	if !isAllowed {
		return fmt.Errorf("unsupported plugin file extension: %s", ext)
	}

	return nil
}

// getAllowedPaths returns allowed file system paths based on permissions
func (sm *SecurityManager) getAllowedPaths(permissions []Permission) []string {
	var paths []string

	for _, permission := range permissions {
		switch permission.Name {
		case string(PermissionRead), string(PermissionWrite):
			// Allow access to plugin data directory
			paths = append(paths, "~/.flakedrop/plugin-data")
		case string(PermissionConfig):
			// Allow access to configuration directory
			paths = append(paths, "~/.flakedrop/plugins")
		case string(PermissionFileSystem):
			// Allow broader file system access (with caution)
			paths = append(paths, "/tmp", "~/Downloads")
		}
	}

	return paths
}

// SecurePluginWrapper wraps plugin execution with security checks
type SecurePluginWrapper struct {
	plugin    PluginInterface
	security  *SecurityManager
	sandbox   *Sandbox
	context   *SecurityContext
}

// NewSecurePluginWrapper creates a new secure plugin wrapper
func NewSecurePluginWrapper(plugin PluginInterface, security *SecurityManager, sandbox *Sandbox) *SecurePluginWrapper {
	return &SecurePluginWrapper{
		plugin:   plugin,
		security: security,
		sandbox:  sandbox,
	}
}

// Initialize initializes the plugin within a security context
func (spw *SecurePluginWrapper) Initialize(ctx context.Context, config PluginConfig) error {
	if spw.sandbox != nil {
		securityContext, err := spw.sandbox.EnterSandbox(ctx)
		if err != nil {
			return fmt.Errorf("failed to enter sandbox: %w", err)
		}
		spw.context = securityContext
		defer spw.sandbox.ExitSandbox()
	}

	return spw.plugin.Initialize(ctx, config)
}

// Name returns the plugin name
func (spw *SecurePluginWrapper) Name() string {
	return spw.plugin.Name()
}

// Version returns the plugin version
func (spw *SecurePluginWrapper) Version() string {
	return spw.plugin.Version()
}

// Description returns the plugin description
func (spw *SecurePluginWrapper) Description() string {
	return spw.plugin.Description()
}

// Shutdown shuts down the plugin within security context
func (spw *SecurePluginWrapper) Shutdown(ctx context.Context) error {
	if spw.sandbox != nil && spw.context != nil {
		defer spw.sandbox.ExitSandbox()
	}

	return spw.plugin.Shutdown(ctx)
}

// SecurityAuditLog provides security event logging
type SecurityAuditLog struct {
	events []SecurityEvent
	mu     sync.RWMutex
}

// SecurityEvent represents a security-related event
type SecurityEvent struct {
	Timestamp   time.Time   `json:"timestamp"`
	PluginName  string      `json:"plugin_name"`
	EventType   string      `json:"event_type"`
	Description string      `json:"description"`
	Severity    string      `json:"severity"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// NewSecurityAuditLog creates a new security audit log
func NewSecurityAuditLog() *SecurityAuditLog {
	return &SecurityAuditLog{
		events: make([]SecurityEvent, 0),
	}
}

// LogEvent logs a security event
func (sal *SecurityAuditLog) LogEvent(event SecurityEvent) {
	sal.mu.Lock()
	defer sal.mu.Unlock()

	event.Timestamp = time.Now()
	sal.events = append(sal.events, event)

	// Keep only the last 1000 events
	if len(sal.events) > 1000 {
		sal.events = sal.events[1:]
	}
}

// GetEvents returns security events
func (sal *SecurityAuditLog) GetEvents(limit int) []SecurityEvent {
	sal.mu.RLock()
	defer sal.mu.RUnlock()

	if limit == 0 || limit > len(sal.events) {
		limit = len(sal.events)
	}

	// Return the most recent events
	start := len(sal.events) - limit
	if start < 0 {
		start = 0
	}

	result := make([]SecurityEvent, limit)
	copy(result, sal.events[start:])
	return result
}

// GetEventsByPlugin returns security events for a specific plugin
func (sal *SecurityAuditLog) GetEventsByPlugin(pluginName string, limit int) []SecurityEvent {
	sal.mu.RLock()
	defer sal.mu.RUnlock()

	var result []SecurityEvent
	for i := len(sal.events) - 1; i >= 0 && len(result) < limit; i-- {
		if sal.events[i].PluginName == pluginName {
			result = append(result, sal.events[i])
		}
	}

	return result
}