package plugin

import (
	"context"
	"time"
)

// PluginInterface defines the basic interface that all plugins must implement
type PluginInterface interface {
	// Initialize initializes the plugin with provided configuration
	Initialize(ctx context.Context, config PluginConfig) error
	
	// Name returns the unique name of the plugin
	Name() string
	
	// Version returns the version of the plugin
	Version() string
	
	// Description returns a description of what the plugin does
	Description() string
	
	// Shutdown gracefully shuts down the plugin
	Shutdown(ctx context.Context) error
}

// HookType represents different types of hooks in the deployment lifecycle
type HookType string

const (
	HookPreDeploy     HookType = "pre_deploy"
	HookPostDeploy    HookType = "post_deploy"
	HookPreFile       HookType = "pre_file"
	HookPostFile      HookType = "post_file"
	HookOnError       HookType = "on_error"
	HookOnSuccess     HookType = "on_success"
	HookOnStart       HookType = "on_start"
	HookOnFinish      HookType = "on_finish"
)

// HookPlugin defines the interface for plugins that can hook into deployment events
type HookPlugin interface {
	PluginInterface
	
	// SupportedHooks returns the list of hooks this plugin supports
	SupportedHooks() []HookType
	
	// ExecuteHook executes the plugin logic for a specific hook
	ExecuteHook(ctx context.Context, hookType HookType, data HookData) error
}

// NotificationPlugin defines the interface for notification plugins
type NotificationPlugin interface {
	PluginInterface
	
	// SendNotification sends a notification with the given message and metadata
	SendNotification(ctx context.Context, message NotificationMessage) error
}

// TransformPlugin defines the interface for plugins that can transform deployment data
type TransformPlugin interface {
	PluginInterface
	
	// Transform applies transformations to the deployment data
	Transform(ctx context.Context, data TransformData) (TransformData, error)
}

// PluginConfig holds configuration for a plugin
type PluginConfig struct {
	Name     string                 `yaml:"name" json:"name"`
	Enabled  bool                   `yaml:"enabled" json:"enabled"`
	Settings map[string]interface{} `yaml:"settings" json:"settings"`
}

// HookData contains data passed to hook plugins
type HookData struct {
	DeploymentID   string                 `json:"deployment_id"`
	Repository     string                 `json:"repository"`
	Commit         string                 `json:"commit"`
	Files          []string               `json:"files"`
	CurrentFile    string                 `json:"current_file,omitempty"`
	Error          error                  `json:"error,omitempty"`
	StartTime      time.Time              `json:"start_time"`
	EndTime        time.Time              `json:"end_time,omitempty"`
	Duration       time.Duration          `json:"duration,omitempty"`
	Success        bool                   `json:"success"`
	Metadata       map[string]interface{} `json:"metadata"`
}

// NotificationMessage represents a notification to be sent
type NotificationMessage struct {
	Title       string                 `json:"title"`
	Message     string                 `json:"message"`
	Level       NotificationLevel      `json:"level"`
	Timestamp   time.Time              `json:"timestamp"`
	Source      string                 `json:"source"`
	DeploymentID string                `json:"deployment_id,omitempty"`
	Repository  string                 `json:"repository,omitempty"`
	Commit      string                 `json:"commit,omitempty"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// NotificationLevel represents the severity level of a notification
type NotificationLevel string

const (
	NotificationLevelInfo    NotificationLevel = "info"
	NotificationLevelWarning NotificationLevel = "warning"
	NotificationLevelError   NotificationLevel = "error"
	NotificationLevelSuccess NotificationLevel = "success"
)

// TransformData represents data that can be transformed by plugins
type TransformData struct {
	Files       []FileData             `json:"files"`
	Variables   map[string]string      `json:"variables"`
	Environment string                 `json:"environment"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// FileData represents a file in the deployment
type FileData struct {
	Path     string            `json:"path"`
	Content  string            `json:"content"`
	Hash     string            `json:"hash"`
	Metadata map[string]string `json:"metadata"`
}

// PluginManifest describes a plugin's metadata and requirements
type PluginManifest struct {
	Name         string            `yaml:"name" json:"name"`
	Version      string            `yaml:"version" json:"version"`
	Description  string            `yaml:"description" json:"description"`
	Author       string            `yaml:"author" json:"author"`
	Homepage     string            `yaml:"homepage" json:"homepage"`
	Repository   string            `yaml:"repository" json:"repository"`
	License      string            `yaml:"license" json:"license"`
	Keywords     []string          `yaml:"keywords" json:"keywords"`
	Dependencies []string          `yaml:"dependencies" json:"dependencies"`
	Capabilities []string          `yaml:"capabilities" json:"capabilities"`
	Config       []ConfigSchema    `yaml:"config" json:"config"`
	Hooks        []HookType        `yaml:"hooks" json:"hooks"`
	Permissions  []Permission      `yaml:"permissions" json:"permissions"`
	MinVersion   string            `yaml:"min_version" json:"min_version"`
	MaxVersion   string            `yaml:"max_version" json:"max_version"`
}

// ConfigSchema defines the configuration schema for a plugin
type ConfigSchema struct {
	Name        string      `yaml:"name" json:"name"`
	Type        string      `yaml:"type" json:"type"`
	Description string      `yaml:"description" json:"description"`
	Required    bool        `yaml:"required" json:"required"`
	Default     interface{} `yaml:"default" json:"default"`
	Options     []string    `yaml:"options,omitempty" json:"options,omitempty"`
}

// Permission represents a permission required by a plugin
type Permission struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	Required    bool   `yaml:"required" json:"required"`
}

// PluginInfo contains runtime information about a loaded plugin
type PluginInfo struct {
	Manifest    PluginManifest `json:"manifest"`
	Config      PluginConfig   `json:"config"`
	Instance    PluginInterface `json:"-"`
	LoadTime    time.Time      `json:"load_time"`
	LastUsed    time.Time      `json:"last_used"`
	UsageCount  int64          `json:"usage_count"`
	Enabled     bool           `json:"enabled"`
	Error       string         `json:"error,omitempty"`
}

// PluginRegistry defines the interface for plugin registries
type PluginRegistry interface {
	// List returns available plugins from the registry
	List(ctx context.Context) ([]PluginManifest, error)
	
	// Get retrieves a specific plugin manifest
	Get(ctx context.Context, name, version string) (*PluginManifest, error)
	
	// Search searches for plugins matching the query
	Search(ctx context.Context, query string) ([]PluginManifest, error)
	
	// Download downloads a plugin from the registry
	Download(ctx context.Context, name, version string) ([]byte, error)
}

// PluginEvent represents an event in the plugin system
type PluginEvent struct {
	Type      string                 `json:"type"`
	Plugin    string                 `json:"plugin"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Error     string                 `json:"error,omitempty"`
}

// PluginStats contains statistics about plugin usage
type PluginStats struct {
	TotalPlugins    int                        `json:"total_plugins"`
	EnabledPlugins  int                        `json:"enabled_plugins"`
	HookExecutions  map[HookType]int64         `json:"hook_executions"`
	PluginUsage     map[string]int64           `json:"plugin_usage"`
	LastExecution   time.Time                  `json:"last_execution"`
	AverageExecTime map[string]time.Duration   `json:"average_exec_time"`
	Errors          []PluginEvent              `json:"errors"`
}