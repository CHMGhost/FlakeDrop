package sdk

import (
	"context"
	"fmt"
	"log"
	"time"

	"flakedrop/pkg/plugin"
)

// BasePlugin provides a foundation for plugin development
type BasePlugin struct {
	name        string
	version     string
	description string
	manifest    plugin.PluginManifest
	config      plugin.PluginConfig
	logger      Logger
	initialized bool
}

// Logger interface for plugin logging
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// DefaultLogger provides a default logging implementation
type DefaultLogger struct {
	pluginName string
}

// NewDefaultLogger creates a new default logger
func NewDefaultLogger(pluginName string) *DefaultLogger {
	return &DefaultLogger{pluginName: pluginName}
}

func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	log.Printf("[DEBUG][%s] %s", l.pluginName, fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	log.Printf("[INFO][%s] %s", l.pluginName, fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	log.Printf("[WARN][%s] %s", l.pluginName, fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	log.Printf("[ERROR][%s] %s", l.pluginName, fmt.Sprintf(msg, args...))
}

// NewBasePlugin creates a new base plugin
func NewBasePlugin(name, version, description string) *BasePlugin {
	return &BasePlugin{
		name:        name,
		version:     version,
		description: description,
		logger:      NewDefaultLogger(name),
		initialized: false,
	}
}

// SetLogger sets a custom logger
func (b *BasePlugin) SetLogger(logger Logger) {
	b.logger = logger
}

// SetManifest sets the plugin manifest
func (b *BasePlugin) SetManifest(manifest plugin.PluginManifest) {
	b.manifest = manifest
}

// Name returns the plugin name
func (b *BasePlugin) Name() string {
	return b.name
}

// Version returns the plugin version
func (b *BasePlugin) Version() string {
	return b.version
}

// Description returns the plugin description
func (b *BasePlugin) Description() string {
	return b.description
}

// Initialize initializes the base plugin
func (b *BasePlugin) Initialize(ctx context.Context, config plugin.PluginConfig) error {
	b.config = config
	b.logger.Info("Plugin initialized with config: %+v", config)
	b.initialized = true
	return nil
}

// Shutdown shuts down the base plugin
func (b *BasePlugin) Shutdown(ctx context.Context) error {
	b.logger.Info("Plugin shutting down")
	b.initialized = false
	return nil
}

// IsInitialized returns whether the plugin is initialized
func (b *BasePlugin) IsInitialized() bool {
	return b.initialized
}

// GetConfig returns the plugin configuration
func (b *BasePlugin) GetConfig() plugin.PluginConfig {
	return b.config
}

// GetSetting gets a configuration setting
func (b *BasePlugin) GetSetting(key string) (interface{}, bool) {
	if b.config.Settings == nil {
		return nil, false
	}
	value, exists := b.config.Settings[key]
	return value, exists
}

// GetStringSetting gets a string configuration setting
func (b *BasePlugin) GetStringSetting(key string) (string, error) {
	value, exists := b.GetSetting(key)
	if !exists {
		return "", fmt.Errorf("setting %s not found", key)
	}
	
	strValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("setting %s is not a string", key)
	}
	
	return strValue, nil
}

// GetIntSetting gets an integer configuration setting
func (b *BasePlugin) GetIntSetting(key string) (int, error) {
	value, exists := b.GetSetting(key)
	if !exists {
		return 0, fmt.Errorf("setting %s not found", key)
	}
	
	switch v := value.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("setting %s is not an integer", key)
	}
}

// GetBoolSetting gets a boolean configuration setting
func (b *BasePlugin) GetBoolSetting(key string) (bool, error) {
	value, exists := b.GetSetting(key)
	if !exists {
		return false, fmt.Errorf("setting %s not found", key)
	}
	
	boolValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("setting %s is not a boolean", key)
	}
	
	return boolValue, nil
}

// GetLogger returns the plugin logger
func (b *BasePlugin) GetLogger() Logger {
	return b.logger
}

// HookPlugin provides a base implementation for hook plugins
type HookPlugin struct {
	*BasePlugin
	supportedHooks []plugin.HookType
	hookHandlers   map[plugin.HookType]func(context.Context, plugin.HookData) error
}

// NewHookPlugin creates a new hook plugin
func NewHookPlugin(name, version, description string) *HookPlugin {
	return &HookPlugin{
		BasePlugin:     NewBasePlugin(name, version, description),
		supportedHooks: make([]plugin.HookType, 0),
		hookHandlers:   make(map[plugin.HookType]func(context.Context, plugin.HookData) error),
	}
}

// RegisterHook registers a hook handler
func (h *HookPlugin) RegisterHook(hookType plugin.HookType, handler func(context.Context, plugin.HookData) error) {
	h.supportedHooks = append(h.supportedHooks, hookType)
	h.hookHandlers[hookType] = handler
}

// SupportedHooks returns the supported hook types
func (h *HookPlugin) SupportedHooks() []plugin.HookType {
	return h.supportedHooks
}

// ExecuteHook executes a hook
func (h *HookPlugin) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	if !h.IsInitialized() {
		return fmt.Errorf("plugin %s is not initialized", h.Name())
	}

	handler, exists := h.hookHandlers[hookType]
	if !exists {
		return fmt.Errorf("hook %s not supported by plugin %s", hookType, h.Name())
	}

	h.logger.Debug("Executing hook %s", hookType)
	start := time.Now()
	
	err := handler(ctx, data)
	
	duration := time.Since(start)
	if err != nil {
		h.logger.Error("Hook %s failed in %s: %v", hookType, duration, err)
	} else {
		h.logger.Debug("Hook %s completed in %s", hookType, duration)
	}
	
	return err
}

// NotificationPlugin provides a base implementation for notification plugins
type NotificationPlugin struct {
	*BasePlugin
	sender func(context.Context, plugin.NotificationMessage) error
}

// NewNotificationPlugin creates a new notification plugin
func NewNotificationPlugin(name, version, description string) *NotificationPlugin {
	return &NotificationPlugin{
		BasePlugin: NewBasePlugin(name, version, description),
	}
}

// RegisterSender registers a notification sender
func (n *NotificationPlugin) RegisterSender(sender func(context.Context, plugin.NotificationMessage) error) {
	n.sender = sender
}

// SendNotification sends a notification
func (n *NotificationPlugin) SendNotification(ctx context.Context, message plugin.NotificationMessage) error {
	if !n.IsInitialized() {
		return fmt.Errorf("plugin %s is not initialized", n.Name())
	}

	if n.sender == nil {
		return fmt.Errorf("no sender registered for plugin %s", n.Name())
	}

	n.logger.Debug("Sending notification: %s", message.Title)
	start := time.Now()
	
	err := n.sender(ctx, message)
	
	duration := time.Since(start)
	if err != nil {
		n.logger.Error("Notification failed in %s: %v", duration, err)
	} else {
		n.logger.Debug("Notification sent in %s", duration)
	}
	
	return err
}

// TransformPlugin provides a base implementation for transform plugins
type TransformPlugin struct {
	*BasePlugin
	transformer func(context.Context, plugin.TransformData) (plugin.TransformData, error)
}

// NewTransformPlugin creates a new transform plugin
func NewTransformPlugin(name, version, description string) *TransformPlugin {
	return &TransformPlugin{
		BasePlugin: NewBasePlugin(name, version, description),
	}
}

// RegisterTransformer registers a data transformer
func (t *TransformPlugin) RegisterTransformer(transformer func(context.Context, plugin.TransformData) (plugin.TransformData, error)) {
	t.transformer = transformer
}

// Transform transforms deployment data
func (t *TransformPlugin) Transform(ctx context.Context, data plugin.TransformData) (plugin.TransformData, error) {
	if !t.IsInitialized() {
		return data, fmt.Errorf("plugin %s is not initialized", t.Name())
	}

	if t.transformer == nil {
		return data, fmt.Errorf("no transformer registered for plugin %s", t.Name())
	}

	t.logger.Debug("Transforming data with %d files", len(data.Files))
	start := time.Now()
	
	result, err := t.transformer(ctx, data)
	
	duration := time.Since(start)
	if err != nil {
		t.logger.Error("Transform failed in %s: %v", duration, err)
		return data, err
	} else {
		t.logger.Debug("Transform completed in %s", duration)
	}
	
	return result, nil
}

// PluginBuilder helps build plugins with fluent interface
type PluginBuilder struct {
	manifest plugin.PluginManifest
}

// NewPluginBuilder creates a new plugin builder
func NewPluginBuilder(name, version, description string) *PluginBuilder {
	return &PluginBuilder{
		manifest: plugin.PluginManifest{
			Name:         name,
			Version:      version,
			Description:  description,
			Config:       make([]plugin.ConfigSchema, 0),
			Hooks:        make([]plugin.HookType, 0),
			Permissions:  make([]plugin.Permission, 0),
			Capabilities: make([]string, 0),
		},
	}
}

// WithAuthor sets the plugin author
func (b *PluginBuilder) WithAuthor(author string) *PluginBuilder {
	b.manifest.Author = author
	return b
}

// WithLicense sets the plugin license
func (b *PluginBuilder) WithLicense(license string) *PluginBuilder {
	b.manifest.License = license
	return b
}

// WithHomepage sets the plugin homepage
func (b *PluginBuilder) WithHomepage(homepage string) *PluginBuilder {
	b.manifest.Homepage = homepage
	return b
}

// WithKeywords adds keywords to the plugin
func (b *PluginBuilder) WithKeywords(keywords ...string) *PluginBuilder {
	b.manifest.Keywords = append(b.manifest.Keywords, keywords...)
	return b
}

// WithCapability adds a capability to the plugin
func (b *PluginBuilder) WithCapability(capability string) *PluginBuilder {
	b.manifest.Capabilities = append(b.manifest.Capabilities, capability)
	return b
}

// WithHook adds a supported hook
func (b *PluginBuilder) WithHook(hook plugin.HookType) *PluginBuilder {
	b.manifest.Hooks = append(b.manifest.Hooks, hook)
	return b
}

// WithPermission adds a required permission
func (b *PluginBuilder) WithPermission(name, description string, required bool) *PluginBuilder {
	b.manifest.Permissions = append(b.manifest.Permissions, plugin.Permission{
		Name:        name,
		Description: description,
		Required:    required,
	})
	return b
}

// WithStringSetting adds a string configuration setting
func (b *PluginBuilder) WithStringSetting(name, description string, required bool, defaultValue string, options ...string) *PluginBuilder {
	schema := plugin.ConfigSchema{
		Name:        name,
		Type:        "string",
		Description: description,
		Required:    required,
		Options:     options,
	}
	
	if defaultValue != "" {
		schema.Default = defaultValue
	}
	
	b.manifest.Config = append(b.manifest.Config, schema)
	return b
}

// WithIntSetting adds an integer configuration setting
func (b *PluginBuilder) WithIntSetting(name, description string, required bool, defaultValue int) *PluginBuilder {
	schema := plugin.ConfigSchema{
		Name:        name,
		Type:        "integer",
		Description: description,
		Required:    required,
	}
	
	if defaultValue != 0 {
		schema.Default = defaultValue
	}
	
	b.manifest.Config = append(b.manifest.Config, schema)
	return b
}

// WithBoolSetting adds a boolean configuration setting
func (b *PluginBuilder) WithBoolSetting(name, description string, required bool, defaultValue bool) *PluginBuilder {
	schema := plugin.ConfigSchema{
		Name:        name,
		Type:        "boolean",
		Description: description,
		Required:    required,
		Default:     defaultValue,
	}
	
	b.manifest.Config = append(b.manifest.Config, schema)
	return b
}

// WithArraySetting adds an array configuration setting
func (b *PluginBuilder) WithArraySetting(name, description string, required bool) *PluginBuilder {
	schema := plugin.ConfigSchema{
		Name:        name,
		Type:        "array",
		Description: description,
		Required:    required,
	}
	
	b.manifest.Config = append(b.manifest.Config, schema)
	return b
}

// WithObjectSetting adds an object configuration setting
func (b *PluginBuilder) WithObjectSetting(name, description string, required bool) *PluginBuilder {
	schema := plugin.ConfigSchema{
		Name:        name,
		Type:        "object",
		Description: description,
		Required:    required,
	}
	
	b.manifest.Config = append(b.manifest.Config, schema)
	return b
}

// Build builds the plugin manifest
func (b *PluginBuilder) Build() plugin.PluginManifest {
	return b.manifest
}

// Utilities for plugin development

// ContextHelper provides utilities for working with context
type ContextHelper struct{}

// WithTimeout creates a context with timeout
func (c *ContextHelper) WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// WithDeadline creates a context with deadline
func (c *ContextHelper) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, deadline)
}

// WithCancel creates a cancellable context
func (c *ContextHelper) WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(parent)
}

// DataHelper provides utilities for working with plugin data
type DataHelper struct{}

// FormatDuration formats a duration for display
func (d *DataHelper) FormatDuration(duration time.Duration) string {
	if duration < time.Second {
		return fmt.Sprintf("%dms", duration.Milliseconds())
	}
	if duration < time.Minute {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	}
	return fmt.Sprintf("%.1fm", duration.Minutes())
}

// FormatTimestamp formats a timestamp for display
func (d *DataHelper) FormatTimestamp(timestamp time.Time) string {
	return timestamp.Format("2006-01-02 15:04:05 MST")
}

// TruncateString truncates a string to a maximum length
func (d *DataHelper) TruncateString(str string, maxLength int) string {
	if len(str) <= maxLength {
		return str
	}
	
	if maxLength < 3 {
		return str[:maxLength]
	}
	
	return str[:maxLength-3] + "..."
}

// ErrorHelper provides utilities for error handling
type ErrorHelper struct{}

// WrapError wraps an error with additional context
func (e *ErrorHelper) WrapError(err error, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(message+": %w", append(args, err)...)
}

// NewError creates a new error with formatting
func (e *ErrorHelper) NewError(message string, args ...interface{}) error {
	return fmt.Errorf(message, args...)
}

// IsRetryableError checks if an error is retryable
func (e *ErrorHelper) IsRetryableError(err error) bool {
	// Simple heuristic - in a real implementation, you might check specific error types
	errStr := err.Error()
	retryableStrings := []string{
		"timeout",
		"connection refused",
		"temporary failure",
		"rate limit",
		"server error",
	}
	
	for _, retryable := range retryableStrings {
		if fmt.Sprintf("%v", errStr) == retryable {
			return true
		}
	}
	
	return false
}

// Global helpers that plugins can use
var (
	Context = &ContextHelper{}
	Data    = &DataHelper{}
	Error   = &ErrorHelper{}
)