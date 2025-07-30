package builtin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"flakedrop/pkg/plugin"
)

// WebhookPlugin provides webhook notifications for deployments
type WebhookPlugin struct {
	config      plugin.PluginConfig
	client      *http.Client
	url         string
	method      string
	headers     map[string]string
	timeout     time.Duration
	retryCount  int
	retryDelay  time.Duration
}

// WebhookPayload represents the payload sent to webhooks
type WebhookPayload struct {
	Event       string                 `json:"event"`
	Timestamp   time.Time              `json:"timestamp"`
	Repository  string                 `json:"repository"`
	Commit      string                 `json:"commit"`
	Files       []string               `json:"files,omitempty"`
	Success     bool                   `json:"success"`
	Error       string                 `json:"error,omitempty"`
	Duration    string                 `json:"duration,omitempty"`
	StartTime   time.Time              `json:"start_time,omitempty"`
	EndTime     time.Time              `json:"end_time,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// NewWebhookPlugin creates a new webhook plugin instance
func NewWebhookPlugin() *WebhookPlugin {
	return &WebhookPlugin{
		method:     "POST",
		timeout:    30 * time.Second,
		retryCount: 3,
		retryDelay: 1 * time.Second,
		headers:    make(map[string]string),
	}
}

// Initialize initializes the webhook plugin
func (w *WebhookPlugin) Initialize(ctx context.Context, config plugin.PluginConfig) error {
	w.config = config

	// Extract URL
	if url, ok := config.Settings["url"].(string); ok {
		w.url = url
	} else {
		return fmt.Errorf("url is required for webhook plugin")
	}

	// Extract HTTP method
	if method, ok := config.Settings["method"].(string); ok {
		w.method = method
	}

	// Extract headers
	if headers, ok := config.Settings["headers"].(map[string]interface{}); ok {
		w.headers = make(map[string]string)
		for key, value := range headers {
			if strValue, ok := value.(string); ok {
				w.headers[key] = strValue
			}
		}
	}

	// Set default Content-Type if not specified
	if _, exists := w.headers["Content-Type"]; !exists {
		w.headers["Content-Type"] = "application/json"
	}

	// Extract timeout
	if timeoutStr, ok := config.Settings["timeout"].(string); ok {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			w.timeout = timeout
		}
	}

	// Extract retry configuration
	if retryCount, ok := config.Settings["retry_count"].(int); ok {
		w.retryCount = retryCount
	}

	if retryDelayStr, ok := config.Settings["retry_delay"].(string); ok {
		if retryDelay, err := time.ParseDuration(retryDelayStr); err == nil {
			w.retryDelay = retryDelay
		}
	}

	// Create HTTP client
	w.client = &http.Client{
		Timeout: w.timeout,
	}

	return nil
}

// Name returns the plugin name
func (w *WebhookPlugin) Name() string {
	return "webhook"
}

// Version returns the plugin version
func (w *WebhookPlugin) Version() string {
	return "1.0.0"
}

// Description returns the plugin description
func (w *WebhookPlugin) Description() string {
	return "Send deployment notifications via HTTP webhooks"
}

// Shutdown shuts down the plugin
func (w *WebhookPlugin) Shutdown(ctx context.Context) error {
	return nil
}

// SupportedHooks returns the hooks this plugin supports
func (w *WebhookPlugin) SupportedHooks() []plugin.HookType {
	return []plugin.HookType{
		plugin.HookPreDeploy,
		plugin.HookPostDeploy,
		plugin.HookPreFile,
		plugin.HookPostFile,
		plugin.HookOnError,
		plugin.HookOnSuccess,
		plugin.HookOnStart,
		plugin.HookOnFinish,
	}
}

// ExecuteHook executes the plugin logic for a specific hook
func (w *WebhookPlugin) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	payload := w.createPayload(hookType, data)
	return w.sendWebhook(ctx, payload)
}

// SendNotification sends a notification message
func (w *WebhookPlugin) SendNotification(ctx context.Context, message plugin.NotificationMessage) error {
	payload := WebhookPayload{
		Event:      "notification",
		Timestamp:  message.Timestamp,
		Repository: message.Repository,
		Commit:     message.Commit,
		Success:    message.Level == plugin.NotificationLevelSuccess,
		Metadata: map[string]interface{}{
			"title":   message.Title,
			"message": message.Message,
			"level":   string(message.Level),
			"source":  message.Source,
		},
	}

	return w.sendWebhook(ctx, payload)
}

// createPayload creates a webhook payload for a specific hook
func (w *WebhookPlugin) createPayload(hookType plugin.HookType, data plugin.HookData) WebhookPayload {
	payload := WebhookPayload{
		Event:      string(hookType),
		Timestamp:  time.Now(),
		Repository: data.Repository,
		Commit:     data.Commit,
		Files:      data.Files,
		Success:    data.Success,
		StartTime:  data.StartTime,
		EndTime:    data.EndTime,
		Duration:   data.Duration.String(),
		Metadata:   data.Metadata,
	}

	if data.Error != nil {
		payload.Error = data.Error.Error()
	}

	// Add current file for file-specific hooks
	if data.CurrentFile != "" {
		if payload.Metadata == nil {
			payload.Metadata = make(map[string]interface{})
		}
		payload.Metadata["current_file"] = data.CurrentFile
	}

	return payload
}

// sendWebhook sends a webhook with retry logic
func (w *WebhookPlugin) sendWebhook(ctx context.Context, payload WebhookPayload) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= w.retryCount; attempt++ {
		if attempt > 0 {
			// Wait before retrying
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.retryDelay * time.Duration(attempt)):
			}
		}

		req, err := http.NewRequestWithContext(ctx, w.method, w.url, bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = fmt.Errorf("failed to create HTTP request: %w", err)
			continue
		}

		// Set headers
		for key, value := range w.headers {
			req.Header.Set(key, value)
		}

		resp, err := w.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to send webhook (attempt %d): %w", attempt+1, err)
			continue
		}

		resp.Body.Close()

		// Check response status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil // Success
		}

		lastErr = fmt.Errorf("webhook returned status %d (attempt %d)", resp.StatusCode, attempt+1)

		// Don't retry on client errors (4xx)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			break
		}
	}

	return fmt.Errorf("webhook failed after %d attempts: %w", w.retryCount+1, lastErr)
}

// GetManifest returns the plugin manifest
func (w *WebhookPlugin) GetManifest() plugin.PluginManifest {
	return plugin.PluginManifest{
		Name:        "webhook",
		Version:     "1.0.0",
		Description: "Send deployment notifications via HTTP webhooks",
		Author:      "FlakeDrop Team",
		License:     "MIT",
		Keywords:    []string{"notification", "webhook", "http", "api"},
		Capabilities: []string{"notification", "hook"},
		Config: []plugin.ConfigSchema{
			{
				Name:        "url",
				Type:        "string",
				Description: "Webhook URL to send notifications to",
				Required:    true,
			},
			{
				Name:        "method",
				Type:        "string",
				Description: "HTTP method to use",
				Required:    false,
				Default:     "POST",
				Options:     []string{"POST", "PUT", "PATCH"},
			},
			{
				Name:        "headers",
				Type:        "object",
				Description: "HTTP headers to include in requests",
				Required:    false,
			},
			{
				Name:        "timeout",
				Type:        "string",
				Description: "Request timeout (e.g., '30s', '1m')",
				Required:    false,
				Default:     "30s",
			},
			{
				Name:        "retry_count",
				Type:        "integer",
				Description: "Number of retries on failure",
				Required:    false,
				Default:     3,
			},
			{
				Name:        "retry_delay",
				Type:        "string",
				Description: "Delay between retries (e.g., '1s', '500ms')",
				Required:    false,
				Default:     "1s",
			},
		},
		Hooks: []plugin.HookType{
			plugin.HookPreDeploy,
			plugin.HookPostDeploy,
			plugin.HookPreFile,
			plugin.HookPostFile,
			plugin.HookOnError,
			plugin.HookOnSuccess,
			plugin.HookOnStart,
			plugin.HookOnFinish,
		},
		Permissions: []plugin.Permission{
			{
				Name:        "network",
				Description: "Send HTTP requests to webhook endpoints",
				Required:    true,
			},
		},
	}
}

// CustomWebhookPlugin allows for customizable webhook configurations
type CustomWebhookPlugin struct {
	*WebhookPlugin
	transformers []PayloadTransformer
}

// PayloadTransformer allows customization of webhook payloads
type PayloadTransformer interface {
	Transform(payload WebhookPayload) (WebhookPayload, error)
}

// NewCustomWebhookPlugin creates a customizable webhook plugin
func NewCustomWebhookPlugin() *CustomWebhookPlugin {
	return &CustomWebhookPlugin{
		WebhookPlugin: NewWebhookPlugin(),
		transformers:  make([]PayloadTransformer, 0),
	}
}

// AddTransformer adds a payload transformer
func (cw *CustomWebhookPlugin) AddTransformer(transformer PayloadTransformer) {
	cw.transformers = append(cw.transformers, transformer)
}

// ExecuteHook executes the plugin logic with payload transformation
func (cw *CustomWebhookPlugin) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	payload := cw.createPayload(hookType, data)

	// Apply transformers
	for _, transformer := range cw.transformers {
		var err error
		payload, err = transformer.Transform(payload)
		if err != nil {
			return fmt.Errorf("payload transformation failed: %w", err)
		}
	}

	return cw.sendWebhook(ctx, payload)
}

// FilterTransformer filters events based on conditions
type FilterTransformer struct {
	allowedEvents map[string]bool
	conditions    map[string]func(WebhookPayload) bool
}

// NewFilterTransformer creates a new filter transformer
func NewFilterTransformer() *FilterTransformer {
	return &FilterTransformer{
		allowedEvents: make(map[string]bool),
		conditions:    make(map[string]func(WebhookPayload) bool),
	}
}

// AllowEvent enables an event type
func (f *FilterTransformer) AllowEvent(event string) {
	f.allowedEvents[event] = true
}

// AddCondition adds a condition for event filtering
func (f *FilterTransformer) AddCondition(name string, condition func(WebhookPayload) bool) {
	f.conditions[name] = condition
}

// Transform applies filtering logic
func (f *FilterTransformer) Transform(payload WebhookPayload) (WebhookPayload, error) {
	// Check if event is allowed
	if len(f.allowedEvents) > 0 && !f.allowedEvents[payload.Event] {
		return payload, fmt.Errorf("event %s is filtered out", payload.Event)
	}

	// Check conditions
	for name, condition := range f.conditions {
		if !condition(payload) {
			return payload, fmt.Errorf("condition %s not met", name)
		}
	}

	return payload, nil
}

// EnrichmentTransformer adds additional data to payloads
type EnrichmentTransformer struct {
	staticData   map[string]interface{}
	dynamicData  map[string]func(WebhookPayload) interface{}
}

// NewEnrichmentTransformer creates a new enrichment transformer
func NewEnrichmentTransformer() *EnrichmentTransformer {
	return &EnrichmentTransformer{
		staticData:  make(map[string]interface{}),
		dynamicData: make(map[string]func(WebhookPayload) interface{}),
	}
}

// AddStaticData adds static enrichment data
func (e *EnrichmentTransformer) AddStaticData(key string, value interface{}) {
	e.staticData[key] = value
}

// AddDynamicData adds dynamic enrichment data
func (e *EnrichmentTransformer) AddDynamicData(key string, valueFunc func(WebhookPayload) interface{}) {
	e.dynamicData[key] = valueFunc
}

// Transform adds enrichment data to the payload
func (e *EnrichmentTransformer) Transform(payload WebhookPayload) (WebhookPayload, error) {
	if payload.Metadata == nil {
		payload.Metadata = make(map[string]interface{})
	}

	// Add static data
	for key, value := range e.staticData {
		payload.Metadata[key] = value
	}

	// Add dynamic data
	for key, valueFunc := range e.dynamicData {
		payload.Metadata[key] = valueFunc(payload)
	}

	return payload, nil
}