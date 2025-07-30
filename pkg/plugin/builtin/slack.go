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

// SlackPlugin provides Slack integration for deployment notifications
type SlackPlugin struct {
	config    plugin.PluginConfig
	client    *http.Client
	webhookURL string
	channel   string
	username  string
	iconEmoji string
}

// SlackMessage represents a Slack message payload
type SlackMessage struct {
	Channel   string       `json:"channel,omitempty"`
	Username  string       `json:"username,omitempty"`
	IconEmoji string       `json:"icon_emoji,omitempty"`
	Text      string       `json:"text"`
	Blocks    []SlackBlock `json:"blocks,omitempty"`
}

// SlackBlock represents a Slack block element
type SlackBlock struct {
	Type string                 `json:"type"`
	Text *SlackText             `json:"text,omitempty"`
	Fields []SlackField         `json:"fields,omitempty"`
	Accessory interface{}       `json:"accessory,omitempty"`
}

// SlackText represents text in a Slack block
type SlackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// SlackField represents a field in a Slack block
type SlackField struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NewSlackPlugin creates a new Slack plugin instance
func NewSlackPlugin() *SlackPlugin {
	return &SlackPlugin{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Initialize initializes the Slack plugin
func (s *SlackPlugin) Initialize(ctx context.Context, config plugin.PluginConfig) error {
	s.config = config

	// Extract configuration settings
	if webhookURL, ok := config.Settings["webhook_url"].(string); ok {
		s.webhookURL = webhookURL
	} else {
		return fmt.Errorf("webhook_url is required for Slack plugin")
	}

	if channel, ok := config.Settings["channel"].(string); ok {
		s.channel = channel
	}

	if username, ok := config.Settings["username"].(string); ok {
		s.username = username
	} else {
		s.username = "FlakeDrop Bot"
	}

	if iconEmoji, ok := config.Settings["icon_emoji"].(string); ok {
		s.iconEmoji = iconEmoji
	} else {
		s.iconEmoji = ":snowflake:"
	}

	return nil
}

// Name returns the plugin name
func (s *SlackPlugin) Name() string {
	return "slack"
}

// Version returns the plugin version
func (s *SlackPlugin) Version() string {
	return "1.0.0"
}

// Description returns the plugin description
func (s *SlackPlugin) Description() string {
	return "Send deployment notifications to Slack channels"
}

// Shutdown shuts down the plugin
func (s *SlackPlugin) Shutdown(ctx context.Context) error {
	return nil
}

// SupportedHooks returns the hooks this plugin supports
func (s *SlackPlugin) SupportedHooks() []plugin.HookType {
	return []plugin.HookType{
		plugin.HookPreDeploy,
		plugin.HookPostDeploy,
		plugin.HookOnSuccess,
		plugin.HookOnError,
		plugin.HookOnStart,
		plugin.HookOnFinish,
	}
}

// ExecuteHook executes the plugin logic for a specific hook
func (s *SlackPlugin) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	switch hookType {
	case plugin.HookOnStart:
		return s.sendStartNotification(ctx, data)
	case plugin.HookPreDeploy:
		return s.sendPreDeployNotification(ctx, data)
	case plugin.HookPostDeploy:
		return s.sendPostDeployNotification(ctx, data)
	case plugin.HookOnSuccess:
		return s.sendSuccessNotification(ctx, data)
	case plugin.HookOnError:
		return s.sendErrorNotification(ctx, data)
	case plugin.HookOnFinish:
		return s.sendFinishNotification(ctx, data)
	default:
		return fmt.Errorf("unsupported hook type: %s", hookType)
	}
}

// SendNotification sends a notification message
func (s *SlackPlugin) SendNotification(ctx context.Context, message plugin.NotificationMessage) error {
	slackMessage := s.convertToSlackMessage(message)
	return s.sendSlackMessage(ctx, slackMessage)
}

// sendStartNotification sends a deployment start notification
func (s *SlackPlugin) sendStartNotification(ctx context.Context, data plugin.HookData) error {
	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":rocket: *Deployment Started*\n*Repository:* %s\n*Commit:* `%s`\n*Files:* %d", 
						data.Repository, data.Commit[:8], len(data.Files)),
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// sendPreDeployNotification sends a pre-deployment notification
func (s *SlackPlugin) sendPreDeployNotification(ctx context.Context, data plugin.HookData) error {
	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":hourglass_flowing_sand: *Pre-deployment checks running*\n*Repository:* %s\n*Commit:* `%s`", 
						data.Repository, data.Commit[:8]),
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// sendPostDeployNotification sends a post-deployment notification
func (s *SlackPlugin) sendPostDeployNotification(ctx context.Context, data plugin.HookData) error {
	status := ":white_check_mark: Success"
	if !data.Success {
		status = ":x: Failed"
	}

	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":gear: *Post-deployment processing*\n*Status:* %s\n*Repository:* %s\n*Duration:* %s", 
						status, data.Repository, data.Duration.String()),
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// sendSuccessNotification sends a success notification
func (s *SlackPlugin) sendSuccessNotification(ctx context.Context, data plugin.HookData) error {
	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":tada: *Deployment Successful!*\n*Repository:* %s\n*Commit:* `%s`\n*Files Deployed:* %d\n*Duration:* %s", 
						data.Repository, data.Commit[:8], len(data.Files), data.Duration.String()),
				},
			},
			{
				Type: "section",
				Fields: []SlackField{
					{Type: "mrkdwn", Text: fmt.Sprintf("*Start Time:*\n%s", data.StartTime.Format("2006-01-02 15:04:05"))},
					{Type: "mrkdwn", Text: fmt.Sprintf("*End Time:*\n%s", data.EndTime.Format("2006-01-02 15:04:05"))},
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// sendErrorNotification sends an error notification
func (s *SlackPlugin) sendErrorNotification(ctx context.Context, data plugin.HookData) error {
	errorMsg := "Unknown error"
	if data.Error != nil {
		errorMsg = data.Error.Error()
	}

	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":warning: *Deployment Failed!*\n*Repository:* %s\n*Commit:* `%s`\n*Error:* %s", 
						data.Repository, data.Commit[:8], errorMsg),
				},
			},
			{
				Type: "section",
				Fields: []SlackField{
					{Type: "mrkdwn", Text: fmt.Sprintf("*Start Time:*\n%s", data.StartTime.Format("2006-01-02 15:04:05"))},
					{Type: "mrkdwn", Text: fmt.Sprintf("*Duration:*\n%s", data.Duration.String())},
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// sendFinishNotification sends a finish notification
func (s *SlackPlugin) sendFinishNotification(ctx context.Context, data plugin.HookData) error {
	status := ":white_check_mark: Completed Successfully"
	if !data.Success {
		status = ":x: Completed with Errors"
	}

	message := SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf(":checkered_flag: *Deployment Finished*\n*Status:* %s\n*Repository:* %s\n*Total Duration:* %s", 
						status, data.Repository, data.Duration.String()),
				},
			},
		},
	}

	return s.sendSlackMessage(ctx, message)
}

// convertToSlackMessage converts a generic notification message to Slack format
func (s *SlackPlugin) convertToSlackMessage(message plugin.NotificationMessage) SlackMessage {
	var icon string

	switch message.Level {
	case plugin.NotificationLevelSuccess:
		// color = "good" // Could be used for attachments in future
		icon = ":white_check_mark:"
	case plugin.NotificationLevelWarning:
		// color = "warning" // Could be used for attachments in future
		icon = ":warning:"
	case plugin.NotificationLevelError:
		// color = "danger" // Could be used for attachments in future
		icon = ":x:"
	default:
		// color = "#36a64f" // Could be used for attachments in future
		icon = ":information_source:"
	}

	return SlackMessage{
		Channel:   s.channel,
		Username:  s.username,
		IconEmoji: s.iconEmoji,
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: fmt.Sprintf("%s *%s*\n%s", icon, message.Title, message.Message),
				},
			},
		},
	}
}

// sendSlackMessage sends a message to Slack
func (s *SlackPlugin) sendSlackMessage(ctx context.Context, message SlackMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal Slack message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.webhookURL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send Slack message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Slack API returned status %d", resp.StatusCode)
	}

	return nil
}

// GetManifest returns the plugin manifest
func (s *SlackPlugin) GetManifest() plugin.PluginManifest {
	return plugin.PluginManifest{
		Name:        "slack",
		Version:     "1.0.0",
		Description: "Send deployment notifications to Slack channels",
		Author:      "FlakeDrop Team",
		License:     "MIT",
		Keywords:    []string{"notification", "slack", "messaging"},
		Capabilities: []string{"notification", "hook"},
		Config: []plugin.ConfigSchema{
			{
				Name:        "webhook_url",
				Type:        "string",
				Description: "Slack webhook URL for sending messages",
				Required:    true,
			},
			{
				Name:        "channel",
				Type:        "string",
				Description: "Slack channel to send messages to (optional, can be set in webhook)",
				Required:    false,
			},
			{
				Name:        "username",
				Type:        "string",
				Description: "Bot username for messages",
				Required:    false,
				Default:     "FlakeDrop Bot",
			},
			{
				Name:        "icon_emoji",
				Type:        "string",
				Description: "Emoji icon for the bot",
				Required:    false,
				Default:     ":snowflake:",
			},
		},
		Hooks: []plugin.HookType{
			plugin.HookPreDeploy,
			plugin.HookPostDeploy,
			plugin.HookOnSuccess,
			plugin.HookOnError,
			plugin.HookOnStart,
			plugin.HookOnFinish,
		},
		Permissions: []plugin.Permission{
			{
				Name:        "network",
				Description: "Send HTTP requests to Slack API",
				Required:    true,
			},
		},
	}
}