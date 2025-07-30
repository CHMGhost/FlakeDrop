package builtin

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"
	"time"

	"flakedrop/pkg/plugin"
)

// EmailPlugin provides email notifications for deployments
type EmailPlugin struct {
	config   plugin.PluginConfig
	smtpHost string
	smtpPort string
	username string
	password string
	from     string
	to       []string
	cc       []string
	bcc      []string
}

// EmailTemplate represents an email template
type EmailTemplate struct {
	Subject string
	Body    string
}

// NewEmailPlugin creates a new email plugin instance
func NewEmailPlugin() *EmailPlugin {
	return &EmailPlugin{}
}

// Initialize initializes the email plugin
func (e *EmailPlugin) Initialize(ctx context.Context, config plugin.PluginConfig) error {
	e.config = config

	// Extract SMTP configuration
	if smtpHost, ok := config.Settings["smtp_host"].(string); ok {
		e.smtpHost = smtpHost
	} else {
		return fmt.Errorf("smtp_host is required for email plugin")
	}

	if smtpPort, ok := config.Settings["smtp_port"].(string); ok {
		e.smtpPort = smtpPort
	} else {
		e.smtpPort = "587" // Default to TLS port
	}

	if username, ok := config.Settings["username"].(string); ok {
		e.username = username
	} else {
		return fmt.Errorf("username is required for email plugin")
	}

	if password, ok := config.Settings["password"].(string); ok {
		e.password = password
	} else {
		return fmt.Errorf("password is required for email plugin")
	}

	if from, ok := config.Settings["from"].(string); ok {
		e.from = from
	} else {
		e.from = e.username // Use username as from address by default
	}

	// Extract recipient configuration
	if to, ok := config.Settings["to"].([]interface{}); ok {
		e.to = make([]string, len(to))
		for i, addr := range to {
			if strAddr, ok := addr.(string); ok {
				e.to[i] = strAddr
			}
		}
	} else if toStr, ok := config.Settings["to"].(string); ok {
		e.to = strings.Split(toStr, ",")
		for i := range e.to {
			e.to[i] = strings.TrimSpace(e.to[i])
		}
	} else {
		return fmt.Errorf("to addresses are required for email plugin")
	}

	// Optional CC addresses
	if cc, ok := config.Settings["cc"].([]interface{}); ok {
		e.cc = make([]string, len(cc))
		for i, addr := range cc {
			if strAddr, ok := addr.(string); ok {
				e.cc[i] = strAddr
			}
		}
	} else if ccStr, ok := config.Settings["cc"].(string); ok {
		e.cc = strings.Split(ccStr, ",")
		for i := range e.cc {
			e.cc[i] = strings.TrimSpace(e.cc[i])
		}
	}

	// Optional BCC addresses
	if bcc, ok := config.Settings["bcc"].([]interface{}); ok {
		e.bcc = make([]string, len(bcc))
		for i, addr := range bcc {
			if strAddr, ok := addr.(string); ok {
				e.bcc[i] = strAddr
			}
		}
	} else if bccStr, ok := config.Settings["bcc"].(string); ok {
		e.bcc = strings.Split(bccStr, ",")
		for i := range e.bcc {
			e.bcc[i] = strings.TrimSpace(e.bcc[i])
		}
	}

	return nil
}

// Name returns the plugin name
func (e *EmailPlugin) Name() string {
	return "email"
}

// Version returns the plugin version
func (e *EmailPlugin) Version() string {
	return "1.0.0"
}

// Description returns the plugin description
func (e *EmailPlugin) Description() string {
	return "Send deployment notifications via email"
}

// Shutdown shuts down the plugin
func (e *EmailPlugin) Shutdown(ctx context.Context) error {
	return nil
}

// SupportedHooks returns the hooks this plugin supports
func (e *EmailPlugin) SupportedHooks() []plugin.HookType {
	return []plugin.HookType{
		plugin.HookOnSuccess,
		plugin.HookOnError,
		plugin.HookOnStart,
		plugin.HookOnFinish,
	}
}

// ExecuteHook executes the plugin logic for a specific hook
func (e *EmailPlugin) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	switch hookType {
	case plugin.HookOnStart:
		return e.sendStartNotification(ctx, data)
	case plugin.HookOnSuccess:
		return e.sendSuccessNotification(ctx, data)
	case plugin.HookOnError:
		return e.sendErrorNotification(ctx, data)
	case plugin.HookOnFinish:
		return e.sendFinishNotification(ctx, data)
	default:
		return fmt.Errorf("unsupported hook type: %s", hookType)
	}
}

// SendNotification sends a notification message
func (e *EmailPlugin) SendNotification(ctx context.Context, message plugin.NotificationMessage) error {
	template := e.convertToEmailTemplate(message)
	return e.sendEmail(ctx, template.Subject, template.Body)
}

// sendStartNotification sends a deployment start notification
func (e *EmailPlugin) sendStartNotification(ctx context.Context, data plugin.HookData) error {
	subject := fmt.Sprintf("Deployment Started: %s", data.Repository)
	body := fmt.Sprintf(`
Deployment has started for repository: %s

Details:
- Repository: %s
- Commit: %s
- Files to deploy: %d
- Start time: %s

This is an automated message from FlakeDrop.
`, 
		data.Repository,
		data.Repository,
		data.Commit,
		len(data.Files),
		data.StartTime.Format("2006-01-02 15:04:05 MST"),
	)

	return e.sendEmail(ctx, subject, body)
}

// sendSuccessNotification sends a success notification
func (e *EmailPlugin) sendSuccessNotification(ctx context.Context, data plugin.HookData) error {
	subject := fmt.Sprintf("Deployment Successful: %s", data.Repository)
	body := fmt.Sprintf(`
Deployment completed successfully for repository: %s

Details:
- Repository: %s
- Commit: %s
- Files deployed: %d
- Duration: %s
- Start time: %s
- End time: %s

Files deployed:
%s

This is an automated message from FlakeDrop.
`, 
		data.Repository,
		data.Repository,
		data.Commit,
		len(data.Files),
		data.Duration.String(),
		data.StartTime.Format("2006-01-02 15:04:05 MST"),
		data.EndTime.Format("2006-01-02 15:04:05 MST"),
		strings.Join(data.Files, "\n- "),
	)

	return e.sendEmail(ctx, subject, body)
}

// sendErrorNotification sends an error notification
func (e *EmailPlugin) sendErrorNotification(ctx context.Context, data plugin.HookData) error {
	errorMsg := "Unknown error"
	if data.Error != nil {
		errorMsg = data.Error.Error()
	}

	subject := fmt.Sprintf("Deployment Failed: %s", data.Repository)
	body := fmt.Sprintf(`
Deployment failed for repository: %s

Details:
- Repository: %s
- Commit: %s
- Error: %s
- Duration: %s
- Start time: %s

Please check the deployment logs for more information.

This is an automated message from FlakeDrop.
`, 
		data.Repository,
		data.Repository,
		data.Commit,
		errorMsg,
		data.Duration.String(),
		data.StartTime.Format("2006-01-02 15:04:05 MST"),
	)

	return e.sendEmail(ctx, subject, body)
}

// sendFinishNotification sends a finish notification
func (e *EmailPlugin) sendFinishNotification(ctx context.Context, data plugin.HookData) error {
	status := "Completed Successfully"
	if !data.Success {
		status = "Completed with Errors"
	}

	subject := fmt.Sprintf("Deployment Finished: %s (%s)", data.Repository, status)
	body := fmt.Sprintf(`
Deployment has finished for repository: %s

Details:
- Repository: %s
- Commit: %s
- Status: %s
- Total duration: %s
- Start time: %s
- End time: %s

This is an automated message from FlakeDrop.
`, 
		data.Repository,
		data.Repository,
		data.Commit,
		status,
		data.Duration.String(),
		data.StartTime.Format("2006-01-02 15:04:05 MST"),
		data.EndTime.Format("2006-01-02 15:04:05 MST"),
	)

	return e.sendEmail(ctx, subject, body)
}

// convertToEmailTemplate converts a generic notification message to email format
func (e *EmailPlugin) convertToEmailTemplate(message plugin.NotificationMessage) EmailTemplate {
	return EmailTemplate{
		Subject: fmt.Sprintf("[FlakeDrop] %s", message.Title),
		Body: fmt.Sprintf(`
%s

%s

Details:
- Source: %s
- Timestamp: %s
- Level: %s
- Deployment ID: %s
- Repository: %s
- Commit: %s

This is an automated message from FlakeDrop.
`,
			message.Title,
			message.Message,
			message.Source,
			message.Timestamp.Format("2006-01-02 15:04:05 MST"),
			string(message.Level),
			message.DeploymentID,
			message.Repository,
			message.Commit,
		),
	}
}

// sendEmail sends an email using SMTP
func (e *EmailPlugin) sendEmail(ctx context.Context, subject, body string) error {
	// Build recipients list
	recipients := make([]string, 0, len(e.to)+len(e.cc)+len(e.bcc))
	recipients = append(recipients, e.to...)
	recipients = append(recipients, e.cc...)
	recipients = append(recipients, e.bcc...)

	// Build email headers
	headers := make(map[string]string)
	headers["From"] = e.from
	headers["To"] = strings.Join(e.to, ", ")
	if len(e.cc) > 0 {
		headers["Cc"] = strings.Join(e.cc, ", ")
	}
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/plain; charset=utf-8"
	headers["Date"] = time.Now().Format(time.RFC1123Z)

	// Build email message
	message := ""
	for key, value := range headers {
		message += fmt.Sprintf("%s: %s\r\n", key, value)
	}
	message += "\r\n" + body

	// Set up authentication
	auth := smtp.PlainAuth("", e.username, e.password, e.smtpHost)

	// Send email
	addr := fmt.Sprintf("%s:%s", e.smtpHost, e.smtpPort)
	err := smtp.SendMail(addr, auth, e.from, recipients, []byte(message))
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

// GetManifest returns the plugin manifest
func (e *EmailPlugin) GetManifest() plugin.PluginManifest {
	return plugin.PluginManifest{
		Name:        "email",
		Version:     "1.0.0",
		Description: "Send deployment notifications via email",
		Author:      "FlakeDrop Team",
		License:     "MIT",
		Keywords:    []string{"notification", "email", "smtp"},
		Capabilities: []string{"notification", "hook"},
		Config: []plugin.ConfigSchema{
			{
				Name:        "smtp_host",
				Type:        "string",
				Description: "SMTP server hostname",
				Required:    true,
			},
			{
				Name:        "smtp_port",
				Type:        "string",
				Description: "SMTP server port",
				Required:    false,
				Default:     "587",
			},
			{
				Name:        "username",
				Type:        "string",
				Description: "SMTP username",
				Required:    true,
			},
			{
				Name:        "password",
				Type:        "string",
				Description: "SMTP password",
				Required:    true,
			},
			{
				Name:        "from",
				Type:        "string",
				Description: "From email address",
				Required:    false,
			},
			{
				Name:        "to",
				Type:        "array",
				Description: "To email addresses",
				Required:    true,
			},
			{
				Name:        "cc",
				Type:        "array",
				Description: "CC email addresses",
				Required:    false,
			},
			{
				Name:        "bcc",
				Type:        "array",
				Description: "BCC email addresses",
				Required:    false,
			},
		},
		Hooks: []plugin.HookType{
			plugin.HookOnSuccess,
			plugin.HookOnError,
			plugin.HookOnStart,
			plugin.HookOnFinish,
		},
		Permissions: []plugin.Permission{
			{
				Name:        "network",
				Description: "Send emails via SMTP",
				Required:    true,
			},
		},
	}
}