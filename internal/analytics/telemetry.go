package analytics

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "os"
    "runtime"
    "strings"
    "time"
)

// TelemetryConfig for remote analytics
type TelemetryConfig struct {
    Enabled        bool   `json:"enabled"`
    Endpoint       string `json:"endpoint"`
    HeartbeatOnly  bool   `json:"heartbeat_only"`  // Only send daily ping
    IncludeMetrics bool   `json:"include_metrics"` // Send usage metrics
}

// Heartbeat represents a daily active user ping
type Heartbeat struct {
    Version   string    `json:"version"`
    OS        string    `json:"os"`
    Arch      string    `json:"arch"`
    UserID    string    `json:"user_id"`    // Anonymous hash
    Timestamp time.Time `json:"timestamp"`
    
    // Optional metrics (if user opts in)
    Metrics *HeartbeatMetrics `json:"metrics,omitempty"`
}

type HeartbeatMetrics struct {
    TotalDeployments  int `json:"total_deployments"`
    WeeklyDeployments int `json:"weekly_deployments"`
    LastActiveDate    string `json:"last_active_date"`
    ErrorRate         float64 `json:"error_rate"`
}

// SendHeartbeat sends a daily active user ping
func (m *Manager) SendHeartbeat() error {
    // Check if we should send
    if !m.shouldSendHeartbeat() {
        return nil
    }
    
    heartbeat := Heartbeat{
        Version:   getVersion(),
        OS:        runtime.GOOS,
        Arch:      runtime.GOARCH,
        UserID:    m.config.AnonymousID,
        Timestamp: time.Now().UTC(),
    }
    
    // Include metrics only if user opted in
    if m.config.Level >= AnalyticsBasic {
        stats := m.GetStats()
        heartbeat.Metrics = &HeartbeatMetrics{
            TotalDeployments:  stats.TotalDeployments,
            WeeklyDeployments: m.getWeeklyDeployments(),
            LastActiveDate:    stats.LastActivity.Format("2006-01-02"),
            ErrorRate:         m.calculateErrorRate(),
        }
    }
    
    // Send the heartbeat
    return m.sendTelemetry("heartbeat", heartbeat)
}

// TrackConfigCreated tracks when a user creates a config file
func (m *Manager) TrackConfigCreated(configType string) {
    if !m.IsEnabled() || !m.config.OptIn {
        return
    }
    
    event := map[string]interface{}{
        "event":       "config_created",
        "config_type": configType,
        "version":     getVersion(),
        "user_id":     m.config.AnonymousID,
        "timestamp":   time.Now().UTC(),
    }
    
    // Send immediately for important events
    go m.sendTelemetry("config_created", event)
}

// TrackRealDeployment tracks when someone runs an actual (non-dry-run) deployment
func (m *Manager) TrackRealDeployment(success bool, fileCount int) {
    if !m.IsEnabled() || !m.config.OptIn {
        return
    }
    
    event := map[string]interface{}{
        "event":      "real_deployment",
        "success":    success,
        "file_count": fileCount,
        "version":    getVersion(),
        "user_id":    m.config.AnonymousID,
        "timestamp":  time.Now().UTC(),
        "os":         runtime.GOOS,
    }
    
    // Send immediately for important events
    go m.sendTelemetry("deployment", event)
}

// CheckForUpdates checks if a new version is available
func (m *Manager) CheckForUpdates() (*UpdateInfo, error) {
    if !m.config.Enabled {
        return nil, nil
    }
    
    endpoint := m.config.RemoteEndpoint + "/v1/version/latest"
    if m.config.RemoteEndpoint == "" {
        endpoint = os.Getenv("FLAKEDROP_TELEMETRY_ENDPOINT")
        if endpoint == "" {
            return nil, nil
        }
        endpoint = endpoint + "/v1/version/latest"
    }
    
    // Validate URL to prevent SSRF
    if !isValidTelemetryURL(endpoint) {
        return nil, fmt.Errorf("invalid telemetry endpoint")
    }
    
    resp, err := http.Get(endpoint) // #nosec G107 - URL is validated
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var info UpdateInfo
    if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
        return nil, err
    }
    
    return &info, nil
}

type UpdateInfo struct {
    LatestVersion string `json:"latest_version"`
    ReleaseDate   string `json:"release_date"`
    UpdateURL     string `json:"update_url"`
    Critical      bool   `json:"critical"`
}

func (m *Manager) shouldSendHeartbeat() bool {
    // Only if telemetry is enabled
    if !m.config.Enabled || !m.config.OptIn {
        return false
    }
    
    // Check last heartbeat time
    lastHeartbeat := m.getLastHeartbeatTime()
    if time.Since(lastHeartbeat) < 24*time.Hour {
        return false
    }
    
    return true
}

func (m *Manager) sendTelemetry(eventType string, data interface{}) error {
    endpoint := m.config.RemoteEndpoint + "/v1/events"
    if endpoint == "/v1/events" {
        // No endpoint configured, use environment variable or default
        endpoint = os.Getenv("FLAKEDROP_TELEMETRY_ENDPOINT")
        if endpoint == "" {
            // Skip telemetry if no endpoint configured
            return nil
        }
        endpoint = endpoint + "/v1/events"
    }
    
    payload := map[string]interface{}{
        "type": eventType,
        "data": data,
    }
    
    body, err := json.Marshal(payload)
    if err != nil {
        return err
    }
    
    req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
    if err != nil {
        return err
    }
    
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("User-Agent", fmt.Sprintf("FlakeDrop/%s", getVersion()))
    
    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        // Silently fail - don't interrupt user's work
        return nil
    }
    defer resp.Body.Close()
    
    // Update last heartbeat time
    m.updateLastHeartbeatTime()
    
    return nil
}

func (m *Manager) getLastHeartbeatTime() time.Time {
    // Read from local storage
    // Implementation would check ~/.flakedrop/last_heartbeat
    return time.Time{}
}

func (m *Manager) updateLastHeartbeatTime() {
    // Save to local storage
    // Implementation would write to ~/.flakedrop/last_heartbeat
}

func (m *Manager) getWeeklyDeployments() int {
    // Calculate deployments in last 7 days
    return 0
}

func (m *Manager) calculateErrorRate() float64 {
    stats := m.GetStats()
    if stats.TotalDeployments == 0 {
        return 0
    }
    return float64(stats.FailedDeploys) / float64(stats.TotalDeployments)
}

// isValidTelemetryURL validates that the URL is a valid telemetry endpoint
func isValidTelemetryURL(endpoint string) bool {
    // Parse the URL
    u, err := url.Parse(endpoint)
    if err != nil {
        return false
    }
    
    // Only allow HTTPS
    if u.Scheme != "https" {
        return false
    }
    
    // Only allow specific domains
    allowedHosts := []string{
        "telemetry.flakedrop.io",
        "api.flakedrop.io",
        "localhost",
        "127.0.0.1",
    }
    
    host := strings.ToLower(u.Hostname())
    for _, allowed := range allowedHosts {
        if host == allowed {
            return true
        }
    }
    
    // Allow custom endpoints via environment variable
    customEndpoint := os.Getenv("FLAKEDROP_ALLOW_CUSTOM_TELEMETRY")
    if customEndpoint == "true" {
        return true
    }
    
    return false
}