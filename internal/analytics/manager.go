package analytics

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

var (
    globalManager *Manager
    once          sync.Once
)

type Manager struct {
    collector     *Collector
    privacyFilter *PrivacyFilter
    config        *Config
    mu            sync.RWMutex
}

func GetManager() *Manager {
    once.Do(func() {
        globalManager = NewManager()
    })
    return globalManager
}

func NewManager() *Manager {
    config := loadOrCreateConfig()
    
    manager := &Manager{
        config:        config,
        collector:     NewCollector(config),
        privacyFilter: NewPrivacyFilter(true, false),
    }
    
    return manager
}

func (m *Manager) Initialize(ctx context.Context) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if !m.hasUserConsent() {
        shouldEnable := m.promptForConsent()
        m.config.OptIn = shouldEnable
        m.config.Enabled = shouldEnable
        m.saveConfig()
    }

    if m.config.Enabled {
        m.collector.SetOptIn(m.config.OptIn)
    }

    return nil
}

func (m *Manager) Track(eventType EventType, properties map[string]interface{}) {
    if !m.IsEnabled() {
        return
    }

    sanitized := m.privacyFilter.SanitizeMap(properties)
    m.collector.TrackEvent(eventType, sanitized)
}

func (m *Manager) TrackCommand(command string, success bool, duration int64) {
    if !m.IsEnabled() {
        return
    }

    sanitizedCommand := m.privacyFilter.SanitizeString(command)
    m.collector.TrackCommand(sanitizedCommand, success, time.Duration(duration))
}

func (m *Manager) TrackError(err error, context string) {
    if !m.IsEnabled() {
        return
    }

    sanitizedContext := m.privacyFilter.SanitizeString(context)
    m.collector.TrackError(err, sanitizedContext)
}

func (m *Manager) IsEnabled() bool {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.config.Enabled && m.config.OptIn
}

func (m *Manager) SetEnabled(enabled bool) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    m.config.Enabled = enabled
    m.collector.SetOptIn(enabled)
    m.saveConfig()
}

func (m *Manager) SetLevel(level AnalyticsLevel) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    m.config.Level = level
    m.saveConfig()
}

func (m *Manager) GetStats() UsageStats {
    return m.collector.GetStats()
}

func (m *Manager) GetCollector() *Collector {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.collector
}

func (m *Manager) ExportData(format string) ([]byte, error) {
    stats := m.GetStats()
    
    switch format {
    case "json":
        return json.MarshalIndent(stats, "", "  ")
    case "csv":
        return m.exportCSV(stats)
    default:
        return nil, fmt.Errorf("unsupported export format: %s", format)
    }
}

func (m *Manager) ClearData() error {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return err
    }

    analyticsDir := filepath.Join(homeDir, ".flakedrop", "analytics")
    return os.RemoveAll(analyticsDir)
}

func (m *Manager) Shutdown() {
    m.collector.Shutdown()
}

func (m *Manager) hasUserConsent() bool {
    return m.config.OptIn
}

func (m *Manager) promptForConsent() bool {
    fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
    fmt.Println("║              FlakeDrop Usage Analytics                      ║")
    fmt.Println("╚════════════════════════════════════════════════════════════╝")
    fmt.Println()
    fmt.Println("Help improve FlakeDrop by sharing anonymous usage data.")
    fmt.Println()
    fmt.Println("What we track (anonymously):")
    fmt.Println("  • When configs are created (to know adoption)")
    fmt.Println("  • When real deployments run (to know actual usage)")
    fmt.Println("  • Success/failure rates (to prioritize fixes)")
    fmt.Println("  • OS and version info (to know what to support)")
    fmt.Println()
    fmt.Println("What we NEVER track:")
    fmt.Println("  • Personal information or credentials")
    fmt.Println("  • SQL content or database schemas")
    fmt.Println("  • Company names or identifiable data")
    fmt.Println()
    fmt.Println("This helps us understand if people actually use FlakeDrop")
    fmt.Println("and what features to build next.")
    fmt.Println()
    fmt.Println("You can change this anytime with: flakedrop analytics disable")
    fmt.Println()
    fmt.Print("Enable anonymous telemetry? (y/N): ")
    
    var response string
    fmt.Scanln(&response)
    
    return response == "y" || response == "Y"
}

func (m *Manager) saveConfig() error {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return err
    }

    configDir := filepath.Join(homeDir, ".flakedrop")
    if err := os.MkdirAll(configDir, 0700); err != nil {
        return err
    }

    configPath := filepath.Join(configDir, "analytics.json")
    data, err := json.MarshalIndent(m.config, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(configPath, data, 0600)
}

func (m *Manager) exportCSV(stats UsageStats) ([]byte, error) {
    // Simple CSV export implementation
    var result string
    
    result += "Metric,Value\n"
    result += fmt.Sprintf("Total Deployments,%d\n", stats.TotalDeployments)
    result += fmt.Sprintf("Successful Deploys,%d\n", stats.SuccessfulDeploys)
    result += fmt.Sprintf("Failed Deploys,%d\n", stats.FailedDeploys)
    result += fmt.Sprintf("Total Rollbacks,%d\n", stats.TotalRollbacks)
    
    result += "\nCommand,Count\n"
    for cmd, count := range stats.CommandCounts {
        result += fmt.Sprintf("%s,%d\n", cmd, count)
    }
    
    result += "\nFeature,Count\n"
    for feature, count := range stats.FeatureCounts {
        result += fmt.Sprintf("%s,%d\n", feature, count)
    }
    
    return []byte(result), nil
}

func loadOrCreateConfig() *Config {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return DefaultConfig()
    }

    configPath := filepath.Join(homeDir, ".flakedrop", "analytics.json")
    data, err := os.ReadFile(configPath)
    if err != nil {
        return DefaultConfig()
    }

    var config Config
    if err := json.Unmarshal(data, &config); err != nil {
        return DefaultConfig()
    }

    return &config
}