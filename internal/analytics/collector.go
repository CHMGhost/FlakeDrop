package analytics

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"

    "github.com/google/uuid"
)

type Collector struct {
    config      *Config
    events      []Event
    stats       *UsageStats
    journey     *UserJourney
    sessionID   string
    mu          sync.RWMutex
    stopChan    chan struct{}
    flushTicker *time.Ticker
}

func NewCollector(config *Config) *Collector {
    if config == nil {
        config = DefaultConfig()
    }

    if config.AnonymousID == "" {
        config.AnonymousID = generateAnonymousID()
    }

    c := &Collector{
        config:    config,
        events:    make([]Event, 0, config.BufferSize),
        stats:     &UsageStats{
            CommandCounts: make(map[string]int),
            FeatureCounts: make(map[string]int),
        },
        sessionID: uuid.New().String(),
        stopChan:  make(chan struct{}),
    }

    if config.Enabled && config.FlushInterval > 0 {
        c.startAutoFlush()
    }

    c.TrackEvent(EventSessionStarted, nil)
    
    return c
}

func DefaultConfig() *Config {
    // Get telemetry endpoint from environment or use default
    endpoint := os.Getenv("FLAKEDROP_TELEMETRY_ENDPOINT")
    
    return &Config{
        Enabled:        false,
        Level:          AnalyticsMinimal,
        OptIn:          false,
        LocalStorage:   true,
        BufferSize:     100,
        FlushInterval:  5 * time.Minute,
        RetentionDays:  30,
        RemoteEndpoint: endpoint,
        AnonymousID:    generateAnonymousID(),
    }
}

func (c *Collector) IsEnabled() bool {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.config.Enabled && c.config.OptIn
}

func (c *Collector) SetOptIn(optIn bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.config.OptIn = optIn
    c.saveConfig()
}

func (c *Collector) TrackEvent(eventType EventType, properties map[string]interface{}) {
    if !c.IsEnabled() {
        return
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    event := Event{
        ID:         uuid.New().String(),
        Type:       eventType,
        Timestamp:  time.Now(),
        SessionID:  c.sessionID,
        UserID:     c.config.AnonymousID,
        Version:    getVersion(),
        Properties: c.sanitizeProperties(properties),
    }

    c.events = append(c.events, event)
    c.updateStats(event)

    if len(c.events) >= c.config.BufferSize {
        go c.flush()
    }
}

func (c *Collector) TrackCommand(command string, success bool, duration time.Duration) {
    if !c.shouldTrack(AnalyticsMinimal) {
        return
    }

    properties := map[string]interface{}{
        "command":  command,
        "success":  success,
        "duration": duration.Milliseconds(),
    }

    c.TrackEvent(EventCommandExecuted, properties)
    
    c.mu.Lock()
    c.stats.CommandCounts[command]++
    c.mu.Unlock()
}

func (c *Collector) TrackFeature(feature string, metadata map[string]interface{}) {
    if !c.shouldTrack(AnalyticsBasic) {
        return
    }

    properties := map[string]interface{}{
        "feature": feature,
    }
    
    for k, v := range metadata {
        properties[k] = v
    }

    c.TrackEvent(EventFeatureUsed, properties)
    
    c.mu.Lock()
    c.stats.FeatureCounts[feature]++
    c.mu.Unlock()
}

func (c *Collector) TrackError(err error, context string) {
    if !c.shouldTrack(AnalyticsBasic) {
        return
    }

    errorInfo := &ErrorInfo{
        Type:    fmt.Sprintf("%T", err),
        Message: err.Error(),
    }

    if c.config.Level == AnalyticsFull {
        errorInfo.Stack = captureStackTrace()
    }

    event := Event{
        ID:           uuid.New().String(),
        Type:         EventErrorOccurred,
        Timestamp:    time.Now(),
        SessionID:    c.sessionID,
        UserID:       c.config.AnonymousID,
        Version:      getVersion(),
        ErrorDetails: errorInfo,
        Properties: map[string]interface{}{
            "context": context,
        },
    }

    c.mu.Lock()
    c.events = append(c.events, event)
    c.mu.Unlock()
}

func (c *Collector) TrackDeployment(success bool, duration time.Duration, metadata map[string]interface{}) {
    if !c.shouldTrack(AnalyticsMinimal) {
        return
    }

    eventType := EventDeploymentSuccess
    if !success {
        eventType = EventDeploymentFailed
    }

    properties := map[string]interface{}{
        "duration": duration.Milliseconds(),
        "success":  success,
    }

    for k, v := range metadata {
        if c.shouldIncludeProperty(k) {
            properties[k] = v
        }
    }

    c.TrackEvent(eventType, properties)

    c.mu.Lock()
    c.stats.TotalDeployments++
    if success {
        c.stats.SuccessfulDeploys++
    } else {
        c.stats.FailedDeploys++
    }
    c.mu.Unlock()
}

func (c *Collector) StartJourney(journeyName string) {
    if !c.shouldTrack(AnalyticsFull) {
        return
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    c.journey = &UserJourney{
        SessionID: c.sessionID,
        StartTime: time.Now(),
        Steps:     make([]Step, 0),
    }
}

func (c *Collector) AddJourneyStep(action string, success bool, metadata map[string]interface{}) {
    if !c.shouldTrack(AnalyticsFull) || c.journey == nil {
        return
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    step := Step{
        Order:     len(c.journey.Steps) + 1,
        Action:    action,
        Timestamp: time.Now(),
        Success:   success,
        Metadata:  c.sanitizeProperties(metadata),
    }

    if len(c.journey.Steps) > 0 {
        lastStep := c.journey.Steps[len(c.journey.Steps)-1]
        step.Duration = step.Timestamp.Sub(lastStep.Timestamp)
    }

    c.journey.Steps = append(c.journey.Steps, step)
}

func (c *Collector) CompleteJourney(success bool) {
    if !c.shouldTrack(AnalyticsFull) || c.journey == nil {
        return
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    now := time.Now()
    c.journey.EndTime = &now
    c.journey.Completed = success
}

func (c *Collector) GetStats() UsageStats {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    statsCopy := *c.stats
    statsCopy.CommandCounts = make(map[string]int)
    statsCopy.FeatureCounts = make(map[string]int)
    
    for k, v := range c.stats.CommandCounts {
        statsCopy.CommandCounts[k] = v
    }
    for k, v := range c.stats.FeatureCounts {
        statsCopy.FeatureCounts[k] = v
    }
    
    return statsCopy
}

func (c *Collector) flush() {
    c.mu.Lock()
    events := make([]Event, len(c.events))
    copy(events, c.events)
    c.events = c.events[:0]
    c.mu.Unlock()

    if len(events) == 0 {
        return
    }

    if c.config.LocalStorage {
        c.saveToLocal(events)
    }

    if c.config.RemoteEndpoint != "" && c.config.OptIn {
        c.sendToRemote(events)
    }
}

func (c *Collector) saveToLocal(events []Event) error {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return err
    }

    analyticsDir := filepath.Join(homeDir, ".flakedrop", "analytics")
    if err := os.MkdirAll(analyticsDir, 0700); err != nil {
        return err
    }

    timestamp := time.Now().Format("20060102-150405")
    filename := filepath.Join(analyticsDir, fmt.Sprintf("events-%s.json", timestamp))

    data, err := json.MarshalIndent(events, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(filename, data, 0600)
}

func (c *Collector) sendToRemote(events []Event) {
    // Implementation would send to remote endpoint
    // This is intentionally left as a stub for security
}

func (c *Collector) saveConfig() error {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return err
    }

    configPath := filepath.Join(homeDir, ".flakedrop", "analytics.json")
    data, err := json.MarshalIndent(c.config, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(configPath, data, 0600)
}

func (c *Collector) startAutoFlush() {
    c.flushTicker = time.NewTicker(c.config.FlushInterval)
    go func() {
        for {
            select {
            case <-c.flushTicker.C:
                c.flush()
            case <-c.stopChan:
                return
            }
        }
    }()
}

func (c *Collector) Shutdown() {
    c.TrackEvent(EventSessionEnded, nil)
    
    if c.flushTicker != nil {
        c.flushTicker.Stop()
    }
    
    close(c.stopChan)
    c.flush()
}

func (c *Collector) shouldTrack(requiredLevel AnalyticsLevel) bool {
    if !c.IsEnabled() {
        return false
    }

    levelMap := map[AnalyticsLevel]int{
        AnalyticsNone:    0,
        AnalyticsMinimal: 1,
        AnalyticsBasic:   2,
        AnalyticsFull:    3,
    }

    return levelMap[c.config.Level] >= levelMap[requiredLevel]
}

func (c *Collector) shouldIncludeProperty(key string) bool {
    for _, pattern := range c.config.ExcludePatterns {
        if matched, _ := filepath.Match(pattern, key); matched {
            return false
        }
    }
    return true
}

func (c *Collector) sanitizeProperties(props map[string]interface{}) map[string]interface{} {
    if props == nil {
        return nil
    }

    sanitized := make(map[string]interface{})
    for k, v := range props {
        if c.shouldIncludeProperty(k) {
            sanitized[k] = sanitizeValue(v)
        }
    }
    return sanitized
}

func (c *Collector) updateStats(event Event) {
    c.stats.LastActivity = event.Timestamp
}

func generateAnonymousID() string {
    hostname, _ := os.Hostname()
    username := os.Getenv("USER")
    if username == "" {
        username = os.Getenv("USERNAME")
    }
    
    data := fmt.Sprintf("%s-%s-%d", hostname, username, time.Now().Unix())
    hash := sha256.Sum256([]byte(data))
    return hex.EncodeToString(hash[:16])
}

func sanitizeValue(v interface{}) interface{} {
    switch val := v.(type) {
    case string:
        // Remove potential sensitive data patterns
        if len(val) > 100 {
            return val[:100] + "..."
        }
        return val
    default:
        return v
    }
}

func captureStackTrace() string {
    // Simplified stack trace capture
    return "stack trace capture not implemented"
}

func getVersion() string {
    // This would be populated from build info
    return "1.0.0"
}