package analytics

import (
    "time"
)

type EventType string

const (
    EventCommandExecuted   EventType = "command_executed"
    EventDeploymentStarted EventType = "deployment_started"
    EventDeploymentSuccess EventType = "deployment_success"
    EventDeploymentFailed  EventType = "deployment_failed"
    EventRollbackExecuted  EventType = "rollback_executed"
    EventFeatureUsed       EventType = "feature_used"
    EventErrorOccurred     EventType = "error_occurred"
    EventSessionStarted    EventType = "session_started"
    EventSessionEnded      EventType = "session_ended"
)

type AnalyticsLevel string

const (
    AnalyticsNone    AnalyticsLevel = "none"
    AnalyticsMinimal AnalyticsLevel = "minimal"
    AnalyticsBasic   AnalyticsLevel = "basic"
    AnalyticsFull    AnalyticsLevel = "full"
)

type Event struct {
    ID           string                 `json:"id"`
    Type         EventType              `json:"type"`
    Timestamp    time.Time              `json:"timestamp"`
    SessionID    string                 `json:"session_id"`
    UserID       string                 `json:"user_id"`
    Version      string                 `json:"version"`
    Properties   map[string]interface{} `json:"properties,omitempty"`
    ErrorDetails *ErrorInfo             `json:"error,omitempty"`
}

type ErrorInfo struct {
    Type    string `json:"type"`
    Message string `json:"message"`
    Stack   string `json:"stack,omitempty"`
}

type UsageStats struct {
    CommandCounts      map[string]int    `json:"command_counts"`
    FeatureCounts      map[string]int    `json:"feature_counts"`
    TotalDeployments   int               `json:"total_deployments"`
    SuccessfulDeploys  int               `json:"successful_deploys"`
    FailedDeploys      int               `json:"failed_deploys"`
    TotalRollbacks     int               `json:"total_rollbacks"`
    SessionDuration    time.Duration     `json:"session_duration"`
    LastActivity       time.Time         `json:"last_activity"`
}

type UserJourney struct {
    SessionID   string     `json:"session_id"`
    StartTime   time.Time  `json:"start_time"`
    EndTime     *time.Time `json:"end_time,omitempty"`
    Steps       []Step     `json:"steps"`
    Completed   bool       `json:"completed"`
}

type Step struct {
    Order     int                    `json:"order"`
    Action    string                 `json:"action"`
    Timestamp time.Time              `json:"timestamp"`
    Duration  time.Duration          `json:"duration"`
    Success   bool                   `json:"success"`
    Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type Config struct {
    Enabled         bool           `json:"enabled"`
    Level           AnalyticsLevel `json:"level"`
    OptIn           bool           `json:"opt_in"`
    AnonymousID     string         `json:"anonymous_id"`
    LocalStorage    bool           `json:"local_storage"`
    RemoteEndpoint  string         `json:"remote_endpoint,omitempty"`
    BufferSize      int            `json:"buffer_size"`
    FlushInterval   time.Duration  `json:"flush_interval"`
    RetentionDays   int            `json:"retention_days"`
    ExcludePatterns []string       `json:"exclude_patterns,omitempty"`
}