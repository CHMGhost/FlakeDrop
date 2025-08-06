// Example telemetry server for FlakeDrop
// This is a simple server that could be deployed to receive anonymous telemetry data
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sync"
    "time"
)

type TelemetryServer struct {
    mu     sync.RWMutex
    events []Event
    stats  Stats
}

type Event struct {
    Type      string                 `json:"type"`
    Data      map[string]interface{} `json:"data"`
    Timestamp time.Time              `json:"timestamp"`
}

type Stats struct {
    UniqueUsers      map[string]bool
    TotalDeployments int
    SuccessfulDeploys int
    FailedDeploys    int
    ConfigsCreated   int
    OSBreakdown      map[string]int
    VersionBreakdown map[string]int
}

func NewTelemetryServer() *TelemetryServer {
    return &TelemetryServer{
        events: make([]Event, 0),
        stats: Stats{
            UniqueUsers:      make(map[string]bool),
            OSBreakdown:      make(map[string]int),
            VersionBreakdown: make(map[string]int),
        },
    }
}

func (ts *TelemetryServer) handleEvent(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var event Event
    if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }

    ts.mu.Lock()
    defer ts.mu.Unlock()

    // Store event
    ts.events = append(ts.events, event)

    // Update stats based on event type
    switch event.Type {
    case "heartbeat":
        if data, ok := event.Data["data"].(map[string]interface{}); ok {
            if userID, ok := data["user_id"].(string); ok {
                ts.stats.UniqueUsers[userID] = true
            }
            if os, ok := data["os"].(string); ok {
                ts.stats.OSBreakdown[os]++
            }
            if version, ok := data["version"].(string); ok {
                ts.stats.VersionBreakdown[version]++
            }
        }

    case "config_created":
        ts.stats.ConfigsCreated++
        if data, ok := event.Data["data"].(map[string]interface{}); ok {
            if userID, ok := data["user_id"].(string); ok {
                ts.stats.UniqueUsers[userID] = true
            }
        }

    case "deployment":
        if data, ok := event.Data["data"].(map[string]interface{}); ok {
            ts.stats.TotalDeployments++
            if success, ok := data["success"].(bool); ok {
                if success {
                    ts.stats.SuccessfulDeploys++
                } else {
                    ts.stats.FailedDeploys++
                }
            }
            if userID, ok := data["user_id"].(string); ok {
                ts.stats.UniqueUsers[userID] = true
            }
        }
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (ts *TelemetryServer) handleStats(w http.ResponseWriter, r *http.Request) {
    ts.mu.RLock()
    defer ts.mu.RUnlock()

    stats := map[string]interface{}{
        "unique_users":       len(ts.stats.UniqueUsers),
        "total_deployments":  ts.stats.TotalDeployments,
        "successful_deploys": ts.stats.SuccessfulDeploys,
        "failed_deploys":     ts.stats.FailedDeploys,
        "configs_created":    ts.stats.ConfigsCreated,
        "os_breakdown":       ts.stats.OSBreakdown,
        "version_breakdown":  ts.stats.VersionBreakdown,
        "total_events":       len(ts.events),
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}

func (ts *TelemetryServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
    ts.mu.RLock()
    defer ts.mu.RUnlock()

    html := `
<!DOCTYPE html>
<html>
<head>
    <title>FlakeDrop Telemetry Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat { display: inline-block; margin: 20px; }
        .stat-value { font-size: 36px; font-weight: bold; color: #2563eb; }
        .stat-label { color: #666; margin-top: 5px; }
        h1 { color: #333; }
        .success { color: #10b981; }
        .failure { color: #ef4444; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 FlakeDrop Telemetry Dashboard</h1>
        
        <div class="card">
            <h2>Active Users</h2>
            <div class="stat">
                <div class="stat-value">%d</div>
                <div class="stat-label">Unique Users</div>
            </div>
            <div class="stat">
                <div class="stat-value">%d</div>
                <div class="stat-label">Configs Created</div>
            </div>
        </div>

        <div class="card">
            <h2>Deployment Stats</h2>
            <div class="stat">
                <div class="stat-value">%d</div>
                <div class="stat-label">Total Deployments</div>
            </div>
            <div class="stat">
                <div class="stat-value success">%d</div>
                <div class="stat-label">Successful</div>
            </div>
            <div class="stat">
                <div class="stat-value failure">%d</div>
                <div class="stat-label">Failed</div>
            </div>
        </div>

        <div class="card">
            <h2>Platform Distribution</h2>
            <table>
                <tr><th>OS</th><th>Count</th></tr>
                %s
            </table>
        </div>

        <div class="card">
            <h2>Version Distribution</h2>
            <table>
                <tr><th>Version</th><th>Count</th></tr>
                %s
            </table>
        </div>
    </div>
</body>
</html>
`

    osRows := ""
    for os, count := range ts.stats.OSBreakdown {
        osRows += fmt.Sprintf("<tr><td>%s</td><td>%d</td></tr>", os, count)
    }

    versionRows := ""
    for version, count := range ts.stats.VersionBreakdown {
        versionRows += fmt.Sprintf("<tr><td>%s</td><td>%d</td></tr>", version, count)
    }

    fmt.Fprintf(w, html,
        len(ts.stats.UniqueUsers),
        ts.stats.ConfigsCreated,
        ts.stats.TotalDeployments,
        ts.stats.SuccessfulDeploys,
        ts.stats.FailedDeploys,
        osRows,
        versionRows,
    )
}

func main() {
    server := NewTelemetryServer()

    http.HandleFunc("/v1/events", server.handleEvent)
    http.HandleFunc("/stats", server.handleStats)
    http.HandleFunc("/", server.handleDashboard)

    fmt.Println("FlakeDrop Telemetry Server")
    fmt.Println("==========================")
    fmt.Println("Dashboard: http://localhost:8080/")
    fmt.Println("Stats API: http://localhost:8080/stats")
    fmt.Println("Event endpoint: http://localhost:8080/v1/events")
    fmt.Println()
    fmt.Println("Listening on :8080...")

    log.Fatal(http.ListenAndServe(":8080", nil))
}