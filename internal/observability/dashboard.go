package observability

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// DashboardServer serves a real-time monitoring dashboard
type DashboardServer struct {
	registry      *MetricsRegistry
	healthManager *HealthManager
	tracer        *Tracer
	logger        *Logger
	templates     *template.Template
}

// NewDashboardServer creates a new dashboard server
func NewDashboardServer(registry *MetricsRegistry, healthManager *HealthManager, tracer *Tracer, logger *Logger) *DashboardServer {
	return &DashboardServer{
		registry:      registry,
		healthManager: healthManager,
		tracer:        tracer,
		logger:        logger,
		templates:     loadTemplates(),
	}
}

// DashboardData represents data for the dashboard
type DashboardData struct {
	Timestamp    time.Time                 `json:"timestamp"`
	Metrics      map[string]interface{}    `json:"metrics"`
	Health       HealthReport              `json:"health"`
	ActiveSpans  int                       `json:"active_spans"`
	SystemInfo   SystemInfo                `json:"system_info"`
	RecentLogs   []LogEntry                `json:"recent_logs,omitempty"`
}

// SystemInfo represents system information
type SystemInfo struct {
	Uptime      time.Duration `json:"uptime"`
	Version     string        `json:"version"`
	GoVersion   string        `json:"go_version"`
	Goroutines  int           `json:"goroutines"`
	MemoryUsage MemoryInfo    `json:"memory"`
	CPUInfo     CPUInfo       `json:"cpu"`
}

// MemoryInfo represents memory information
type MemoryInfo struct {
	AllocMB      float64 `json:"alloc_mb"`
	TotalAllocMB float64 `json:"total_alloc_mb"`
	SysMB        float64 `json:"sys_mb"`
	NumGC        uint32  `json:"num_gc"`
}

// CPUInfo represents CPU information
type CPUInfo struct {
	NumCPU int `json:"num_cpu"`
}

// ServeHTTP handles HTTP requests for the dashboard
func (ds *DashboardServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/", "/dashboard":
		ds.serveDashboard(w, r)
	case "/api/data":
		ds.serveAPIData(w, r)
	case "/api/metrics":
		ds.serveMetrics(w, r)
	case "/api/health":
		ds.serveHealth(w, r)
	case "/api/traces":
		ds.serveTraces(w, r)
	default:
		http.NotFound(w, r)
	}
}

// serveDashboard serves the main dashboard HTML
func (ds *DashboardServer) serveDashboard(w http.ResponseWriter, r *http.Request) {
	data := ds.collectDashboardData(r.Context())
	
	w.Header().Set("Content-Type", "text/html")
	if err := ds.templates.ExecuteTemplate(w, "dashboard.html", data); err != nil {
		ds.logger.ErrorWithFields("Failed to render dashboard", map[string]interface{}{
			"error": err.Error(),
		})
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// serveAPIData serves dashboard data as JSON
func (ds *DashboardServer) serveAPIData(w http.ResponseWriter, r *http.Request) {
	data := ds.collectDashboardData(r.Context())
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		ds.logger.ErrorWithFields("Failed to encode dashboard data", map[string]interface{}{
			"error": err.Error(),
		})
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// serveMetrics serves metrics data
func (ds *DashboardServer) serveMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ds.registry.GetAllMetrics()
	
	// Convert metrics to a more dashboard-friendly format
	formattedMetrics := make(map[string]interface{})
	for name, metric := range metrics {
		switch m := metric.(type) {
		case *Counter:
			formattedMetrics[name] = map[string]interface{}{
				"type":  "counter",
				"value": m.Value(),
				"help":  m.Help(),
			}
		case *Gauge:
			formattedMetrics[name] = map[string]interface{}{
				"type":  "gauge",
				"value": m.Value(),
				"help":  m.Help(),
			}
		case *Histogram:
			formattedMetrics[name] = map[string]interface{}{
				"type":  "histogram",
				"count": m.Count(),
				"sum":   m.Sum(),
				"help":  m.Help(),
			}
		}
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(formattedMetrics)
}

// serveHealth serves health check data
func (ds *DashboardServer) serveHealth(w http.ResponseWriter, r *http.Request) {
	health := ds.healthManager.CheckHealth(r.Context())
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Set status code based on health
	switch health.Status {
	case HealthStatusUp:
		w.WriteHeader(http.StatusOK)
	case HealthStatusDegraded:
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(health)
}

// serveTraces serves active traces data
func (ds *DashboardServer) serveTraces(w http.ResponseWriter, r *http.Request) {
	spans := ds.tracer.GetActiveSpans()
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(spans)
}

// collectDashboardData collects all data for the dashboard
func (ds *DashboardServer) collectDashboardData(ctx context.Context) DashboardData {
	// Collect metrics
	metrics := make(map[string]interface{})
	for name, metric := range ds.registry.GetAllMetrics() {
		switch m := metric.(type) {
		case *Counter:
			metrics[name] = m.Value()
		case *Gauge:
			metrics[name] = m.Value()
		case *Histogram:
			metrics[name] = map[string]interface{}{
				"count": m.Count(),
				"sum":   m.Sum(),
			}
		}
	}
	
	// Collect health data
	health := ds.healthManager.CheckHealth(ctx)
	
	// Collect active spans
	activeSpans := len(ds.tracer.GetActiveSpans())
	
	// Collect system info
	systemInfo := ds.collectSystemInfo()
	
	return DashboardData{
		Timestamp:   time.Now(),
		Metrics:     metrics,
		Health:      health,
		ActiveSpans: activeSpans,
		SystemInfo:  systemInfo,
	}
}

// collectSystemInfo collects system information
func (ds *DashboardServer) collectSystemInfo() SystemInfo {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	return SystemInfo{
		Uptime:     time.Since(startTime),
		Version:    version,
		GoVersion:  runtime.Version(),
		Goroutines: runtime.NumGoroutine(),
		MemoryUsage: MemoryInfo{
			AllocMB:      float64(memStats.Alloc) / 1024 / 1024,
			TotalAllocMB: float64(memStats.TotalAlloc) / 1024 / 1024,
			SysMB:        float64(memStats.Sys) / 1024 / 1024,
			NumGC:        memStats.NumGC,
		},
		CPUInfo: CPUInfo{
			NumCPU: runtime.NumCPU(),
		},
	}
}

// loadTemplates loads dashboard HTML templates
func loadTemplates() *template.Template {
	dashboardHTML := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Snowflake Deploy - Monitoring Dashboard</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .header h1 {
            margin: 0;
            color: #333;
        }
        .status {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            text-transform: uppercase;
        }
        .status.up {
            background-color: #d4edda;
            color: #155724;
        }
        .status.degraded {
            background-color: #fff3cd;
            color: #856404;
        }
        .status.down {
            background-color: #f8d7da;
            color: #721c24;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h2 {
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
        }
        .metric {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }
        .metric:last-child {
            border-bottom: none;
        }
        .metric-name {
            font-weight: 500;
        }
        .metric-value {
            font-family: monospace;
            font-weight: bold;
        }
        .health-component {
            margin: 10px 0;
            padding: 10px;
            border-radius: 4px;
            border-left: 4px solid #ccc;
        }
        .health-component.up {
            background-color: #d4edda;
            border-left-color: #28a745;
        }
        .health-component.degraded {
            background-color: #fff3cd;
            border-left-color: #ffc107;
        }
        .health-component.down {
            background-color: #f8d7da;
            border-left-color: #dc3545;
        }
        .refresh-info {
            text-align: center;
            margin-top: 20px;
            color: #666;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Snowflake Deploy - Monitoring Dashboard</h1>
            <p>System Status: <span class="status {{.Health.Status}}">{{.Health.Status}}</span></p>
            <p>Last Updated: {{.Timestamp.Format "2006-01-02 15:04:05"}}</p>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>System Overview</h2>
                <div class="metric">
                    <span class="metric-name">Uptime</span>
                    <span class="metric-value">{{.SystemInfo.Uptime}}</span>
                </div>
                <div class="metric">
                    <span class="metric-name">Version</span>
                    <span class="metric-value">{{.SystemInfo.Version}}</span>
                </div>
                <div class="metric">
                    <span class="metric-name">Go Version</span>
                    <span class="metric-value">{{.SystemInfo.GoVersion}}</span>
                </div>
                <div class="metric">
                    <span class="metric-name">Goroutines</span>
                    <span class="metric-value">{{.SystemInfo.Goroutines}}</span>
                </div>
                <div class="metric">
                    <span class="metric-name">Memory (MB)</span>
                    <span class="metric-value">{{printf "%.2f" .SystemInfo.MemoryUsage.AllocMB}}</span>
                </div>
                <div class="metric">
                    <span class="metric-name">Active Spans</span>
                    <span class="metric-value">{{.ActiveSpans}}</span>
                </div>
            </div>
            
            <div class="card">
                <h2>Key Metrics</h2>
                {{range $name, $value := .Metrics}}
                <div class="metric">
                    <span class="metric-name">{{$name}}</span>
                    <span class="metric-value">{{printf "%.2f" $value}}</span>
                </div>
                {{end}}
            </div>
            
            <div class="card">
                <h2>Health Checks</h2>
                {{range $name, $component := .Health.Components}}
                <div class="health-component {{$component.Status}}">
                    <strong>{{$name}}</strong>
                    <p>Status: {{$component.Status}}</p>
                    {{if $component.Message}}
                    <p>{{$component.Message}}</p>
                    {{end}}
                    <small>Duration: {{printf "%.2f" $component.Duration.Seconds}}s</small>
                </div>
                {{end}}
            </div>
        </div>
        
        <div class="refresh-info">
            <p>Dashboard auto-refreshes every 30 seconds</p>
        </div>
    </div>
    
    <script>
        // Auto-refresh the dashboard every 30 seconds
        setTimeout(function() {
            location.reload();
        }, 30000);
    </script>
</body>
</html>
`
	
	tmpl := template.New("dashboard.html")
	template.Must(tmpl.Parse(dashboardHTML))
	
	return tmpl
}

// Global variables for dashboard
var (
	startTime = time.Now()
	version   = "1.0.0"
)

// SetVersion sets the application version for the dashboard
func SetVersion(v string) {
	version = v
}

// AlertManager manages alerting based on metrics and health checks
type AlertManager struct {
	rules   []AlertRule
	logger  *Logger
	mu      sync.RWMutex
}

// AlertRule defines an alerting rule
type AlertRule struct {
	Name        string
	Description string
	Condition   func(data DashboardData) bool
	Severity    AlertSeverity
	Cooldown    time.Duration
	lastFired   time.Time
}

// AlertSeverity represents alert severity levels
type AlertSeverity int

const (
	AlertSeverityInfo AlertSeverity = iota
	AlertSeverityWarning
	AlertSeverityError
	AlertSeverityCritical
)

// NewAlertManager creates a new alert manager
func NewAlertManager(logger *Logger) *AlertManager {
	return &AlertManager{
		rules:  make([]AlertRule, 0),
		logger: logger,
	}
}

// AddRule adds an alerting rule
func (am *AlertManager) AddRule(rule AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules = append(am.rules, rule)
}

// CheckAlerts evaluates all alert rules
func (am *AlertManager) CheckAlerts(data DashboardData) {
	am.mu.Lock()
	defer am.mu.Unlock()
	
	for i, rule := range am.rules {
		// Check cooldown
		if time.Since(rule.lastFired) < rule.Cooldown {
			continue
		}
		
		// Evaluate condition
		if rule.Condition(data) {
			am.fireAlert(rule)
			am.rules[i].lastFired = time.Now()
		}
	}
}

// fireAlert fires an alert
func (am *AlertManager) fireAlert(rule AlertRule) {
	am.logger.WarnWithFields("Alert fired", map[string]interface{}{
		"alert_name":        rule.Name,
		"alert_description": rule.Description,
		"alert_severity":    rule.Severity,
	})
}

// Default alert rules
func GetDefaultAlertRules() []AlertRule {
	return []AlertRule{
		{
			Name:        "HighMemoryUsage",
			Description: "Memory usage is above 80%",
			Condition: func(data DashboardData) bool {
				return data.SystemInfo.MemoryUsage.AllocMB > 100 // Simplified condition
			},
			Severity: AlertSeverityWarning,
			Cooldown: 5 * time.Minute,
		},
		{
			Name:        "ServiceDown",
			Description: "Service health check failed",
			Condition: func(data DashboardData) bool {
				return data.Health.Status == HealthStatusDown
			},
			Severity: AlertSeverityCritical,
			Cooldown: 1 * time.Minute,
		},
		{
			Name:        "HighErrorRate",
			Description: "Error rate is above threshold",
			Condition: func(data DashboardData) bool {
				if failures, ok := data.Metrics["deployments_failure_total"].(float64); ok {
					if total, ok := data.Metrics["deployments_total"].(float64); ok && total > 0 {
						errorRate := failures / total
						return errorRate > 0.1 // 10% error rate
					}
				}
				return false
			},
			Severity: AlertSeverityError,
			Cooldown: 2 * time.Minute,
		},
	}
}