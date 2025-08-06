# FlakeDrop Telemetry Server

A simple telemetry server to receive anonymous usage data from FlakeDrop installations.

## What This Tracks

When users opt-in to telemetry, FlakeDrop sends:
- **Config Creation**: When someone sets up FlakeDrop (shows adoption)
- **Real Deployments**: When actual deployments run (shows real usage vs testing)
- **Success/Failure Rates**: Helps prioritize bug fixes
- **Platform Info**: OS and version distribution

## Privacy First

- All data is anonymous (hashed user IDs)
- No personal information
- No SQL content
- No database schemas
- No credentials

## Running the Server

### Local Development
```bash
go run server.go
```

Visit http://localhost:8080 to see the dashboard.

### Production Deployment

1. **Deploy to a cloud provider** (Heroku, Fly.io, etc.):
```bash
# Example with Fly.io
flyctl deploy
```

2. **Update FlakeDrop to point to your server**:

In `internal/analytics/telemetry.go`, update the endpoint:
```go
endpoint := "https://your-telemetry-server.com/v1/events"
```

## API Endpoints

- `GET /` - Web dashboard
- `GET /stats` - JSON statistics
- `POST /v1/events` - Receive telemetry events

## Example Stats Response

```json
{
  "unique_users": 142,
  "total_deployments": 1823,
  "successful_deploys": 1687,
  "failed_deploys": 136,
  "configs_created": 178,
  "os_breakdown": {
    "darwin": 89,
    "linux": 48,
    "windows": 5
  },
  "version_breakdown": {
    "1.0.0": 45,
    "1.1.0": 97
  }
}
```

## Why This Matters

This data helps you understand:
- **Is anyone using FlakeDrop?** (unique users)
- **Are they just trying it or really using it?** (configs vs deployments)
- **Is it working for them?** (success rates)
- **What platforms to support?** (OS distribution)
- **Are people updating?** (version distribution)

## Minimal Infrastructure

This server is intentionally simple:
- Single Go file
- In-memory storage (restart = data reset)
- No database required
- Can run on free tier of most cloud providers

For production, you might want to:
- Add PostgreSQL for persistence
- Use a real analytics service (Posthog, Mixpanel)
- Add authentication for the dashboard
- Implement data retention policies