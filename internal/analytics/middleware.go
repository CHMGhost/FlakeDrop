package analytics

import (
    "context"
    "time"

    "github.com/spf13/cobra"
)

type contextKey string

const (
    collectorKey contextKey = "analytics_collector"
    startTimeKey contextKey = "command_start_time"
)

func WithCollector(ctx context.Context, collector *Collector) context.Context {
    return context.WithValue(ctx, collectorKey, collector)
}

func GetCollector(ctx context.Context) *Collector {
    if collector, ok := ctx.Value(collectorKey).(*Collector); ok {
        return collector
    }
    return nil
}

func CommandMiddleware(collector *Collector) func(cmd *cobra.Command, args []string) {
    return func(cmd *cobra.Command, args []string) {
        if collector == nil || !collector.IsEnabled() {
            return
        }

        startTime := time.Now()
        ctx := cmd.Context()
        ctx = context.WithValue(ctx, startTimeKey, startTime)
        cmd.SetContext(ctx)

        collector.TrackCommand(cmd.Name(), true, 0)
    }
}

func TrackCommandCompletion(cmd *cobra.Command, success bool) {
    ctx := cmd.Context()
    collector := GetCollector(ctx)
    if collector == nil || !collector.IsEnabled() {
        return
    }

    var duration time.Duration
    if startTime, ok := ctx.Value(startTimeKey).(time.Time); ok {
        duration = time.Since(startTime)
    }

    collector.TrackCommand(cmd.Name(), success, duration)
}

func TrackFeatureUsage(ctx context.Context, feature string, metadata map[string]interface{}) {
    if collector := GetCollector(ctx); collector != nil {
        collector.TrackFeature(feature, metadata)
    }
}

func TrackError(ctx context.Context, err error, context string) {
    if collector := GetCollector(ctx); collector != nil {
        collector.TrackError(err, context)
    }
}

func TrackDeployment(ctx context.Context, success bool, duration time.Duration, metadata map[string]interface{}) {
    if collector := GetCollector(ctx); collector != nil {
        collector.TrackDeployment(success, duration, metadata)
    }
}