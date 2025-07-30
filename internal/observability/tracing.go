package observability

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// TraceID represents a unique trace identifier
type TraceID string

// SpanID represents a unique span identifier within a trace
type SpanID string

// SpanKind represents the kind of span
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// SpanStatus represents the status of a span
type SpanStatus int

const (
	SpanStatusUnset SpanStatus = iota
	SpanStatusOK
	SpanStatusError
)

// Span represents a distributed trace span
type Span struct {
	TraceID     TraceID               `json:"trace_id"`
	SpanID      SpanID                `json:"span_id"`
	ParentID    SpanID                `json:"parent_id,omitempty"`
	OperationName string              `json:"operation_name"`
	StartTime   time.Time             `json:"start_time"`
	EndTime     time.Time             `json:"end_time,omitempty"`
	Duration    time.Duration         `json:"duration"`
	Tags        map[string]interface{} `json:"tags,omitempty"`
	Logs        []LogRecord           `json:"logs,omitempty"`
	Status      SpanStatus            `json:"status"`
	Kind        SpanKind              `json:"kind"`
	Component   string                `json:"component,omitempty"`
	mu          sync.RWMutex
	finished    bool
	tracer      *Tracer
}

// LogRecord represents a structured log within a span
type LogRecord struct {
	Timestamp time.Time              `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields"`
}

// SetTag sets a tag on the span
func (s *Span) SetTag(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.Tags == nil {
		s.Tags = make(map[string]interface{})
	}
	s.Tags[key] = value
}

// SetStatus sets the span status
func (s *Span) SetStatus(status SpanStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
}

// LogFields logs structured data to the span
func (s *Span) LogFields(fields map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	log := LogRecord{
		Timestamp: time.Now(),
		Fields:    fields,
	}
	s.Logs = append(s.Logs, log)
}

// LogEvent logs an event to the span
func (s *Span) LogEvent(event string) {
	s.LogFields(map[string]interface{}{
		"event": event,
	})
}

// LogError logs an error to the span
func (s *Span) LogError(err error) {
	s.SetStatus(SpanStatusError)
	s.LogFields(map[string]interface{}{
		"error":        true,
		"error.object": err,
		"message":      err.Error(),
	})
}

// Finish finishes the span
func (s *Span) Finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.finished {
		return
	}
	
	s.EndTime = time.Now()
	s.Duration = s.EndTime.Sub(s.StartTime)
	s.finished = true
	
	if s.tracer != nil {
		s.tracer.finishSpan(s)
	}
}

// IsFinished returns whether the span is finished
func (s *Span) IsFinished() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.finished
}

// Context creates a context with this span
func (s *Span) Context() context.Context {
	return WithSpan(context.Background(), s)
}

// Tracer manages distributed tracing
type Tracer struct {
	serviceName string
	spans       map[SpanID]*Span
	mu          sync.RWMutex
	exporter    SpanExporter
	sampler     Sampler
}

// SpanExporter exports spans to external systems
type SpanExporter interface {
	ExportSpans(spans []*Span) error
}

// Sampler determines whether a trace should be sampled
type Sampler interface {
	ShouldSample(traceID TraceID, parentSpanID SpanID, operationName string) bool
}

// AlwaysSample is a sampler that samples all traces
type AlwaysSample struct{}

func (AlwaysSample) ShouldSample(traceID TraceID, parentSpanID SpanID, operationName string) bool {
	return true
}

// ProbabilitySampler samples traces based on probability
type ProbabilitySampler struct {
	probability float64
}

// NewProbabilitySampler creates a new probability sampler
func NewProbabilitySampler(probability float64) *ProbabilitySampler {
	return &ProbabilitySampler{probability: probability}
}

func (p *ProbabilitySampler) ShouldSample(traceID TraceID, parentSpanID SpanID, operationName string) bool {
	if p.probability <= 0.0 {
		return false
	}
	if p.probability >= 1.0 {
		return true
	}
	
	// Simple hash-based sampling based on trace ID
	hash := uint32(0)
	for _, b := range []byte(traceID) {
		hash = hash*31 + uint32(b)
	}
	// Use modulo to get a value between 0 and 99, then compare as percentage
	return float64(hash%100) < p.probability*100
}

// NewTracer creates a new tracer
func NewTracer(serviceName string, exporter SpanExporter, sampler Sampler) *Tracer {
	if sampler == nil {
		sampler = AlwaysSample{}
	}
	
	return &Tracer{
		serviceName: serviceName,
		spans:       make(map[SpanID]*Span),
		exporter:    exporter,
		sampler:     sampler,
	}
}

// StartSpan starts a new span
func (t *Tracer) StartSpan(operationName string, options ...SpanOption) *Span {
	span := &Span{
		TraceID:       generateTraceID(),
		SpanID:        generateSpanID(),
		OperationName: operationName,
		StartTime:     time.Now(),
		Tags:          make(map[string]interface{}),
		Logs:          make([]LogRecord, 0),
		Status:        SpanStatusUnset,
		Kind:          SpanKindInternal,
		tracer:        t,
	}
	
	// Apply options
	for _, option := range options {
		option(span)
	}
	
	// Check if we should sample this trace
	if !t.sampler.ShouldSample(span.TraceID, span.ParentID, operationName) {
		return &Span{} // Return no-op span
	}
	
	// Set service name
	span.SetTag("service.name", t.serviceName)
	
	// Store span
	t.mu.Lock()
	t.spans[span.SpanID] = span
	t.mu.Unlock()
	
	return span
}

// StartSpanFromContext starts a new span from context
func (t *Tracer) StartSpanFromContext(ctx context.Context, operationName string, options ...SpanOption) (*Span, context.Context) {
	parentSpan := SpanFromContext(ctx)
	if parentSpan != nil {
		options = append(options, ChildOf(parentSpan))
	}
	
	span := t.StartSpan(operationName, options...)
	newCtx := WithSpan(ctx, span)
	
	return span, newCtx
}

// finishSpan is called when a span is finished
func (t *Tracer) finishSpan(span *Span) {
	t.mu.Lock()
	delete(t.spans, span.SpanID)
	t.mu.Unlock()
	
	// Export span if exporter is configured
	if t.exporter != nil {
		_ = t.exporter.ExportSpans([]*Span{span})
	}
}

// GetActiveSpans returns all active spans
func (t *Tracer) GetActiveSpans() []*Span {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	spans := make([]*Span, 0, len(t.spans))
	for _, span := range t.spans {
		spans = append(spans, span)
	}
	
	return spans
}

// SpanOption configures a span
type SpanOption func(*Span)

// ChildOf sets the parent span
func ChildOf(parent *Span) SpanOption {
	return func(span *Span) {
		if parent != nil {
			span.TraceID = parent.TraceID
			span.ParentID = parent.SpanID
		}
	}
}

// WithTag sets a tag on the span
func WithTag(key string, value interface{}) SpanOption {
	return func(span *Span) {
		span.SetTag(key, value)
	}
}

// WithSpanKind sets the span kind
func WithSpanKind(kind SpanKind) SpanOption {
	return func(span *Span) {
		span.Kind = kind
	}
}

// WithComponent sets the component name
func WithComponent(component string) SpanOption {
	return func(span *Span) {
		span.Component = component
	}
}

// Context keys for storing tracing information
type contextKey int

const (
	spanContextKey contextKey = iota
	traceIDContextKey
	spanIDContextKey
)

// WithSpan adds a span to the context
func WithSpan(ctx context.Context, span *Span) context.Context {
	ctx = context.WithValue(ctx, spanContextKey, span)
	ctx = context.WithValue(ctx, traceIDContextKey, span.TraceID)
	ctx = context.WithValue(ctx, spanIDContextKey, span.SpanID)
	return ctx
}

// SpanFromContext extracts a span from the context
func SpanFromContext(ctx context.Context) *Span {
	span, _ := ctx.Value(spanContextKey).(*Span)
	return span
}

// GetTraceID extracts the trace ID from context
func GetTraceID(ctx context.Context) string {
	traceID, _ := ctx.Value(traceIDContextKey).(TraceID)
	return string(traceID)
}

// GetSpanID extracts the span ID from context
func GetSpanID(ctx context.Context) string {
	spanID, _ := ctx.Value(spanIDContextKey).(SpanID)
	return string(spanID)
}

// generateTraceID generates a new trace ID
func generateTraceID() TraceID {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return TraceID(fmt.Sprintf("%x", b))
}

// generateSpanID generates a new span ID
func generateSpanID() SpanID {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return SpanID(fmt.Sprintf("%x", b))
}

// LoggingSpanExporter exports spans to logs
type LoggingSpanExporter struct {
	logger *Logger
}

// NewLoggingSpanExporter creates a new logging span exporter
func NewLoggingSpanExporter(logger *Logger) *LoggingSpanExporter {
	return &LoggingSpanExporter{logger: logger}
}

// ExportSpans exports spans to logs
func (e *LoggingSpanExporter) ExportSpans(spans []*Span) error {
	for _, span := range spans {
		fields := map[string]interface{}{
			"trace_id":       span.TraceID,
			"span_id":        span.SpanID,
			"parent_id":      span.ParentID,
			"operation_name": span.OperationName,
			"start_time":     span.StartTime,
			"end_time":       span.EndTime,
			"duration_ms":    span.Duration.Milliseconds(),
			"status":         span.Status,
			"kind":           span.Kind,
		}
		
		// Add tags
		for k, v := range span.Tags {
			fields[k] = v
		}
		
		e.logger.InfoWithFields("Span completed", fields)
	}
	
	return nil
}

// Global tracer instance
var defaultTracer *Tracer

// InitTracing initializes the global tracer
func InitTracing(serviceName string, exporter SpanExporter, sampler Sampler) {
	defaultTracer = NewTracer(serviceName, exporter, sampler)
}

// GetTracer returns the global tracer instance
func GetTracer() *Tracer {
	if defaultTracer == nil {
		// Initialize with default settings
		defaultTracer = NewTracer("flakedrop", nil, AlwaysSample{})
	}
	return defaultTracer
}

// Package-level convenience functions

// StartSpan starts a new span using the global tracer
func StartSpan(operationName string, options ...SpanOption) *Span {
	return GetTracer().StartSpan(operationName, options...)
}

// StartSpanFromContext starts a new span from context using the global tracer
func StartSpanFromContext(ctx context.Context, operationName string, options ...SpanOption) (*Span, context.Context) {
	return GetTracer().StartSpanFromContext(ctx, operationName, options...)
}

// TraceFunction traces a function execution
func TraceFunction(ctx context.Context, operationName string, fn func(context.Context) error) error {
	span, ctx := StartSpanFromContext(ctx, operationName)
	defer span.Finish()
	
	err := fn(ctx)
	if err != nil {
		span.LogError(err)
	}
	
	return err
}

// TraceFunctionWithResult traces a function execution with result
func TraceFunctionWithResult[T any](ctx context.Context, operationName string, fn func(context.Context) (T, error)) (T, error) {
	span, ctx := StartSpanFromContext(ctx, operationName)
	defer span.Finish()
	
	result, err := fn(ctx)
	if err != nil {
		span.LogError(err)
	}
	
	return result, err
}