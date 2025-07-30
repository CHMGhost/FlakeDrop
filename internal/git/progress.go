package git

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// ProgressWriter provides progress feedback for Git operations
type ProgressWriter struct {
	operation   string
	lastUpdate  time.Time
	mu          sync.Mutex
	currentBytes int64
	lastLine    string
}

// NewProgressWriter creates a new progress writer
func NewProgressWriter(operation string) *ProgressWriter {
	pw := &ProgressWriter{
		operation:  operation,
		lastUpdate: time.Now(),
	}
	
	// Print initial message
	fmt.Printf("%s...\n", operation)
	
	return pw
}

// Write implements io.Writer interface
func (pw *ProgressWriter) Write(p []byte) (n int, err error) {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	n = len(p)
	pw.currentBytes += int64(n)

	// Parse git progress output
	lines := strings.Split(string(p), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Only update if enough time has passed or content is different
		now := time.Now()
		if now.Sub(pw.lastUpdate) > 100*time.Millisecond || line != pw.lastLine {
			pw.printProgress(line)
			pw.lastUpdate = now
			pw.lastLine = line
		}
	}

	return n, nil
}

// printProgress prints formatted progress
func (pw *ProgressWriter) printProgress(line string) {
	// Clear previous line and print new one
	fmt.Printf("\r%-80s", "") // Clear line
	
	// Parse different types of git progress
	if strings.Contains(line, "Counting objects:") {
		fmt.Printf("\r  📊 %s", line)
	} else if strings.Contains(line, "Compressing objects:") {
		fmt.Printf("\r  🗜️  %s", line)
	} else if strings.Contains(line, "Receiving objects:") {
		fmt.Printf("\r  📥 %s", line)
	} else if strings.Contains(line, "Resolving deltas:") {
		fmt.Printf("\r  🔄 %s", line)
	} else if strings.Contains(line, "Checking out files:") {
		fmt.Printf("\r  📁 %s", line)
	} else if strings.Contains(line, "remote:") {
		// Remote messages
		fmt.Printf("\r  🌐 %s", strings.TrimPrefix(line, "remote:"))
	} else if strings.Contains(line, "Writing objects:") {
		fmt.Printf("\r  📤 %s", line)
	} else if strings.Contains(line, "Total") {
		// Summary line
		fmt.Printf("\r  ✅ %s", line)
	} else {
		// Default
		fmt.Printf("\r  ⏳ %s", line)
	}
}

// Complete marks the operation as complete
func (pw *ProgressWriter) Complete() {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	fmt.Printf("\r%-80s", "") // Clear line
	fmt.Printf("\r✅ %s completed\n", pw.operation)
}

// MultiOperationProgress tracks progress across multiple operations
type MultiOperationProgress struct {
	operations map[string]*OperationStatus
	mu         sync.RWMutex
	writer     io.Writer
}

// OperationStatus represents the status of a single operation
type OperationStatus struct {
	Name        string
	Status      string
	Progress    float64
	StartTime   time.Time
	EndTime     *time.Time
	Error       error
}

// NewMultiOperationProgress creates a new multi-operation progress tracker
func NewMultiOperationProgress(writer io.Writer) *MultiOperationProgress {
	if writer == nil {
		writer = os.Stdout
	}
	
	return &MultiOperationProgress{
		operations: make(map[string]*OperationStatus),
		writer:     writer,
	}
}

// StartOperation starts tracking a new operation
func (mop *MultiOperationProgress) StartOperation(name string) {
	mop.mu.Lock()
	defer mop.mu.Unlock()

	mop.operations[name] = &OperationStatus{
		Name:      name,
		Status:    "Running",
		Progress:  0,
		StartTime: time.Now(),
	}

	mop.render()
}

// UpdateOperation updates the progress of an operation
func (mop *MultiOperationProgress) UpdateOperation(name string, progress float64, status string) {
	mop.mu.Lock()
	defer mop.mu.Unlock()

	if op, exists := mop.operations[name]; exists {
		op.Progress = progress
		op.Status = status
		mop.render()
	}
}

// CompleteOperation marks an operation as complete
func (mop *MultiOperationProgress) CompleteOperation(name string, err error) {
	mop.mu.Lock()
	defer mop.mu.Unlock()

	if op, exists := mop.operations[name]; exists {
		now := time.Now()
		op.EndTime = &now
		op.Progress = 100
		
		if err != nil {
			op.Status = "Failed"
			op.Error = err
		} else {
			op.Status = "Completed"
		}
		
		mop.render()
	}
}

// render displays the current progress
func (mop *MultiOperationProgress) render() {
	// Clear screen (simplified - in real implementation would handle terminals better)
	fmt.Fprint(mop.writer, "\033[2J\033[H")
	
	fmt.Fprintln(mop.writer, "Git Operations Progress")
	fmt.Fprintln(mop.writer, strings.Repeat("=", 80))
	
	for _, op := range mop.operations {
		mop.renderOperation(op)
	}
}

// renderOperation renders a single operation
func (mop *MultiOperationProgress) renderOperation(op *OperationStatus) {
	// Status icon
	icon := "⏳"
	switch op.Status {
	case "Completed":
		icon = "✅"
	case "Failed":
		icon = "❌"
	case "Running":
		icon = "🔄"
	}
	
	// Progress bar
	barWidth := 30
	filled := int(float64(barWidth) * op.Progress / 100)
	empty := barWidth - filled
	progressBar := fmt.Sprintf("[%s%s]", strings.Repeat("█", filled), strings.Repeat("░", empty))
	
	// Duration
	duration := ""
	if op.EndTime != nil {
		duration = fmt.Sprintf(" (%.1fs)", op.EndTime.Sub(op.StartTime).Seconds())
	} else {
		duration = fmt.Sprintf(" (%.1fs)", time.Since(op.StartTime).Seconds())
	}
	
	// Print operation line
	fmt.Fprintf(mop.writer, "%s %-30s %s %3.0f%% %s%s\n", 
		icon, op.Name, progressBar, op.Progress, op.Status, duration)
	
	// Print error if any
	if op.Error != nil {
		fmt.Fprintf(mop.writer, "   └─ Error: %v\n", op.Error)
	}
}

// GitOperationTracker provides detailed tracking for Git operations
type GitOperationTracker struct {
	operation   string
	steps       []Step
	currentStep int
	mu          sync.Mutex
	startTime   time.Time
}

// Step represents a single step in an operation
type Step struct {
	Name      string
	Status    StepStatus
	StartTime time.Time
	EndTime   *time.Time
	Error     error
}

// StepStatus represents the status of a step
type StepStatus int

const (
	StepPending StepStatus = iota
	StepRunning
	StepCompleted
	StepFailed
	StepSkipped
)

// NewGitOperationTracker creates a new operation tracker
func NewGitOperationTracker(operation string, stepNames []string) *GitOperationTracker {
	steps := make([]Step, len(stepNames))
	for i, name := range stepNames {
		steps[i] = Step{
			Name:   name,
			Status: StepPending,
		}
	}

	tracker := &GitOperationTracker{
		operation:   operation,
		steps:       steps,
		currentStep: -1,
		startTime:   time.Now(),
	}

	tracker.display()
	return tracker
}

// StartStep starts a new step
func (got *GitOperationTracker) StartStep(index int) {
	got.mu.Lock()
	defer got.mu.Unlock()

	if index < 0 || index >= len(got.steps) {
		return
	}

	got.currentStep = index
	got.steps[index].Status = StepRunning
	got.steps[index].StartTime = time.Now()
	
	got.display()
}

// CompleteStep completes a step
func (got *GitOperationTracker) CompleteStep(index int, err error) {
	got.mu.Lock()
	defer got.mu.Unlock()

	if index < 0 || index >= len(got.steps) {
		return
	}

	now := time.Now()
	got.steps[index].EndTime = &now
	
	if err != nil {
		got.steps[index].Status = StepFailed
		got.steps[index].Error = err
	} else {
		got.steps[index].Status = StepCompleted
	}
	
	got.display()
}

// SkipStep marks a step as skipped
func (got *GitOperationTracker) SkipStep(index int) {
	got.mu.Lock()
	defer got.mu.Unlock()

	if index < 0 || index >= len(got.steps) {
		return
	}

	got.steps[index].Status = StepSkipped
	got.display()
}

// display shows the current progress
func (got *GitOperationTracker) display() {
	// Clear previous output
	fmt.Print("\033[2K\r")
	
	// Print header
	fmt.Printf("\n%s Progress:\n", got.operation)
	fmt.Println(strings.Repeat("-", 60))
	
	// Print each step
	for i, step := range got.steps {
		icon := got.getStepIcon(step.Status)
		timing := ""
		
		if step.StartTime.IsZero() {
			timing = ""
		} else if step.EndTime != nil {
			timing = fmt.Sprintf(" (%.1fs)", step.EndTime.Sub(step.StartTime).Seconds())
		} else if step.Status == StepRunning {
			timing = fmt.Sprintf(" (%.1fs...)", time.Since(step.StartTime).Seconds())
		}
		
		fmt.Printf("  %s %s%s\n", icon, step.Name, timing)
		
		if step.Error != nil {
			fmt.Printf("     └─ Error: %v\n", step.Error)
		}
		
		// Show spinner for current step
		if i == got.currentStep && step.Status == StepRunning {
			fmt.Printf("     └─ %s\n", got.getSpinner())
		}
	}
	
	// Print total time
	fmt.Printf("\nTotal time: %.1fs\n", time.Since(got.startTime).Seconds())
}

// getStepIcon returns an icon for a step status
func (got *GitOperationTracker) getStepIcon(status StepStatus) string {
	switch status {
	case StepPending:
		return "⏸️ "
	case StepRunning:
		return "🔄"
	case StepCompleted:
		return "✅"
	case StepFailed:
		return "❌"
	case StepSkipped:
		return "⏭️ "
	default:
		return "❓"
	}
}

// getSpinner returns a spinner character
func (got *GitOperationTracker) getSpinner() string {
	spinners := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	index := int(time.Now().UnixNano()/100000000) % len(spinners)
	return spinners[index] + " Working..."
}

// Complete marks the entire operation as complete
func (got *GitOperationTracker) Complete() {
	got.mu.Lock()
	defer got.mu.Unlock()

	// Mark any pending steps as skipped
	for i := range got.steps {
		if got.steps[i].Status == StepPending {
			got.steps[i].Status = StepSkipped
		}
	}

	got.display()
	
	// Print summary
	completed := 0
	failed := 0
	skipped := 0
	
	for _, step := range got.steps {
		switch step.Status {
		case StepCompleted:
			completed++
		case StepFailed:
			failed++
		case StepSkipped:
			skipped++
		}
	}
	
	fmt.Printf("\nSummary: %d completed, %d failed, %d skipped\n", 
		completed, failed, skipped)
}