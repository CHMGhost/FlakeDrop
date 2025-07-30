package ui

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ProgressBar represents a progress indicator for deployments
type ProgressBar struct {
	total     int
	current   int
	startTime time.Time
	mu        sync.Mutex
	
	successCount int
	failureCount int
	currentFile  string
}

// NewProgressBar creates a new progress bar
func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		total:     total,
		current:   0,
		startTime: time.Now(),
	}
}

// Update updates the progress bar with current status
func (p *ProgressBar) Update(current int, file string, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.current = current
	p.currentFile = file
	
	if success {
		p.successCount++
	} else {
		p.failureCount++
	}
	
	p.render()
}

// Finish completes the progress bar
func (p *ProgressBar) Finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	elapsed := time.Since(p.startTime)
	
	fmt.Printf("\n\n%s Deployment completed in %s\n",
		ColorSuccess("✓"),
		formatDuration(elapsed),
	)
	fmt.Printf("  %s %d successful\n", ColorSuccess("✓"), p.successCount)
	if p.failureCount > 0 {
		fmt.Printf("  %s %d failed\n", ColorError("✗"), p.failureCount)
	}
}

func (p *ProgressBar) render() {
	// Clear line
	fmt.Print("\r\033[K")
	
	// Calculate percentage
	percentage := float64(p.current) / float64(p.total) * 100
	
	// Create progress bar
	barWidth := 30
	filled := int(percentage / 100 * float64(barWidth))
	
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	
	// Format current file (truncate if needed)
	file := p.currentFile
	if len(file) > 40 {
		file = "..." + file[len(file)-37:]
	}
	
	// Elapsed time
	elapsed := time.Since(p.startTime)
	
	// Print progress
	fmt.Printf("%s %s %.0f%% [%d/%d] %s - %s",
		ColorProgress("►"),
		bar,
		percentage,
		p.current,
		p.total,
		file,
		formatDuration(elapsed),
	)
}

// Spinner represents an animated spinner for long operations
type Spinner struct {
	frames  []string
	current int
	message string
	stop    chan bool
	stopped bool
	mu      sync.Mutex
}

// NewSpinner creates a new spinner
func NewSpinner(message string) *Spinner {
	return &Spinner{
		frames:  []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		current: 0,
		message: message,
		stop:    make(chan bool),
	}
}

// Start begins the spinner animation
func (s *Spinner) Start() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.mu.Lock()
				if !s.stopped {
					fmt.Printf("\r%s %s %s",
						ColorProgress(s.frames[s.current]),
						s.message,
						strings.Repeat(" ", 20), // Clear extra characters
					)
					s.current = (s.current + 1) % len(s.frames)
				}
				s.mu.Unlock()
			}
		}
	}()
}

// Stop stops the spinner
func (s *Spinner) Stop(success bool, message string) {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	
	close(s.stop)
	
	// Clear line and print final status
	fmt.Print("\r\033[K")
	
	if success {
		fmt.Printf("%s %s\n", ColorSuccess("✓"), message)
	} else {
		fmt.Printf("%s %s\n", ColorError("✗"), message)
	}
}

// UpdateMessage updates the spinner message
func (s *Spinner) UpdateMessage(message string) {
	s.mu.Lock()
	s.message = message
	s.mu.Unlock()
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", hours, minutes)
}