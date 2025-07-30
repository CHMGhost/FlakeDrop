package ui

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewProgressBar(t *testing.T) {
	total := 50
	pb := NewProgressBar(total)
	
	if pb.total != total {
		t.Errorf("Expected total to be %d, got %d", total, pb.total)
	}
	
	if pb.current != 0 {
		t.Errorf("Expected current to be 0, got %d", pb.current)
	}
	
	if pb.successCount != 0 {
		t.Errorf("Expected successCount to be 0, got %d", pb.successCount)
	}
	
	if pb.failureCount != 0 {
		t.Errorf("Expected failureCount to be 0, got %d", pb.failureCount)
	}
	
	// Verify start time is set
	if pb.startTime.IsZero() {
		t.Error("Expected startTime to be set")
	}
}

func TestProgressBar_Update(t *testing.T) {
	pb := NewProgressBar(10)
	
	// Capture output
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w
	
	// Test successful update
	pb.Update(5, "test1.sql", true)
	
	if pb.current != 5 {
		t.Errorf("Expected current to be 5, got %d", pb.current)
	}
	
	if pb.currentFile != "test1.sql" {
		t.Errorf("Expected currentFile to be test1.sql, got %s", pb.currentFile)
	}
	
	if pb.successCount != 1 {
		t.Errorf("Expected successCount to be 1, got %d", pb.successCount)
	}
	
	// Test failed update
	pb.Update(6, "test2.sql", false)
	
	if pb.current != 6 {
		t.Errorf("Expected current to be 6, got %d", pb.current)
	}
	
	if pb.failureCount != 1 {
		t.Errorf("Expected failureCount to be 1, got %d", pb.failureCount)
	}
	
	w.Close()
	os.Stdout = oldStdout
}

func TestProgressBar_Finish(t *testing.T) {
	pb := NewProgressBar(10)
	pb.successCount = 8
	pb.failureCount = 2
	
	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	pb.Finish()
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify completion message
	if !strings.Contains(output, "Deployment completed") {
		t.Error("Completion message not found")
	}
	
	if !strings.Contains(output, "8 successful") {
		t.Error("Success count not displayed correctly")
	}
	
	if !strings.Contains(output, "2 failed") {
		t.Error("Failure count not displayed correctly")
	}
}

func TestProgressBar_render(t *testing.T) {
	pb := NewProgressBar(100)
	pb.current = 25
	pb.currentFile = "very_long_file_name_that_should_be_truncated_in_the_display.sql"
	
	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	pb.render()
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify progress bar elements
	if !strings.Contains(output, "25%") {
		t.Error("Percentage not displayed")
	}
	
	if !strings.Contains(output, "[25/100]") {
		t.Error("Progress counter not displayed")
	}
	
	if !strings.Contains(output, "...") {
		t.Error("Long filename not truncated")
	}
	
	if !strings.Contains(output, "█") || !strings.Contains(output, "░") {
		t.Error("Progress bar graphics not displayed")
	}
}

func TestProgressBar_Concurrency(t *testing.T) {
	pb := NewProgressBar(100)
	
	// Test concurrent updates
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pb.Update(index*10, "file.sql", index%2 == 0)
		}(i)
	}
	
	wg.Wait()
	
	// Verify counts are consistent
	totalProcessed := pb.successCount + pb.failureCount
	if totalProcessed != 10 {
		t.Errorf("Expected 10 total processed, got %d", totalProcessed)
	}
}

func TestNewSpinner(t *testing.T) {
	message := "Processing..."
	spinner := NewSpinner(message)
	
	if spinner.message != message {
		t.Errorf("Expected message to be '%s', got '%s'", message, spinner.message)
	}
	
	if len(spinner.frames) == 0 {
		t.Error("Spinner frames not initialized")
	}
	
	if spinner.current != 0 {
		t.Errorf("Expected current frame to be 0, got %d", spinner.current)
	}
	
	if spinner.stopped {
		t.Error("Spinner should not be stopped initially")
	}
}

func TestSpinner_StartStop(t *testing.T) {
	spinner := NewSpinner("Testing spinner")
	
	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	spinner.Start()
	
	// Let it spin for a bit
	time.Sleep(250 * time.Millisecond)
	
	// Stop with success
	spinner.Stop(true, "Operation completed")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 2048)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify spinner was active
	if !strings.Contains(output, "Testing spinner") {
		t.Error("Spinner message not displayed")
	}
	
	// Verify completion message
	if !strings.Contains(output, "Operation completed") {
		t.Error("Completion message not displayed")
	}
	
	if !strings.Contains(output, "✓") {
		t.Error("Success checkmark not displayed")
	}
}

func TestSpinner_UpdateMessage(t *testing.T) {
	spinner := NewSpinner("Initial message")
	
	// Start spinner
	spinner.Start()
	defer spinner.Stop(true, "Done")
	
	// Update message
	newMessage := "Updated message"
	spinner.UpdateMessage(newMessage)
	
	// Give it a moment to update
	time.Sleep(150 * time.Millisecond)
	
	spinner.mu.Lock()
	if spinner.message != newMessage {
		t.Errorf("Expected message to be '%s', got '%s'", newMessage, spinner.message)
	}
	spinner.mu.Unlock()
}

func TestSpinner_StopWithError(t *testing.T) {
	spinner := NewSpinner("Processing")
	
	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	spinner.Start()
	time.Sleep(100 * time.Millisecond)
	
	// Stop with error
	spinner.Stop(false, "Operation failed")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify error message
	if !strings.Contains(output, "Operation failed") {
		t.Error("Error message not displayed")
	}
	
	if !strings.Contains(output, "✗") {
		t.Error("Error symbol not displayed")
	}
}

func TestSpinner_Concurrency(t *testing.T) {
	spinner := NewSpinner("Concurrent test")
	
	spinner.Start()
	
	// Test concurrent message updates
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			spinner.UpdateMessage(string(rune('A' + index)))
			time.Sleep(50 * time.Millisecond)
		}(i)
	}
	
	wg.Wait()
	spinner.Stop(true, "Concurrent test completed")
	
	// Verify spinner stopped correctly
	if !spinner.stopped {
		t.Error("Spinner should be stopped")
	}
}

// BenchmarkProgressBar benchmarks progress bar update performance
func BenchmarkProgressBar(b *testing.B) {
	pb := NewProgressBar(b.N)
	
	// Disable output for benchmark
	oldStdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	defer func() {
		os.Stdout = oldStdout
	}()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		pb.Update(i, "file.sql", true)
	}
}

// BenchmarkSpinner benchmarks spinner message update performance
func BenchmarkSpinner(b *testing.B) {
	spinner := NewSpinner("Benchmark")
	
	// Disable output for benchmark
	oldStdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	defer func() {
		os.Stdout = oldStdout
	}()
	
	spinner.Start()
	defer spinner.Stop(true, "Done")
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		spinner.UpdateMessage("Message " + string(rune(i%26+'A')))
	}
}