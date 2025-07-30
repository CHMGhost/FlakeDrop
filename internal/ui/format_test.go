package ui

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/mattn/go-isatty"
)

func TestColorFunc(t *testing.T) {
	// Save original state
	originalSupportsColor := supportsColor
	defer func() {
		supportsColor = originalSupportsColor
	}()
	
	tests := []struct {
		name          string
		supportsColor bool
		input         string
		expectColored bool
	}{
		{
			name:          "with color support",
			supportsColor: true,
			input:         "test text",
			expectColored: true,
		},
		{
			name:          "without color support",
			supportsColor: false,
			input:         "test text",
			expectColored: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			supportsColor = tt.supportsColor
			
			// Test various color functions
			funcs := []func(string) string{
				ColorSuccess,
				ColorError,
				ColorWarning,
				ColorInfo,
				ColorProgress,
				ColorBold,
				ColorDim,
			}
			
			for _, colorFunc := range funcs {
				result := colorFunc(tt.input)
				
				if tt.expectColored && result == tt.input {
					t.Error("Expected colored output, got plain text")
				}
				
				if !tt.expectColored && result != tt.input {
					t.Error("Expected plain text, got colored output")
				}
			}
		})
	}
}

func TestShowHeader(t *testing.T) {
	// Capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	ShowHeader("Test Title")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify header structure
	if !strings.Contains(output, "┌") || !strings.Contains(output, "┐") {
		t.Error("Header missing top border")
	}
	
	if !strings.Contains(output, "└") || !strings.Contains(output, "┘") {
		t.Error("Header missing bottom border")
	}
	
	if !strings.Contains(output, "Test Title") {
		t.Error("Header missing title")
	}
}

func TestShowError(t *testing.T) {
	tests := []struct {
		name              string
		err               error
		expectSuggestion  bool
		suggestionKeyword string
	}{
		{
			name:              "authentication error",
			err:               errors.New("authentication failed: invalid credentials"),
			expectSuggestion:  true,
			suggestionKeyword: "username and password",
		},
		{
			name:              "connection error",
			err:               errors.New("connection refused"),
			expectSuggestion:  true,
			suggestionKeyword: "network connectivity",
		},
		{
			name:              "syntax error",
			err:               errors.New("SQL syntax error at line 5"),
			expectSuggestion:  true,
			suggestionKeyword: "SQL syntax",
		},
		{
			name:              "permission error",
			err:               errors.New("permission denied for table USERS"),
			expectSuggestion:  true,
			suggestionKeyword: "privileges",
		},
		{
			name:              "object not found",
			err:               errors.New("object does not exist: TABLE1"),
			expectSuggestion:  true,
			suggestionKeyword: "database objects",
		},
		{
			name:             "generic error",
			err:              errors.New("unknown error occurred"),
			expectSuggestion: false,
		},
		{
			name:              "multiline error",
			err:               errors.New("error occurred\ndetailed message\nadditional info"),
			expectSuggestion:  false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture output
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			
			ShowError(tt.err)
			
			w.Close()
			os.Stdout = oldStdout
			
			buf := make([]byte, 1024)
			n, _ := r.Read(buf)
			output := string(buf[:n])
			
			// Verify error is displayed
			// For multiline errors, only check the first line
			errorLines := strings.Split(tt.err.Error(), "\n")
			if !strings.Contains(output, errorLines[0]) {
				t.Errorf("Error message not found in output. Expected: %s, Got: %s", errorLines[0], output)
			}
			
			// Verify suggestion
			if tt.expectSuggestion && !strings.Contains(output, tt.suggestionKeyword) {
				t.Errorf("Expected suggestion containing '%s' not found", tt.suggestionKeyword)
			}
		})
	}
}

func TestShowSuccess(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	ShowSuccess("Operation completed successfully")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	if !strings.Contains(output, "✓") {
		t.Error("Success checkmark not found")
	}
	
	if !strings.Contains(output, "Operation completed successfully") {
		t.Error("Success message not found")
	}
}

func TestShowWarning(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	ShowWarning("This is a warning")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	if !strings.Contains(output, "!") {
		t.Error("Warning symbol not found")
	}
	
	if !strings.Contains(output, "This is a warning") {
		t.Error("Warning message not found")
	}
}

func TestShowInfo(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	ShowInfo("Information message")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	if !strings.Contains(output, "i") {
		t.Error("Info symbol not found")
	}
	
	if !strings.Contains(output, "Information message") {
		t.Error("Info message not found")
	}
}

func TestTable(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	table := NewTable()
	table.AddHeader("Name", "Status", "Count")
	table.AddRow("Item1", "Active", "10")
	table.AddRow("Item2", "Inactive", "5")
	table.Render()
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify table structure
	if !strings.Contains(output, "Name") || !strings.Contains(output, "Status") || !strings.Contains(output, "Count") {
		t.Error("Table headers not found")
	}
	
	if !strings.Contains(output, "Item1") || !strings.Contains(output, "Active") || !strings.Contains(output, "10") {
		t.Error("Table row 1 not found")
	}
	
	if !strings.Contains(output, "Item2") || !strings.Contains(output, "Inactive") || !strings.Contains(output, "5") {
		t.Error("Table row 2 not found")
	}
}

func TestBox(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	Box("Test Box", "Line 1\nLine 2\nLine 3")
	
	w.Close()
	os.Stdout = oldStdout
	
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	
	// Verify box structure
	if !strings.Contains(output, "┌") || !strings.Contains(output, "┐") {
		t.Error("Box missing top corners")
	}
	
	if !strings.Contains(output, "└") || !strings.Contains(output, "┘") {
		t.Error("Box missing bottom corners")
	}
	
	if !strings.Contains(output, "Test Box") {
		t.Error("Box title not found")
	}
	
	if !strings.Contains(output, "Line 1") || !strings.Contains(output, "Line 2") || !strings.Contains(output, "Line 3") {
		t.Error("Box content not found")
	}
}

func TestGetSuggestion(t *testing.T) {
	tests := []struct {
		name       string
		error      string
		suggestion string
	}{
		{
			name:       "authentication error",
			error:      "Authentication failed: invalid password",
			suggestion: "Check your username and password in the configuration",
		},
		{
			name:       "connection error",
			error:      "Connection refused to snowflake.com",
			suggestion: "Verify your Snowflake account URL and network connectivity",
		},
		{
			name:       "syntax error",
			error:      "SQL syntax error near 'SELCT'",
			suggestion: "Review the SQL syntax in the affected file",
		},
		{
			name:       "permission error",
			error:      "Permission denied: insufficient privileges",
			suggestion: "Ensure your role has the necessary privileges",
		},
		{
			name:       "object not found",
			error:      "Object does not exist: USERS_TABLE",
			suggestion: "Verify the database objects exist or check your database/schema context",
		},
		{
			name:       "unknown error",
			error:      "Some random error",
			suggestion: "",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSuggestion(tt.error)
			if result != tt.suggestion {
				t.Errorf("getSuggestion() = %v, want %v", result, tt.suggestion)
			}
		})
	}
}

func TestConfirm(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		defaultValue bool
		expected     bool
	}{
		{
			name:         "empty input with default true",
			input:        "\n",
			defaultValue: true,
			expected:     true,
		},
		{
			name:         "empty input with default false",
			input:        "\n",
			defaultValue: false,
			expected:     false,
		},
		{
			name:         "yes input",
			input:        "yes\n",
			defaultValue: false,
			expected:     true,
		},
		{
			name:         "y input",
			input:        "y\n",
			defaultValue: false,
			expected:     true,
		},
		{
			name:         "no input",
			input:        "n\n",
			defaultValue: true,
			expected:     false,
		},
		{
			name:         "invalid input",
			input:        "maybe\n",
			defaultValue: true,
			expected:     false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock stdin
			oldStdin := os.Stdin
			r, w, _ := os.Pipe()
			os.Stdin = r
			
			go func() {
				_, _ = w.Write([]byte(tt.input))
				w.Close()
			}()
			
			result, err := Confirm("Continue?", tt.defaultValue)
			
			os.Stdin = oldStdin
			
			if err != nil && err.Error() != "unexpected newline" {
				t.Errorf("Unexpected error: %v", err)
			}
			
			if result != tt.expected {
				t.Errorf("Confirm() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// BenchmarkColorFunc benchmarks the color function performance
func BenchmarkColorFunc(b *testing.B) {
	text := "Sample text for coloring"
	
	b.Run("with color", func(b *testing.B) {
		supportsColor = true
		for i := 0; i < b.N; i++ {
			_ = ColorSuccess(text)
		}
	})
	
	b.Run("without color", func(b *testing.B) {
		supportsColor = false
		for i := 0; i < b.N; i++ {
			_ = ColorSuccess(text)
		}
	})
}

// TestColorDetection tests the terminal color detection
func TestColorDetection(t *testing.T) {
	// The actual value depends on the test environment
	// Just ensure it's set
	if !isatty.IsTerminal(os.Stdout.Fd()) && !isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		if supportsColor {
			t.Error("Color support should be false in non-terminal environment")
		}
	}
}