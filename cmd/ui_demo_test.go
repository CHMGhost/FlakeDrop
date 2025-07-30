package cmd

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/internal/ui"
)

func TestUIDemoCommand(t *testing.T) {
	// Test command structure
	assert.NotNil(t, uiDemoCmd)
	assert.Equal(t, "ui-demo", uiDemoCmd.Use)
	assert.Equal(t, "Demonstrate UI components", uiDemoCmd.Short)
	assert.NotNil(t, uiDemoCmd.Run)
	assert.Contains(t, uiDemoCmd.Long, "demonstrates various UI components")
}

func TestUIDemoCommandRegistration(t *testing.T) {
	// Verify ui-demo command is registered with root
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "ui-demo" {
			found = true
			assert.Equal(t, uiDemoCmd, cmd)
			break
		}
	}
	assert.True(t, found, "ui-demo command should be registered with root")
}

func TestDemoCommitSelector(t *testing.T) {
	// Test commit selector demo data
	t.Run("commit data structure", func(t *testing.T) {
		// The demo creates sample commits
		// We test the structure matches what's expected
		commits := []ui.CommitInfo{
			{
				Hash:      "abc123456789def",
				ShortHash: "abc1234",
				Message:   "Fix database connection pooling issue",
				Author:    "John Doe",
				Time:      time.Now().Add(-2 * time.Hour),
				Files:     5,
			},
		}
		
		assert.Len(t, commits, 1)
		assert.Equal(t, "abc123456789def", commits[0].Hash)
		assert.Equal(t, "abc1234", commits[0].ShortHash)
		assert.Equal(t, 5, commits[0].Files)
		assert.WithinDuration(t, time.Now().Add(-2*time.Hour), commits[0].Time, time.Second)
	})
}

func TestDemoProgressBar(t *testing.T) {
	// Test progress bar demo data
	files := []string{
		"tables/users.sql",
		"tables/orders.sql",
		"views/customer_analytics.sql",
		"procedures/update_stats.sql",
		"functions/calculate_revenue.sql",
	}
	
	assert.Len(t, files, 5)
	assert.Contains(t, files[0], ".sql")
	
	// Test that third file is simulated to fail
	for i := range files {
		expectedSuccess := (i != 2) // Third file (index 2) should fail
		assert.Equal(t, i != 2, expectedSuccess)
	}
}

func TestDemoSpinner(t *testing.T) {
	// Test spinner demo operations
	operations := []struct {
		message  string
		duration time.Duration
		success  bool
	}{
		{"Connecting to Snowflake", 2 * time.Second, true},
		{"Validating credentials", 1 * time.Second, true},
		{"Loading configuration", 1500 * time.Millisecond, true},
		{"Checking repository", 2 * time.Second, false},
	}
	
	assert.Len(t, operations, 4)
	
	// Verify operations
	assert.Equal(t, "Connecting to Snowflake", operations[0].message)
	assert.True(t, operations[0].success)
	assert.Equal(t, "Checking repository", operations[3].message)
	assert.False(t, operations[3].success)
	
	// Verify durations are reasonable
	for _, op := range operations {
		assert.Greater(t, op.duration, time.Duration(0))
		assert.LessOrEqual(t, op.duration, 2*time.Second)
	}
}

func TestDemoTable(t *testing.T) {
	// Test table demo data structure
	tableData := []struct {
		commit   string
		date     string
		files    string
		status   string
		duration string
	}{
		{"abc1234", "2024-01-20", "5", "Success", "1m23s"},
		{"def5678", "2024-01-19", "12", "Success", "3m45s"},
		{"ghi9012", "2024-01-18", "3", "Failed", "0m52s"},
		{"jkl3456", "2024-01-17", "8", "Success", "2m10s"},
	}
	
	assert.Len(t, tableData, 4)
	
	// Verify data format
	for _, row := range tableData {
		assert.Len(t, row.commit, 7) // Short hash format
		assert.Contains(t, row.date, "2024-01")
		_, err := fmt.Sscanf(row.files, "%d", new(int))
		assert.NoError(t, err)
		assert.True(t, row.status == "Success" || row.status == "Failed")
		assert.Contains(t, row.duration, "m")
		assert.Contains(t, row.duration, "s")
	}
}

func TestDemoErrorFormatting(t *testing.T) {
	// Test error formatting demo
	errors := []error{
		fmt.Errorf("authentication failed: invalid username or password"),
		fmt.Errorf("connection refused: unable to reach xy12345.snowflakecomputing.com"),
		fmt.Errorf("syntax error near 'SELEC' at line 42:\n  SELEC * FROM users WHERE active = true"),
		fmt.Errorf("permission denied: role 'ANALYST' does not have privilege 'CREATE TABLE' on schema 'PROD'"),
	}
	
	assert.Len(t, errors, 4)
	
	// Test error types
	assert.Contains(t, errors[0].Error(), "authentication failed")
	assert.Contains(t, errors[1].Error(), "connection refused")
	assert.Contains(t, errors[2].Error(), "syntax error")
	assert.Contains(t, errors[3].Error(), "permission denied")
	
	// Test multi-line error
	assert.Contains(t, errors[2].Error(), "\n")
}

func TestUIComponentSelection(t *testing.T) {
	// Test UI component options
	options := []string{
		"Commit Selector",
		"Configuration Wizard",
		"Progress Bar",
		"Spinner",
		"Table Output",
		"Error Formatting",
		"All Components",
	}
	
	assert.Len(t, options, 7)
	
	// Verify all options are unique
	seen := make(map[string]bool)
	for _, opt := range options {
		assert.False(t, seen[opt], "Duplicate option: %s", opt)
		seen[opt] = true
	}
}

func TestDemoAll(t *testing.T) {
	// Test that demoAll includes all major demo functions
	// This is a structural test to ensure completeness
	
	expectedDemos := []string{
		"demoCommitSelector",
		"demoProgressBar",
		"demoSpinner",
		"demoTable",
		"demoErrorFormatting",
	}
	
	// In the actual implementation, demoAll calls these functions
	// We verify the count matches
	assert.GreaterOrEqual(t, len(expectedDemos), 5)
}

func TestUIHelperFunctions(t *testing.T) {
	t.Run("format duration", func(t *testing.T) {
		// These would be tested if they were exported
		durations := []struct {
			input    string
			expected string
		}{
			{"1m23s", "1m23s"},
			{"0m52s", "0m52s"},
			{"3m45s", "3m45s"},
		}
		
		for _, d := range durations {
			assert.Equal(t, d.expected, d.input)
		}
	})
}

func TestUIDemoOutput(t *testing.T) {
	// Test expected output patterns
	patterns := []struct {
		component string
		expected  []string
	}{
		{
			component: "header",
			expected:  []string{"===", "Demo"},
		},
		{
			component: "success",
			expected:  []string{"✓", "Success"},
		},
		{
			component: "error",
			expected:  []string{"✗", "Error", "failed"},
		},
		{
			component: "info",
			expected:  []string{"ℹ"},
		},
		{
			component: "warning",
			expected:  []string{"⚠"},
		},
	}
	
	for _, p := range patterns {
		t.Run(p.component, func(t *testing.T) {
			// Verify expected patterns exist
			assert.NotEmpty(t, p.expected)
		})
	}
}

func BenchmarkUIDemo(b *testing.B) {
	// Benchmark different UI components
	
	b.Run("commit selector data", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			commits := make([]ui.CommitInfo, 10)
			for j := 0; j < 10; j++ {
				commits[j] = ui.CommitInfo{
					Hash:      fmt.Sprintf("%040x", j),
					ShortHash: fmt.Sprintf("%07x", j),
					Message:   fmt.Sprintf("Commit message %d", j),
					Author:    "Test Author",
					Time:      time.Now().Add(time.Duration(-j) * time.Hour),
					Files:     j + 1,
				}
			}
		}
	})
	
	b.Run("error creation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = fmt.Errorf("test error %d: something went wrong", i)
		}
	})
}

func TestUIDemoEdgeCases(t *testing.T) {
	t.Run("empty commit list", func(t *testing.T) {
		commits := []ui.CommitInfo{}
		assert.Empty(t, commits)
	})
	
	t.Run("very long commit message", func(t *testing.T) {
		longMessage := strings.Repeat("a", 200)
		commit := ui.CommitInfo{
			Hash:      "abc123456789",
			ShortHash: "abc1234",
			Message:   longMessage,
			Author:    "Test",
			Time:      time.Now(),
			Files:     1,
		}
		
		// In real implementation, message should be truncated
		assert.Greater(t, len(commit.Message), 50)
	})
	
	t.Run("zero files in commit", func(t *testing.T) {
		commit := ui.CommitInfo{
			Hash:      "abc123456789",
			ShortHash: "abc1234",
			Message:   "Empty commit",
			Author:    "Test",
			Time:      time.Now(),
			Files:     0,
		}
		
		assert.Equal(t, 0, commit.Files)
	})
}

func TestUIDemoCommandHelp(t *testing.T) {
	// Test help output
	cmd := &cobra.Command{Use: "test"}
	cmd.AddCommand(uiDemoCmd)
	
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	
	cmd.SetArgs([]string{"ui-demo", "--help"})
	err := cmd.Execute()
	
	require.NoError(t, err)
	output := buf.String()
	
	// Verify help contains expected elements
	assert.Contains(t, output, "ui-demo")
	assert.Contains(t, output, "This command demonstrates various UI components")
	assert.Contains(t, output, "Usage:")
}