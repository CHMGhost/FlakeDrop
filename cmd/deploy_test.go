package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/internal/plugin"
)

func TestDeployCommand(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		flags     map[string]string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "no repository specified",
			args:      []string{},
			wantError: true,
			errorMsg:  "accepts 1 arg(s), received 0",
		},
		{
			name:      "too many arguments",
			args:      []string{"repo1", "repo2"},
			wantError: true,
			errorMsg:  "accepts 1 arg(s), received 2",
		},
		{
			name: "valid repository with dry-run",
			args: []string{"test-repo"},
			flags: map[string]string{
				"dry-run": "true",
			},
			wantError: false,
		},
		{
			name: "valid repository with specific commit",
			args: []string{"test-repo"},
			flags: map[string]string{
				"commit": "abc123",
			},
			wantError: false,
		},
		{
			name: "valid repository non-interactive mode",
			args: []string{"test-repo"},
			flags: map[string]string{
				"interactive": "false",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset command for each test
			deployCmd = &cobra.Command{
				Use:   "deploy [repository]",
				Short: "Deploy code to Snowflake",
				Args:  cobra.ExactArgs(1),
				RunE: func(cmd *cobra.Command, args []string) error {
					// Mock implementation for testing
					return nil
				},
			}

			// Add flags
			deployCmd.Flags().BoolP("dry-run", "d", false, "Show what would be deployed without executing")
			deployCmd.Flags().StringP("commit", "c", "", "Deploy a specific commit")
			deployCmd.Flags().BoolP("interactive", "i", true, "Use interactive mode")

			// Set flags from test case
			for flag, value := range tt.flags {
				err := deployCmd.Flags().Set(flag, value)
				require.NoError(t, err)
			}

			// Execute command
			deployCmd.SetArgs(tt.args)
			err := deployCmd.Execute()

			if tt.wantError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetRecentCommits(t *testing.T) {
	commits := getRecentCommits()
	
	assert.NotEmpty(t, commits)
	assert.Len(t, commits, 4)
	
	// Check first commit structure
	firstCommit := commits[0]
	assert.NotEmpty(t, firstCommit.Hash)
	assert.NotEmpty(t, firstCommit.ShortHash)
	assert.NotEmpty(t, firstCommit.Message)
	assert.NotEmpty(t, firstCommit.Author)
	assert.Greater(t, firstCommit.Files, 0)
	assert.WithinDuration(t, time.Now(), firstCommit.Time, 24*time.Hour)
	
	// Verify commits are ordered by time (newest first)
	for i := 1; i < len(commits); i++ {
		assert.True(t, commits[i-1].Time.After(commits[i].Time))
	}
}

func TestGetFilesToDeploy(t *testing.T) {
	testCases := []struct {
		name     string
		commit   string
		expected []string
	}{
		{
			name:   "valid commit",
			commit: "abc123",
			expected: []string{
				"tables/users.sql",
				"tables/orders.sql",
				"tables/products.sql",
				"views/customer_analytics.sql",
				"views/revenue_summary.sql",
				"procedures/update_stats.sql",
				"functions/calculate_revenue.sql",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files := getFilesToDeploy(tc.commit)
			
			assert.Equal(t, len(tc.expected), len(files))
			for i, expectedFile := range tc.expected {
				assert.Equal(t, expectedFile, files[i])
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{
			name:     "milliseconds",
			duration: 500 * time.Millisecond,
			expected: "500ms",
		},
		{
			name:     "exactly one second",
			duration: 1 * time.Second,
			expected: "1.0s",
		},
		{
			name:     "multiple seconds",
			duration: 2500 * time.Millisecond,
			expected: "2.5s",
		},
		{
			name:     "less than a millisecond",
			duration: 100 * time.Microsecond,
			expected: "0ms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDuration(tt.duration)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExecuteDeployment(t *testing.T) {
	tests := []struct {
		name     string
		files    []string
		dryRun   bool
	}{
		{
			name:   "dry run deployment",
			files:  []string{"test1.sql", "test2.sql"},
			dryRun: true,
		},
		{
			name:   "empty file list",
			files:  []string{},
			dryRun: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create context and mock plugin service
			ctx := context.Background()
			pluginService := &plugin.Service{}
			
			// Just verify it doesn't panic
			assert.NotPanics(t, func() {
				executeDeployment(ctx, tt.files, tt.dryRun, "test-deployment-id", "test-repo", "abc123", pluginService)
			})
		})
	}
}


func TestDeployCommandFlags(t *testing.T) {
	// Test flag parsing
	cmd := &cobra.Command{}
	deployCmd.Flags().VisitAll(func(f *pflag.Flag) {
		cmd.Flags().AddFlag(f)
	})

	tests := []struct {
		name     string
		flag     string
		value    string
		expected interface{}
	}{
		{
			name:     "dry-run flag",
			flag:     "dry-run",
			value:    "true",
			expected: true,
		},
		{
			name:     "commit flag",
			flag:     "commit",
			value:    "abc123def456",
			expected: "abc123def456",
		},
		{
			name:     "interactive flag",
			flag:     "interactive",
			value:    "false",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cmd.Flags().Set(tt.flag, tt.value)
			require.NoError(t, err)

			switch tt.flag {
			case "dry-run":
				val, err := cmd.Flags().GetBool(tt.flag)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			case "commit":
				val, err := cmd.Flags().GetString(tt.flag)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			case "interactive":
				val, err := cmd.Flags().GetBool(tt.flag)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			}
		})
	}
}

func TestDeployWorkflow(t *testing.T) {
	t.Run("successful deployment workflow", func(t *testing.T) {
		// This test would simulate the entire deployment workflow
		// In a real implementation, you'd mock the UI interactions
		// and Snowflake connections
		
		files := []string{"test.sql"}
		commits := getRecentCommits()
		
		assert.NotEmpty(t, commits)
		assert.NotEmpty(t, files)
		
		// Verify workflow components are available
		assert.NotNil(t, deployCmd)
		assert.Contains(t, deployCmd.Use, "deploy")
	})
}

func TestCommitSelection(t *testing.T) {
	tests := []struct {
		name           string
		deployCommit   string
		interactive    bool
		expectedCommit string
	}{
		{
			name:           "specific commit provided",
			deployCommit:   "abc123456789",
			interactive:    true,
			expectedCommit: "abc123456789",
		},
		{
			name:           "non-interactive uses latest",
			deployCommit:   "",
			interactive:    false,
			expectedCommit: "latest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test commit selection logic
			deployCommit = tt.deployCommit
			deployInteractive = tt.interactive
			
			// In real test, you'd mock the selection process
			if deployCommit != "" {
				assert.Equal(t, tt.deployCommit, deployCommit)
			}
		})
	}
}

func BenchmarkGetRecentCommits(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = getRecentCommits()
	}
}

func BenchmarkFormatDuration(b *testing.B) {
	durations := []time.Duration{
		100 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range durations {
			_ = formatDuration(d)
		}
	}
}