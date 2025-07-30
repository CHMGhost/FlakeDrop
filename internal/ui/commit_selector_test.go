package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/go-git/go-git/v5/plumbing"
)

func TestFormatCommit(t *testing.T) {
	tests := []struct {
		name           string
		commitMessage  string
		fileCount      int
		expectedMsg    string
		checkTruncated bool
	}{
		{
			name:          "short commit message",
			commitMessage: "Fix bug",
			fileCount:     3,
			expectedMsg:   "Fix bug",
		},
		{
			name:          "long commit message truncated",
			commitMessage: "This is a very long commit message that should be truncated to fit within the display limit",
			fileCount:     5,
			expectedMsg:   "This is a very long commit message that should ...",
			checkTruncated: true,
		},
		{
			name:          "multi-line commit message",
			commitMessage: "Fix bug\n\nThis is a detailed description of the bug fix",
			fileCount:     2,
			expectedMsg:   "Fix bug",
		},
		{
			name:          "empty commit message",
			commitMessage: "",
			fileCount:     0,
			expectedMsg:   "",
		},
		{
			name:          "commit message exactly 50 chars",
			commitMessage: strings.Repeat("a", 50),
			fileCount:     1,
			expectedMsg:   strings.Repeat("a", 50), // Exactly 50 chars is not truncated
			checkTruncated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock commit
			sig := &object.Signature{
				Name:  "Test Author",
				Email: "test@example.com",
				When:  time.Now(),
			}
			
			commit := &object.Commit{
				Hash:      plumbing.NewHash("1234567890abcdef1234567890abcdef12345678"),
				Author:    *sig,
				Committer: *sig,
				Message:   tt.commitMessage,
			}

			result := FormatCommit(commit, tt.fileCount)

			assert.Equal(t, "1234567890abcdef1234567890abcdef12345678", result.Hash)
			assert.Equal(t, "1234567", result.ShortHash)
			assert.Equal(t, tt.expectedMsg, result.Message)
			assert.Equal(t, "Test Author", result.Author)
			assert.Equal(t, tt.fileCount, result.Files)
			assert.WithinDuration(t, time.Now(), result.Time, time.Second)

			if tt.checkTruncated {
				assert.True(t, strings.HasSuffix(result.Message, "..."))
				assert.LessOrEqual(t, len(result.Message), 50)
			}
		})
	}
}

func TestSelectCommit(t *testing.T) {
	t.Run("empty commit list", func(t *testing.T) {
		commits := []CommitInfo{}
		_, err := SelectCommit(commits)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no commits available")
	})

	t.Run("commit list formatting", func(t *testing.T) {
		commits := []CommitInfo{
			{
				Hash:      "abc123456789def0123456789abcdef0123456789",
				ShortHash: "abc1234",
				Message:   "Fix database connection",
				Author:    "John Doe",
				Time:      time.Now().Add(-2 * time.Hour),
				Files:     5,
			},
			{
				Hash:      "def456789012ghi3456789012def456789012ghi3",
				ShortHash: "def4567",
				Message:   "Add new feature",
				Author:    "Jane Smith",
				Time:      time.Now().Add(-24 * time.Hour),
				Files:     10,
			},
		}

		// Since SelectCommit uses interactive survey, we can't fully test it
		// But we can verify the commit data structure
		assert.Len(t, commits, 2)
		assert.Equal(t, "abc1234", commits[0].ShortHash)
		assert.Equal(t, 5, commits[0].Files)
	})
}

func TestFormatRelativeTimeComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		// Seconds
		{"0 seconds", 0, "just now"},
		{"30 seconds", 30 * time.Second, "just now"},
		{"59 seconds", 59 * time.Second, "just now"},
		
		// Minutes
		{"1 minute", 1 * time.Minute, "1 minute ago"},
		{"2 minutes", 2 * time.Minute, "2 minutes ago"},
		{"59 minutes", 59 * time.Minute, "59 minutes ago"},
		
		// Hours
		{"1 hour", 1 * time.Hour, "1 hour ago"},
		{"2 hours", 2 * time.Hour, "2 hours ago"},
		{"23 hours", 23 * time.Hour, "23 hours ago"},
		
		// Days
		{"1 day", 24 * time.Hour, "1 day ago"},
		{"2 days", 48 * time.Hour, "2 days ago"},
		{"6 days", 6 * 24 * time.Hour, "6 days ago"},
		
		// Weeks
		{"1 week", 7 * 24 * time.Hour, "1 week ago"},
		{"2 weeks", 14 * 24 * time.Hour, "2 weeks ago"},
		{"3 weeks", 21 * 24 * time.Hour, "3 weeks ago"},
		
		// Months
		{"1 month", 30 * 24 * time.Hour, "1 month ago"},
		{"2 months", 60 * 24 * time.Hour, "2 months ago"},
		{"11 months", 330 * 24 * time.Hour, "11 months ago"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testTime := time.Now().Add(-tt.duration)
			result := formatRelativeTime(testTime)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCommitInfoStruct(t *testing.T) {
	// Test CommitInfo struct initialization and field access
	now := time.Now()
	commit := CommitInfo{
		Hash:      "1234567890abcdef1234567890abcdef12345678",
		ShortHash: "1234567",
		Message:   "Test commit message",
		Author:    "Test Author",
		Time:      now,
		Files:     42,
	}

	assert.Equal(t, "1234567890abcdef1234567890abcdef12345678", commit.Hash)
	assert.Equal(t, "1234567", commit.ShortHash)
	assert.Equal(t, "Test commit message", commit.Message)
	assert.Equal(t, "Test Author", commit.Author)
	assert.Equal(t, now, commit.Time)
	assert.Equal(t, 42, commit.Files)
}

func TestCommitOptionFormatting(t *testing.T) {
	// Test how commits are formatted for display in the selector
	commits := []CommitInfo{
		{
			Hash:      "abc123456789",
			ShortHash: "abc1234",
			Message:   "Fix critical bug",
			Author:    "John Doe",
			Time:      time.Now().Add(-2 * time.Hour),
			Files:     3,
		},
	}

	// Simulate the option formatting logic from SelectCommit
	for _, commit := range commits {
		relTime := formatRelativeTime(commit.Time)
		option := fmt.Sprintf("%s - %s (%s) [%d files]",
			commit.ShortHash,
			commit.Message,
			relTime,
			commit.Files,
		)

		assert.Contains(t, option, "abc1234")
		assert.Contains(t, option, "Fix critical bug")
		assert.Contains(t, option, "2 hours ago")
		assert.Contains(t, option, "[3 files]")
	}
}

func TestEdgeCases(t *testing.T) {
	t.Run("future time", func(t *testing.T) {
		futureTime := time.Now().Add(1 * time.Hour)
		result := formatRelativeTime(futureTime)
		// Future times should show as "just now"
		assert.Equal(t, "just now", result)
	})

	t.Run("very old commit", func(t *testing.T) {
		oldTime := time.Now().Add(-365 * 24 * time.Hour) // 1 year ago
		result := formatRelativeTime(oldTime)
		assert.Contains(t, result, "months ago")
	})

	t.Run("unicode in commit message", func(t *testing.T) {
		sig := &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
			When:  time.Now(),
		}
		
		commit := &object.Commit{
			Hash:      plumbing.NewHash("1234567890abcdef1234567890abcdef12345678"),
			Author:    *sig,
			Committer: *sig,
			Message:   "Fix 🐛 in database connection 数据库",
		}

		result := FormatCommit(commit, 1)
		assert.Equal(t, "Fix 🐛 in database connection 数据库", result.Message)
	})

	t.Run("very long author name", func(t *testing.T) {
		sig := &object.Signature{
			Name:  strings.Repeat("A", 100),
			Email: "test@example.com",
			When:  time.Now(),
		}
		
		commit := &object.Commit{
			Hash:      plumbing.NewHash("1234567890abcdef1234567890abcdef12345678"),
			Author:    *sig,
			Committer: *sig,
			Message:   "Test",
		}

		result := FormatCommit(commit, 1)
		assert.Equal(t, strings.Repeat("A", 100), result.Author)
	})
}

func BenchmarkFormatCommit(b *testing.B) {
	sig := &object.Signature{
		Name:  "Test Author",
		Email: "test@example.com",
		When:  time.Now(),
	}
	
	commit := &object.Commit{
		Hash:      plumbing.NewHash("1234567890abcdef1234567890abcdef12345678"),
		Author:    *sig,
		Committer: *sig,
		Message:   "This is a test commit message that might be truncated",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatCommit(commit, 5)
	}
}

func BenchmarkFormatRelativeTime(b *testing.B) {
	times := []time.Time{
		time.Now().Add(-30 * time.Second),
		time.Now().Add(-5 * time.Minute),
		time.Now().Add(-2 * time.Hour),
		time.Now().Add(-3 * 24 * time.Hour),
		time.Now().Add(-2 * 7 * 24 * time.Hour),
		time.Now().Add(-60 * 24 * time.Hour),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range times {
			_ = formatRelativeTime(t)
		}
	}
}

func TestHashMapping(t *testing.T) {
	// Test the hash mapping logic used in SelectCommit
	commits := []CommitInfo{
		{
			Hash:      "abc123456789def0123456789abcdef0123456789",
			ShortHash: "abc1234",
			Message:   "Commit 1",
			Time:      time.Now(),
			Files:     1,
		},
		{
			Hash:      "def456789012ghi3456789012def456789012ghi3",
			ShortHash: "def4567",
			Message:   "Commit 2",
			Time:      time.Now(),
			Files:     2,
		},
	}

	// Simulate hash mapping
	hashMap := make(map[string]string)
	for _, commit := range commits {
		option := fmt.Sprintf("%s - %s (%s) [%d files]",
			commit.ShortHash,
			commit.Message,
			formatRelativeTime(commit.Time),
			commit.Files,
		)
		hashMap[option] = commit.Hash
	}

	assert.Len(t, hashMap, 2)
	for option, hash := range hashMap {
		assert.Contains(t, option, hash[:7]) // Short hash should be in option
	}
}