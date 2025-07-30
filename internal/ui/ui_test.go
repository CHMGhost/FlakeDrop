package ui

import (
	"testing"
	"time"
)

func TestFormatRelativeTime(t *testing.T) {
	tests := []struct {
		name     string
		time     time.Time
		expected string
	}{
		{
			name:     "just now",
			time:     time.Now().Add(-30 * time.Second),
			expected: "just now",
		},
		{
			name:     "minutes ago",
			time:     time.Now().Add(-5 * time.Minute),
			expected: "5 minutes ago",
		},
		{
			name:     "hours ago",
			time:     time.Now().Add(-3 * time.Hour),
			expected: "3 hours ago",
		},
		{
			name:     "days ago",
			time:     time.Now().Add(-2 * 24 * time.Hour),
			expected: "2 days ago",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatRelativeTime(tt.time)
			if result != tt.expected {
				t.Errorf("formatRelativeTime() = %v, want %v", result, tt.expected)
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
			name:     "seconds",
			duration: 45 * time.Second,
			expected: "45.0s",
		},
		{
			name:     "minutes",
			duration: 3*time.Minute + 30*time.Second,
			expected: "3m30s",
		},
		{
			name:     "hours",
			duration: 2*time.Hour + 15*time.Minute,
			expected: "2h15m",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDuration(tt.duration)
			if result != tt.expected {
				t.Errorf("formatDuration() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCommitInfo(t *testing.T) {
	commit := CommitInfo{
		Hash:      "abc123456789def",
		ShortHash: "abc1234",
		Message:   "Fix database connection",
		Author:    "John Doe",
		Time:      time.Now().Add(-2 * time.Hour),
		Files:     5,
	}
	
	if commit.ShortHash != "abc1234" {
		t.Errorf("Expected short hash to be abc1234, got %s", commit.ShortHash)
	}
}

func TestProgressBar(t *testing.T) {
	pb := NewProgressBar(10)
	
	if pb.total != 10 {
		t.Errorf("Expected total to be 10, got %d", pb.total)
	}
	
	pb.Update(5, "test.sql", true)
	
	if pb.current != 5 {
		t.Errorf("Expected current to be 5, got %d", pb.current)
	}
	
	if pb.successCount != 1 {
		t.Errorf("Expected success count to be 1, got %d", pb.successCount)
	}
}

func TestFormatFileChange(t *testing.T) {
	tests := []struct {
		name      string
		additions int
		deletions int
	}{
		{
			name:      "additions only",
			additions: 10,
			deletions: 0,
		},
		{
			name:      "deletions only",
			additions: 0,
			deletions: 5,
		},
		{
			name:      "both",
			additions: 10,
			deletions: 5,
		},
		{
			name:      "none",
			additions: 0,
			deletions: 0,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFileChange(tt.additions, tt.deletions)
			// Just ensure it doesn't panic
			if result == "" {
				t.Error("FormatFileChange returned empty string")
			}
		})
	}
}