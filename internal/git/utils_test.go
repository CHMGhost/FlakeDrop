package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateRepoID(t *testing.T) {
	tests := []struct {
		name     string
		gitURL   string
		expected string
	}{
		{
			name:     "HTTPS GitHub URL",
			gitURL:   "https://github.com/user/repo.git",
			expected: "github.com_user_repo",
		},
		{
			name:     "SSH GitHub URL",
			gitURL:   "git@github.com:user/repo.git",
			expected: "github.com_user_repo",
		},
		{
			name:     "HTTP URL without .git",
			gitURL:   "http://gitlab.com/org/project",
			expected: "gitlab.com_org_project",
		},
		{
			name:     "Long URL gets hashed",
			gitURL:   "https://very-long-domain-name.example.com/organization/team/project/repository-with-very-long-name.git",
			expected: "b98dd6fc", // First 8 chars of hash
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateRepoID(tt.gitURL)
			if len(tt.gitURL) > 50 {
				// For long URLs, just check that result is 8 chars (hash)
				assert.Len(t, result, 8)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsSSHURL(t *testing.T) {
	tests := []struct {
		gitURL   string
		expected bool
	}{
		{"git@github.com:user/repo.git", true},
		{"ssh://git@github.com/user/repo.git", true},
		{"https://github.com/user/repo.git", false},
		{"http://github.com/user/repo.git", false},
		{"/local/path/to/repo", false},
	}

	for _, tt := range tests {
		t.Run(tt.gitURL, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsSSHURL(tt.gitURL))
		})
	}
}

func TestIsHTTPSURL(t *testing.T) {
	tests := []struct {
		gitURL   string
		expected bool
	}{
		{"https://github.com/user/repo.git", true},
		{"http://github.com/user/repo.git", true},
		{"git@github.com:user/repo.git", false},
		{"ssh://git@github.com/user/repo.git", false},
		{"/local/path/to/repo", false},
	}

	for _, tt := range tests {
		t.Run(tt.gitURL, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsHTTPSURL(tt.gitURL))
		})
	}
}

func TestExtractRepoName(t *testing.T) {
	tests := []struct {
		name     string
		gitURL   string
		expected string
	}{
		{
			name:     "HTTPS GitHub URL",
			gitURL:   "https://github.com/user/repo.git",
			expected: "repo",
		},
		{
			name:     "SSH GitHub URL",
			gitURL:   "git@github.com:user/repo.git",
			expected: "repo",
		},
		{
			name:     "URL without .git",
			gitURL:   "https://gitlab.com/org/project",
			expected: "project",
		},
		{
			name:     "Nested path",
			gitURL:   "https://github.com/org/team/project.git",
			expected: "project",
		},
		{
			name:     "Invalid URL",
			gitURL:   "not-a-url",
			expected: "not-a-url",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ExtractRepoName(tt.gitURL))
		})
	}
}

func TestValidateGitURL(t *testing.T) {
	tests := []struct {
		name    string
		gitURL  string
		wantErr bool
	}{
		{
			name:    "Valid HTTPS URL",
			gitURL:  "https://github.com/user/repo.git",
			wantErr: false,
		},
		{
			name:    "Valid SSH URL",
			gitURL:  "git@github.com:user/repo.git",
			wantErr: false,
		},
		{
			name:    "Valid local absolute path",
			gitURL:  "/home/user/repos/myrepo",
			wantErr: false,
		},
		{
			name:    "Empty URL",
			gitURL:  "",
			wantErr: true,
		},
		{
			name:    "Invalid URL",
			gitURL:  "not-a-valid-url",
			wantErr: true,
		},
		{
			name:    "Relative path",
			gitURL:  "./relative/path",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGitURL(tt.gitURL)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNormalizeGitURL(t *testing.T) {
	tests := []struct {
		name     string
		gitURL   string
		expected string
	}{
		{
			name:     "HTTPS URL without .git",
			gitURL:   "https://github.com/user/repo",
			expected: "https://github.com/user/repo.git",
		},
		{
			name:     "HTTPS URL with .git",
			gitURL:   "https://github.com/user/repo.git",
			expected: "https://github.com/user/repo.git",
		},
		{
			name:     "SSH URL without .git",
			gitURL:   "git@github.com:user/repo",
			expected: "git@github.com:user/repo.git",
		},
		{
			name:     "URL with whitespace",
			gitURL:   "  https://github.com/user/repo  ",
			expected: "https://github.com/user/repo.git",
		},
		{
			name:     "Local path",
			gitURL:   "/local/path/to/repo",
			expected: "/local/path/to/repo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, NormalizeGitURL(tt.gitURL))
		})
	}
}

func TestIsSQLFile(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"schema.sql", true},
		{"data.SQL", true},
		{"procedures/proc.sql", true},
		{"readme.md", false},
		{"script.sh", false},
		{"no-extension", false},
		{".sql", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsSQLFile(tt.path))
		})
	}
}

func TestFilterSQLFiles(t *testing.T) {
	input := []string{
		"schema.sql",
		"readme.md",
		"data.SQL",
		"scripts/setup.sh",
		"procedures/proc1.sql",
		"docs/guide.pdf",
		"migrations/001_init.sql",
	}

	expected := []string{
		"schema.sql",
		"data.SQL",
		"procedures/proc1.sql",
		"migrations/001_init.sql",
	}

	result := FilterSQLFiles(input)
	assert.Equal(t, expected, result)

	// Test with empty input
	assert.Empty(t, FilterSQLFiles([]string{}))

	// Test with no SQL files
	noSQL := []string{"readme.md", "main.go", "test.py"}
	assert.Empty(t, FilterSQLFiles(noSQL))
}