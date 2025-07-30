package common

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CleanPath sanitizes a file path to prevent directory traversal attacks
func CleanPath(path string) (string, error) {
	// Clean the path to remove any ../ or ./ sequences
	cleaned := filepath.Clean(path)
	
	// Check for suspicious patterns
	if strings.Contains(cleaned, "..") {
		return "", fmt.Errorf("invalid path: contains directory traversal")
	}
	
	// Convert to absolute path if needed
	if !filepath.IsAbs(cleaned) {
		abs, err := filepath.Abs(cleaned)
		if err != nil {
			return "", fmt.Errorf("failed to resolve absolute path: %w", err)
		}
		cleaned = abs
	}
	
	return cleaned, nil
}

// ValidatePath ensures a path is within an allowed directory
func ValidatePath(path, baseDir string) (string, error) {
	// Clean both paths
	cleanedPath, err := CleanPath(path)
	if err != nil {
		return "", err
	}
	
	cleanedBase, err := CleanPath(baseDir)
	if err != nil {
		return "", err
	}
	
	// Ensure the path is within the base directory
	if !strings.HasPrefix(cleanedPath, cleanedBase) {
		return "", fmt.Errorf("path is outside allowed directory")
	}
	
	return cleanedPath, nil
}

// JoinPath safely joins path components
func JoinPath(base string, elements ...string) (string, error) {
	// Clean the base path
	cleanedBase, err := CleanPath(base)
	if err != nil {
		return "", err
	}
	
	// Join all elements
	joined := filepath.Join(append([]string{cleanedBase}, elements...)...)
	
	// Validate the result is still within base
	return ValidatePath(joined, cleanedBase)
}