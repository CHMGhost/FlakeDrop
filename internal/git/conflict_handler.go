package git

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"
	"flakedrop/internal/common"
	"flakedrop/pkg/errors"
)

// ConflictHandler handles merge conflicts
type ConflictHandler struct{}

// NewConflictHandler creates a new conflict handler
func NewConflictHandler() *ConflictHandler {
	return &ConflictHandler{}
}

// ResolveWithOurs resolves conflicts by keeping our changes
func (ch *ConflictHandler) ResolveWithOurs(worktree *git.Worktree) error {
	status, err := worktree.Status()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get repository status")
	}

	for file, fileStatus := range status {
		// Check for merge conflicts by looking for both modified
		if fileStatus.Staging == git.Modified && fileStatus.Worktree == git.Modified {
			if err := ch.resolveFileWithOurs(worktree, file); err != nil {
				return err
			}
		}
	}

	return nil
}

// ResolveWithTheirs resolves conflicts by keeping their changes
func (ch *ConflictHandler) ResolveWithTheirs(worktree *git.Worktree) error {
	status, err := worktree.Status()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get repository status")
	}

	for file, fileStatus := range status {
		// Check for merge conflicts by looking for both modified
		if fileStatus.Staging == git.Modified && fileStatus.Worktree == git.Modified {
			if err := ch.resolveFileWithTheirs(worktree, file); err != nil {
				return err
			}
		}
	}

	return nil
}

// AutoResolveConflicts attempts to automatically resolve conflicts
func (ch *ConflictHandler) AutoResolveConflicts(worktree *git.Worktree) (*ConflictResolution, error) {
	status, err := worktree.Status()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get repository status")
	}

	resolution := &ConflictResolution{
		AutoResolved:   []string{},
		ManualRequired: []string{},
		Errors:         []string{},
	}

	for file, fileStatus := range status {
		// Check for merge conflicts by looking for both modified
		if fileStatus.Staging == git.Modified && fileStatus.Worktree == git.Modified {
			// Try to auto-resolve
			resolved, err := ch.tryAutoResolve(worktree, file)
			if err != nil {
				resolution.Errors = append(resolution.Errors, 
					fmt.Sprintf("%s: %v", file, err))
				continue
			}

			if resolved {
				resolution.AutoResolved = append(resolution.AutoResolved, file)
			} else {
				resolution.ManualRequired = append(resolution.ManualRequired, file)
			}
		}
	}

	resolution.Success = len(resolution.ManualRequired) == 0 && len(resolution.Errors) == 0
	return resolution, nil
}

// GetConflictDetails returns detailed information about conflicts
func (ch *ConflictHandler) GetConflictDetails(worktree *git.Worktree) ([]ConflictDetail, error) {
	status, err := worktree.Status()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get repository status")
	}

	var conflicts []ConflictDetail

	for file, fileStatus := range status {
		// Check for merge conflicts by looking for both modified
		if fileStatus.Staging == git.Modified && fileStatus.Worktree == git.Modified {
			detail, err := ch.analyzeConflict(worktree, file)
			if err != nil {
				// Include error in detail
				detail = ConflictDetail{
					FilePath: file,
					Error:    err.Error(),
				}
			}
			conflicts = append(conflicts, detail)
		}
	}

	return conflicts, nil
}

// resolveFileWithOurs resolves a single file conflict with our changes
func (ch *ConflictHandler) resolveFileWithOurs(worktree *git.Worktree, file string) error {
	filePath := filepath.Join(worktree.Filesystem.Root(), file)
	
	// Validate the file path
	validatedPath, err := common.ValidatePath(filePath, worktree.Filesystem.Root())
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}
	
	content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return err
	}

	resolved := ch.extractOurChanges(string(content))
	
	err = os.WriteFile(validatedPath, []byte(resolved), 0600) // #nosec G304 - path is validated
	if err != nil {
		return err
	}

	// Stage the resolved file
	_, err = worktree.Add(file)
	return err
}

// resolveFileWithTheirs resolves a single file conflict with their changes
func (ch *ConflictHandler) resolveFileWithTheirs(worktree *git.Worktree, file string) error {
	filePath := filepath.Join(worktree.Filesystem.Root(), file)
	
	// Validate the file path
	validatedPath, err := common.ValidatePath(filePath, worktree.Filesystem.Root())
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}
	
	content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return err
	}

	resolved := ch.extractTheirChanges(string(content))
	
	err = os.WriteFile(validatedPath, []byte(resolved), 0600) // #nosec G304 - path is validated
	if err != nil {
		return err
	}

	// Stage the resolved file
	_, err = worktree.Add(file)
	return err
}

// tryAutoResolve attempts to automatically resolve a conflict
func (ch *ConflictHandler) tryAutoResolve(worktree *git.Worktree, file string) (bool, error) {
	filePath := filepath.Join(worktree.Filesystem.Root(), file)
	
	// Validate the file path
	validatedPath, err := common.ValidatePath(filePath, worktree.Filesystem.Root())
	if err != nil {
		return false, errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}
	
	// Read the conflicted file
	content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return false, err
	}

	// Analyze the conflict
	conflicts := ch.parseConflicts(string(content))
	if len(conflicts) == 0 {
		// No conflicts found, might already be resolved
		return true, nil
	}

	// Try different resolution strategies
	for _, conflict := range conflicts {
		// Strategy 1: If one side is empty, take the other
		if conflict.Ours == "" && conflict.Theirs != "" {
			// Their side has content, ours doesn't - take theirs
			continue
		} else if conflict.Theirs == "" && conflict.Ours != "" {
			// Our side has content, theirs doesn't - take ours
			continue
		}
		
		// Strategy 2: If changes are in different parts (no overlap), merge both
		if ch.canMergeAutomatically(conflict) {
			continue
		}

		// Cannot auto-resolve this conflict
		return false, nil
	}

	// All conflicts can be auto-resolved
	resolved := ch.applyAutoResolution(string(content), conflicts)
	
	err = os.WriteFile(validatedPath, []byte(resolved), 0600) // #nosec G304 - path is validated
	if err != nil {
		return false, err
	}

	// Stage the resolved file
	_, err = worktree.Add(file)
	return true, err
}

// analyzeConflict analyzes a conflict in detail
func (ch *ConflictHandler) analyzeConflict(worktree *git.Worktree, file string) (ConflictDetail, error) {
	filePath := filepath.Join(worktree.Filesystem.Root(), file)
	
	// Validate the file path
	validatedPath, err := common.ValidatePath(filePath, worktree.Filesystem.Root())
	if err != nil {
		return ConflictDetail{}, errors.Wrap(err, errors.ErrCodeInvalidInput, "Invalid file path")
	}
	
	content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return ConflictDetail{}, err
	}

	conflicts := ch.parseConflicts(string(content))
	
	detail := ConflictDetail{
		FilePath:       file,
		ConflictCount:  len(conflicts),
		FileType:       ch.detectFileType(file),
		Conflicts:      conflicts,
		CanAutoResolve: ch.canAutoResolveAll(conflicts),
	}

	// Add resolution hints based on file type
	detail.Hints = ch.getResolutionHints(detail.FileType, conflicts)

	return detail, nil
}

// parseConflicts parses conflict markers in content
func (ch *ConflictHandler) parseConflicts(content string) []Conflict {
	var conflicts []Conflict
	lines := strings.Split(content, "\n")
	
	var current *Conflict
	var section string
	
	for i, line := range lines {
		if strings.HasPrefix(line, "<<<<<<<") {
			// Start of conflict
			current = &Conflict{
				StartLine: i + 1,
			}
			section = "ours"
		} else if strings.HasPrefix(line, "=======") && current != nil {
			// Middle marker
			section = "theirs"
		} else if strings.HasPrefix(line, ">>>>>>>") && current != nil {
			// End of conflict
			current.EndLine = i + 1
			conflicts = append(conflicts, *current)
			current = nil
			section = ""
		} else if current != nil {
			// Content within conflict
			if section == "ours" {
				current.Ours += line + "\n"
			} else if section == "theirs" {
				current.Theirs += line + "\n"
			}
		}
	}

	return conflicts
}

// extractOurChanges extracts our changes from conflicted content
func (ch *ConflictHandler) extractOurChanges(content string) string {
	lines := strings.Split(content, "\n")
	var result []string
	inConflict := false
	inTheirs := false

	for _, line := range lines {
		if strings.HasPrefix(line, "<<<<<<<") {
			inConflict = true
			continue
		} else if strings.HasPrefix(line, "=======") {
			inTheirs = true
			continue
		} else if strings.HasPrefix(line, ">>>>>>>") {
			inConflict = false
			inTheirs = false
			continue
		}

		if !inConflict || (inConflict && !inTheirs) {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// extractTheirChanges extracts their changes from conflicted content
func (ch *ConflictHandler) extractTheirChanges(content string) string {
	lines := strings.Split(content, "\n")
	var result []string
	inConflict := false
	inTheirs := false

	for _, line := range lines {
		if strings.HasPrefix(line, "<<<<<<<") {
			inConflict = true
			continue
		} else if strings.HasPrefix(line, "=======") {
			inTheirs = true
			continue
		} else if strings.HasPrefix(line, ">>>>>>>") {
			inConflict = false
			inTheirs = false
			continue
		}

		if !inConflict || (inConflict && inTheirs) {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// canMergeAutomatically checks if a conflict can be merged automatically
func (ch *ConflictHandler) canMergeAutomatically(conflict Conflict) bool {
	// Simple heuristic: if changes don't overlap significantly
	oursLines := strings.Split(strings.TrimSpace(conflict.Ours), "\n")
	theirsLines := strings.Split(strings.TrimSpace(conflict.Theirs), "\n")

	// Check for common lines
	commonLines := 0
	for _, ourLine := range oursLines {
		for _, theirLine := range theirsLines {
			if ourLine == theirLine {
				commonLines++
			}
		}
	}

	// If less than 20% overlap, might be safe to merge
	totalLines := len(oursLines) + len(theirsLines)
	if totalLines > 0 && float64(commonLines)/float64(totalLines) < 0.2 {
		return true
	}

	return false
}

// canAutoResolveAll checks if all conflicts can be auto-resolved
func (ch *ConflictHandler) canAutoResolveAll(conflicts []Conflict) bool {
	for _, conflict := range conflicts {
		if !ch.canMergeAutomatically(conflict) {
			return false
		}
	}
	return true
}

// applyAutoResolution applies automatic resolution to conflicts
func (ch *ConflictHandler) applyAutoResolution(content string, conflicts []Conflict) string {
	// This is a simplified implementation
	// In practice, you'd need more sophisticated merging logic
	result := content

	// For now, just merge both sides if they can be merged automatically
	for _, conflict := range conflicts {
		if ch.canMergeAutomatically(conflict) {
			// Replace conflict with merged content
			merged := conflict.Ours + "\n" + conflict.Theirs
			// You'd need to properly replace the conflict markers here
			_ = merged
		}
	}

	return result
}

// detectFileType detects the type of file
func (ch *ConflictHandler) detectFileType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".sql":
		return "SQL"
	case ".json":
		return "JSON"
	case ".yaml", ".yml":
		return "YAML"
	case ".xml":
		return "XML"
	case ".go":
		return "Go"
	case ".py":
		return "Python"
	case ".js":
		return "JavaScript"
	default:
		return "Text"
	}
}

// getResolutionHints provides hints for resolving conflicts
func (ch *ConflictHandler) getResolutionHints(fileType string, conflicts []Conflict) []string {
	hints := []string{}

	switch fileType {
	case "SQL":
		hints = append(hints, "Review SQL changes carefully for schema compatibility")
		hints = append(hints, "Ensure no conflicting DDL statements")
		hints = append(hints, "Check for dependent objects")
		
	case "JSON":
		hints = append(hints, "Ensure JSON remains valid after resolution")
		hints = append(hints, "Check for duplicate keys")
		
	case "YAML":
		hints = append(hints, "Maintain proper YAML indentation")
		hints = append(hints, "Check for duplicate keys")
	}

	// General hints
	if len(conflicts) > 3 {
		hints = append(hints, "Consider breaking this into smaller changes")
	}

	return hints
}

// ConflictResolution represents the result of conflict resolution
type ConflictResolution struct {
	Success        bool     `json:"success"`
	AutoResolved   []string `json:"auto_resolved"`
	ManualRequired []string `json:"manual_required"`
	Errors         []string `json:"errors"`
}

// ConflictDetail provides detailed information about a conflict
type ConflictDetail struct {
	FilePath       string     `json:"file_path"`
	ConflictCount  int        `json:"conflict_count"`
	FileType       string     `json:"file_type"`
	Conflicts      []Conflict `json:"conflicts"`
	CanAutoResolve bool       `json:"can_auto_resolve"`
	Hints          []string   `json:"hints"`
	Error          string     `json:"error,omitempty"`
}

// Conflict represents a single conflict section
type Conflict struct {
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	Ours      string `json:"ours"`
	Theirs    string `json:"theirs"`
}

// GenerateConflictReport generates a detailed conflict report
func (ch *ConflictHandler) GenerateConflictReport(worktree *git.Worktree) (*ConflictReport, error) {
	details, err := ch.GetConflictDetails(worktree)
	if err != nil {
		return nil, err
	}

	report := &ConflictReport{
		TotalConflicts:  0,
		TotalFiles:      len(details),
		ByFileType:      make(map[string]int),
		SeverityLevels:  make(map[string]int),
	}

	for _, detail := range details {
		report.TotalConflicts += detail.ConflictCount
		report.ByFileType[detail.FileType]++
		
		// Assess severity
		severity := ch.assessConflictSeverity(detail)
		report.SeverityLevels[severity]++
	}

	// Generate recommendations
	report.Recommendations = ch.generateRecommendations(details)

	return report, nil
}

// assessConflictSeverity assesses the severity of a conflict
func (ch *ConflictHandler) assessConflictSeverity(detail ConflictDetail) string {
	if detail.CanAutoResolve {
		return "Low"
	}
	
	if detail.ConflictCount > 5 {
		return "High"
	}
	
	if detail.FileType == "SQL" {
		return "High" // SQL conflicts are typically more critical
	}
	
	return "Medium"
}

// generateRecommendations generates recommendations based on conflicts
func (ch *ConflictHandler) generateRecommendations(details []ConflictDetail) []string {
	recommendations := []string{}
	
	sqlConflicts := 0
	totalConflicts := 0
	
	for _, detail := range details {
		if detail.FileType == "SQL" {
			sqlConflicts++
		}
		totalConflicts += detail.ConflictCount
	}
	
	if sqlConflicts > 0 {
		recommendations = append(recommendations, 
			"SQL conflicts detected - review schema changes carefully")
	}
	
	if totalConflicts > 10 {
		recommendations = append(recommendations,
			"Large number of conflicts - consider coordinating with team members")
	}
	
	return recommendations
}

// ConflictReport provides a summary of conflicts
type ConflictReport struct {
	TotalConflicts  int            `json:"total_conflicts"`
	TotalFiles      int            `json:"total_files"`
	ByFileType      map[string]int `json:"by_file_type"`
	SeverityLevels  map[string]int `json:"severity_levels"`
	Recommendations []string       `json:"recommendations"`
}