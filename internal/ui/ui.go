package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
)

// CommitInfo represents git commit information for UI display
type CommitInfo struct {
	Hash      string
	ShortHash string
	Message   string
	Author    string
	Time      time.Time
	Files     int
}

// UI represents the main UI interface
type UI struct {
	Verbose bool
	Quiet   bool
	spinner *Spinner
}

// UIManager manages UI interactions and state
type UIManager struct {
	ui      *UI
	history []string
}

// NewUI creates a new UI instance
func NewUI(verbose, quiet bool) *UI {
	return &UI{
		Verbose: verbose,
		Quiet:   quiet,
	}
}

// NewUIManager creates a new UIManager instance
func NewUIManager(ui *UI) *UIManager {
	return &UIManager{
		ui:      ui,
		history: make([]string, 0),
	}
}

// IsVerbose returns true if verbose mode is enabled
func (u *UI) IsVerbose() bool {
	return u.Verbose
}

// IsQuiet returns true if quiet mode is enabled
func (u *UI) IsQuiet() bool {
	return u.Quiet
}

// Printf prints formatted output if not in quiet mode
func (u *UI) Printf(format string, args ...interface{}) {
	if !u.Quiet {
		fmt.Printf(format, args...)
	}
}

// Println prints a line if not in quiet mode
func (u *UI) Println(args ...interface{}) {
	if !u.Quiet {
		fmt.Println(args...)
	}
}

// VerbosePrintf prints formatted output only in verbose mode
func (u *UI) VerbosePrintf(format string, args ...interface{}) {
	if u.Verbose && !u.Quiet {
		fmt.Printf(format, args...)
	}
}

// StartProgress starts a progress indicator with a message
func (u *UI) StartProgress(message string) {
	if !u.Quiet {
		u.spinner = NewSpinner(message)
		u.spinner.Start()
	}
}

// StopProgress stops the progress indicator
func (u *UI) StopProgress() {
	if u.spinner != nil && !u.Quiet {
		u.spinner.Stop(true, "Done")
		u.spinner = nil
	}
}

// Warning prints a warning message
func (u *UI) Warning(message string) {
	if !u.Quiet {
		fmt.Printf("%s %s\n", ColorWarning("⚠"), message)
	}
}

// Error prints an error message
func (u *UI) Error(message string) {
	if !u.Quiet {
		fmt.Printf("%s %s\n", ColorError("✗"), message)
	}
}

// Print prints a message (alias for Println for compatibility)
func (u *UI) Print(message string) {
	u.Println(message)
}

// Info prints an information message
func (u *UI) Info(message string) {
	if !u.Quiet {
		ShowInfo(message)
	}
}

// Success prints a success message
func (u *UI) Success(message string) {
	if !u.Quiet {
		ShowSuccess(message)
	}
}

// PrintSuccess prints a success message (global function for compatibility)
func PrintSuccess(message string) {
	ShowSuccess(message)
}

// PrintError prints an error message (global function for compatibility)
func PrintError(err error) {
	ShowError(err)
}

// PrintErrorString prints an error message from a string (for compatibility)
func PrintErrorString(message string) {
	fmt.Printf("%s %s\n", ColorError("✗"), ColorError(message))
}

// PrintWarning prints a warning message (global function for compatibility)
func PrintWarning(message string) {
	ShowWarning(message)
}

// PrintInfo prints an information message (global function for compatibility)
func PrintInfo(message string) {
	ShowInfo(message)
}

// PrintSection prints a section header
func PrintSection(title string) {
	fmt.Printf("\n%s %s\n", ColorBold("▶"), ColorBold(title))
	fmt.Println(strings.Repeat("─", 50))
}

// PrintSubsection prints a subsection header
func PrintSubsection(title string) {
	fmt.Printf("\n  %s %s\n", ColorInfo("▸"), ColorInfo(title))
}

// PrintKeyValue prints a key-value pair in a formatted way
func PrintKeyValue(key, value string) {
	fmt.Printf("  %-20s %s\n", ColorDim(key+":"), value)
}

// AddToHistory adds a command to the UIManager history
func (um *UIManager) AddToHistory(command string) {
	um.history = append(um.history, command)
}

// GetHistory returns the command history
func (um *UIManager) GetHistory() []string {
	return um.history
}

// GetUI returns the underlying UI instance
func (um *UIManager) GetUI() *UI {
	return um.ui
}

// ShowHeader displays a header message
func (um *UIManager) ShowHeader(title string) {
	ShowHeader(title)
}

// CreateProgress creates a progress tracker
func (um *UIManager) CreateProgress(label string, total int) *ProgressTracker {
	return &ProgressTracker{
		label: label,
		total: total,
		ui:    um.ui,
	}
}

// ProgressTracker represents a progress tracking interface
type ProgressTracker struct {
	label string
	total int
	ui    *UI
}

// Start begins the progress tracking
func (pt *ProgressTracker) Start() {
	pt.ui.Printf("Starting %s...\n", pt.label)
}

// Update updates the progress
func (pt *ProgressTracker) Update(current int) {
	if !pt.ui.IsQuiet() {
		fmt.Printf("\r%s: %d/%d", pt.label, current, pt.total)
	}
}

// Finish completes the progress tracking
func (pt *ProgressTracker) Finish() {
	pt.ui.Printf("\n%s completed.\n", pt.label)
}

// UpdateMessage updates the progress message
func (pt *ProgressTracker) UpdateMessage(message string) {
	pt.ui.Printf("\r%s: %s", pt.label, message)
}

// MultiSelect displays a multi-select prompt
func MultiSelect(message string, options []string) ([]string, error) {
	selected := []string{}
	prompt := &survey.MultiSelect{
		Message:  message,
		Options:  options,
		PageSize: 10,
	}
	
	err := survey.AskOne(prompt, &selected)
	return selected, err
}

// Input displays a text input prompt
func Input(message, defaultValue, help string) (string, error) {
	var result string
	prompt := &survey.Input{
		Message: message,
		Default: defaultValue,
		Help:    help,
	}
	
	err := survey.AskOne(prompt, &result)
	return result, err
}

// Password displays a password input prompt
func Password(message, help string) (string, error) {
	var result string
	prompt := &survey.Password{
		Message: message,
		Help:    help,
	}
	
	err := survey.AskOne(prompt, &result)
	return result, err
}

// Select displays a selection prompt
func Select(message string, options []string) (string, error) {
	var result string
	prompt := &survey.Select{
		Message:  message,
		Options:  options,
		PageSize: 10,
	}
	
	err := survey.AskOne(prompt, &result)
	return result, err
}

// SearchableSelect displays a searchable selection prompt
func SearchableSelect(message string, options []string) (string, error) {
	var result string
	prompt := &survey.Select{
		Message: message,
		Options: options,
		PageSize: 10,
		VimMode: true,
		Filter: func(filter string, value string, index int) bool {
			// Case-insensitive search
			return strings.Contains(
				strings.ToLower(value),
				strings.ToLower(filter),
			)
		},
	}
	
	err := survey.AskOne(prompt, &result)
	return result, err
}

// ShowDeploymentSummary displays a deployment summary
func ShowDeploymentSummary(files []string, commit string) {
	ShowHeader("Deployment Summary")
	
	fmt.Printf("\n%s %s\n", ColorBold("Commit:"), commit[:7])
	fmt.Printf("%s %d files\n\n", ColorBold("Files:"), len(files))
	
	if len(files) <= 10 {
		for _, file := range files {
			fmt.Printf("  • %s\n", file)
		}
	} else {
		// Show first 5 and last 5
		for i := 0; i < 5; i++ {
			fmt.Printf("  • %s\n", files[i])
		}
		fmt.Printf("  %s\n", ColorDim(fmt.Sprintf("... %d more files ...", len(files)-10)))
		for i := len(files) - 5; i < len(files); i++ {
			fmt.Printf("  • %s\n", files[i])
		}
	}
	
	fmt.Println()
}

// ShowFileExecution displays current file execution status
func ShowFileExecution(file string, current, total int) {
	fmt.Printf("\n%s Executing [%d/%d]: %s\n",
		ColorProgress("►"),
		current,
		total,
		ColorBold(file),
	)
}

// ShowExecutionResult displays the result of a file execution
func ShowExecutionResult(file string, success bool, message string, duration string) {
	if success {
		fmt.Printf("  %s %s (%s)\n",
			ColorSuccess("✓"),
			file,
			ColorDim(duration),
		)
	} else {
		fmt.Printf("  %s %s\n",
			ColorError("✗"),
			file,
		)
		if message != "" {
			fmt.Printf("    %s\n", ColorError(message))
		}
	}
}

// ClearScreen clears the terminal screen
func ClearScreen() {
	fmt.Print("\033[H\033[2J")
}


// ShowLogo displays the application logo
func ShowLogo() {
	logo := `
   _____ _       _        ____                  
  |  ___| | __ _| | _____|  _ \ _ __ ___  _ __  
  | |_  | |/ _` + "`" + ` | |/ / _ \ | | | '__/ _ \| '_ \ 
  |  _| | | (_| |   <  __/ |_| | | | (_) | |_) |
  |_|   |_|\__,_|_|\_\___|____/|_|  \___/| .__/ 
                                          |_|    
           Deploy to Snowflake with confidence              
`
	fmt.Println(ColorInfo(logo))
}
// ShowHelp displays contextual help
func ShowHelp(commands map[string]string) {
	fmt.Println(ColorBold("\nAvailable Commands:"))
	fmt.Println(strings.Repeat("─", 50))
	
	for cmd, desc := range commands {
		fmt.Printf("  %-20s %s\n", ColorInfo(cmd), desc)
	}
	
	fmt.Println()
}