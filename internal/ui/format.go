package ui

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mattn/go-isatty"
	"github.com/mgutz/ansi"
)

var (
	// Check if output supports colors
	supportsColor = isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
	
	// Color functions
	ColorSuccess  = colorFunc(ansi.Green)
	ColorError    = colorFunc(ansi.Red)
	ColorWarning  = colorFunc(ansi.Yellow)
	ColorInfo     = colorFunc(ansi.Cyan)
	ColorProgress = colorFunc(ansi.Blue)
	ColorBold     = colorFunc("default+b")
	ColorDim      = colorFunc("default+h")
)

// colorFunc returns a function that colors text if supported
func colorFunc(color string) func(string) string {
	return func(text string) string {
		if supportsColor {
			return ansi.Color(text, color)
		}
		return text
	}
}

// ShowHeader displays a formatted header
func ShowHeader(title string) {
	width := 50
	padding := (width - len(title) - 2) / 2
	
	fmt.Println("\n+" + strings.Repeat("-", width-2) + "+")
	fmt.Printf("|%s%s%s|\n",
		strings.Repeat(" ", padding),
		ColorBold(title),
		strings.Repeat(" ", width-2-padding-len(title)),
	)
	fmt.Println("+" + strings.Repeat("-", width-2) + "+")
}

// ShowError displays a formatted error message
func ShowError(err error) {
	fmt.Printf("\n%s %s\n", ColorError("ERROR:"), ColorError("Error:"))
	
	// Parse error message for better formatting
	message := err.Error()
	lines := strings.Split(message, "\n")
	
	for i, line := range lines {
		if i == 0 {
			fmt.Printf("  %s\n", line)
		} else {
			fmt.Printf("  %s\n", ColorDim(line))
		}
	}
	
	// Add helpful suggestions if applicable
	if suggestion := getSuggestion(message); suggestion != "" {
		fmt.Printf("\n  %s %s\n", ColorInfo("TIP:"), ColorInfo(suggestion))
	}
}

// ShowSuccess displays a success message
func ShowSuccess(message string) {
	fmt.Printf("%s %s\n", ColorSuccess("SUCCESS:"), message)
}

// ShowWarning displays a warning message
func ShowWarning(message string) {
	fmt.Printf("%s %s\n", ColorWarning("WARNING:"), ColorWarning(message))
}

// ShowInfo displays an info message
func ShowInfo(message string) {
	fmt.Printf("%s %s\n", ColorInfo("INFO:"), message)
}

// Table creates a formatted table
type Table struct {
	writer *tabwriter.Writer
}

// NewTable creates a new table
func NewTable() *Table {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	return &Table{writer: w}
}

// AddHeader adds a header row to the table
func (t *Table) AddHeader(columns ...string) {
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = ColorBold(col)
	}
	fmt.Fprintln(t.writer, strings.Join(headers, "\t"))
	
	// Add separator
	separators := make([]string, len(columns))
	for i := range columns {
		separators[i] = strings.Repeat("-", len(columns[i]))
	}
	fmt.Fprintln(t.writer, strings.Join(separators, "\t"))
}

// AddRow adds a data row to the table
func (t *Table) AddRow(values ...string) {
	fmt.Fprintln(t.writer, strings.Join(values, "\t"))
}

// Render displays the table
func (t *Table) Render() {
	t.writer.Flush()
}

// FormatFileChange formats file change statistics
func FormatFileChange(additions, deletions int) string {
	result := ""
	
	if additions > 0 {
		result += ColorSuccess(fmt.Sprintf("+%d", additions))
	}
	
	if additions > 0 && deletions > 0 {
		result += " "
	}
	
	if deletions > 0 {
		result += ColorError(fmt.Sprintf("-%d", deletions))
	}
	
	if result == "" {
		result = ColorDim("0")
	}
	
	return result
}

// Box draws a box around content
func Box(title, content string) {
	lines := strings.Split(content, "\n")
	maxLen := len(title)
	
	for _, line := range lines {
		if len(line) > maxLen {
			maxLen = len(line)
		}
	}
	
	// Top border
	borderLen := maxLen - len(title) - 1
	if borderLen < 0 {
		borderLen = 0
	}
	fmt.Printf("+- %s %s+\n",
		ColorBold(title),
		strings.Repeat("─", borderLen),
	)
	
	// Content
	for _, line := range lines {
		fmt.Printf("| %s%s |\n",
			line,
			strings.Repeat(" ", maxLen-len(line)),
		)
	}
	
	// Bottom border
	fmt.Printf("+%s+\n", strings.Repeat("-", maxLen+3))
}

// getSuggestion returns helpful suggestions based on error messages
func getSuggestion(error string) string {
	lower := strings.ToLower(error)
	
	switch {
	case strings.Contains(lower, "authentication failed"):
		return "Check your username and password in the configuration"
	case strings.Contains(lower, "connection refused"):
		return "Verify your Snowflake account URL and network connectivity"
	case strings.Contains(lower, "syntax error"):
		return "Review the SQL syntax in the affected file"
	case strings.Contains(lower, "permission denied"):
		return "Ensure your role has the necessary privileges"
	case strings.Contains(lower, "object does not exist"):
		return "Verify the database objects exist or check your database/schema context"
	default:
		return ""
	}
}

// Confirm shows a confirmation prompt
func Confirm(message string, defaultValue bool) (bool, error) {
	suffix := " [Y/n]"
	if !defaultValue {
		suffix = " [y/N]"
	}
	
	fmt.Printf("%s%s ", message, suffix)
	
	var response string
	_, err := fmt.Scanln(&response)
	if err != nil && err.Error() != "unexpected newline" {
		return false, err
	}
	
	response = strings.ToLower(strings.TrimSpace(response))
	
	if response == "" {
		return defaultValue, nil
	}
	
	return response == "y" || response == "yes", nil
}