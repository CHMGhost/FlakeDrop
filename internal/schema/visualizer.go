package schema

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

// Visualizer provides visual representation of schema differences
type Visualizer struct {
	useColor bool
	width    int
}

// NewVisualizer creates a new visualizer
func NewVisualizer(useColor bool) *Visualizer {
	return &Visualizer{
		useColor: useColor,
		width:    120, // Default terminal width
	}
}

// SetWidth sets the display width
func (v *Visualizer) SetWidth(width int) {
	v.width = width
}

// DisplaySideBySide displays differences in a side-by-side format
func (v *Visualizer) DisplaySideBySide(diff Difference) string {
	var buf strings.Builder

	// Header
	v.writeHeader(&buf, diff)

	// Display based on difference type
	switch diff.DiffType {
	case DiffTypeAdded:
		v.displayAdded(&buf, diff)
	case DiffTypeRemoved:
		v.displayRemoved(&buf, diff)
	case DiffTypeModified:
		v.displayModified(&buf, diff)
	}

	return buf.String()
}

// DisplaySummaryTable displays a summary table of all differences
func (v *Visualizer) DisplaySummaryTable(result *ComparisonResult) string {
	var buf strings.Builder

	// Create table
	table := tablewriter.NewWriter(&buf)
	table.SetHeader([]string{"#", "Object", "Type", "Status", "Details"})
	table.SetBorder(false)
	table.SetAutoWrapText(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	// Add rows
	for i, diff := range result.Differences {
		status := string(diff.DiffType)
		if v.useColor {
			switch diff.DiffType {
			case DiffTypeAdded:
				status = color.GreenString("+ADDED")
			case DiffTypeRemoved:
				status = color.RedString("-REMOVED")
			case DiffTypeModified:
				status = color.YellowString("~MODIFIED")
			}
		}

		objectName := fmt.Sprintf("%s.%s.%s", diff.Database, diff.Schema, diff.ObjectName)
		details := diff.Description
		if len(diff.Details) > 0 {
			details = fmt.Sprintf("%d changes", len(diff.Details))
		}

		table.Append([]string{
			fmt.Sprintf("%d", i+1),
			objectName,
			string(diff.ObjectType),
			status,
			details,
		})
	}

	table.Render()
	return buf.String()
}

// DisplayDiffDetails displays detailed differences for an object
func (v *Visualizer) DisplayDiffDetails(diff Difference) string {
	var buf strings.Builder

	// Object header
	buf.WriteString(v.formatObjectHeader(diff))
	buf.WriteString("\n")

	// Display details
	if len(diff.Details) == 0 {
		buf.WriteString("No detailed differences available.\n")
	} else {
		for _, detail := range diff.Details {
			buf.WriteString(v.formatDetailLine(detail))
			buf.WriteString("\n")
		}
	}

	// Display SQL definitions if available
	if diff.SourceObject != nil && diff.TargetObject != nil {
		if diff.SourceObject.Definition != "" || diff.TargetObject.Definition != "" {
			buf.WriteString("\n")
			buf.WriteString(v.formatDefinitionDiff(diff))
		}
	}

	return buf.String()
}

// Helper methods

func (v *Visualizer) writeHeader(buf *strings.Builder, diff Difference) {
	header := fmt.Sprintf("=== %s %s.%s.%s ===", 
		diff.ObjectType, diff.Database, diff.Schema, diff.ObjectName)
	
	if v.useColor {
		switch diff.DiffType {
		case DiffTypeAdded:
			header = color.GreenString(header)
		case DiffTypeRemoved:
			header = color.RedString(header)
		case DiffTypeModified:
			header = color.YellowString(header)
		}
	}
	
	buf.WriteString(header)
	buf.WriteString("\n\n")
}

func (v *Visualizer) displayAdded(buf *strings.Builder, diff Difference) {
	leftCol := "[ NOT PRESENT ]"
	rightCol := v.formatObjectInfo(diff.TargetObject)

	if v.useColor {
		leftCol = color.RedString(leftCol)
		rightCol = color.GreenString(rightCol)
	}

	v.writeSideBySide(buf, "SOURCE", "TARGET", leftCol, rightCol)
}

func (v *Visualizer) displayRemoved(buf *strings.Builder, diff Difference) {
	leftCol := v.formatObjectInfo(diff.SourceObject)
	rightCol := "[ NOT PRESENT ]"

	if v.useColor {
		leftCol = color.GreenString(leftCol)
		rightCol = color.RedString(rightCol)
	}

	v.writeSideBySide(buf, "SOURCE", "TARGET", leftCol, rightCol)
}

func (v *Visualizer) displayModified(buf *strings.Builder, diff Difference) {
	leftCol := v.formatObjectInfo(diff.SourceObject)
	rightCol := v.formatObjectInfo(diff.TargetObject)

	v.writeSideBySide(buf, "SOURCE", "TARGET", leftCol, rightCol)

	// Show detailed changes
	if len(diff.Details) > 0 {
		buf.WriteString("\nChanges:\n")
		for _, detail := range diff.Details {
			change := fmt.Sprintf("  %s: %v → %v", 
				detail.Property, detail.SourceValue, detail.TargetValue)
			
			if v.useColor {
				change = color.YellowString(change)
			}
			
			buf.WriteString(change)
			buf.WriteString("\n")
		}
	}
}

func (v *Visualizer) formatObjectInfo(obj *SchemaObject) string {
	if obj == nil {
		return ""
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("Name: %s", obj.Name))
	lines = append(lines, fmt.Sprintf("Type: %s", obj.Type))
	lines = append(lines, fmt.Sprintf("Owner: %s", obj.Owner))
	
	if obj.Comment != "" {
		lines = append(lines, fmt.Sprintf("Comment: %s", obj.Comment))
	}

	lines = append(lines, fmt.Sprintf("Created: %s", obj.CreatedOn.Format("2006-01-02 15:04:05")))
	
	if !obj.LastModified.IsZero() {
		lines = append(lines, fmt.Sprintf("Modified: %s", obj.LastModified.Format("2006-01-02 15:04:05")))
	}

	return strings.Join(lines, "\n")
}

func (v *Visualizer) writeSideBySide(buf *strings.Builder, leftHeader, rightHeader, leftContent, rightContent string) {
	colWidth := (v.width - 3) / 2 // Account for separator

	// Headers
	buf.WriteString(fmt.Sprintf("%-*s | %s\n", colWidth, leftHeader, rightHeader))
	buf.WriteString(strings.Repeat("-", v.width))
	buf.WriteString("\n")

	// Content
	leftLines := strings.Split(leftContent, "\n")
	rightLines := strings.Split(rightContent, "\n")

	maxLines := len(leftLines)
	if len(rightLines) > maxLines {
		maxLines = len(rightLines)
	}

	for i := 0; i < maxLines; i++ {
		leftLine := ""
		if i < len(leftLines) {
			leftLine = leftLines[i]
		}
		
		rightLine := ""
		if i < len(rightLines) {
			rightLine = rightLines[i]
		}

		// Truncate if necessary
		if len(leftLine) > colWidth {
			leftLine = leftLine[:colWidth-3] + "..."
		}
		if len(rightLine) > colWidth {
			rightLine = rightLine[:colWidth-3] + "..."
		}

		buf.WriteString(fmt.Sprintf("%-*s | %s\n", colWidth, leftLine, rightLine))
	}
}

func (v *Visualizer) formatObjectHeader(diff Difference) string {
	header := fmt.Sprintf("%s %s.%s.%s [%s]",
		diff.ObjectType, diff.Database, diff.Schema, diff.ObjectName, diff.DiffType)

	if v.useColor {
		switch diff.DiffType {
		case DiffTypeAdded:
			return color.GreenString(header)
		case DiffTypeRemoved:
			return color.RedString(header)
		case DiffTypeModified:
			return color.YellowString(header)
		}
	}

	return header
}

func (v *Visualizer) formatDetailLine(detail DifferenceDetail) string {
	line := fmt.Sprintf("  • %s: %v → %v", 
		detail.Property, detail.SourceValue, detail.TargetValue)

	if v.useColor {
		return color.YellowString(line)
	}

	return line
}

func (v *Visualizer) formatDefinitionDiff(diff Difference) string {
	var buf strings.Builder

	buf.WriteString("SQL Definitions:\n")
	buf.WriteString(strings.Repeat("-", v.width))
	buf.WriteString("\n")

	sourceDef := ""
	targetDef := ""

	if diff.SourceObject != nil {
		sourceDef = diff.SourceObject.Definition
	}
	if diff.TargetObject != nil {
		targetDef = diff.TargetObject.Definition
	}

	// Simple line-based diff
	sourceLines := strings.Split(sourceDef, "\n")
	targetLines := strings.Split(targetDef, "\n")

	maxLines := len(sourceLines)
	if len(targetLines) > maxLines {
		maxLines = len(targetLines)
	}

	for i := 0; i < maxLines; i++ {
		sourceLine := ""
		targetLine := ""
		
		if i < len(sourceLines) {
			sourceLine = sourceLines[i]
		}
		if i < len(targetLines) {
			targetLine = targetLines[i]
		}

		if sourceLine != targetLine {
			if v.useColor {
				if sourceLine != "" && targetLine == "" {
					buf.WriteString(color.RedString("- " + sourceLine))
				} else if sourceLine == "" && targetLine != "" {
					buf.WriteString(color.GreenString("+ " + targetLine))
				} else {
					buf.WriteString(color.YellowString("~ " + targetLine))
				}
			} else {
				if sourceLine != "" && targetLine == "" {
					buf.WriteString("- " + sourceLine)
				} else if sourceLine == "" && targetLine != "" {
					buf.WriteString("+ " + targetLine)
				} else {
					buf.WriteString("~ " + targetLine)
				}
			}
		} else {
			buf.WriteString("  " + sourceLine)
		}
		buf.WriteString("\n")
	}

	return buf.String()
}

// DisplayProgressBar displays a progress bar for long operations
func (v *Visualizer) DisplayProgressBar(current, total int, message string) string {
	percentage := float64(current) / float64(total) * 100
	barWidth := 40
	filled := int(float64(barWidth) * float64(current) / float64(total))

	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	
	return fmt.Sprintf("\r%s [%s] %.1f%% (%d/%d)", message, bar, percentage, current, total)
}