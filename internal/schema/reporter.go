package schema

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"time"
)

// Reporter generates comparison reports in various formats
type Reporter struct {
	result *ComparisonResult
}

// NewReporter creates a new reporter
func NewReporter(result *ComparisonResult) *Reporter {
	return &Reporter{
		result: result,
	}
}

// GenerateReport generates a report in the specified format
func (r *Reporter) GenerateReport(format ReportFormat) (*ComparisonReport, error) {
	report := &ComparisonReport{
		Result:      *r.result,
		Format:      format,
		GeneratedAt: time.Now(),
		Sections:    []ReportSection{},
	}

	var err error
	switch format {
	case ReportFormatText:
		report.Content, err = r.generateTextReport()
	case ReportFormatHTML:
		report.Content, err = r.generateHTMLReport()
	case ReportFormatJSON:
		report.Content, err = r.generateJSONReport()
	case ReportFormatMarkdown:
		report.Content, err = r.generateMarkdownReport()
	case ReportFormatCSV:
		report.Content, err = r.generateCSVReport()
	default:
		err = fmt.Errorf("unsupported report format: %s", format)
	}

	return report, err
}

// generateTextReport generates a plain text report
func (r *Reporter) generateTextReport() (string, error) {
	var buf bytes.Buffer

	// Header
	buf.WriteString("SCHEMA COMPARISON REPORT\n")
	buf.WriteString(strings.Repeat("=", 80) + "\n\n")
	
	buf.WriteString(fmt.Sprintf("Generated: %s\n", time.Now().Format(time.RFC3339)))
	buf.WriteString(fmt.Sprintf("Source Environment: %s\n", r.result.SourceEnvironment))
	buf.WriteString(fmt.Sprintf("Target Environment: %s\n", r.result.TargetEnvironment))
	buf.WriteString("\n")

	// Summary
	buf.WriteString("SUMMARY\n")
	buf.WriteString(strings.Repeat("-", 40) + "\n")
	buf.WriteString(fmt.Sprintf("Total Objects: %d\n", r.result.Summary.TotalObjects))
	buf.WriteString(fmt.Sprintf("Matching Objects: %d\n", r.result.Summary.MatchingObjects))
	buf.WriteString(fmt.Sprintf("Different Objects: %d\n", r.result.Summary.DifferentObjects))
	buf.WriteString(fmt.Sprintf("  - Added: %d\n", r.result.Summary.AddedObjects))
	buf.WriteString(fmt.Sprintf("  - Removed: %d\n", r.result.Summary.RemovedObjects))
	buf.WriteString(fmt.Sprintf("  - Modified: %d\n", r.result.Summary.ModifiedObjects))
	buf.WriteString("\n")

	// By Object Type
	buf.WriteString("BY OBJECT TYPE\n")
	buf.WriteString(strings.Repeat("-", 40) + "\n")
	
	for objType, summary := range r.result.Summary.ByObjectType {
		if summary.Total > 0 {
			buf.WriteString(fmt.Sprintf("\n%s:\n", objType))
			buf.WriteString(fmt.Sprintf("  Total: %d\n", summary.Total))
			buf.WriteString(fmt.Sprintf("  Matching: %d\n", summary.Matching))
			if summary.Added > 0 {
				buf.WriteString(fmt.Sprintf("  Added: %d\n", summary.Added))
			}
			if summary.Removed > 0 {
				buf.WriteString(fmt.Sprintf("  Removed: %d\n", summary.Removed))
			}
			if summary.Modified > 0 {
				buf.WriteString(fmt.Sprintf("  Modified: %d\n", summary.Modified))
			}
		}
	}
	buf.WriteString("\n")

	// Detailed Differences
	if len(r.result.Differences) > 0 {
		buf.WriteString("DETAILED DIFFERENCES\n")
		buf.WriteString(strings.Repeat("=", 80) + "\n\n")

		currentType := ObjectType("")
		for i, diff := range r.result.Differences {
			// Add type separator
			if diff.ObjectType != currentType {
				if i > 0 {
					buf.WriteString("\n")
				}
				buf.WriteString(fmt.Sprintf("%s OBJECTS\n", diff.ObjectType))
				buf.WriteString(strings.Repeat("-", 40) + "\n\n")
				currentType = diff.ObjectType
			}

			// Object info
			buf.WriteString(fmt.Sprintf("[%d] %s.%s.%s\n", i+1, diff.Database, diff.Schema, diff.ObjectName))
			buf.WriteString(fmt.Sprintf("    Status: %s\n", diff.DiffType))
			buf.WriteString(fmt.Sprintf("    %s\n", diff.Description))

			// Details
			if len(diff.Details) > 0 {
				buf.WriteString("    Changes:\n")
				for _, detail := range diff.Details {
					buf.WriteString(fmt.Sprintf("      - %s: %v → %v\n", 
						detail.Property, detail.SourceValue, detail.TargetValue))
				}
			}
			buf.WriteString("\n")
		}
	}

	return buf.String(), nil
}

// generateHTMLReport generates an HTML report
func (r *Reporter) generateHTMLReport() (string, error) {
	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>Schema Comparison Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1, h2, h3 { color: #333; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .added { color: #008000; }
        .removed { color: #ff0000; }
        .modified { color: #ff8c00; }
        .summary-box { background-color: #f0f0f0; padding: 15px; margin-bottom: 20px; border-radius: 5px; }
        .diff-detail { margin-left: 20px; font-size: 0.9em; }
    </style>
</head>
<body>
    <h1>Schema Comparison Report</h1>
    
    <div class="summary-box">
        <h2>Summary</h2>
        <p><strong>Generated:</strong> {{.GeneratedAt}}</p>
        <p><strong>Source:</strong> {{.Result.SourceEnvironment}}</p>
        <p><strong>Target:</strong> {{.Result.TargetEnvironment}}</p>
        <p><strong>Total Objects:</strong> {{.Result.Summary.TotalObjects}}</p>
        <p><strong>Differences:</strong> {{.Result.Summary.DifferentObjects}} 
           (<span class="added">{{.Result.Summary.AddedObjects}} added</span>, 
            <span class="removed">{{.Result.Summary.RemovedObjects}} removed</span>, 
            <span class="modified">{{.Result.Summary.ModifiedObjects}} modified</span>)</p>
    </div>

    <h2>Object Type Summary</h2>
    <table>
        <tr>
            <th>Object Type</th>
            <th>Total</th>
            <th>Matching</th>
            <th>Added</th>
            <th>Removed</th>
            <th>Modified</th>
        </tr>
        {{range $type, $summary := .Result.Summary.ByObjectType}}
        {{if gt $summary.Total 0}}
        <tr>
            <td>{{$type}}</td>
            <td>{{$summary.Total}}</td>
            <td>{{$summary.Matching}}</td>
            <td class="added">{{$summary.Added}}</td>
            <td class="removed">{{$summary.Removed}}</td>
            <td class="modified">{{$summary.Modified}}</td>
        </tr>
        {{end}}
        {{end}}
    </table>

    {{if .Result.Differences}}
    <h2>Detailed Differences</h2>
    <table>
        <tr>
            <th>#</th>
            <th>Object</th>
            <th>Type</th>
            <th>Status</th>
            <th>Description</th>
        </tr>
        {{range $idx, $diff := .Result.Differences}}
        <tr>
            <td>{{inc $idx}}</td>
            <td>{{$diff.Database}}.{{$diff.Schema}}.{{$diff.ObjectName}}</td>
            <td>{{$diff.ObjectType}}</td>
            <td class="{{lower $diff.DiffType}}">{{$diff.DiffType}}</td>
            <td>
                {{$diff.Description}}
                {{if $diff.Details}}
                <div class="diff-detail">
                    {{range $detail := $diff.Details}}
                    <br>• {{$detail.Property}}: {{$detail.SourceValue}} → {{$detail.TargetValue}}
                    {{end}}
                </div>
                {{end}}
            </td>
        </tr>
        {{end}}
    </table>
    {{end}}
</body>
</html>
`

	// Create template functions
	funcs := template.FuncMap{
		"inc": func(i int) int { return i + 1 },
		"lower": func(s DifferenceType) string { return strings.ToLower(string(s)) },
	}

	// Parse and execute template
	tmpl, err := template.New("report").Funcs(funcs).Parse(htmlTemplate)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, r); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// generateJSONReport generates a JSON report
func (r *Reporter) generateJSONReport() (string, error) {
	data, err := json.MarshalIndent(r.result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// generateMarkdownReport generates a Markdown report
func (r *Reporter) generateMarkdownReport() (string, error) {
	var buf bytes.Buffer

	// Header
	buf.WriteString("# Schema Comparison Report\n\n")
	
	buf.WriteString("## Summary\n\n")
	buf.WriteString(fmt.Sprintf("**Generated:** %s  \n", time.Now().Format(time.RFC3339)))
	buf.WriteString(fmt.Sprintf("**Source Environment:** %s  \n", r.result.SourceEnvironment))
	buf.WriteString(fmt.Sprintf("**Target Environment:** %s  \n\n", r.result.TargetEnvironment))

	// Summary table
	buf.WriteString("### Overall Statistics\n\n")
	buf.WriteString("| Metric | Count |\n")
	buf.WriteString("|--------|-------|\n")
	buf.WriteString(fmt.Sprintf("| Total Objects | %d |\n", r.result.Summary.TotalObjects))
	buf.WriteString(fmt.Sprintf("| Matching Objects | %d |\n", r.result.Summary.MatchingObjects))
	buf.WriteString(fmt.Sprintf("| Different Objects | %d |\n", r.result.Summary.DifferentObjects))
	buf.WriteString(fmt.Sprintf("| Added | %d |\n", r.result.Summary.AddedObjects))
	buf.WriteString(fmt.Sprintf("| Removed | %d |\n", r.result.Summary.RemovedObjects))
	buf.WriteString(fmt.Sprintf("| Modified | %d |\n\n", r.result.Summary.ModifiedObjects))

	// By object type
	buf.WriteString("### By Object Type\n\n")
	buf.WriteString("| Object Type | Total | Matching | Added | Removed | Modified |\n")
	buf.WriteString("|-------------|-------|----------|-------|---------|----------|\n")
	
	for objType, summary := range r.result.Summary.ByObjectType {
		if summary.Total > 0 {
			buf.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %d | %d |\n",
				objType, summary.Total, summary.Matching, 
				summary.Added, summary.Removed, summary.Modified))
		}
	}
	buf.WriteString("\n")

	// Detailed differences
	if len(r.result.Differences) > 0 {
		buf.WriteString("## Detailed Differences\n\n")

		currentType := ObjectType("")
		for i, diff := range r.result.Differences {
			// Add type header
			if diff.ObjectType != currentType {
				buf.WriteString(fmt.Sprintf("### %s Objects\n\n", diff.ObjectType))
				currentType = diff.ObjectType
			}

			// Object details
			buf.WriteString(fmt.Sprintf("#### %d. %s.%s.%s\n\n", 
				i+1, diff.Database, diff.Schema, diff.ObjectName))
			
			statusEmoji := ""
			switch diff.DiffType {
			case DiffTypeAdded:
				statusEmoji = "➕"
			case DiffTypeRemoved:
				statusEmoji = "➖"
			case DiffTypeModified:
				statusEmoji = "🔄"
			}
			
			buf.WriteString(fmt.Sprintf("**Status:** %s %s  \n", statusEmoji, diff.DiffType))
			buf.WriteString(fmt.Sprintf("**Description:** %s  \n\n", diff.Description))

			// Changes
			if len(diff.Details) > 0 {
				buf.WriteString("**Changes:**\n\n")
				for _, detail := range diff.Details {
					buf.WriteString(fmt.Sprintf("- **%s:** `%v` → `%v`\n", 
						detail.Property, detail.SourceValue, detail.TargetValue))
				}
				buf.WriteString("\n")
			}
		}
	}

	return buf.String(), nil
}

// generateCSVReport generates a CSV report
func (r *Reporter) generateCSVReport() (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write header
	header := []string{
		"Index", "Database", "Schema", "Object Name", "Object Type",
		"Difference Type", "Description", "Property", "Source Value", "Target Value",
	}
	if err := writer.Write(header); err != nil {
		return "", err
	}

	// Write data
	for i, diff := range r.result.Differences {
		if len(diff.Details) == 0 {
			// No details, write single row
			row := []string{
				fmt.Sprintf("%d", i+1),
				diff.Database,
				diff.Schema,
				diff.ObjectName,
				string(diff.ObjectType),
				string(diff.DiffType),
				diff.Description,
				"", "", "",
			}
			if err := writer.Write(row); err != nil {
				return "", err
			}
		} else {
			// Write row for each detail
			for _, detail := range diff.Details {
				row := []string{
					fmt.Sprintf("%d", i+1),
					diff.Database,
					diff.Schema,
					diff.ObjectName,
					string(diff.ObjectType),
					string(diff.DiffType),
					diff.Description,
					detail.Property,
					fmt.Sprintf("%v", detail.SourceValue),
					fmt.Sprintf("%v", detail.TargetValue),
				}
				if err := writer.Write(row); err != nil {
					return "", err
				}
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// GenerateSummaryReport generates a brief summary report
func (r *Reporter) GenerateSummaryReport() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("Schema Comparison: %s vs %s\n", 
		r.result.SourceEnvironment, r.result.TargetEnvironment))
	buf.WriteString(fmt.Sprintf("Total differences: %d ", r.result.Summary.DifferentObjects))
	buf.WriteString(fmt.Sprintf("(+%d, -%d, ~%d)\n", 
		r.result.Summary.AddedObjects, 
		r.result.Summary.RemovedObjects, 
		r.result.Summary.ModifiedObjects))

	return buf.String()
}