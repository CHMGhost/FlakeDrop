//go:build example
// +build example

package main

import (
	"fmt"
	"time"

	"flakedrop/internal/ui"
)

func main() {
	fmt.Println("FlakeDrop - Enhanced UI Demo")
	fmt.Println("===================================\n")

	// Demo 1: Basic UI Features
	fmt.Println("1. Basic UI Features:")
	demoBasicUI()

	// Demo 2: Progress Indicators
	fmt.Println("\n2. Progress Indicators:")
	demoProgressIndicators()

	// Demo 3: Interactive Prompts
	fmt.Println("\n3. Interactive Prompts:")
	demoInteractivePrompts()

	// Demo 4: Tables and Formatting
	fmt.Println("\n4. Tables and Formatting:")
	demoTablesAndFormatting()

	// Demo 5: Error Handling
	fmt.Println("\n5. Error Handling:")
	demoErrorHandling()
}

func demoBasicUI() {
	// Create a UI instance
	uiInstance := ui.NewUI(false, false)
	
	// Show various message types
	uiInstance.Success("Deployment completed successfully")
	uiInstance.Warning("Large dataset detected - deployment may take longer")
	uiInstance.Error("Connection timeout - retrying...")
	uiInstance.Info("Using cached repository data")
	
	// Verbose mode
	verboseUI := ui.NewUI(true, false)
	verboseUI.VerbosePrintf("Debug: Connecting to %s...\n", "account.snowflakecomputing.com")
}

func demoProgressIndicators() {
	// Spinner demo
	fmt.Println("\nSpinner Demo:")
	spinner := ui.NewSpinner("Connecting to Snowflake...")
	spinner.Start()
	time.Sleep(2 * time.Second)
	spinner.UpdateMessage("Executing SQL scripts...")
	time.Sleep(2 * time.Second)
	spinner.Stop(true, "Deployment completed")
	
	// Progress bar demo
	fmt.Println("\nProgress Bar Demo:")
	progressBar := ui.NewProgressBar(10)
	for i := 1; i <= 10; i++ {
		progressBar.Update(i, fmt.Sprintf("file_%d.sql", i), true)
		time.Sleep(300 * time.Millisecond)
	}
	progressBar.Finish()
}

func demoInteractivePrompts() {
	fmt.Println("\nNote: Interactive prompts are available but skipped in demo mode")
	fmt.Println("Available prompt types:")
	fmt.Println("- Input: Text input with default values")
	fmt.Println("- Password: Secure password input")
	fmt.Println("- Select: Single selection from list")
	fmt.Println("- MultiSelect: Multiple selections from list")
	fmt.Println("- SearchableSelect: Select with search capability")
	fmt.Println("- Confirm: Yes/No confirmation")
}

func demoTablesAndFormatting() {
	// Show header
	ui.ShowHeader("Deployment Summary")
	
	// Create a table
	table := ui.NewTable()
	table.AddHeader("Repository", "Branch", "Files", "Status")
	table.AddRow("analytics", "main", "15", ui.ColorSuccess("Deployed"))
	table.AddRow("reporting", "develop", "8", ui.ColorWarning("Pending"))
	table.AddRow("etl", "feature/v2", "23", ui.ColorError("Failed"))
	table.Render()
	
	// Show deployment summary
	files := []string{
		"tables/users.sql",
		"views/user_activity.sql",
		"procedures/update_stats.sql",
		"functions/calculate_revenue.sql",
		"tables/transactions.sql",
		"views/daily_summary.sql",
		"procedures/cleanup_old_data.sql",
		"functions/get_user_score.sql",
		"tables/products.sql",
		"views/product_metrics.sql",
		"procedures/refresh_materialized.sql",
		"functions/format_currency.sql",
	}
	ui.ShowDeploymentSummary(files, "abc123def456")
	
	// Show file execution
	ui.ShowFileExecution("tables/users.sql", 1, 3)
	ui.ShowExecutionResult("tables/users.sql", true, "", "125ms")
	
	ui.ShowFileExecution("views/user_activity.sql", 2, 3)
	ui.ShowExecutionResult("views/user_activity.sql", false, "Syntax error at line 42", "")
	
	// Box formatting
	ui.Box("Configuration", "Account: myaccount\nRole: SYSADMIN\nWarehouse: COMPUTE_WH\nDatabase: PROD")
}

func demoErrorHandling() {
	// Show different error types
	ui.ShowError(fmt.Errorf("connection failed: unable to reach Snowflake"))
	ui.ShowWarning("Deprecated function usage detected in procedures/old_proc.sql")
	ui.ShowInfo("Pro tip: Use --parallel flag to speed up deployments")
	
	// Section headers
	ui.PrintSection("Validation Results")
	ui.PrintKeyValue("Total Files", "25")
	ui.PrintKeyValue("Valid", "23")
	ui.PrintKeyValue("Warnings", "1")
	ui.PrintKeyValue("Errors", "1")
	
	ui.PrintSubsection("Error Details")
	ui.PrintErrorString("File: procedures/broken.sql")
	ui.PrintErrorString("Line 15: Missing semicolon")
	ui.PrintErrorString("Line 22: Undefined variable 'user_id'")
}