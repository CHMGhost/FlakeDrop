package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"flakedrop/internal/ui"
)

// uiDemoCmd represents the ui-demo command
var uiDemoCmd = &cobra.Command{
	Use:   "ui-demo",
	Short: "Demonstrate UI components",
	Long: `This command demonstrates various UI components available in the FlakeDrop CLI.
It shows examples of prompts, progress bars, tables, and formatted output.`,
	Run: runUIDemo,
}

func init() {
	rootCmd.AddCommand(uiDemoCmd)
}

func runUIDemo(cmd *cobra.Command, args []string) {
	// Show logo
	ui.ShowLogo()
	
	// Demonstrate different UI components
	option, err := ui.Select(
		"Select a UI component to demonstrate:",
		[]string{
			"Commit Selector",
			"Configuration Wizard",
			"Progress Bar",
			"Spinner",
			"Table Output",
			"Error Formatting",
			"All Components",
		},
	)
	
	if err != nil {
		ui.ShowError(err)
		return
	}
	
	switch option {
	case "Commit Selector":
		demoCommitSelector()
	case "Configuration Wizard":
		demoConfigWizard()
	case "Progress Bar":
		demoProgressBar()
	case "Spinner":
		demoSpinner()
	case "Table Output":
		demoTable()
	case "Error Formatting":
		demoErrorFormatting()
	case "All Components":
		demoAll()
	}
}

func demoCommitSelector() {
	ui.ShowHeader("Commit Selector Demo")
	
	// Create sample commits
	commits := []ui.CommitInfo{
		{
			Hash:      "abc123456789def",
			ShortHash: "abc1234",
			Message:   "Fix database connection pooling issue",
			Author:    "John Doe",
			Time:      time.Now().Add(-2 * time.Hour),
			Files:     5,
		},
		{
			Hash:      "def456789012ghi",
			ShortHash: "def4567",
			Message:   "Add new customer analytics tables",
			Author:    "Jane Smith",
			Time:      time.Now().Add(-5 * time.Hour),
			Files:     12,
		},
		{
			Hash:      "ghi789012345jkl",
			ShortHash: "ghi7890",
			Message:   "Update schema for performance optimization",
			Author:    "Bob Johnson",
			Time:      time.Now().Add(-24 * time.Hour),
			Files:     3,
		},
	}
	
	selected, err := ui.SelectCommit(commits)
	if err != nil {
		ui.ShowError(err)
		return
	}
	
	ui.ShowSuccess(fmt.Sprintf("Selected commit: %s", selected[:7]))
}

func demoConfigWizard() {
	ui.ShowHeader("Configuration Wizard Demo")
	ui.ShowInfo("This would run the full configuration wizard")
	
	// For demo, just show what it would look like
	wizard := ui.NewConfigWizard()
	config, err := wizard.Run()
	if err != nil {
		ui.ShowError(err)
		return
	}
	
	ui.ShowSuccess("Configuration saved successfully!")
	fmt.Printf("Account: %s\n", config.Snowflake.Account)
}

func demoProgressBar() {
	ui.ShowHeader("Progress Bar Demo")
	
	files := []string{
		"tables/users.sql",
		"tables/orders.sql",
		"views/customer_analytics.sql",
		"procedures/update_stats.sql",
		"functions/calculate_revenue.sql",
	}
	
	pb := ui.NewProgressBar(len(files))
	
	for i, file := range files {
		time.Sleep(1 * time.Second)
		// Simulate success/failure
		success := i != 2 // Third file fails for demo
		pb.Update(i+1, file, success)
	}
	
	pb.Finish()
}

func demoSpinner() {
	ui.ShowHeader("Spinner Demo")
	
	operations := []struct {
		message string
		duration time.Duration
		success bool
	}{
		{"Connecting to Snowflake", 2 * time.Second, true},
		{"Validating credentials", 1 * time.Second, true},
		{"Loading configuration", 1500 * time.Millisecond, true},
		{"Checking repository", 2 * time.Second, false},
	}
	
	for _, op := range operations {
		spinner := ui.NewSpinner(op.message)
		spinner.Start()
		time.Sleep(op.duration)
		spinner.Stop(op.success, op.message)
	}
}

func demoTable() {
	ui.ShowHeader("Table Output Demo")
	
	fmt.Println("\nDeployment History:")
	table := ui.NewTable()
	table.AddHeader("Commit", "Date", "Files", "Status", "Duration")
	table.AddRow("abc1234", "2024-01-20", "5", ui.ColorSuccess("Success"), "1m23s")
	table.AddRow("def5678", "2024-01-19", "12", ui.ColorSuccess("Success"), "3m45s")
	table.AddRow("ghi9012", "2024-01-18", "3", ui.ColorError("Failed"), "0m52s")
	table.AddRow("jkl3456", "2024-01-17", "8", ui.ColorSuccess("Success"), "2m10s")
	table.Render()
}

func demoErrorFormatting() {
	ui.ShowHeader("Error Formatting Demo")
	
	// Various error types
	errors := []error{
		fmt.Errorf("authentication failed: invalid username or password"),
		fmt.Errorf("connection refused: unable to reach xy12345.snowflakecomputing.com"),
		fmt.Errorf("syntax error near 'SELEC' at line 42:\n  SELEC * FROM users WHERE active = true"),
		fmt.Errorf("permission denied: role 'ANALYST' does not have privilege 'CREATE TABLE' on schema 'PROD'"),
	}
	
	for _, err := range errors {
		ui.ShowError(err)
		fmt.Println()
	}
}

func demoAll() {
	demos := []func(){
		demoCommitSelector,
		demoProgressBar,
		demoSpinner,
		demoTable,
		demoErrorFormatting,
	}
	
	for _, demo := range demos {
		demo()
		fmt.Println("\nPress Enter to continue...")
		fmt.Scanln()
		ui.ClearScreen()
	}
	
	ui.ShowSuccess("UI demo completed!")
}