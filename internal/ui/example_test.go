package ui_test

import (
	"fmt"
	"flakedrop/internal/ui"
)


// ExampleTable demonstrates table formatting
func ExampleTable() {
	table := ui.NewTable()
	table.AddHeader("File", "Status", "Time")
	table.AddRow("users.sql", "Success", "1.2s")
	table.AddRow("orders.sql", "Success", "0.8s")
	table.AddRow("products.sql", "Failed", "0.5s")
	table.Render()
	
	// Output:
	// File          Status   Time
	// ────          ──────   ────
	// users.sql     Success  1.2s
	// orders.sql    Success  0.8s
	// products.sql  Failed   0.5s
}


// ExampleConfigWizard demonstrates the configuration wizard
func ExampleConfigWizard() {
	_ = ui.NewConfigWizard() // wizard would be used interactively
	
	// In a real scenario, this would be interactive
	// config, err := wizard.Run()
	
	fmt.Println("Configuration wizard example")
	
	// Output:
	// Configuration wizard example
}