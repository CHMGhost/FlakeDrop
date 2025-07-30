package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cobra"
	
	"flakedrop/internal/schema"
	"flakedrop/internal/ui"
)

var (
	listVersions   bool
	version1       string
	version2       string
	cleanupDays    int
)

var compareVersionsCmd = &cobra.Command{
	Use:   "versions",
	Short: "Manage and compare schema versions",
	Long:  `List, compare, and manage saved schema versions across environments.`,
	RunE:  runCompareVersions,
}

func init() {
	compareCmd.AddCommand(compareVersionsCmd)

	// Version management flags
	compareVersionsCmd.Flags().BoolVarP(&listVersions, "list", "l", false, "List all saved versions")
	compareVersionsCmd.Flags().StringVar(&version1, "v1", "", "First version to compare")
	compareVersionsCmd.Flags().StringVar(&version2, "v2", "", "Second version to compare")
	compareVersionsCmd.Flags().IntVar(&cleanupDays, "cleanup", 0, "Remove versions older than N days")
}

func runCompareVersions(cmd *cobra.Command, args []string) error {
	// Get version store path
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}
	versionPath := filepath.Join(homeDir, ".flakedrop", "schema-versions")

	// Create version store (service is nil as we're working with saved versions)
	versionStore := schema.NewVersionStore(versionPath, nil)

	// Handle cleanup
	if cleanupDays > 0 {
		return handleCleanup(versionStore, cleanupDays)
	}

	// Handle list versions
	if listVersions {
		return handleListVersions(versionStore)
	}

	// Handle version comparison
	if version1 != "" && version2 != "" {
		return handleVersionComparison(versionStore, version1, version2)
	}

	// Interactive mode
	return runInteractiveVersionMode(versionStore)
}

func handleCleanup(store *schema.VersionStore, days int) error {
	fmt.Printf("Cleaning up versions older than %d days...\n", days)

	// Get all environments
	versionsDir := filepath.Join(store.GetBasePath(), "versions")
	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		return fmt.Errorf("failed to read versions directory: %w", err)
	}

	maxAge := time.Duration(days) * 24 * time.Hour
	
	for _, entry := range entries {
		if entry.IsDir() {
			env := entry.Name()
			fmt.Printf("\nCleaning up %s...\n", env)
			if err := store.CleanupOldVersions(env, maxAge); err != nil {
				fmt.Printf("Warning: failed to cleanup %s: %v\n", env, err)
			}
		}
	}

	return nil
}

func handleListVersions(store *schema.VersionStore) error {
	// Get all environments
	versionsDir := filepath.Join(store.GetBasePath(), "versions")
	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		return fmt.Errorf("failed to read versions directory: %w", err)
	}

	fmt.Println("Saved Schema Versions:")
	fmt.Println(strings.Repeat("=", 80))

	for _, entry := range entries {
		if entry.IsDir() {
			env := entry.Name()
			versions, err := store.ListVersions(env)
			if err != nil {
				fmt.Printf("\nError reading versions for %s: %v\n", env, err)
				continue
			}

			if len(versions) > 0 {
				fmt.Printf("\n%s:\n", env)
				fmt.Println(strings.Repeat("-", 40))
				
				for _, v := range versions {
					fmt.Printf("  %s - %s", v.Version, v.CapturedAt.Format("2006-01-02 15:04:05"))
					if v.Description != "" {
						fmt.Printf(" - %s", v.Description)
					}
					fmt.Printf(" (%d objects)\n", len(v.Objects))
				}
			}
		}
	}

	return nil
}

func handleVersionComparison(store *schema.VersionStore, v1, v2 string) error {
	// Parse version strings (format: environment:version)
	env1, ver1, err := parseVersionString(v1)
	if err != nil {
		return fmt.Errorf("invalid version 1 format: %w", err)
	}

	env2, ver2, err := parseVersionString(v2)
	if err != nil {
		return fmt.Errorf("invalid version 2 format: %w", err)
	}

	// Load and compare versions
	fmt.Printf("Comparing versions:\n")
	fmt.Printf("  Version 1: %s (%s)\n", ver1, env1)
	fmt.Printf("  Version 2: %s (%s)\n\n", ver2, env2)

	result, err := store.CompareVersions(env1, ver1, env2, ver2)
	if err != nil {
		return fmt.Errorf("failed to compare versions: %w", err)
	}

	// Display results
	displaySummary(result)

	// Generate report if output is specified
	if outputFile != "" {
		reporter := schema.NewReporter(result)
		report, err := reporter.GenerateReport(schema.ReportFormat(outputFormat))
		if err != nil {
			return fmt.Errorf("failed to generate report: %w", err)
		}

		if err := os.WriteFile(outputFile, []byte(report.Content), 0600); err != nil {
			return fmt.Errorf("failed to save report: %w", err)
		}
		fmt.Printf("\nReport saved to: %s\n", outputFile)
	}

	return nil
}

func runInteractiveVersionMode(store *schema.VersionStore) error {
	uiInstance := ui.NewUI(false, false)
	uiManager := ui.NewUIManager(uiInstance)
	uiManager.ShowHeader("Schema Version Management")

	// Get all environments with versions
	versionsDir := filepath.Join(store.GetBasePath(), "versions")
	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		return fmt.Errorf("failed to read versions directory: %w", err)
	}

	var environments []string
	for _, entry := range entries {
		if entry.IsDir() {
			environments = append(environments, entry.Name())
		}
	}

	if len(environments) == 0 {
		fmt.Println("No saved versions found.")
		return nil
	}

	for {
		var action string
		prompt := &survey.Select{
			Message: "What would you like to do?",
			Options: []string{
				"Compare two versions",
				"View version details",
				"List all versions",
				"Cleanup old versions",
				"Exit",
			},
		}

		if err := survey.AskOne(prompt, &action); err != nil {
			return err
		}

		switch action {
		case "Compare two versions":
			if err := interactiveCompareVersions(store, environments); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "View version details":
			if err := interactiveViewVersion(store, environments); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "List all versions":
			if err := handleListVersions(store); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "Cleanup old versions":
			if err := interactiveCleanup(store, environments); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "Exit":
			return nil
		}
	}
}

func interactiveCompareVersions(store *schema.VersionStore, environments []string) error {
	// Select first environment
	var env1 string
	prompt1 := &survey.Select{
		Message: "Select first environment:",
		Options: environments,
	}
	if err := survey.AskOne(prompt1, &env1); err != nil {
		return err
	}

	// Get versions for first environment
	versions1, err := store.ListVersions(env1)
	if err != nil {
		return err
	}

	if len(versions1) == 0 {
		return fmt.Errorf("no versions found for %s", env1)
	}

	// Select first version
	options1 := make([]string, len(versions1))
	for i, v := range versions1 {
		options1[i] = fmt.Sprintf("%s - %s", v.Version, v.CapturedAt.Format("2006-01-02 15:04:05"))
		if v.Description != "" {
			options1[i] += " - " + v.Description
		}
	}

	var selectedIdx1 int
	prompt2 := &survey.Select{
		Message: "Select first version:",
		Options: options1,
	}
	if err := survey.AskOne(prompt2, &selectedIdx1); err != nil {
		return err
	}

	// Select second environment
	var env2 string
	prompt3 := &survey.Select{
		Message: "Select second environment:",
		Options: environments,
	}
	if err := survey.AskOne(prompt3, &env2); err != nil {
		return err
	}

	// Get versions for second environment
	versions2, err := store.ListVersions(env2)
	if err != nil {
		return err
	}

	if len(versions2) == 0 {
		return fmt.Errorf("no versions found for %s", env2)
	}

	// Select second version
	options2 := make([]string, len(versions2))
	for i, v := range versions2 {
		options2[i] = fmt.Sprintf("%s - %s", v.Version, v.CapturedAt.Format("2006-01-02 15:04:05"))
		if v.Description != "" {
			options2[i] += " - " + v.Description
		}
	}

	var selectedIdx2 int
	prompt4 := &survey.Select{
		Message: "Select second version:",
		Options: options2,
	}
	if err := survey.AskOne(prompt4, &selectedIdx2); err != nil {
		return err
	}

	// Compare versions
	result, err := store.CompareVersions(env1, versions1[selectedIdx1].Version, env2, versions2[selectedIdx2].Version)
	if err != nil {
		return err
	}

	// Display results
	fmt.Println("\n" + strings.Repeat("=", 80))
	displaySummary(result)

	// Ask if user wants to save the report
	var saveReport bool
	promptSave := &survey.Confirm{
		Message: "Save comparison report?",
		Default: false,
	}
	if err := survey.AskOne(promptSave, &saveReport); err == nil && saveReport {
		_ = saveComparisonResults(result)
	}

	return nil
}

func interactiveViewVersion(store *schema.VersionStore, environments []string) error {
	// Select environment
	var env string
	prompt1 := &survey.Select{
		Message: "Select environment:",
		Options: environments,
	}
	if err := survey.AskOne(prompt1, &env); err != nil {
		return err
	}

	// Get versions
	versions, err := store.ListVersions(env)
	if err != nil {
		return err
	}

	if len(versions) == 0 {
		return fmt.Errorf("no versions found for %s", env)
	}

	// Select version
	options := make([]string, len(versions))
	for i, v := range versions {
		options[i] = fmt.Sprintf("%s - %s", v.Version, v.CapturedAt.Format("2006-01-02 15:04:05"))
		if v.Description != "" {
			options[i] += " - " + v.Description
		}
	}

	var selectedIdx int
	prompt2 := &survey.Select{
		Message: "Select version to view:",
		Options: options,
	}
	if err := survey.AskOne(prompt2, &selectedIdx); err != nil {
		return err
	}

	// Display version details
	version := versions[selectedIdx]
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("Version: %s\n", version.Version)
	fmt.Printf("Environment: %s\n", version.Environment)
	fmt.Printf("Captured: %s\n", version.CapturedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Captured By: %s\n", version.CapturedBy)
	if version.Description != "" {
		fmt.Printf("Description: %s\n", version.Description)
	}
	fmt.Printf("Total Objects: %d\n\n", len(version.Objects))

	// Count objects by type
	byType := make(map[schema.ObjectType]int)
	for _, obj := range version.Objects {
		byType[obj.Type]++
	}

	fmt.Println("Objects by Type:")
	for objType, count := range byType {
		fmt.Printf("  %s: %d\n", objType, count)
	}

	return nil
}

func interactiveCleanup(store *schema.VersionStore, environments []string) error {
	var days int
	prompt := &survey.Input{
		Message: "Remove versions older than (days):",
		Default: "30",
	}
	
	var daysStr string
	if err := survey.AskOne(prompt, &daysStr); err != nil {
		return err
	}
	
	if _, err := fmt.Sscanf(daysStr, "%d", &days); err != nil {
		return fmt.Errorf("invalid number of days")
	}

	// Confirm
	var confirm bool
	confirmPrompt := &survey.Confirm{
		Message: fmt.Sprintf("Remove all versions older than %d days?", days),
		Default: false,
	}
	if err := survey.AskOne(confirmPrompt, &confirm); err != nil || !confirm {
		return nil
	}

	// Cleanup
	maxAge := time.Duration(days) * 24 * time.Hour
	for _, env := range environments {
		fmt.Printf("Cleaning up %s...\n", env)
		if err := store.CleanupOldVersions(env, maxAge); err != nil {
			fmt.Printf("Warning: failed to cleanup %s: %v\n", env, err)
		}
	}

	return nil
}

func parseVersionString(versionStr string) (environment, version string, err error) {
	parts := strings.Split(versionStr, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected format: environment:version")
	}
	return parts[0], parts[1], nil
}