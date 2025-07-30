package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	
	"flakedrop/internal/config"
	"flakedrop/internal/schema"
	"flakedrop/internal/snowflake"
	"flakedrop/internal/ui"
	"flakedrop/pkg/models"
)

var (
	sourceEnv        string
	targetEnv        string
	outputFormat     string
	outputFile       string
	includeTypes     []string
	excludeTypes     []string
	includePatterns  []string
	excludePatterns  []string
	ignoreWhitespace bool
	ignoreComments   bool
	ignoreOwner      bool
	generateSync     bool
	interactive      bool
	saveVersion      bool
	versionDesc      string
)

var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare schemas between Snowflake environments",
	Long: `Compare database schemas between different Snowflake environments to detect drift,
generate synchronization scripts, and maintain schema version history.`,
	RunE: runCompare,
}

func init() {
	rootCmd.AddCommand(compareCmd)

	// Environment flags
	compareCmd.Flags().StringVarP(&sourceEnv, "source", "s", "", "Source environment (required)")
	compareCmd.Flags().StringVarP(&targetEnv, "target", "t", "", "Target environment (required)")
	_ = compareCmd.MarkFlagRequired("source")
	_ = compareCmd.MarkFlagRequired("target")

	// Output flags
	compareCmd.Flags().StringVarP(&outputFormat, "format", "f", "text", "Output format: text, html, json, markdown, csv")
	compareCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file (default: stdout)")

	// Filter flags
	compareCmd.Flags().StringSliceVar(&includeTypes, "include-types", []string{}, "Include only these object types")
	compareCmd.Flags().StringSliceVar(&excludeTypes, "exclude-types", []string{}, "Exclude these object types")
	compareCmd.Flags().StringSliceVar(&includePatterns, "include", []string{}, "Include objects matching these patterns")
	compareCmd.Flags().StringSliceVar(&excludePatterns, "exclude", []string{}, "Exclude objects matching these patterns")

	// Comparison options
	compareCmd.Flags().BoolVar(&ignoreWhitespace, "ignore-whitespace", true, "Ignore whitespace differences in SQL")
	compareCmd.Flags().BoolVar(&ignoreComments, "ignore-comments", true, "Ignore comment differences in SQL")
	compareCmd.Flags().BoolVar(&ignoreOwner, "ignore-owner", false, "Ignore object ownership differences")

	// Sync options
	compareCmd.Flags().BoolVar(&generateSync, "generate-sync", false, "Generate synchronization script")
	compareCmd.Flags().BoolVar(&interactive, "interactive", false, "Interactive mode for selective sync")

	// Version tracking
	compareCmd.Flags().BoolVar(&saveVersion, "save-version", false, "Save schema snapshots for version tracking")
	compareCmd.Flags().StringVar(&versionDesc, "version-desc", "", "Description for saved version")
}

func runCompare(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Get environment configurations
	sourceConfig, err := getEnvironmentConfig(cfg, sourceEnv)
	if err != nil {
		return fmt.Errorf("source environment error: %w", err)
	}

	targetConfig, err := getEnvironmentConfig(cfg, targetEnv)
	if err != nil {
		return fmt.Errorf("target environment error: %w", err)
	}

	// Initialize UI
	uiInstance := ui.NewUI(false, false)
	uiManager := ui.NewUIManager(uiInstance)
	
	// Show comparison header
	uiManager.ShowHeader("Schema Comparison")
	fmt.Printf("Source: %s (%s.%s)\n", sourceEnv, sourceConfig.Database, sourceConfig.Schema)
	fmt.Printf("Target: %s (%s.%s)\n\n", targetEnv, targetConfig.Database, targetConfig.Schema)

	// Create progress tracker
	progress := uiManager.CreateProgress("Comparing schemas", 100)
	progress.Start()

	// Connect to source environment
	progress.UpdateMessage("Connecting to source environment...")
	sourceSF := snowflake.NewService(*sourceConfig)
	if err := sourceSF.Connect(); err != nil {
		progress.Finish()
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceSF.Close()

	// Connect to target environment
	progress.UpdateMessage("Connecting to target environment...")
	targetSF := snowflake.NewService(*targetConfig)
	if err := targetSF.Connect(); err != nil {
		progress.Finish()
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetSF.Close()

	// Create schema service
	sourceSchema := schema.NewService(sourceSF)
	targetSchema := schema.NewService(targetSF)

	// Create version store if needed
	var versionStore *schema.VersionStore
	if saveVersion {
		homeDir, _ := os.UserHomeDir()
		versionPath := filepath.Join(homeDir, ".flakedrop", "schema-versions")
		versionStore = schema.NewVersionStore(versionPath, sourceSchema)
	}

	ctx := context.Background()

	// Capture source schema
	progress.Update(25)
	progress.UpdateMessage("Capturing source schema...")
	
	objectTypes := parseObjectTypes(includeTypes)
	sourceObjects, err := sourceSchema.GetDatabaseObjects(ctx, sourceConfig.Database, sourceConfig.Schema, objectTypes)
	if err != nil {
		progress.Finish()
		return fmt.Errorf("failed to get source objects: %w", err)
	}

	// Save source version if requested
	if saveVersion && versionDesc != "" {
		if _, err := versionStore.CaptureVersion(ctx, sourceEnv, sourceConfig.Database, sourceConfig.Schema, versionDesc); err != nil {
			fmt.Printf("Warning: failed to save source version: %v\n", err)
		}
	}

	// Capture target schema
	progress.Update(50)
	progress.UpdateMessage("Capturing target schema...")
	
	targetObjects, err := targetSchema.GetDatabaseObjects(ctx, targetConfig.Database, targetConfig.Schema, objectTypes)
	if err != nil {
		progress.Finish()
		return fmt.Errorf("failed to get target objects: %w", err)
	}

	// Save target version if requested
	if saveVersion && versionDesc != "" {
		if _, err := versionStore.CaptureVersion(ctx, targetEnv, targetConfig.Database, targetConfig.Schema, versionDesc); err != nil {
			fmt.Printf("Warning: failed to save target version: %v\n", err)
		}
	}

	// Create comparison options
	compOptions := schema.ComparisonOptions{
		IncludeTypes:      objectTypes,
		ExcludeTypes:      parseObjectTypes(excludeTypes),
		IncludePatterns:   includePatterns,
		ExcludePatterns:   excludePatterns,
		IgnoreWhitespace:  ignoreWhitespace,
		IgnoreComments:    ignoreComments,
		IgnoreOwner:       ignoreOwner,
		DeepCompare:       true,
	}

	// Perform comparison
	progress.Update(75)
	progress.UpdateMessage("Analyzing differences...")
	
	comparator := schema.NewComparator(sourceSchema, compOptions)
	
	sourceSnapshot := schema.SchemaSnapshot{
		Environment: sourceEnv,
		Objects:     sourceObjects,
		CapturedAt:  time.Now(),
	}
	
	targetSnapshot := schema.SchemaSnapshot{
		Environment: targetEnv,
		Objects:     targetObjects,
		CapturedAt:  time.Now(),
	}
	
	result, err := comparator.CompareSchemas(ctx, sourceSnapshot, targetSnapshot)
	if err != nil {
		progress.Finish()
		return fmt.Errorf("failed to compare schemas: %w", err)
	}

	progress.Update(100)
	progress.Finish()

	// Display summary
	displaySummary(result)

	// Generate sync script if requested
	if generateSync {
		if err := handleSyncGeneration(result, uiManager); err != nil {
			return err
		}
	}

	// Generate report
	if err := generateReport(result); err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	// Interactive mode
	if interactive && len(result.Differences) > 0 {
		if err := runInteractiveMode(result, uiManager); err != nil {
			return err
		}
	}

	return nil
}

func getEnvironmentConfig(cfg *models.Config, env string) (*snowflake.Config, error) {
	// Check if it's a named environment
	for _, e := range cfg.Environments {
		if e.Name == env {
			return &snowflake.Config{
				Account:   e.Account,
				Username:  e.Username,
				Password:  e.Password,
				Database:  e.Database,
				Schema:    e.Schema,
				Warehouse: e.Warehouse,
				Role:      e.Role,
				Timeout:   30 * time.Second,
			}, nil
		}
	}

	// Try to parse as connection string
	parts := strings.Split(env, ".")
	if len(parts) >= 2 {
		return &snowflake.Config{
			Account:   viper.GetString("snowflake.account"),
			Username:  viper.GetString("snowflake.username"),
			Password:  viper.GetString("snowflake.password"),
			Database:  parts[0],
			Schema:    parts[1],
			Warehouse: viper.GetString("snowflake.warehouse"),
			Role:      viper.GetString("snowflake.role"),
			Timeout:   30 * time.Second,
		}, nil
	}

	return nil, fmt.Errorf("environment '%s' not found in configuration", env)
}

func parseObjectTypes(types []string) []schema.ObjectType {
	var objectTypes []schema.ObjectType
	
	for _, t := range types {
		switch strings.ToUpper(t) {
		case "TABLE":
			objectTypes = append(objectTypes, schema.ObjectTypeTable)
		case "VIEW":
			objectTypes = append(objectTypes, schema.ObjectTypeView)
		case "PROCEDURE":
			objectTypes = append(objectTypes, schema.ObjectTypeProcedure)
		case "FUNCTION":
			objectTypes = append(objectTypes, schema.ObjectTypeFunction)
		case "SEQUENCE":
			objectTypes = append(objectTypes, schema.ObjectTypeSequence)
		case "STAGE":
			objectTypes = append(objectTypes, schema.ObjectTypeStage)
		case "PIPE":
			objectTypes = append(objectTypes, schema.ObjectTypePipe)
		case "STREAM":
			objectTypes = append(objectTypes, schema.ObjectTypeStream)
		case "TASK":
			objectTypes = append(objectTypes, schema.ObjectTypeTask)
		case "FILE_FORMAT":
			objectTypes = append(objectTypes, schema.ObjectTypeFileFormat)
		}
	}
	
	return objectTypes
}

func displaySummary(result *schema.ComparisonResult) {
	visualizer := schema.NewVisualizer(true)
	
	fmt.Println("\nComparison Summary:")
	fmt.Println(strings.Repeat("-", 60))
	
	reporter := schema.NewReporter(result)
	fmt.Println(reporter.GenerateSummaryReport())
	
	if len(result.Differences) > 0 {
		fmt.Println("\nDifferences by Type:")
		fmt.Println(visualizer.DisplaySummaryTable(result))
	} else {
		fmt.Println("\nNo differences found. Schemas are identical.")
	}
}

func handleSyncGeneration(result *schema.ComparisonResult, uiManager *ui.UIManager) error {
	syncOptions := schema.SyncOptions{
		IncludeDrops:    false, // Safe by default
		IncludeCreates:  true,
		IncludeAlters:   true,
		UseTransactions: true,
		GenerateBackup:  true,
		AddComments:     true,
		SafeMode:        true,
	}

	// Ask for sync options in interactive mode
	if interactive {
		prompt := &survey.Confirm{
			Message: "Include DROP statements for objects only in target?",
			Default: false,
		}
		_ = survey.AskOne(prompt, &syncOptions.IncludeDrops)

		prompt = &survey.Confirm{
			Message: "Generate backup statements?",
			Default: true,
		}
		_ = survey.AskOne(prompt, &syncOptions.GenerateBackup)
	}

	generator := schema.NewSyncGenerator(syncOptions)
	syncScript := generator.GenerateSyncScript(result)

	// Save sync script
	syncFile := fmt.Sprintf("sync_%s_to_%s_%s.sql", 
		sourceEnv, targetEnv, time.Now().Format("20060102_150405"))
	
	if outputFile != "" {
		syncFile = strings.TrimSuffix(outputFile, filepath.Ext(outputFile)) + "_sync.sql"
	}

	if err := os.WriteFile(syncFile, []byte(syncScript), 0600); err != nil {
		return fmt.Errorf("failed to save sync script: %w", err)
	}

	fmt.Printf("\nSync script saved to: %s\n", syncFile)
	return nil
}

func generateReport(result *schema.ComparisonResult) error {
	reporter := schema.NewReporter(result)
	
	format := schema.ReportFormat(outputFormat)
	report, err := reporter.GenerateReport(format)
	if err != nil {
		return err
	}

	// Output report
	if outputFile != "" {
		if err := os.WriteFile(outputFile, []byte(report.Content), 0600); err != nil {
			return fmt.Errorf("failed to save report: %w", err)
		}
		fmt.Printf("\nReport saved to: %s\n", outputFile)
	} else if outputFormat != "text" {
		// For non-text formats, always save to file
		filename := fmt.Sprintf("comparison_%s_%s.%s", 
			sourceEnv, targetEnv, outputFormat)
		if err := os.WriteFile(filename, []byte(report.Content), 0600); err != nil {
			return fmt.Errorf("failed to save report: %w", err)
		}
		fmt.Printf("\nReport saved to: %s\n", filename)
	}

	return nil
}

func runInteractiveMode(result *schema.ComparisonResult, uiManager *ui.UIManager) error {
	visualizer := schema.NewVisualizer(true)

	for {
		var choice string
		prompt := &survey.Select{
			Message: "What would you like to do?",
			Options: []string{
				"View detailed differences",
				"Generate selective sync script",
				"Save comparison results",
				"Exit",
			},
		}

		if err := survey.AskOne(prompt, &choice); err != nil {
			return err
		}

		switch choice {
		case "View detailed differences":
			if err := viewDetailedDifferences(result, visualizer); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "Generate selective sync script":
			if err := generateSelectiveSync(result); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "Save comparison results":
			if err := saveComparisonResults(result); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "Exit":
			return nil
		}
	}
}

func viewDetailedDifferences(result *schema.ComparisonResult, visualizer *schema.Visualizer) error {
	// Create options for selection
	options := make([]string, len(result.Differences))
	for i, diff := range result.Differences {
		options[i] = fmt.Sprintf("[%s] %s.%s.%s - %s", 
			diff.DiffType, diff.Database, diff.Schema, diff.ObjectName, diff.Description)
	}

	var selected []int
	prompt := &survey.MultiSelect{
		Message: "Select differences to view details:",
		Options: options,
	}

	if err := survey.AskOne(prompt, &selected); err != nil {
		return err
	}

	// Display selected differences
	for _, idx := range selected {
		if idx < len(result.Differences) {
			fmt.Println("\n" + strings.Repeat("=", 80))
			fmt.Println(visualizer.DisplaySideBySide(result.Differences[idx]))
			fmt.Println(visualizer.DisplayDiffDetails(result.Differences[idx]))
		}
	}

	return nil
}

func generateSelectiveSync(result *schema.ComparisonResult) error {
	// Create options for selection
	options := make([]string, len(result.Differences))
	for i, diff := range result.Differences {
		options[i] = fmt.Sprintf("[%s] %s.%s.%s", 
			diff.DiffType, diff.Database, diff.Schema, diff.ObjectName)
	}

	var selected []int
	prompt := &survey.MultiSelect{
		Message: "Select changes to include in sync script:",
		Options: options,
	}

	if err := survey.AskOne(prompt, &selected); err != nil {
		return err
	}

	if len(selected) == 0 {
		fmt.Println("No changes selected.")
		return nil
	}

	// Generate selective sync script
	syncOptions := schema.SyncOptions{
		IncludeDrops:    false,
		IncludeCreates:  true,
		IncludeAlters:   true,
		UseTransactions: true,
		GenerateBackup:  true,
		AddComments:     true,
		SafeMode:        true,
	}

	generator := schema.NewSyncGenerator(syncOptions)
	
	// Convert selected indices
	indices := make([]int, len(selected))
	for i, s := range selected {
		indices[i] = s
	}
	
	syncScript := generator.GenerateSelectiveSyncScript(result, indices)

	// Save script
	filename := fmt.Sprintf("selective_sync_%s.sql", time.Now().Format("20060102_150405"))
	if err := os.WriteFile(filename, []byte(syncScript), 0600); err != nil {
		return fmt.Errorf("failed to save sync script: %w", err)
	}

	fmt.Printf("\nSelective sync script saved to: %s\n", filename)
	return nil
}

func saveComparisonResults(result *schema.ComparisonResult) error {
	formats := []string{"json", "html", "markdown", "csv"}
	
	var selectedFormat string
	prompt := &survey.Select{
		Message: "Select output format:",
		Options: formats,
	}

	if err := survey.AskOne(prompt, &selectedFormat); err != nil {
		return err
	}

	reporter := schema.NewReporter(result)
	report, err := reporter.GenerateReport(schema.ReportFormat(selectedFormat))
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("comparison_%s_%s.%s", 
		result.SourceEnvironment, result.TargetEnvironment, selectedFormat)
	
	if err := os.WriteFile(filename, []byte(report.Content), 0600); err != nil {
		return fmt.Errorf("failed to save report: %w", err)
	}

	fmt.Printf("\nComparison results saved to: %s\n", filename)
	return nil
}