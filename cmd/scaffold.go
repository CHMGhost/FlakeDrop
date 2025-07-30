package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"flakedrop/internal/scaffold"
	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cobra"
)

var scaffoldCmd = &cobra.Command{
	Use:   "scaffold",
	Short: "Generate new files for your FlakeDrop project",
	Long:  `Generate migrations, scripts, tests, and other files with proper templates and structure.`,
}

var scaffoldMigrationCmd = &cobra.Command{
	Use:   "migration [name]",
	Short: "Generate a new migration file",
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldMigration,
}

var scaffoldScriptCmd = &cobra.Command{
	Use:   "script [name]",
	Short: "Generate a new script file",
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldScript,
}

var scaffoldTestCmd = &cobra.Command{
	Use:   "test [name]",
	Short: "Generate a new test file",
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldTest,
}

var scaffoldPipelineCmd = &cobra.Command{
	Use:   "pipeline [platform]",
	Short: "Generate CI/CD pipeline configuration",
	Long:  `Generate CI/CD pipeline configuration for GitHub Actions, GitLab CI, Jenkins, or Azure DevOps.`,
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldPipeline,
}

var scaffoldEnvCmd = &cobra.Command{
	Use:   "env [environment]",
	Short: "Generate environment configuration",
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldEnv,
}

var scaffoldDocCmd = &cobra.Command{
	Use:   "doc [type]",
	Short: "Generate documentation",
	Long:  `Generate documentation files (deployment, migration, troubleshooting, architecture).`,
	Args:  cobra.ExactArgs(1),
	Run:   runScaffoldDoc,
}

func init() {
	// Add subcommands
	scaffoldCmd.AddCommand(scaffoldMigrationCmd)
	scaffoldCmd.AddCommand(scaffoldScriptCmd)
	scaffoldCmd.AddCommand(scaffoldTestCmd)
	scaffoldCmd.AddCommand(scaffoldPipelineCmd)
	scaffoldCmd.AddCommand(scaffoldEnvCmd)
	scaffoldCmd.AddCommand(scaffoldDocCmd)
	
	// Add to root
	rootCmd.AddCommand(scaffoldCmd)
}

func runScaffoldMigration(cmd *cobra.Command, args []string) {
	name := args[0]
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		fmt.Println("Run 'flakedrop init' to create a new project")
		os.Exit(1)
	}
	
	// Ask for migration category
	var category string
	categoryPrompt := &survey.Select{
		Message: "Select migration category:",
		Options: []string{"schema", "data", "procedures", "views", "functions"},
		Default: "schema",
	}
	_ = survey.AskOne(categoryPrompt, &category)
	
	// Ask for additional details
	var author string
	authorPrompt := &survey.Input{
		Message: "Author name:",
		Default: os.Getenv("USER"),
	}
	_ = survey.AskOne(authorPrompt, &author)
	
	// Create generator
	config := &scaffold.Config{
		ProjectName: filepath.Base(projectDir),
		Author:      author,
		Database:    "DATABASE_NAME", // Would be loaded from config
		Schema:      "PUBLIC",
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate migration
	filePath, err := gen.GenerateMigration(name, category)
	if err != nil {
		fmt.Printf("Error generating migration: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created migration: %s\n", filePath)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("1. Edit the migration file: %s\n", filePath)
	fmt.Println("2. Add your SQL statements")
	fmt.Println("3. Test with: flakedrop validate")
	fmt.Println("4. Deploy with: flakedrop deploy <repo-name>")
}

func runScaffoldScript(cmd *cobra.Command, args []string) {
	name := args[0]
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		os.Exit(1)
	}
	
	// Ask for script type
	var scriptType string
	typePrompt := &survey.Select{
		Message: "Select script type:",
		Options: []string{"pre-deploy", "post-deploy", "validation", "rollback"},
		Default: "pre-deploy",
	}
	_ = survey.AskOne(typePrompt, &scriptType)
	
	// Ask for author
	var author string
	authorPrompt := &survey.Input{
		Message: "Author name:",
		Default: os.Getenv("USER"),
	}
	_ = survey.AskOne(authorPrompt, &author)
	
	// Create generator
	config := &scaffold.Config{
		ProjectName: filepath.Base(projectDir),
		Author:      author,
		Database:    "DATABASE_NAME",
		Schema:      "PUBLIC",
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate script
	filePath, err := gen.GenerateScript(name, scriptType)
	if err != nil {
		fmt.Printf("Error generating script: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created script: %s\n", filePath)
}

func runScaffoldTest(cmd *cobra.Command, args []string) {
	name := args[0]
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		os.Exit(1)
	}
	
	// Ask for test type
	var testType string
	typePrompt := &survey.Select{
		Message: "Select test type:",
		Options: []string{"unit", "integration", "performance"},
		Default: "unit",
	}
	_ = survey.AskOne(typePrompt, &testType)
	
	// Ask for author
	var author string
	authorPrompt := &survey.Input{
		Message: "Author name:",
		Default: os.Getenv("USER"),
	}
	_ = survey.AskOne(authorPrompt, &author)
	
	// Create generator
	config := &scaffold.Config{
		ProjectName: filepath.Base(projectDir),
		Author:      author,
		Database:    "DATABASE_NAME",
		Schema:      "PUBLIC",
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate test
	filePath, err := gen.GenerateTest(name, testType)
	if err != nil {
		fmt.Printf("Error generating test: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created test: %s\n", filePath)
}

func runScaffoldPipeline(cmd *cobra.Command, args []string) {
	platform := strings.ToLower(args[0])
	
	// Validate platform
	validPlatforms := []string{"github", "gitlab", "jenkins", "azure"}
	valid := false
	for _, p := range validPlatforms {
		if platform == p {
			valid = true
			break
		}
	}
	
	if !valid {
		fmt.Printf("Error: Invalid platform '%s'\n", platform)
		fmt.Printf("Valid platforms: %s\n", strings.Join(validPlatforms, ", "))
		os.Exit(1)
	}
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		os.Exit(1)
	}
	
	// Ask for environments
	var environments string
	envPrompt := &survey.Input{
		Message: "Environments (comma-separated):",
		Default: "dev,staging,prod",
	}
	_ = survey.AskOne(envPrompt, &environments)
	
	// Create generator
	envList := strings.Split(environments, ",")
	for i := range envList {
		envList[i] = strings.TrimSpace(envList[i])
	}
	
	config := &scaffold.Config{
		ProjectName:  filepath.Base(projectDir),
		Environments: envList,
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate pipeline
	filePath, err := gen.GeneratePipeline(platform)
	if err != nil {
		fmt.Printf("Error generating pipeline: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created pipeline configuration: %s\n", filePath)
	fmt.Println()
	fmt.Println("Next steps:")
	
	switch platform {
	case "github":
		fmt.Println("1. Commit and push the workflow file")
		fmt.Println("2. Set up secrets in GitHub repository settings:")
		for _, env := range envList {
			fmt.Printf("   - SNOWFLAKE_%s_PASSWORD\n", strings.ToUpper(env))
		}
	case "gitlab":
		fmt.Println("1. Commit and push the .gitlab-ci.yml file")
		fmt.Println("2. Set up variables in GitLab project settings:")
		for _, env := range envList {
			fmt.Printf("   - SNOWFLAKE_%s_PASSWORD\n", strings.ToUpper(env))
		}
	case "jenkins":
		fmt.Println("1. Create a new Pipeline job in Jenkins")
		fmt.Println("2. Point it to your repository and Jenkinsfile")
		fmt.Println("3. Configure credentials in Jenkins:")
		for _, env := range envList {
			fmt.Printf("   - snowflake-%s-password\n", env)
		}
	case "azure":
		fmt.Println("1. Commit and push the azure-pipelines.yml file")
		fmt.Println("2. Set up pipeline in Azure DevOps")
		fmt.Println("3. Configure variables:")
		for _, env := range envList {
			fmt.Printf("   - Snowflake%sPassword\n", strings.Title(env))
		}
	}
}

func runScaffoldEnv(cmd *cobra.Command, args []string) {
	environment := args[0]
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		os.Exit(1)
	}
	
	// Ask for company name
	var company string
	companyPrompt := &survey.Input{
		Message: "Company name:",
		Default: "company",
	}
	_ = survey.AskOne(companyPrompt, &company)
	
	// Create generator
	config := &scaffold.Config{
		ProjectName: filepath.Base(projectDir),
		Company:     company,
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate environment config
	filePath, err := gen.GenerateEnvironmentConfig(environment)
	if err != nil {
		fmt.Printf("Error generating environment config: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created environment configuration: %s\n", filePath)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("1. Edit %s with your Snowflake credentials\n", filePath)
	fmt.Printf("2. Set environment variable: SNOWFLAKE_%s_PASSWORD\n", strings.ToUpper(environment))
	fmt.Printf("3. Deploy to this environment: flakedrop deploy <repo> --env %s\n", environment)
}

func runScaffoldDoc(cmd *cobra.Command, args []string) {
	docType := strings.ToLower(args[0])
	
	// Validate doc type
	validTypes := []string{"deployment", "migration", "troubleshooting", "architecture"}
	valid := false
	for _, t := range validTypes {
		if docType == t {
			valid = true
			break
		}
	}
	
	if !valid {
		fmt.Printf("Error: Invalid documentation type '%s'\n", docType)
		fmt.Printf("Valid types: %s\n", strings.Join(validTypes, ", "))
		os.Exit(1)
	}
	
	// Get project directory
	projectDir, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting current directory: %v\n", err)
		os.Exit(1)
	}
	
	// Check if we're in a valid project
	if !isValidProject(projectDir) {
		fmt.Println("Error: Not in a valid FlakeDrop project directory")
		os.Exit(1)
	}
	
	// Get environments from config
	_ = filepath.Join(projectDir, "configs", "config.yaml")
	environments := []string{"dev", "staging", "prod"} // Default
	
	// Get features based on project structure
	features := detectProjectFeatures(projectDir)
	
	// Create generator
	config := &scaffold.Config{
		ProjectName:  filepath.Base(projectDir),
		Environments: environments,
		Features:     features,
	}
	
	gen := scaffold.NewGenerator(projectDir, config)
	
	// Generate documentation
	filePath, err := gen.GenerateDocumentation(docType)
	if err != nil {
		fmt.Printf("Error generating documentation: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Created documentation: %s\n", filePath)
}

// Helper functions

func isValidProject(dir string) bool {
	// Check for essential project directories/files
	requiredPaths := []string{
		"configs",
		"migrations",
	}
	
	for _, path := range requiredPaths {
		fullPath := filepath.Join(dir, path)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			return false
		}
	}
	
	return true
}

func detectProjectFeatures(dir string) []string {
	features := []string{}
	
	// Check for various features based on directory structure
	featureChecks := map[string]string{
		"monitoring":       "Monitoring and observability",
		"security":         "Advanced security features",
		"tests":            "Automated testing",
		"ci":               "CI/CD integration",
		"scripts/rollback": "Rollback procedures",
		"backups":          "Backup and recovery",
	}
	
	for path, feature := range featureChecks {
		fullPath := filepath.Join(dir, path)
		if _, err := os.Stat(fullPath); err == nil {
			features = append(features, feature)
		}
	}
	
	return features
}