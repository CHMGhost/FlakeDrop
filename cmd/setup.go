package cmd

import (
    "fmt"
    "os"
    "flakedrop/internal/analytics"
    "flakedrop/internal/config"
    "flakedrop/pkg/models"
    "github.com/AlecAivazis/survey/v2"
    "github.com/spf13/cobra"
)

var setupCmd = &cobra.Command{
    Use:   "setup",
    Short: "Initial configuration setup",
    Run:   runSetup,
}

func runSetup(cmd *cobra.Command, args []string) {
    fmt.Println("🚀 Setting up FlakeDrop CLI...")
    fmt.Println()

    // Check if config already exists
    if config.Exists() {
        var overwrite bool
        prompt := &survey.Confirm{
            Message: "Configuration already exists. Do you want to overwrite it?",
            Default: false,
        }
        survey.AskOne(prompt, &overwrite)
        if !overwrite {
            fmt.Println("Setup cancelled.")
            return
        }
    }

    cfg := &models.Config{
        Repositories: []models.Repository{},
    }

    // Collect Snowflake credentials
    fmt.Println("📄 Snowflake Configuration")
    fmt.Println("-------------------------")
    
    snowflakeQs := []*survey.Question{
        {
            Name: "account",
            Prompt: &survey.Input{
                Message: "Snowflake Account (e.g., xy12345.us-east-1):",
            },
            Validate: survey.Required,
        },
        {
            Name: "username",
            Prompt: &survey.Input{
                Message: "Username:",
            },
            Validate: survey.Required,
        },
        {
            Name: "password",
            Prompt: &survey.Password{
                Message: "Password:",
            },
            Validate: survey.Required,
        },
        {
            Name: "role",
            Prompt: &survey.Input{
                Message: "Role:",
                Default: "ACCOUNTADMIN",
            },
            Validate: survey.Required,
        },
        {
            Name: "warehouse",
            Prompt: &survey.Input{
                Message: "Warehouse:",
                Default: "COMPUTE_WH",
            },
            Validate: survey.Required,
        },
    }

    err := survey.Ask(snowflakeQs, &cfg.Snowflake)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        os.Exit(1)
    }

    // Ask if user wants to add a repository
    fmt.Println()
    fmt.Println("📦 Repository Configuration")
    fmt.Println("-------------------------")
    
    var addRepo bool
    prompt := &survey.Confirm{
        Message: "Do you want to add a repository now?",
        Default: true,
    }
    survey.AskOne(prompt, &addRepo)

    if addRepo {
        repo := models.Repository{}
        repoQs := []*survey.Question{
            {
                Name: "name",
                Prompt: &survey.Input{
                    Message: "Repository name (identifier for CLI):",
                },
                Validate: survey.Required,
            },
            {
                Name: "giturl",
                Prompt: &survey.Input{
                    Message: "Git URL:",
                },
                Validate: survey.Required,
            },
            {
                Name: "branch",
                Prompt: &survey.Input{
                    Message: "Branch:",
                    Default: "main",
                },
                Validate: survey.Required,
            },
            {
                Name: "database",
                Prompt: &survey.Input{
                    Message: "Target Snowflake database:",
                },
                Validate: survey.Required,
            },
            {
                Name: "schema",
                Prompt: &survey.Input{
                    Message: "Target Snowflake schema:",
                    Default: "PUBLIC",
                },
                Validate: survey.Required,
            },
        }

        answers := struct {
            Name     string
            GitURL   string `survey:"giturl"`
            Branch   string
            Database string
            Schema   string
        }{}

        err := survey.Ask(repoQs, &answers)
        if err != nil {
            fmt.Printf("Error: %v\n", err)
            os.Exit(1)
        }

        repo.Name = answers.Name
        repo.GitURL = answers.GitURL
        repo.Branch = answers.Branch
        repo.Database = answers.Database
        repo.Schema = answers.Schema

        cfg.Repositories = append(cfg.Repositories, repo)
    }

    // Save configuration
    if err := config.Save(cfg); err != nil {
        fmt.Printf("Error saving configuration: %v\n", err)
        os.Exit(1)
    }

    // Track config creation (telemetry)
    manager := analytics.GetManager()
    manager.TrackConfigCreated("initial_setup")

    fmt.Println()
    fmt.Println("✅ Configuration saved to:", config.GetConfigFile())
    fmt.Println()
    fmt.Println("You can now deploy using: flakedrop deploy <repository-name>")
    if !addRepo {
        fmt.Println("To add repositories later, edit the config file or use: flakedrop repo add")
    }
}

func init() {
    rootCmd.AddCommand(setupCmd)
}