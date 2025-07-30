package cmd

import (
    "fmt"
    "flakedrop/internal/config"
    "flakedrop/pkg/models"
    "github.com/AlecAivazis/survey/v2"
    "github.com/spf13/cobra"
    "text/tabwriter"
)

var repoCmd = &cobra.Command{
    Use:   "repo",
    Short: "Manage repository configurations",
}

var repoListCmd = &cobra.Command{
    Use:   "list",
    Short: "List configured repositories",
    Run:   runRepoList,
}

var repoAddCmd = &cobra.Command{
    Use:   "add",
    Short: "Add a new repository",
    Run:   runRepoAdd,
}

var repoRemoveCmd = &cobra.Command{
    Use:   "remove [name]",
    Short: "Remove a repository",
    Args:  cobra.ExactArgs(1),
    Run:   runRepoRemove,
}

func runRepoList(cmd *cobra.Command, args []string) {
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error loading configuration: %v\n", err)
        fmt.Fprintln(cmd.OutOrStderr(), "Run 'flakedrop setup' to create initial configuration")
        return
    }

    if len(cfg.Repositories) == 0 {
        fmt.Fprintln(cmd.OutOrStdout(), "No repositories configured.")
        fmt.Fprintln(cmd.OutOrStdout(), "Use 'flakedrop repo add' to add a repository")
        return
    }

    fmt.Fprintln(cmd.OutOrStdout(), "Configured Repositories:")
    fmt.Fprintln(cmd.OutOrStdout())
    
    w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
    fmt.Fprintln(w, "NAME\tGIT URL\tBRANCH\tDATABASE\tSCHEMA")
    fmt.Fprintln(w, "----\t-------\t------\t--------\t------")
    
    for _, repo := range cfg.Repositories {
        fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", 
            repo.Name, repo.GitURL, repo.Branch, repo.Database, repo.Schema)
    }
    _ = w.Flush()
}

func runRepoAdd(cmd *cobra.Command, args []string) {
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error loading configuration: %v\n", err)
        fmt.Fprintln(cmd.OutOrStderr(), "Run 'flakedrop setup' first")
        return
    }

    repo := models.Repository{}
    
    qs := []*survey.Question{
        {
            Name: "name",
            Prompt: &survey.Input{
                Message: "Repository name (identifier for CLI):",
            },
            Validate: func(val interface{}) error {
                name, ok := val.(string)
                if !ok || name == "" {
                    return fmt.Errorf("name is required")
                }
                // Check for duplicates
                for _, r := range cfg.Repositories {
                    if r.Name == name {
                        return fmt.Errorf("repository '%s' already exists", name)
                    }
                }
                return nil
            },
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

    err = survey.Ask(qs, &answers)
    if err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error: %v\n", err)
        return
    }

    repo.Name = answers.Name
    repo.GitURL = answers.GitURL
    repo.Branch = answers.Branch
    repo.Database = answers.Database
    repo.Schema = answers.Schema

    cfg.Repositories = append(cfg.Repositories, repo)

    if err := config.Save(cfg); err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error saving configuration: %v\n", err)
        return
    }

    fmt.Fprintf(cmd.OutOrStdout(), "✅ Repository '%s' added successfully\n", repo.Name)
}

func runRepoRemove(cmd *cobra.Command, args []string) {
    name := args[0]
    
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error loading configuration: %v\n", err)
        return
    }

    found := false
    newRepos := []models.Repository{}
    
    for _, repo := range cfg.Repositories {
        if repo.Name == name {
            found = true
            continue
        }
        newRepos = append(newRepos, repo)
    }

    if !found {
        fmt.Fprintf(cmd.OutOrStderr(), "Repository '%s' not found\n", name)
        return
    }

    // Confirm removal
    var confirm bool
    prompt := &survey.Confirm{
        Message: fmt.Sprintf("Remove repository '%s'?", name),
        Default: false,
    }
    if err := survey.AskOne(prompt, &confirm); err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error: %v\n", err)
        return
    }
    
    if !confirm {
        fmt.Fprintln(cmd.OutOrStdout(), "Removal cancelled")
        return
    }

    cfg.Repositories = newRepos

    if err := config.Save(cfg); err != nil {
        fmt.Fprintf(cmd.OutOrStderr(), "Error saving configuration: %v\n", err)
        return
    }

    fmt.Fprintf(cmd.OutOrStdout(), "✅ Repository '%s' removed\n", name)
}

func init() {
    rootCmd.AddCommand(repoCmd)
    repoCmd.AddCommand(repoListCmd)
    repoCmd.AddCommand(repoAddCmd)
    repoCmd.AddCommand(repoRemoveCmd)
}