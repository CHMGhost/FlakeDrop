package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"flakedrop/internal/config"
	"flakedrop/internal/plugin"
	"flakedrop/internal/ui"
	pkgplugin "flakedrop/pkg/plugin"
)

var pluginCmd = &cobra.Command{
	Use:   "plugin",
	Short: "Plugin management commands",
	Long:  "Manage Snowflake Deploy plugins - install, configure, enable, disable, and list plugins",
}

var pluginListCmd = &cobra.Command{
	Use:   "list",
	Short: "List installed plugins",
	Long:  "Display information about all installed plugins including their status and configuration",
	Run:   runPluginList,
}

var pluginSearchCmd = &cobra.Command{
	Use:   "search [query]",
	Short: "Search for plugins in registries",
	Long:  "Search for available plugins in configured registries",
	Args:  cobra.MaximumNArgs(1),
	Run:   runPluginSearch,
}

var pluginInstallCmd = &cobra.Command{
	Use:   "install [plugin-name] [version]",
	Short: "Install a plugin",
	Long:  "Install a plugin from the registry. Version is optional and defaults to latest",
	Args:  cobra.RangeArgs(1, 2),
	Run:   runPluginInstall,
}

var pluginEnableCmd = &cobra.Command{
	Use:   "enable [plugin-name]",
	Short: "Enable a plugin",
	Long:  "Enable an installed plugin",
	Args:  cobra.ExactArgs(1),
	Run:   runPluginEnable,
}

var pluginDisableCmd = &cobra.Command{
	Use:   "disable [plugin-name]",
	Short: "Disable a plugin",
	Long:  "Disable an installed plugin",
	Args:  cobra.ExactArgs(1),
	Run:   runPluginDisable,
}

var pluginConfigCmd = &cobra.Command{
	Use:   "config [plugin-name]",
	Short: "Configure a plugin",
	Long:  "Configure plugin settings interactively",
	Args:  cobra.ExactArgs(1),
	Run:   runPluginConfig,
}

var pluginStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show plugin system statistics",
	Long:  "Display statistics about plugin usage and performance",
	Run:   runPluginStats,
}

func init() {
	rootCmd.AddCommand(pluginCmd)
	
	pluginCmd.AddCommand(pluginListCmd)
	pluginCmd.AddCommand(pluginSearchCmd)
	pluginCmd.AddCommand(pluginInstallCmd)
	pluginCmd.AddCommand(pluginEnableCmd)
	pluginCmd.AddCommand(pluginDisableCmd)
	pluginCmd.AddCommand(pluginConfigCmd)
	pluginCmd.AddCommand(pluginStatsCmd)
}

func runPluginList(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowInfo("Plugin system is disabled")
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	if err := pluginService.Initialize(ctx); err != nil {
		ui.ShowError(fmt.Errorf("failed to initialize plugins: %w", err))
		return
	}
	defer pluginService.Shutdown(ctx)
	
	// Get plugin information
	plugins := pluginService.GetPluginInfo()
	
	if len(plugins) == 0 {
		ui.ShowInfo("No plugins installed")
		return
	}
	
	// Display plugins in table format
	ui.ShowHeader("Installed Plugins")
	
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tVERSION\tSTATUS\tLAST USED\tUSAGE COUNT")
	fmt.Fprintln(w, "----\t-------\t------\t---------\t-----------")
	
	for name, info := range plugins {
		status := "Disabled"
		if info.Enabled {
			status = "Enabled"
		}
		
		lastUsed := "Never"
		if !info.LastUsed.IsZero() {
			lastUsed = info.LastUsed.Format("2006-01-02 15:04")
		}
		
		version := "Unknown"
		if info.Instance != nil {
			version = info.Instance.Version()
		}
		
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n", 
			name, version, status, lastUsed, info.UsageCount)
	}
	
	_ = w.Flush()
}

func runPluginSearch(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	query := ""
	if len(args) > 0 {
		query = args[0]
	}
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowError(fmt.Errorf("plugin system is disabled"))
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	// Search for plugins
	var plugins []pkgplugin.PluginManifest
	if query == "" {
		plugins, err = pluginService.ListAvailablePlugins(ctx)
		if err != nil {
			ui.ShowError(fmt.Errorf("failed to list plugins: %w", err))
			return
		}
	} else {
		plugins, err = pluginService.SearchPlugins(ctx, query)
		if err != nil {
			ui.ShowError(fmt.Errorf("failed to search plugins: %w", err))
			return
		}
	}
	
	if len(plugins) == 0 {
		if query == "" {
			ui.ShowInfo("No plugins available")
		} else {
			ui.ShowInfo(fmt.Sprintf("No plugins found matching '%s'", query))
		}
		return
	}
	
	// Display results
	if query == "" {
		ui.ShowHeader("Available Plugins")
	} else {
		ui.ShowHeader(fmt.Sprintf("Search Results for '%s'", query))
	}
	
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tVERSION\tAUTHOR\tDESCRIPTION")
	fmt.Fprintln(w, "----\t-------\t------\t-----------")
	
	for _, plugin := range plugins {
		description := plugin.Description
		if len(description) > 50 {
			description = description[:47] + "..."
		}
		
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", 
			plugin.Name, plugin.Version, plugin.Author, description)
	}
	
	_ = w.Flush()
}

func runPluginInstall(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	pluginName := args[0]
	version := ""
	if len(args) > 1 {
		version = args[1]
	}
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowError(fmt.Errorf("plugin system is disabled"))
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	// Install plugin
	ui.ShowInfo(fmt.Sprintf("Installing plugin %s...", pluginName))
	
	spinner := ui.NewSpinner("Downloading plugin...")
	spinner.Start()
	
	err = pluginService.InstallPlugin(ctx, pluginName, version)
	
	spinner.Stop(err == nil, "")
	
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to install plugin: %w", err))
		return
	}
	
	ui.ShowSuccess(fmt.Sprintf("Plugin %s installed successfully", pluginName))
	ui.ShowInfo("Run 'flakedrop plugin enable " + pluginName + "' to enable the plugin")
}

func runPluginEnable(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	pluginName := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowError(fmt.Errorf("plugin system is disabled"))
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	// Enable plugin
	if err := pluginService.EnablePlugin(ctx, pluginName); err != nil {
		ui.ShowError(fmt.Errorf("failed to enable plugin: %w", err))
		return
	}
	
	ui.ShowSuccess(fmt.Sprintf("Plugin %s enabled", pluginName))
}

func runPluginDisable(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	pluginName := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowError(fmt.Errorf("plugin system is disabled"))
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	// Disable plugin
	if err := pluginService.DisablePlugin(ctx, pluginName); err != nil {
		ui.ShowError(fmt.Errorf("failed to disable plugin: %w", err))
		return
	}
	
	ui.ShowSuccess(fmt.Sprintf("Plugin %s disabled", pluginName))
}

func runPluginConfig(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	pluginName := args[0]
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowError(fmt.Errorf("plugin system is disabled"))
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	ui.ShowHeader(fmt.Sprintf("Configure Plugin: %s", pluginName))
	
	// This is a simplified configuration interface
	// In a real implementation, you'd use the ConfigWizard from the plugin system
	ui.ShowInfo("Plugin configuration functionality would be implemented here")
	ui.ShowInfo("This would provide an interactive interface to configure plugin settings")
	
	// Example configuration update
	settings := map[string]interface{}{
		"example_setting": "example_value",
	}
	
	if err := pluginService.ConfigurePlugin(ctx, pluginName, settings); err != nil {
		ui.ShowError(fmt.Errorf("failed to configure plugin: %w", err))
		return
	}
	
	ui.ShowSuccess(fmt.Sprintf("Plugin %s configured", pluginName))
}

func runPluginStats(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	
	// Load configuration
	appConfig, err := config.Load()
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to load configuration: %w", err))
		return
	}
	
	if !appConfig.Plugins.Enabled {
		ui.ShowInfo("Plugin system is disabled")
		return
	}
	
	// Initialize plugin service
	pluginService, err := plugin.NewService(appConfig)
	if err != nil {
		ui.ShowError(fmt.Errorf("failed to create plugin service: %w", err))
		return
	}
	
	if err := pluginService.Initialize(ctx); err != nil {
		ui.ShowError(fmt.Errorf("failed to initialize plugins: %w", err))
		return
	}
	defer pluginService.Shutdown(ctx)
	
	// Get statistics
	stats := pluginService.GetStats()
	
	ui.ShowHeader("Plugin System Statistics")
	
	fmt.Printf("Total Plugins: %d\n", stats.TotalPlugins)
	fmt.Printf("Enabled Plugins: %d\n", stats.EnabledPlugins)
	
	if !stats.LastExecution.IsZero() {
		fmt.Printf("Last Execution: %s\n", stats.LastExecution.Format("2006-01-02 15:04:05"))
	}
	
	if len(stats.HookExecutions) > 0 {
		fmt.Println("\nHook Executions:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "HOOK\tEXECUTIONS")
		fmt.Fprintln(w, "----\t----------")
		
		for hook, count := range stats.HookExecutions {
			fmt.Fprintf(w, "%s\t%d\n", hook, count)
		}
		
		_ = w.Flush()
	}
	
	if len(stats.PluginUsage) > 0 {
		fmt.Println("\nPlugin Usage:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "PLUGIN\tUSAGE COUNT\tAVG EXEC TIME")
		fmt.Fprintln(w, "------\t-----------\t-------------")
		
		for plugin, count := range stats.PluginUsage {
			avgTime := "N/A"
			if execTime, exists := stats.AverageExecTime[plugin]; exists {
				avgTime = execTime.String()
			}
			fmt.Fprintf(w, "%s\t%d\t%s\n", plugin, count, avgTime)
		}
		
		_ = w.Flush()
	}
	
	if len(stats.Errors) > 0 {
		fmt.Printf("\nRecent Errors: %d\n", len(stats.Errors))
		for i, err := range stats.Errors {
			if i >= 5 { // Show only last 5 errors
				break
			}
			fmt.Printf("  %s: %s - %s\n", 
				err.Timestamp.Format("15:04:05"), err.Plugin, err.Error)
		}
	}
}