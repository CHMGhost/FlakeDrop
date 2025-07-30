# UI Package

The UI package provides comprehensive user interface components for the FlakeDrop CLI tool, offering rich terminal interactions, progress indicators, and formatted output capabilities.

## Features

- **Interactive Components**: Built on top of Survey v2 for rich interactive prompts
- **Progress Indicators**: Advanced progress bars and spinners for long-running operations
- **Formatted Output**: Color-coded messages, tables, and structured displays
- **Error Handling**: Smart error formatting with contextual suggestions
- **UI Management**: Centralized UI state management with history tracking
- **Responsive Design**: Adapts to terminal width and supports color/no-color modes
- **Quiet/Verbose Modes**: Flexible output control for different use cases

## Components

### Interactive Prompts

```go
// Single selection
selected, err := ui.Select("Choose an option:", []string{"Option 1", "Option 2"})

// Multi-selection
choices, err := ui.MultiSelect("Select files:", fileList)

// Text input
name, err := ui.Input("Enter name:", "default", "Help text")

// Password input
password, err := ui.Password("Enter password:", "Your Snowflake password")

// Confirmation
confirm, err := ui.Confirm("Proceed?", true)
```

### Commit Selector

```go
commits := []ui.CommitInfo{
    {
        Hash:      "abc123...",
        ShortHash: "abc1234",
        Message:   "Fix bug",
        Author:    "John Doe",
        Time:      time.Now(),
        Files:     5,
    },
}

selected, err := ui.SelectCommit(commits)
```

### Configuration Wizard

```go
wizard := ui.NewConfigWizard()
config, err := wizard.Run()
```

### Progress Indicators

```go
// Progress bar
pb := ui.NewProgressBar(totalFiles)
for i, file := range files {
    // Process file...
    pb.Update(i+1, file, success)
}
pb.Finish()

// Spinner
spinner := ui.NewSpinner("Loading...")
spinner.Start()
// Do work...
spinner.Stop(true, "Loaded successfully")
```

### Formatted Output

```go
// Headers
ui.ShowHeader("Deployment Summary")

// Status messages
ui.ShowSuccess("Deployment completed")
ui.ShowError(err)
ui.ShowWarning("No changes detected")
ui.ShowInfo("Using default configuration")

// Tables
table := ui.NewTable()
table.AddHeader("Commit", "Author", "Files")
table.AddRow("abc1234", "John Doe", "5")
table.Render()

// Boxes
ui.Box("Summary", "5 files deployed\n2 tables created")
```

## Color Scheme

- **Success**: Green (✓)
- **Error**: Red (✗)
- **Warning**: Yellow (!)
- **Info**: Blue (i)
- **Progress**: Cyan (►)

## Usage Example

```go
func deployWithUI() error {
    ui.ShowHeader("FlakeDrop")
    
    // Connect with spinner
    spinner := ui.NewSpinner("Connecting to Snowflake...")
    spinner.Start()
    err := connect()
    if err != nil {
        spinner.Stop(false, "Connection failed")
        ui.ShowError(err)
        return err
    }
    spinner.Stop(true, "Connected")
    
    // Select commit
    commits := getCommits()
    selected, err := ui.SelectCommit(commits)
    if err != nil {
        return err
    }
    
    // Deploy with progress
    files := getFiles(selected)
    pb := ui.NewProgressBar(len(files))
    
    for i, file := range files {
        ui.ShowFileExecution(file, i+1, len(files))
        err := deployFile(file)
        pb.Update(i+1, file, err == nil)
    }
    
    pb.Finish()
    ui.ShowSuccess("Deployment completed!")
    return nil
}
```

## New Features Added

### 1. Enhanced UI Struct
- Added spinner management
- Progress indication methods (`StartProgress`, `StopProgress`)
- Message methods (`Warning`, `Error`, `Info`, `Success`, `Print`)

### 2. Global UI Functions
- `PrintSuccess`, `PrintError`, `PrintErrorString` - Compatibility functions
- `PrintWarning`, `PrintInfo` - Status messages
- `PrintSection`, `PrintSubsection`, `PrintKeyValue` - Structured output

### 3. Progress Enhancements
- Real-time progress updates with file names
- Success/failure tracking
- Duration formatting (ms, seconds, minutes, hours)
- Thread-safe operations

### 4. Color Functions
- Automatic color detection (TTY support)
- Fallback for non-color terminals
- Consistent color scheme across the application

## Testing

Run the UI demo to see all components in action:

```bash
go run examples/ui_demo.go
```

Or use the CLI command:
```bash
flakedrop ui-demo
```

## Best Practices

1. **Use UI Instance**: Create a UI instance with appropriate verbose/quiet settings
2. **Handle Errors**: Always show errors with context and suggestions
3. **Progress Feedback**: Use spinners for indeterminate operations, progress bars for determinate ones
4. **Consistent Formatting**: Use the provided formatting functions for consistency
5. **Interactive Only When Needed**: Check for TTY before showing interactive prompts