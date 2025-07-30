# Project Templates

This directory contains embedded templates for the flakedrop project initialization system.

## Template Structure

Templates are organized by project type:
- `standard/` - Standard project template with common features
- `enterprise/` - Enterprise-grade template with advanced features
- `minimal/` - Minimal template for simple projects

## Adding New Templates

To add a new template:
1. Create a new directory for your template type
2. Add template files with appropriate structure
3. Update the template manager to recognize the new template

## Template Variables

Common template variables:
- `{{.ProjectName}}` - Name of the project
- `{{.Company}}` - Company/organization name
- `{{.Author}}` - Author name
- `{{.Database}}` - Default database name
- `{{.Schema}}` - Default schema name
- `{{.Environments}}` - List of environments
- `{{.Features}}` - List of enabled features