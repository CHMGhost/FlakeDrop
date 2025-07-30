# FlakeDrop Examples

This directory contains example code demonstrating various FlakeDrop features.

## Running Examples

Each example is a standalone program. To run an example:

```bash
# Run a specific example
go run examples/observability_demo.go

# Or build and run
go build -o demo examples/observability_demo.go
./demo
```

## Available Examples

- **observability_demo.go** - Demonstrates observability features including logging, metrics, and tracing
- **snowflake_optimizations_demo.go** - Shows performance optimization techniques for Snowflake deployments
- **ui_demo.go** - Interactive UI components demonstration
- **error-handling-example.go** - Error handling patterns and best practices

## Additional Resources

- **secure-deployment.md** - Guide for secure deployment practices
- **schema-drift-check.sh** - Script for checking schema drift
- **github-actions-schema-compare.yml** - GitHub Actions workflow example

## Note

These examples are for demonstration purposes and may need modification for production use. They are excluded from the main build process to avoid conflicts.