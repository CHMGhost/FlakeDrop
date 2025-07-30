package cmd

import (
    "bytes"
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestRootCommand(t *testing.T) {
    // Test root command without arguments
    cmd := rootCmd
    b := bytes.NewBufferString("")
    cmd.SetOut(b)
    cmd.SetErr(b)
    cmd.SetArgs([]string{})
    
    err := cmd.Execute()
    assert.NoError(t, err)
    
    output := b.String()
    assert.Contains(t, output, "flakedrop")
    assert.Contains(t, output, "Deploy code to Snowflake")
}

func TestRootCommandHelp(t *testing.T) {
    // Test help flag
    cmd := rootCmd
    b := bytes.NewBufferString("")
    cmd.SetOut(b)
    cmd.SetErr(b)
    cmd.SetArgs([]string{"--help"})
    
    err := cmd.Execute()
    assert.NoError(t, err)
    
    output := b.String()
    assert.Contains(t, output, "Available Commands:")
    assert.Contains(t, output, "deploy")
    assert.Contains(t, output, "setup")
    assert.Contains(t, output, "license")
    assert.Contains(t, output, "repo")
}

func TestInvalidCommand(t *testing.T) {
    // Test invalid command
    cmd := rootCmd
    b := bytes.NewBufferString("")
    cmd.SetOut(b)
    cmd.SetErr(b)
    cmd.SetArgs([]string{"invalid-command"})
    
    err := cmd.Execute()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "unknown command")
}