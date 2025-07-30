#!/bin/bash
#
# Schema Drift Detection Script
# 
# This script demonstrates how to use the flakedrop compare command
# to detect schema drift between environments and generate reports.
#

set -e

# Configuration
BASELINE_ENV="prod"
ENVIRONMENTS=("dev" "staging" "uat")
REPORT_DIR="./schema-reports"
DATE=$(date +%Y%m%d_%H%M%S)

# Create report directory
mkdir -p "$REPORT_DIR"

echo "Schema Drift Detection Report"
echo "============================="
echo "Date: $(date)"
echo "Baseline: $BASELINE_ENV"
echo ""

# Function to check drift between environments
check_drift() {
    local source=$1
    local target=$2
    local report_file="$REPORT_DIR/drift_${source}_to_${target}_${DATE}.md"
    
    echo "Comparing $source → $target..."
    
    # Run comparison and capture exit code
    if flakedrop compare \
        --source "$source" \
        --target "$target" \
        --format markdown \
        --output "$report_file" \
        --exclude "^TEMP_.*" \
        --exclude ".*_BACKUP$" \
        --ignore-whitespace \
        --ignore-comments; then
        
        # Extract summary from report
        local differences=$(grep -E "Total differences:|Different Objects:" "$report_file" | head -1 || echo "No differences found")
        echo "  ✓ $differences"
        echo "  Report saved: $report_file"
    else
        echo "  ✗ Comparison failed"
        return 1
    fi
}

# Function to generate sync scripts
generate_sync() {
    local source=$1
    local target=$2
    local sync_file="$REPORT_DIR/sync_${source}_to_${target}_${DATE}.sql"
    
    echo "Generating sync script for $source → $target..."
    
    if flakedrop compare \
        --source "$source" \
        --target "$target" \
        --generate-sync \
        --output "$sync_file"; then
        echo "  ✓ Sync script saved: $sync_file"
    else
        echo "  ✗ Sync generation failed"
    fi
}

# Check drift for each environment
echo "Drift Detection Results:"
echo "-----------------------"
for env in "${ENVIRONMENTS[@]}"; do
    check_drift "$BASELINE_ENV" "$env"
done

echo ""
echo "Cross-Environment Comparison:"
echo "----------------------------"
# Optional: Compare non-production environments
if [[ ${#ENVIRONMENTS[@]} -gt 1 ]]; then
    check_drift "${ENVIRONMENTS[0]}" "${ENVIRONMENTS[1]}"
fi

# Generate sync scripts for critical environments
echo ""
echo "Sync Script Generation:"
echo "----------------------"
for env in "${ENVIRONMENTS[@]}"; do
    if [[ "$env" == "staging" || "$env" == "uat" ]]; then
        generate_sync "$env" "$BASELINE_ENV"
    fi
done

# Save schema versions
echo ""
echo "Saving Schema Versions:"
echo "----------------------"
for env in "${ENVIRONMENTS[@]}" "$BASELINE_ENV"; do
    echo "Saving version for $env..."
    if flakedrop compare \
        --source "$env" \
        --target "$env" \
        --save-version \
        --version-desc "Drift check snapshot - $DATE"; then
        echo "  ✓ Version saved"
    else
        echo "  ✗ Failed to save version"
    fi
done

# Generate summary report
SUMMARY_FILE="$REPORT_DIR/drift_summary_${DATE}.txt"
{
    echo "Schema Drift Summary Report"
    echo "=========================="
    echo "Generated: $(date)"
    echo ""
    echo "Reports Generated:"
    find "$REPORT_DIR" -name "*_${DATE}.*" -type f | while read -r file; do
        echo "  - $(basename "$file")"
    done
} > "$SUMMARY_FILE"

echo ""
echo "Summary report saved: $SUMMARY_FILE"
echo ""
echo "Drift detection complete!"

# Optional: Send notifications if drift is detected
# This is where you could integrate with your notification system
# Example:
# if grep -q "Different Objects: [1-9]" "$REPORT_DIR"/*_${DATE}.md 2>/dev/null; then
#     send_slack_notification "Schema drift detected! Check reports in $REPORT_DIR"
# fi