#!/bin/bash

# FlakeDrop Rollback Feature Demo
# This script demonstrates the automatic rollback functionality

echo "=== FlakeDrop Rollback Feature Demo ==="
echo

# Check if flakedrop binary exists
if ! command -v flakedrop &> /dev/null; then
    echo "Error: flakedrop command not found. Please build and install FlakeDrop first."
    exit 1
fi

# Create a temporary demo directory
DEMO_DIR=$(mktemp -d -t flakedrop-rollback-demo)
echo "Creating demo environment in: $DEMO_DIR"

# Create SQL files for demonstration
mkdir -p "$DEMO_DIR/migrations"

# Create a successful SQL file
cat > "$DEMO_DIR/migrations/001_create_demo_table.sql" << 'EOF'
-- This will succeed
CREATE TABLE IF NOT EXISTS rollback_demo_users (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert some data
INSERT INTO rollback_demo_users (username, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com');
EOF

# Create a failing SQL file
cat > "$DEMO_DIR/migrations/002_bad_migration.sql" << 'EOF'
-- This will succeed
CREATE TABLE IF NOT EXISTS rollback_demo_orders (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    user_id INTEGER,
    total DECIMAL(10,2)
);

-- This will fail with syntax error
CREATE INVALID SQL SYNTAX ERROR;

-- This won't be executed due to the error above
CREATE TABLE IF NOT EXISTS rollback_demo_items (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id INTEGER
);
EOF

# Create a temporary config file
cat > "$DEMO_DIR/demo-config.yaml" << EOF
repositories:
  - name: rollback-demo
    git_url: $DEMO_DIR
    branch: main
    database: \${SNOWFLAKE_DATABASE:-DEMO_DB}
    schema: ROLLBACK_DEMO_\$(date +%s)
    path: migrations

snowflake:
  account: \${SNOWFLAKE_ACCOUNT}
  username: \${SNOWFLAKE_USERNAME}
  password: \${SNOWFLAKE_PASSWORD}
  warehouse: \${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}
  role: \${SNOWFLAKE_ROLE:-ACCOUNTADMIN}

deployment:
  rollback:
    enabled: true
    on_failure: true
    backup_retention: 7
    strategy: "snapshot"
EOF

echo
echo "Demo environment created. Configuration:"
echo "- SQL files: $DEMO_DIR/migrations/"
echo "- Config: $DEMO_DIR/demo-config.yaml"
echo

# Initialize git repository (required for FlakeDrop)
cd "$DEMO_DIR"
git init
git add .
git commit -m "Initial demo setup"

echo "=== Step 1: First Successful Deployment ==="
echo "Deploying 001_create_demo_table.sql..."
echo
echo "Command: flakedrop deploy rollback-demo --config $DEMO_DIR/demo-config.yaml --commit HEAD"
echo

# Remove the second file temporarily for first deployment
mv migrations/002_bad_migration.sql migrations/002_bad_migration.sql.bak

if flakedrop deploy rollback-demo --config "$DEMO_DIR/demo-config.yaml" --commit HEAD; then
    echo "✅ First deployment succeeded!"
else
    echo "❌ First deployment failed. Check your Snowflake credentials."
    exit 1
fi

# Restore the second file for the failing deployment
mv migrations/002_bad_migration.sql.bak migrations/002_bad_migration.sql
git add .
git commit -m "Add failing migration"

echo
echo "=== Step 2: Second Deployment with Failure ==="
echo "This deployment will fail and trigger automatic rollback..."
echo
echo "Command: flakedrop deploy rollback-demo --config $DEMO_DIR/demo-config.yaml --commit HEAD"
echo

# This should fail and trigger rollback
if flakedrop deploy rollback-demo --config "$DEMO_DIR/demo-config.yaml" --commit HEAD; then
    echo "⚠️  Deployment succeeded (unexpected - should have failed)"
else
    echo "✅ Deployment failed as expected"
    echo "🔄 Automatic rollback should have been triggered"
fi

echo
echo "=== Checking Rollback Results ==="
echo "You can verify the rollback by:"
echo "1. Checking if the 'rollback_demo_orders' table was removed"
echo "2. Verifying that 'rollback_demo_users' table still exists"
echo "3. Reviewing the deployment history: flakedrop rollback list"
echo

echo "=== Cleanup ==="
echo "To clean up the demo:"
echo "  rm -rf $DEMO_DIR"
echo
echo "Demo complete!"