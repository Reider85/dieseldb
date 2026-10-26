#!/bin/bash
# DieselDB Physical Restore Tool
# Usage: diesel_restore.sh <backup-dir> <target-dir>

if [ $# -lt 2 ]; then
    echo "Usage: $0 <backup-dir> <target-dir>"
    echo
    echo "Example: $0 /path/to/backup /path/to/restored_database"
    exit 1
fi

BACKUP_DIR="$1"
TARGET_DIR="$2"

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    exit 1
fi

# Set classpath - adjust based on your environment
CLASSPATH="target/classes:lib/*"

# Check if backup directory exists and contains required files
if [ ! -d "$BACKUP_DIR" ]; then
    echo "Error: Backup directory does not exist: $BACKUP_DIR"
    exit 1
fi

if [ ! -f "$BACKUP_DIR/backup_manifest.json" ]; then
    echo "Error: Invalid backup directory: $BACKUP_DIR"
    echo "The backup directory must contain backup_manifest.json"
    exit 1
fi

# Check if target directory exists and is empty
if [ -d "$TARGET_DIR" ]; then
    if [ "$(ls -A "$TARGET_DIR" 2>/dev/null)" ]; then
        echo "Error: Target directory is not empty: $TARGET_DIR"
        echo "Restore requires an empty target directory."
        echo "Use a different directory or empty it first."
        exit 1
    fi
else
    mkdir -p "$TARGET_DIR"
fi

echo "DieselDB Physical Restore Tool"
echo "=============================="
echo "Backup: $BACKUP_DIR"
echo "Target: $TARGET_DIR"
echo

# Display backup information
echo "Backup Information:"
java -cp "$CLASSPATH" diesel.backup.DieselRestore info "$BACKUP_DIR"
echo

# Ask for confirmation
read -p "Proceed with restore? (yes/no): " response
response=$(echo "$response" | tr '[:upper:]' '[:lower:]')

if [ "$response" != "yes" ] && [ "$response" != "y" ]; then
    echo "Restore cancelled."
    exit 0
fi

# Run the Java application
java -cp "$CLASSPATH" diesel.backup.DieselRestore restore "$BACKUP_DIR" "$TARGET_DIR"

if [ $? -ne 0 ]; then
    echo "Restore failed"
    exit 1
fi

echo
echo "Restore completed successfully"
echo
echo "Next steps:"
echo "1. Start the DieselDB server with the restored database:"
echo "   diesel_server $TARGET_DIR"
echo "2. Verify the database integrity:"
echo "   diesel_cli --database $TARGET_DIR \"SELECT COUNT(*) FROM information_schema.tables\""
exit 0