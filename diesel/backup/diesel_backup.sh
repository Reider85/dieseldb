#!/bin/bash
# DieselDB Physical Backup Tool
# Usage: diesel_backup.sh <database-dir> <backup-dir>

if [ $# -lt 2 ]; then
    echo "Usage: $0 <database-dir> <backup-dir>"
    echo
    echo "Example: $0 /path/to/database /path/to/backup"
    exit 1
fi

DATABASE_DIR="$1"
BACKUP_DIR="$2"

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    exit 1
fi

# Set classpath - adjust based on your environment
CLASSPATH="target/classes:lib/*"

# Check if database directory exists
if [ ! -d "$DATABASE_DIR" ]; then
    echo "Error: Database directory does not exist: $DATABASE_DIR"
    exit 1
fi

# Check if backup directory exists and is empty
if [ -d "$BACKUP_DIR" ]; then
    if [ "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
        echo "Error: Backup directory is not empty: $BACKUP_DIR"
        echo "Use a different directory or empty it first."
        exit 1
    fi
else
    mkdir -p "$BACKUP_DIR"
fi

echo "DieselDB Physical Backup Tool"
echo "============================="
echo "Database: $DATABASE_DIR"
echo "Backup: $BACKUP_DIR"
echo

# Run the Java application
java -cp "$CLASSPATH" diesel.backup.DieselBackup backup "$DATABASE_DIR" "$BACKUP_DIR"

if [ $? -ne 0 ]; then
    echo "Backup failed"
    exit 1
fi

echo
echo "Backup completed successfully"
exit 0