#!/bin/bash

# Script to reword commits with proper version prefixes
# This script will reword each commit to add the appropriate version prefix

# Get the commit hashes that need to be reworded
commits=("9be2ab7" "b1456ea" "bf207aa" "ed371f8" "567ea7b")

# Version prefixes for each commit
versions=("3.4.9" "3.4.8" "3.4.7" "3.4.6" "3.4.5")

# New commit messages
messages=(
    "3.4.9 Fix logging system compilation issues and JUL to SLF4J migration"
    "3.4.8 Fix JUL to SLF4J compilation errors - replace warning/fine with warn/debug, fix Level.FINEST to trace"
    "3.4.7 Phase 2: Per-module logging switches with priority"
    "3.4.6 Migrate all legacy classes from JUL to SLF4J logging"
    "3.4.5 TESTS-16,17: Add all-tests-all-storages target and remove unused Maven profiles"
)

# Loop through each commit and reword it
for i in "${!commits[@]}"; do
    commit="${commits[$i]}"
    version="${versions[$i]}"
    message="${messages[$i]}"
    
    echo "Rewording commit $commit with message: $message"
    
    # Checkout the commit
    git checkout $commit
    
    # Amend the commit message
    git commit --amend -m "$message"
    
    # Move back to main
    git checkout main
done

echo "All commits have been reworded with proper version prefixes."