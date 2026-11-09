#!/bin/bash

# GitHub Runner Limit Detection Script
# Checks current GitHub Actions usage and determines if we should switch to local CI/CD
# Exit codes: 0 = use GitHub, 101 = switch to local fallback

set -e

# Configuration
GITHUB_REPO="${GITHUB_REPOSITORY:-Reider85/dieseldb}"
MONTHLY_LIMIT=2000
WARNING_THRESHOLD=1800  # 90% of limit

echo "=== GitHub Runner Limit Check ==="
echo "Repository: $GITHUB_REPO"
echo "Monthly Limit: $MONTHLY_LIMIT minutes"
echo "Warning Threshold: $WARNING_THRESHOLD minutes"

# Get current usage from GitHub API
echo "Fetching current usage from GitHub API..."
CURRENT_USAGE=$(gh api /repos/$GITHUB_REPO/actions/runs --jq '.total_count' 2>/dev/null || echo "0")

if [ "$CURRENT_USAGE" = "0" ]; then
    echo "Warning: Could not fetch GitHub API data, assuming GitHub mode"
    exit 0
fi

echo "Current GitHub Actions runs: $CURRENT_USAGE"

# Calculate remaining capacity
REMAINING=$((MONTHLY_LIMIT - CURRENT_USAGE))
echo "Remaining capacity: $REMAINING minutes"

# Check if we should switch to local mode
if [ "$CURRENT_USAGE" -ge "$WARNING_THRESHOLD" ]; then
    echo "⚠️  WARNING: Approaching GitHub runner limit ($CURRENT_USAGE/$MONTHLY_LIMIT)"
    echo "Switching to local CI/CD fallback mode"
    exit 101
elif [ "$REMAINING" -lt 200 ]; then
    echo "⚠️  WARNING: Low GitHub runner capacity remaining ($REMAINING minutes)"
    echo "Consider switching to local CI/CD"
    exit 0
else
    echo "✅ GitHub runner capacity is sufficient ($REMAINING minutes remaining)"
    exit 0
fi