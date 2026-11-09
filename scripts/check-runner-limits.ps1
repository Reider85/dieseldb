# GitHub Runner Limit Detection Script (PowerShell)
# Checks current GitHub Actions usage and determines if we should switch to local CI/CD
# Exit codes: 0 = use GitHub, 101 = switch to local fallback

param(
    [string]$GitHubRepo = "Reider85/dieseldb",
    [int]$MonthlyLimit = 2000,
    [int]$WarningThreshold = 1800  # 90% of limit
)

Write-Host "=== GitHub Runner Limit Check ===" -ForegroundColor Cyan
Write-Host "Repository: $GitHubRepo"
Write-Host "Monthly Limit: $MonthlyLimit minutes"
Write-Host "Warning Threshold: $WarningThreshold minutes"

try {
    # Get current usage from GitHub API
    Write-Host "Fetching current usage from GitHub API..."
    $apiResponse = gh api /repos/$GitHubRepo/actions/runs --jq '.total_count' 2>$null
    $currentUsage = if ($apiResponse) { [int]$apiResponse } else { 0 }
    
    if ($currentUsage -eq 0) {
        Write-Host "Warning: Could not fetch GitHub API data, assuming GitHub mode" -ForegroundColor Yellow
        exit 0
    }
    
    Write-Host "Current GitHub Actions runs: $currentUsage"
    
    # Calculate remaining capacity
    $remaining = $MonthlyLimit - $currentUsage
    Write-Host "Remaining capacity: $remaining minutes"
    
    # Check if we should switch to local mode
    if ($currentUsage -ge $WarningThreshold) {
        Write-Host "⚠️  WARNING: Approaching GitHub runner limit ($currentUsage/$MonthlyLimit)" -ForegroundColor Red
        Write-Host "Switching to local CI/CD fallback mode" -ForegroundColor Red
        exit 101
    }
    elseif ($remaining -lt 200) {
        Write-Host "⚠️  WARNING: Low GitHub runner capacity remaining ($remaining minutes)" -ForegroundColor Yellow
        Write-Host "Consider switching to local CI/CD" -ForegroundColor Yellow
        exit 0
    }
    else {
        Write-Host "✅ GitHub runner capacity is sufficient ($remaining minutes remaining)" -ForegroundColor Green
        exit 0
    }
}
catch {
    Write-Host "Error checking GitHub limits: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Assuming GitHub mode for safety" -ForegroundColor Yellow
    exit 0
}