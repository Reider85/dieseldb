param(
    [Parameter(Mandatory=$true)]
    [string]$Description
)

$ErrorActionPreference = "Stop"

# Get the last commit message
$lastCommit = git log --format="%s" -1 2>$null
if (-not $lastCommit) {
    Write-Host "No commits found. Using version 0.0.1"
    $major = 0; $minor = 0; $patch = 1
} else {
    # Extract version prefix from the last commit (e.g. "3.1.32 fix(...)" -> "3.1.32")
    if ($lastCommit -match '^(\d+)\.(\d+)\.(\d+)\s') {
        $major = [int]$Matches[1]
        $minor = [int]$Matches[2]
        $patch = [int]$Matches[3] + 1
    } else {
        Write-Host "WARNING: Could not parse version from last commit: '$lastCommit'"
        Write-Host "Falling back to version 0.0.1"
        $major = 0; $minor = 0; $patch = 1
    }
}

$newVersion = "$major.$minor.$patch"
$entry = "$newVersion $Description"

Write-Host "New changelog entry: $entry"

# Resolve paths relative to the repo root (parent of scripts/)
$repoRoot = Split-Path -Parent $PSScriptRoot
$changelogPath = Join-Path $repoRoot "Changelog.md"
$entryPath = Join-Path $repoRoot "changelog_entry.txt"

# Append to Changelog.md using .NET for reliable UTF-8 without BOM
if (Test-Path $changelogPath) {
    $existing = [System.IO.File]::ReadAllText($changelogPath)
    $separator = "`n"
    if ($existing.Length -gt 0 -and -not $existing.EndsWith("`n")) {
        $separator = "`n`n"
    }
    [System.IO.File]::WriteAllText($changelogPath, $existing + $separator + $entry)
    Write-Host "Changelog.md updated with: $entry"
} else {
    Write-Host "WARNING: Changelog.md not found at $changelogPath"
}

# Create changelog_entry.txt for git commit -F (UTF-8 without BOM)
[System.IO.File]::WriteAllText($entryPath, $entry)
Write-Host "changelog_entry.txt created: $entry"
