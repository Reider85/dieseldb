param(
    [Parameter(Mandatory=$true)]
    [string]$Description
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "native.ps1")

$gitCmd = Get-NativeTool -Name "git"

# Get the last commit message
$lastCommit = (Invoke-Native -FilePath $gitCmd -Arguments @("log", "--format=%s", "-1") | Select-Object -First 1)
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
$syncPom = Join-Path $PSScriptRoot "sync-pom-version.ps1"

# Keep pom.xml project version aligned with the changelog/commit version.
# Without this, Changelog and git history advance while pom.xml stays stale
# (e.g. 0.5.1 vs 3.2.66). The caller's `git add -A` stages the pom change
# into the same release commit.
Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $syncPom, "-Version", $newVersion)
if ($LASTEXITCODE -ne 0) {
    Write-Error "sync-pom-version.ps1 failed (exit $LASTEXITCODE). pom.xml not updated."
    exit 1
}

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

# Informational only: PROMPT_STATUS.md must be updated BEFORE this script runs
# (AGENTS.md workflow step 7) so its DONE entry joins the release commit.
# The release targets (make.ps1 / Makefile) stage it explicitly.
$promptStatusPath = Join-Path $repoRoot "PROMPT_STATUS.md"
if (Test-Path $promptStatusPath) {
    $promptStatus = Invoke-Native -FilePath $gitCmd -Arguments @("status", "--porcelain", "--", "PROMPT_STATUS.md")
    if ($promptStatus -match "PROMPT_STATUS\.md") {
        Write-Host "PROMPT_STATUS.md has pending changes - they will be included in the release commit." -ForegroundColor Green
    }
    else {
        Write-Host "PROMPT_STATUS.md unchanged - if a prompt was completed, update it BEFORE running make release." -ForegroundColor Yellow
    }
}
