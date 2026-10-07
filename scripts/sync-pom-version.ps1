# sync-pom-version.ps1 - keep pom.xml project version in sync with Changelog/commit
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1
#       Sync pom.xml with the last Changelog entry version (default source).
#
#   powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1 -Source Commit
#       Sync pom.xml with the version prefix of the last git commit message.
#
#   powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1 -Version 3.2.67
#       Sync pom.xml to an explicit version (used by commit-and-changelog.ps1).
#
#   powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1 -CheckOnly
#       Compare pom.xml against the resolved version; exit 1 on mismatch, 0 when in sync.
#       Does NOT write the file.
#
# Exit codes:
#   0 = pom.xml already in sync, or was updated successfully (or check passed)
#   1 = error (pom.xml not found, version unresolvable, write failed, or check mismatch)
#
# Only the PROJECT version is updated: the <version> that follows
# <artifactId>dieseldb</artifactId>. Dependency/plugin versions are never touched.

param(
    [string]$Version = "",

    [ValidateSet("Changelog", "Commit")]
    [string]$Source = "Changelog",

    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pomPath = Join-Path $repoRoot "pom.xml"
$changelogPath = Join-Path $repoRoot "Changelog.md"

if (-not (Test-Path $pomPath)) {
    Write-Error "pom.xml not found at $pomPath"
    exit 1
}

# --- Resolve target version ---------------------------------------------------

function Get-FromChangelog {
    if (-not (Test-Path $changelogPath)) {
        Write-Error "Changelog.md not found at $changelogPath (needed for -Source Changelog)"
        exit 1
    }
    $lines = [System.IO.File]::ReadAllLines($changelogPath)
    $found = $null
    foreach ($line in $lines) {
        if ($line -match '^(\d+\.\d+\.\d+)\s') {
            $found = $Matches[1]
        }
    }
    if (-not $found) {
        Write-Error "No X.Y.Z version entry found in Changelog.md"
        exit 1
    }
    return $found
}

function Get-FromCommit {
    . (Join-Path $PSScriptRoot "native.ps1")
    $gitCmd = Get-NativeTool -Name "git"
    $lastCommit = (Invoke-Native -FilePath $gitCmd -Arguments @("log", "--format=%s", "-1") | Select-Object -First 1)
    if (-not $lastCommit) {
        Write-Error "No commits found (needed for -Source Commit)"
        exit 1
    }
    if ($lastCommit -match '^(\d+\.\d+\.\d+)\s') {
        return $Matches[1]
    }
    Write-Error "Could not parse X.Y.Z version from last commit message: '$lastCommit'"
    exit 1
}

if ($Version -ne "") {
    if ($Version -notmatch '^\d+\.\d+\.\d+$') {
        Write-Error "Invalid -Version '$Version' (expected X.Y.Z)"
        exit 1
    }
    $targetVersion = $Version
    $targetSource = "explicit -Version"
}
elseif ($Source -eq "Commit") {
    $targetVersion = Get-FromCommit
    $targetSource = "last git commit"
}
else {
    $targetVersion = Get-FromChangelog
    $targetSource = "last Changelog entry"
}

# --- Read current pom.xml project version -------------------------------------

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$pomText = [System.IO.File]::ReadAllText($pomPath, $utf8NoBom)

# Project version = <version> immediately after <artifactId>dieseldb</artifactId>.
# Dependency versions follow other artifactIds and must never match.
$pomPattern = '(?<prefix><artifactId>dieseldb</artifactId>\s*<version>)(?<ver>[^<]+)(?<suffix></version>)'
$match = [regex]::Match($pomText, $pomPattern)
if (-not $match.Success) {
    Write-Error "Could not find project version in pom.xml (pattern: artifactId dieseldb + version)"
    exit 1
}

$currentVersion = $match.Groups["ver"].Value
Write-Host "pom.xml project version : $currentVersion"
Write-Host "Target version ($targetSource) : $targetVersion"

# --- Check-only mode ----------------------------------------------------------

if ($CheckOnly) {
    if ($currentVersion -eq $targetVersion) {
        Write-Host "OK: pom.xml is in sync with $targetSource." -ForegroundColor Green
        exit 0
    }
    Write-Host "MISMATCH: pom.xml=$currentVersion, expected=$targetVersion ($targetSource)" -ForegroundColor Red
    Write-Host "Run: powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1"
    exit 1
}

# --- Write (only when different) ----------------------------------------------

if ($currentVersion -eq $targetVersion) {
    Write-Host "Already in sync - pom.xml unchanged." -ForegroundColor Green
    exit 0
}

$newPomText = $pomText.Substring(0, $match.Groups["ver"].Index) +
              $targetVersion +
              $pomText.Substring($match.Groups["ver"].Index + $match.Groups["ver"].Length)

# Preserve original line endings / BOM characteristics: ReadAllText already
# stripped a leading BOM if present; WriteAllText with UTF8(no BOM) keeps the
# file clean. pom.xml declares encoding="UTF-8" and historically has no BOM.
[System.IO.File]::WriteAllText($pomPath, $newPomText, $utf8NoBom)
Write-Host "pom.xml updated: $currentVersion -> $targetVersion" -ForegroundColor Green
exit 0
