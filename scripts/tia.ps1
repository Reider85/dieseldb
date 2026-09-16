# tia.ps1 - Test-Impact Analysis for DieselDB
# Detects which test buckets to run based on changed files via git diff.
#
# Usage:
#   .\scripts\tia.ps1                    # compare with origin/main
#   .\scripts\tia.ps1 HEAD~1             # compare with previous commit
#   .\scripts\tia.ps1 --run              # run impacted profiles
#   .\scripts\tia.ps1 --tags             # output tags only (for CI)

param(
    [string]$Ref = "",
    [switch]$Run,
    [switch]$TagsOnly
)

$ErrorActionPreference = "Stop"

# Use the script's parent directory as repo root (avoids git encoding issues with Cyrillic paths)
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $repoRoot) { $repoRoot = (Get-Location).Path }
Set-Location -LiteralPath $repoRoot

$mappingFile = "$repoRoot\scripts\tia-mapping.txt"
if (-not (Test-Path $mappingFile)) {
    Write-Error "Mapping file not found: $mappingFile"
    exit 1
}

# Parse arguments
if ($Ref -eq "--run") {
    $Run = $true
    $Ref = ""
} elseif ($Ref -eq "--tags") {
    $TagsOnly = $true
    $Ref = ""
}

if ($Ref -eq "") { $Ref = "origin/main...HEAD" }

# Get changed files
$changed = git diff --name-only $Ref 2>$null | Sort-Object -Unique
if (-not $changed) {
    Write-Host "No changes detected against $Ref"
    if ($TagsOnly) { Write-Host "" }
    else { Write-Host "Recommended: no tests to run." }
    exit 0
}

Write-Host "=== Changed files (vs $Ref) ==="
$changed | ForEach-Object { Write-Host "  $_" }
Write-Host ""

# Map changed files to tags
$tagsSet = @{}

$lines = Get-Content $mappingFile
foreach ($line in $lines) {
    if ($line -match '^\s*#' -or $line -match '^\s*$') { continue }
    
    $parts = $line -split "`t"
    if ($parts.Count -lt 2) { continue }
    
    $sourceGlob = $parts[0].Trim()
    $tags = $parts[1].Trim()
    
    if ([string]::IsNullOrEmpty($sourceGlob) -or [string]::IsNullOrEmpty($tags)) { continue }
    
    # Check if any changed file matches
    $matched = $false
    foreach ($cf in $changed) {
        # Simple glob matching: convert sourceGlob to regex
        # diesel/storage/*.java -> diesel/storage/.*\.java
        # diesel/*Message.java -> diesel/.*Message\.java
        $regexPattern = [regex]::Escape($sourceGlob)
        $regexPattern = $regexPattern -replace '\\\*', '.*'
        $regexPattern = $regexPattern -replace '\.java$', '\.java$'
        if ($cf -match $regexPattern) {
            $matched = $true
            break
        }
    }
    
    if ($matched) {
        $tagList = $tags -split ','
        foreach ($t in $tagList) {
            $t = $t.Trim()
            if ($t -ne "" -and $t -ne "none") {
                $tagsSet[$t] = $true
            }
        }
    }
}

# If pom.xml changed, run all
if ($changed -match "^pom\.xml$") {
    $tagsSet["all"] = $true
    $tagsSet.Remove("none")
}

if ($tagsSet.Count -eq 0) {
    Write-Host "No test buckets impacted by changes."
    if ($TagsOnly) { Write-Host "" }
    else { Write-Host "Recommended: skip tests (or run 'mvn -P fast test' as smoke)." }
    exit 0
}

# Sort tags - if "all" is present, show it prominently
if ($tagsSet.ContainsKey("all")) {
    Write-Host "=== Recommended test tags ==="
    Write-Host "  ALL (pom.xml changed - run every profile)"
    Write-Host ""
    if ($TagsOnly) {
        Write-Output "all"
    } else {
        Write-Host "=== Recommended Maven profiles ==="
        Write-Host "  fast, core, concurrency, network, perf"
        Write-Host ""
        Write-Host "=== Commands ==="
        Write-Host "  mvn -B clean test -P fast"
        Write-Host "  mvn -B clean test -P core"
        Write-Host "  mvn -B clean test -P concurrency"
        Write-Host "  mvn -B clean test -P network"
        Write-Host "  mvn -B clean test -P perf"
    }
    exit 0
}

$tagsSorted = ($tagsSet.Keys | Where-Object { $_ -ne "all" -and $_ -ne "none" } | Sort-Object) -join ", "

Write-Host "=== Recommended test tags ==="
$tagsSorted -split ',' | ForEach-Object { Write-Host "  - $($_.Trim())" }
Write-Host ""

if ($TagsOnly) {
    Write-Output $tagsSorted
    exit 0
}

# Determine profiles
$profiles = @()
foreach ($tag in ($tagsSorted -split ',')) {
    $tag = $tag.Trim()
    switch -Wildcard ($tag) {
        "smoke" { $profiles += "fast" }
        "query" { $profiles += "fast" }
        "index" { $profiles += "fast" }
        "query-full" { $profiles += "core" }
        "storage" { $profiles += "core" }
        "concurrency" { $profiles += "concurrency" }
        "network" { $profiles += "network" }
        "perf" { $profiles += "perf" }
    }
}
$profiles = $profiles | Sort-Object -Unique

Write-Host "=== Recommended Maven profiles ==="
$profiles | ForEach-Object { Write-Host "  - $_" }
Write-Host ""

Write-Host "=== Commands ==="
Write-Host "  (do NOT use 'mvn -Dgroups=...' without -P: default surefire excludes all tests)"
foreach ($p in $profiles) {
    Write-Host "  mvn -B clean test -P $p"
}
Write-Host ""

# Run mode
if ($Run -and $profiles.Count -gt 0) {
    Write-Host "=== Running ==="
    foreach ($p in $profiles) {
        Write-Host ""
        Write-Host "--- $p ---"
        mvn -B clean test -P $p
        if ($LASTEXITCODE -ne 0) {
            Write-Error "FAILED: $p"
            exit 1
        }
    }
    Write-Host ""
    Write-Host "=== All impacted profiles passed ==="
}
