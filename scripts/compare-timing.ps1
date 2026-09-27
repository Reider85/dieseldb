# compare-timing.ps1 - PowerShell port of compare-timing.sh
# Compares timing/timingN.md against baseline timing/timing.md
# Fails if any test with baseline >= 11ms shows >20% degradation
#
# Usage:
#   .\scripts\compare-timing.ps1
#   .\scripts\compare-timing.ps1 -Base timing\timing.md -New timing\timingN.md
#   .\scripts\compare-timing.ps1 -Base timing\timing.md -New timing\timingN.md -Threshold 1.2

param(
    [string]$Base = "timing\timing.md",
    [string]$New = "timing\timingN.md",
    [double]$Threshold = 1.2,
    [double]$MinBaseline = 0.011
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

if (-not (Test-Path $Base)) {
    Write-Error "Baseline file '$Base' not found."
    exit 1
}
if (-not (Test-Path $New)) {
    Write-Error "New timing file '$New' not found."
    exit 1
}

Write-Host "Comparing $New against $Base (threshold: ${Threshold}x, min baseline: ${MinBaseline}s)" -ForegroundColor Cyan
Write-Host ("=" * 60)

# Read baseline into hashtable
$baseTimes = @{}
$baseLines = Get-Content $Base
foreach ($line in $baseLines) {
    $parts = $line.Trim() -split '\s+', 2
    if ($parts.Count -ge 2 -and $parts[0] -ne "Test") {
        $testName = $parts[0]
        $time = 0.0
        if ([double]::TryParse($parts[1], [ref]$time)) {
            $baseTimes[$testName] = $time
        }
    }
}

$regressions = 0
$improvements = 0
$unchanged = 0
$newTests = 0

$newLines = Get-Content $New
foreach ($line in $newLines) {
    $parts = $line.Trim() -split '\s+', 2
    if ($parts.Count -lt 2 -or $parts[0] -eq "Test") { continue }

    $testName = $parts[0]
    $newTime = 0.0
    if (-not [double]::TryParse($parts[1], [ref]$newTime)) { continue }

    if ($baseTimes.ContainsKey($testName)) {
        $baseTime = $baseTimes[$testName]

        # Ignore micro-queries (< MinBaseline) - machine noise
        if ($baseTime -lt $MinBaseline) { continue }

        $ratio = $newTime / $baseTime
        $invRatio = if ($ratio -gt 0) { 1.0 / $ratio } else { 0 }

        if ($ratio -gt $Threshold) {
            Write-Host ("  REGRESSION: {0,-45} {1:N2}x (was: {2:N3}s, now: {3:N3}s)" -f $testName, $ratio, $baseTime, $newTime) -ForegroundColor Red
            $regressions++
        } elseif ($ratio -lt 0.8) {
            Write-Host ("  IMPROVEMENT: {0,-44} {1:N2}x faster (was: {2:N3}s, now: {3:N3}s)" -f $testName, $invRatio, $baseTime, $newTime) -ForegroundColor Green
            $improvements++
        } else {
            $unchanged++
        }
    } else {
        Write-Host ("  NEW TEST: {0} = {1:N3}s (no baseline)" -f $testName, $newTime) -ForegroundColor DarkGray
        $newTests++
    }
}

Write-Host ("=" * 60)
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  Regressions (>20%): $regressions" -ForegroundColor $(if ($regressions -gt 0) { "Red" } else { "Green" })
Write-Host "  Improvements (>20%): $improvements" -ForegroundColor Green
Write-Host "  Unchanged: $unchanged"
Write-Host "  New tests (no baseline): $newTests" -ForegroundColor DarkGray

if ($regressions -gt 0) {
    Write-Host ""
    Write-Host "FAILED: $regressions test(s) show performance regression" -ForegroundColor Red
    exit 1
} else {
    Write-Host ""
    Write-Host "PASSED: No significant regressions detected" -ForegroundColor Green
    exit 0
}
