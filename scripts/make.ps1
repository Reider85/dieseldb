# make.ps1 - PowerShell replacement for make targets
# Solves the "make not found" problem on Windows
#
# Usage:
#   .\scripts\make.ps1 compile
#   .\scripts\make.ps1 test
#   .\scripts\make.ps1 release -Desc "fix: something"
#   .\scripts\make.ps1 help

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [string]$Target,

    [string]$T = "",
    [string]$Desc = "",
    [string]$BASE = "",
    [string]$NEW = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

. (Join-Path $PSScriptRoot "native.ps1")

# Resolve mvn via mvn.ps1 wrapper
$mvnPs = Join-Path $PSScriptRoot "mvn.ps1"
$gitHelpers = Join-Path $PSScriptRoot "git-helpers.ps1"
$compareTiming = Join-Path $PSScriptRoot "compare-timing.ps1"
$commitChangelog = Join-Path $PSScriptRoot "commit-and-changelog.ps1"
$tiaScript = Join-Path $PSScriptRoot "tia.ps1"
$gitCmd = Get-NativeTool -Name "git"

# THE ONLY WAY TO RUN TESTS.
#
# Every test target in this script routes through here so that a single place
# owns the Maven invocation, the "-P <profile>" argument and the post-run
# assertion that tests actually executed. Previously each target spelled out its
# own `Invoke-Mvn -B clean test -P X` line, and mvn.ps1 silently dropped the
# "-P" argument - so every target ran zero tests and still reported
# BUILD SUCCESS. Do not add a test target that calls mvn.ps1 directly.
function Invoke-ProfileTests {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Profile,

        [string[]]$ExtraArgs = @(),

        [switch]$NoClean
    )

    $mvnArgs = @("-B")
    if (-not $NoClean) {
        $mvnArgs += "clean"
    }
    $mvnArgs += @("test", "-P", $Profile)
    if ($ExtraArgs.Count -gt 0) {
        $mvnArgs += $ExtraArgs
    }

    $label = "profile '$Profile'"
    if ($ExtraArgs.Count -gt 0) {
        $label += " $($ExtraArgs -join ' ')"
    }
    if ($NoClean) {
        $label += " (no clean)"
    }
    Write-Host "Running $label..."

    # Captured before Maven so Assert-TestsRan can reject stale reports.
    $startedAt = Get-Date

    Invoke-Native -FilePath $mvnPs -Arguments $mvnArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Test run failed: $label (exit $LASTEXITCODE)."
        exit 1
    }

    Assert-TestsRan -StartedAt $startedAt -Target $label
}

# Creates the changelog entry, then commits it. Shared by release/release-local.
function Invoke-ChangelogCommit {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Description,

        [switch]$Push
    )

    Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $commitChangelog, "-Description", $Description)
    if ($LASTEXITCODE -ne 0) {
        Write-Error "commit-and-changelog.ps1 failed (exit $LASTEXITCODE)."
        exit 1
    }

    Invoke-Native -FilePath $gitCmd -Arguments @("add", "Changelog.md", "changelog_entry.txt")
    if ($LASTEXITCODE -ne 0) {
        Write-Error "git add failed (exit $LASTEXITCODE)."
        exit 1
    }

    Invoke-Native -FilePath $gitCmd -Arguments @("commit", "-F", "changelog_entry.txt")
    if ($LASTEXITCODE -ne 0) {
        Write-Error "git commit failed (exit $LASTEXITCODE)."
        exit 1
    }

    if ($Push) {
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $gitHelpers, "push")
        if ($LASTEXITCODE -ne 0) {
            Write-Error "git push failed (exit $LASTEXITCODE)."
            exit 1
        }
    }
}

switch ($Target) {
    "compile" {
        Write-Host "Compiling (no tests)..."
        Invoke-Native -FilePath $mvnPs -Arguments @("-B", "compile", "-q")
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "test" {
        Invoke-ProfileTests -Profile "fast"
    }
    "test-incr" {
        Invoke-ProfileTests -Profile "fast" -NoClean
    }
    "test-one" {
        if (-not $T) {
            Write-Error "Usage: .\scripts\make.ps1 test-one -T ""ClassName#methodName"""
            exit 1
        }
        Invoke-ProfileTests -Profile "fast" -ExtraArgs @("-Dtest=$T") -NoClean
    }
    "test-core" {
        Invoke-ProfileTests -Profile "core"
    }
    "test-network" {
        Invoke-ProfileTests -Profile "network"
    }
    "test-concurrency" {
        Invoke-ProfileTests -Profile "concurrency"
    }
    "test-perf" {
        Invoke-ProfileTests -Profile "perf"
    }
    "large-test" {
        Invoke-ProfileTests -Profile "large"
    }
    "all-tests" {
        Write-Host "Running ALL profiles sequentially (release gate)..."
        foreach ($p in @("fast", "core", "concurrency", "network", "perf", "large")) {
            Write-Host ""
            Invoke-ProfileTests -Profile $p
        }
        Write-Host ""
        Write-Host "All profiles passed." -ForegroundColor Green
    }
    "build" {
        Write-Host "Building DieselDB..."
        Invoke-Native -FilePath $mvnPs -Arguments @("-B", "package", "-DskipTests")
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "test-storage-csv" {
        Invoke-ProfileTests -Profile "storage-csv"
    }
    "test-storage-tsv" {
        Invoke-ProfileTests -Profile "storage-tsv"
    }
    "test-storage-jsonl" {
        Invoke-ProfileTests -Profile "storage-jsonl"
    }
    "test-storage-avro" {
        Invoke-ProfileTests -Profile "storage-avro"
    }
    "changelog" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 changelog -Desc ""description"""
            exit 1
        }
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $commitChangelog, "-Description", $Desc)
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "release" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 release -Desc ""description"""
            exit 1
        }
        Invoke-ChangelogCommit -Description $Desc -Push
    }
    "release-local" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 release-local -Desc ""description"""
            exit 1
        }
        Invoke-ChangelogCommit -Description $Desc
    }
    "timing" {
        Invoke-ProfileTests -Profile "large"
        Write-Host "Collecting timing data..."
        Invoke-Native -FilePath "python" -Arguments @("scripts/collect-timing.py")
        if (Test-Path "timing\timing.md") {
            Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $compareTiming, "-Base", "timing/timing.md", "-New", "timing/timingN.md")
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        } else {
            Write-Host "Baseline timing/timing.md not found - creating it from this run."
            Copy-Item "timing\timingN.md" "timing\timing.md"
            Write-Host "Baseline created. Re-run to compare against it."
        }
    }
    "compare-timing" {
        if (-not $BASE -or -not $NEW) {
            Write-Error "Usage: .\scripts\make.ps1 compare-timing -BASE timing\timing.md -NEW timing\timingN.md"
            exit 1
        }
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $compareTiming, "-Base", $BASE, "-New", $NEW)
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "tia" {
        Write-Host "Running test-impact analysis..."
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $tiaScript)
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "tia-run" {
        Write-Host "Running TIA + impacted tests..."
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $tiaScript, "-Run")
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    "clean" {
        Write-Host "Cleaning..."
        Invoke-Native -FilePath $mvnPs -Arguments @("clean")
        if (Test-Path "target\.cache") { Remove-Item -Recurse -Force "target\.cache" }
        Get-ChildItem "data\*.csv", "data\*.table", "*.log", "timing\timingN.md", "classpath.txt" -ErrorAction SilentlyContinue | Remove-Item -Force
    }
    "clean-test-cache" {
        Write-Host "Cleaning test cache..."
        @("target\.cache", "target\surefire-reports", "target\test-classes") | ForEach-Object {
            if (Test-Path $_) { Remove-Item -Recurse -Force $_ }
        }
    }
    "clean-cache" {
        Write-Host "Cleaning build cache..."
        $cachePath = Join-Path $env:HOME ".m2\build-cache\v1\com.dieseldb\dieseldb"
        if (Test-Path $cachePath) { Remove-Item -Recurse -Force $cachePath }
    }
    "setup" {
        Write-Host "Checking build environment..."
        $javaVer = (Invoke-Native -FilePath "cmd" -Arguments @("/c", "java -version 2>&1") | Select-Object -First 1)
        if ($javaVer) { Write-Host "Java: $javaVer" } else { Write-Host "Java: not found" -ForegroundColor Red }
        $mvnOut = (Invoke-Native -FilePath $mvnPs -Arguments @("--version") | Out-String).Trim()
        Write-Host $mvnOut.Split("`n")[0]
        Write-Host "Build environment OK." -ForegroundColor Green
    }
    "help" {
        Write-Host "DieselDB make.ps1 - PowerShell Makefile"
        Write-Host ""
        Write-Host "Quick workflow (simple changes):"
        Write-Host "  .\scripts\make.ps1 compile             - Compile only, no tests (3-5s)"
        Write-Host "  .\scripts\make.ps1 test-incr           - Fast profile without clean"
        Write-Host "  .\scripts\make.ps1 test-one -T X#m     - Run a single test method (quote it: -T ""X#m"")"
        Write-Host "  .\scripts\make.ps1 release -Desc '...' - Changelog + commit + push"
        Write-Host ""
        Write-Host "Targets:"
        Write-Host "  build, compile, test, test-incr, test-one, test-core"
        Write-Host "  test-network, test-concurrency, test-perf, large-test"
        Write-Host "  all-tests, timing, compare-timing, tia, tia-run"
        Write-Host "  changelog, release, release-local"
        Write-Host "  clean, clean-test-cache, clean-cache"
        Write-Host "  test-storage-csv, test-storage-tsv, test-storage-jsonl, test-storage-avro"
        Write-Host "  setup, help"
        Write-Host ""
        Write-Host "Parameters:"
        Write-Host "  -T ClassName#method    for test-one (quote in PowerShell, escape as \# in make)"
        Write-Host "  -Desc 'description'    for changelog, release, release-local"
        Write-Host "  -BASE / -NEW           for compare-timing"
        Write-Host ""
        Write-Host "Every test target asserts that surefire actually produced reports and"
        Write-Host "fails if Maven reported success without running a single test."
    }
    default {
        Write-Error "Unknown target: $Target. Run '.\scripts\make.ps1 help' for available targets."
        exit 1
    }
}
