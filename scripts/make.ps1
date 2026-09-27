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

# Resolve mvn via mvn.ps1 wrapper
$mvnPs = Join-Path $PSScriptRoot "mvn.ps1"
$gitHelpers = Join-Path $PSScriptRoot "git-helpers.ps1"
$compareTiming = Join-Path $PSScriptRoot "compare-timing.ps1"

function Invoke-Mvn {
    param([string[]]$Args)
    & $mvnPs @Args
}

switch ($Target) {
    "compile" {
        Write-Host "Compiling (no tests)..."
        Invoke-Mvn -B compile -q
    }
    "test" {
        Write-Host "Running fast profile (smoke + index + query)..."
        Invoke-Mvn -B clean test -P fast
    }
    "test-incr" {
        Write-Host "Running fast profile (incremental, no clean)..."
        Invoke-Mvn -B test -P fast
    }
    "test-one" {
        if (-not $T) {
            Write-Error "Usage: .\scripts\make.ps1 test-one -T ClassName#methodName"
            exit 1
        }
        Write-Host "Running single test: $T..."
        Invoke-Mvn -B test -P fast -Dtest=$T
    }
    "test-core" {
        Write-Host "Running core profile..."
        Invoke-Mvn -B clean test -P core
    }
    "test-network" {
        Write-Host "Running network profile..."
        Invoke-Mvn -B clean test -P network
    }
    "test-concurrency" {
        Write-Host "Running concurrency profile..."
        Invoke-Mvn -B clean test -P concurrency
    }
    "test-perf" {
        Write-Host "Running perf profile..."
        Invoke-Mvn -B clean test -P perf
    }
    "large-test" {
        Write-Host "Running large profile (@LargeTest)..."
        Invoke-Mvn -B clean test -P large
    }
    "all-tests" {
        Write-Host "Running ALL profiles sequentially..."
        foreach ($p in @("fast", "core", "concurrency", "network", "perf", "large")) {
            Write-Host "--- $p ---"
            Invoke-Mvn -B clean test -P $p
            if ($LASTEXITCODE -ne 0) {
                Write-Error "FAILED: profile $p"
                exit 1
            }
        }
    }
    "build" {
        Write-Host "Building DieselDB..."
        Invoke-Mvn -B package -DskipTests
    }
    "test-storage-csv" {
        Write-Host "Running CSV storage tests..."
        Invoke-Mvn -B clean test -P storage-csv
    }
    "test-storage-tsv" {
        Write-Host "Running TSV storage tests..."
        Invoke-Mvn -B clean test -P storage-tsv
    }
    "test-storage-jsonl" {
        Write-Host "Running JSONL storage tests..."
        Invoke-Mvn -B clean test -P storage-jsonl
    }
    "test-storage-avro" {
        Write-Host "Running AVRO storage tests..."
        Invoke-Mvn -B clean test -P storage-avro
    }
    "changelog" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 changelog -Desc 'description'"
            exit 1
        }
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\commit-and-changelog.ps1") -Description $Desc
    }
    "release" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 release -Desc 'description'"
            exit 1
        }
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\commit-and-changelog.ps1") -Description $Desc
        git add Changelog.md changelog_entry.txt
        git commit -F changelog_entry.txt
        & $gitHelpers push
    }
    "release-local" {
        if (-not $Desc) {
            Write-Error "Usage: .\scripts\make.ps1 release-local -Desc 'description'"
            exit 1
        }
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\commit-and-changelog.ps1") -Description $Desc
        git add Changelog.md changelog_entry.txt
        git commit -F changelog_entry.txt
    }
    "timing" {
        Write-Host "Running acceptance gate: large profile (600x600 joins, 4GB heap)..."
        Invoke-Mvn -B clean test -P large
        python scripts/collect-timing.py
        if (Test-Path "timing\timing.md") {
            & $compareTiming -Base "timing\timing.md" -New "timing\timingN.md"
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
        & $compareTiming -Base $BASE -New $NEW
    }
    "tia" {
        Write-Host "Running test-impact analysis..."
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\tia.ps1")
    }
    "tia-run" {
        Write-Host "Running TIA + impacted tests..."
        & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\tia.ps1") -Run
    }
    "clean" {
        Write-Host "Cleaning..."
        Invoke-Mvn clean
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
        $ErrorActionPreference = "SilentlyContinue"
        $javaVer = & cmd /c "java -version 2>&1" | Select-Object -First 1
        if ($javaVer) { Write-Host "Java: $javaVer" } else { Write-Host "Java: not found" -ForegroundColor Red }
        $mvnOut = (& $mvnPs --version 2>&1 | Out-String).Trim()
        Write-Host $mvnOut.Split("`n")[0]
        $ErrorActionPreference = "Stop"
        Write-Host "Build environment OK." -ForegroundColor Green
    }
    "help" {
        Write-Host "DieselDB make.ps1 - PowerShell Makefile"
        Write-Host ""
        Write-Host "Quick workflow (simple changes):"
        Write-Host "  .\scripts\make.ps1 compile             - Compile only, no tests (3-5s)"
        Write-Host "  .\scripts\make.ps1 test-incr           - Fast profile without clean"
        Write-Host "  .\scripts\make.ps1 test-one -T X#m     - Run a single test method"
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
        Write-Host "  -T ClassName#method    for test-one"
        Write-Host "  -Desc 'description'    for changelog, release, release-local"
        Write-Host "  -BASE / -NEW           for compare-timing"
    }
    default {
        Write-Error "Unknown target: $Target. Run '.\scripts\make.ps1 help' for available targets."
        exit 1
    }
}
