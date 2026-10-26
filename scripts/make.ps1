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
$collectTiming = Join-Path $PSScriptRoot "collect-timing.ps1"
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

        [switch]$Clean,

        # Maven lifecycle phase - "test" for every profile target, "verify"
        # for the all-tests release gate (TESTS-7: single 'mvn clean verify
        # -P all' invocation, matching the CI release-gate job).
        [string]$Phase = "test",

        # Continue on a red run instead of exiting: record the failure in
        # $script:ProfileTestFailures so the caller can run the remaining
        # invocations and aggregate (used by test-flaky  - a red quarantined
        # flake must not prevent the other quarantined tests from running).
        [switch]$ContinueOnError
    )

    $mvnArgs = @("-B")
    if ($Clean) {
        $mvnArgs += "clean"
    }
    $mvnArgs += @($Phase, "-P", $Profile)
    if ($ExtraArgs.Count -gt 0) {
        $mvnArgs += $ExtraArgs
    }

    $label = "profile '$Profile'"
    if ($ExtraArgs.Count -gt 0) {
        $label += " $($ExtraArgs -join ' ')"
    }
    if ($Phase -ne "test") {
        $label += " ($Phase)"
    }
    if ($Clean) {
        $label += " (clean)"
    } else {
        $label += " (no clean)"
    }
    Write-Host "Running $label..."

    # Captured before Maven so Assert-TestsRan can reject stale reports.
    $startedAt = Get-Date

    Invoke-Native -FilePath $mvnPs -Arguments $mvnArgs
    if ($LASTEXITCODE -ne 0) {
        if ($ContinueOnError) {
            # NB: Write-Warning, not Write-Error  - $ErrorActionPreference is
            # "Stop" here, so Write-Error would abort the whole script.
            Write-Warning "Test run failed (continuing): $label (exit $LASTEXITCODE)."
            $script:ProfileTestFailures += $label
        } else {
            Write-Error "Test run failed: $label (exit $LASTEXITCODE)."
            exit 1
        }
    }

    Assert-TestsRan -StartedAt $startedAt -Target $label
}

# Shared timing-gate body (TESTS-6): wipe target/surefire-reports first
# (collect-timing sweeps the WHOLE reports directory and surefire never
# deletes stale reports from earlier profiles - previously leaked 0.000-time
# skips and Avro/Jsonl rows into timingN.md), run the profile, collect
# per-test times, compare against the tracked baseline. Shared by
# timing-quick / timing-full / timing (alias).
function Invoke-TimingGate {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Profile
    )

    if (Test-Path "target\surefire-reports") {
        Remove-Item -Recurse -Force "target\surefire-reports"
    }
    Invoke-ProfileTests -Profile $Profile

    Write-Host "Collecting timing data..."
    Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $collectTiming)
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    if (Test-Path "timing\timing.md") {
        Invoke-Native -FilePath "powershell" -Arguments @("-ExecutionPolicy", "Bypass", "-File", $compareTiming, "-Base", "timing/timing.md", "-New", "timing/timingN.md")
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } else {
        Write-Host "Baseline timing/timing.md not found - creating it from this run."
        Copy-Item "timing\timingN.md" "timing\timing.md"
        Write-Host "Baseline created. Re-run to compare against it."
    }
}

# Creates the changelog entry, then commits it. Shared by release/release-local.
#
# PROMPT_STATUS.md must be updated BEFORE this runs (AGENTS.md workflow step 7)
# so its DONE entry joins the same commit instead of becoming a separate
# "Update PROMPT_STATUS.md" commit. We stage it explicitly (not just via the
# blanket add -A) so the intent is visible in the script and the file is
# guaranteed to be part of the release commit.
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

    Invoke-Native -FilePath $gitCmd -Arguments @("add", "PROMPT_STATUS.md")
    if ($LASTEXITCODE -ne 0) {
        Write-Error "git add PROMPT_STATUS.md failed (exit $LASTEXITCODE)."
        exit 1
    }

    $promptStatus = Invoke-Native -FilePath $gitCmd -Arguments @("diff", "--cached", "--name-only", "--", "PROMPT_STATUS.md")
    if ($promptStatus -match "PROMPT_STATUS\.md") {
        Write-Host "PROMPT_STATUS.md staged - its DONE entry joins this commit." -ForegroundColor Green
    }
    else {
        Write-Host "PROMPT_STATUS.md unchanged - if a prompt was completed, update it BEFORE make release." -ForegroundColor Yellow
    }

    Invoke-Native -FilePath $gitCmd -Arguments @("add", "-A")
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
        Invoke-ProfileTests -Profile "fast"
    }
    "test-one" {
        if (-not $T) {
            Write-Error "Usage: .\scripts\make.ps1 test-one -T ""ClassName#methodName"""
            exit 1
        }
        Invoke-ProfileTests -Profile "fast" -ExtraArgs @("-Dtest=$T")
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
    "test-flaky" {
        # TESTS-5 quarantine opt-in: four invocations, together all 5 tests,
        # ordered so the load-sensitive assertions run first on a settled machine
        # (NO clean  - a full recompile right before the p99 test measured 42ms;
        # after FlusherTest's heavy insert writes in the same fork it was 105ms).
        # Continue-on-error: a red quarantined flake does NOT stop the remaining
        # invocations; the exit code aggregates at the end (red = the signal).
        $script:ProfileTestFailures = @()
        Invoke-ProfileTests -Profile "flaky" -ExtraArgs @("-Dtest=GroupCommitThroughputTest") -ContinueOnError
        if ($script:ProfileTestFailures.Count -gt 0) {
            # ONE isolated retry with cooldown  - AGENTS.md Known-Flakes protocol.
            # The retry replaces the first attempt's record (same label).
            Write-Host "1/4 red  - one isolated retry after 10s cooldown (Known-Flakes protocol)..."
            Start-Sleep -Seconds 10
            $script:ProfileTestFailures = @()
            Invoke-ProfileTests -Profile "flaky" -ExtraArgs @("-Dtest=GroupCommitThroughputTest") -ContinueOnError
        }
        Invoke-ProfileTests -Profile "flaky-jsonl" -ContinueOnError
        Invoke-ProfileTests -Profile "flaky" -ExtraArgs @("-Dtest=FlusherTest") -ContinueOnError
        Invoke-ProfileTests -Profile "flaky" -ExtraArgs @("-Dtest=WALReadAllIsAlreadySortedTest") -ContinueOnError
        if ($script:ProfileTestFailures.Count -gt 0) {
            $failed = ($script:ProfileTestFailures | Select-Object -Unique) -join "; "
            Write-Error ("test-flaky: RED  - {0} invocation(s) failed in opt-in mode. A red quarantined flake is the signal (investigate, do NOT re-run until green); gates stay green either way. Failed: {1}" -f $script:ProfileTestFailures.Count, $failed)
            exit 1
        }
        Write-Host "test-flaky: all 5 quarantined tests ran, all green." -ForegroundColor Green
    }
    "all-tests" {
        # TESTS-7: ONE 'mvn clean verify -P all' instead of a 6-invocation
        # loop. The 'all' profile aggregates the fast+core+concurrency+network
        # tag groups (excludes perf+large by design - those gate via
        # all-tests-large / test-perf / large-test).
        Invoke-ProfileTests -Profile "all" -Clean -Phase "verify"
        Write-Host ""
        Write-Host "All profiles passed." -ForegroundColor Green
    }
    "all-tests-large" {
        # Full release gate (TESTS-7): all-tests + perf + large. perf runs
        # BEFORE large (old loop order) - large's disk churn would contaminate
        # the perf profile's wall-clock assertions.
        Invoke-ProfileTests -Profile "all" -Clean -Phase "verify"
        Write-Host ""
        Invoke-ProfileTests -Profile "perf"
        Invoke-ProfileTests -Profile "large"
        Write-Host ""
        Write-Host "Full gate (all + perf + large) passed." -ForegroundColor Green
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
    "sync-version" {
        $syncPom = Join-Path $PSScriptRoot "sync-pom-version.ps1"
        $syncArgs = @("-ExecutionPolicy", "Bypass", "-File", $syncPom)
        if ($Desc -eq "check") {
            $syncArgs += "-CheckOnly"
        }
        elseif ($Desc -ne "") {
            $syncArgs += @("-Version", $Desc)
        }
        Invoke-Native -FilePath "powershell" -Arguments $syncArgs
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
    "timing-quick" {
        # TESTS-6 fast timing gate - only @Tag("timing") tests (~3-5 min).
        Invoke-TimingGate -Profile "timing-quick"
    }
    "timing-full" {
        # TESTS-6 full timing gate - entire large profile (10-30 min),
        # the pre-TESTS-6 timing behavior, for schema/hash-join/large work.
        Invoke-TimingGate -Profile "large"
    }
    "timing" {
        # Backward-compat alias - defaults to quick (TESTS-6)
        Invoke-TimingGate -Profile "timing-quick"
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
        Write-Host "  .\scripts\make.ps1 test                - Fast profile (incremental, no clean)"
        Write-Host "  .\scripts\make.ps1 test-incr           - Fast profile (same as test now)"
        Write-Host "  .\scripts\make.ps1 test-one -T X#m     - Run a single test method (quote it: -T ""X#m"")"
        Write-Host "  .\scripts\make.ps1 release -Desc '...' - Changelog + commit + push"
        Write-Host "    (update PROMPT_STATUS.md FIRST so its DONE entry joins the commit)"
        Write-Host ""
        Write-Host "Targets:"
        Write-Host "  build, compile, test, test-incr, test-one, test-core"
        Write-Host "  test-network, test-concurrency, test-perf, large-test"
        Write-Host "  test-flaky (quarantined flakes, opt-in)"
        Write-Host "  all-tests (single mvn verify -P all), all-tests-large (+perf,large)"
        Write-Host "  timing-quick (3-5min), timing-full (10-30min),"
        Write-Host "  timing (alias -> quick), compare-timing, tia, tia-run"
        Write-Host "  changelog, release, release-local, sync-version"
        Write-Host "  clean, clean-test-cache, clean-cache"
        Write-Host "  test-storage-csv, test-storage-tsv, test-storage-jsonl, test-storage-avro"
        Write-Host "  setup, help"
        Write-Host ""
        Write-Host "Note: Most test targets now run incrementally (no clean). Use"
        Write-Host "clean-test-cache or clean to reset if needed."
        Write-Host ""
        Write-Host "Parameters:"
        Write-Host "  -T ClassName#method    for test-one (quote in PowerShell, escape as \# in make)"
        Write-Host "  -Desc 'description'    for changelog, release, release-local"
        Write-Host "  -Desc 'check'          for sync-version (check-only, no write)"
        Write-Host "  -Desc 'X.Y.Z'          for sync-version (explicit version)"
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
