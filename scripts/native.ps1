# native.ps1 - shared helpers for invoking native executables from PowerShell 5.1
#
# Dot-source this file instead of calling git/mvn directly:
#   . (Join-Path $PSScriptRoot "native.ps1")
#
# It exists to prevent two independent, previously silent infrastructure bugs.
#
# 1. $ErrorActionPreference="Stop" turns native stderr into a terminating error.
#    PowerShell 5.1 converts a native command's stderr into an ErrorRecord, and
#    under "Stop" that terminates the script. Both git and maven write to stderr
#    routinely (CRLF conversion warnings, "To https://...", incubator-module
#    warnings), so any redirected invocation died. Symptom: `make release` never
#    reached `git commit`, because `git add` tripped over a CRLF warning.
#
# 2. Maven can report BUILD SUCCESS without running a single test.
#    The default surefire <configuration> in pom.xml excludes every source file,
#    so a run without -P <profile> selects zero tests and still succeeds. That
#    let a commit which did not compile pass the local gate. Assert-TestsRan
#    turns "no tests executed" into a hard failure.

$script:NativeRepoRoot = Split-Path -Parent $PSScriptRoot

# Invokes a native executable, tolerating whatever it writes to stderr.
#
# Emits nothing on purpose: the caller must read $LASTEXITCODE, which the native
# invocation has just set. Returning the code from this function would mix it
# into the output stream of any caller that captures the result, e.g.
# $out = Invoke-Native ... would interleave code with the command's stdout.
function Invoke-Native {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @()
    )

    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $FilePath @Arguments
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
}

# Resolves a native tool to an absolute path, failing loudly when absent.
# Used for git, which AGENTS.md requires to be addressed as "origin main".
function Get-NativeTool {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        Write-Host "ERROR: native tool '$Name' not found on PATH." -ForegroundColor Red
        exit 1
    }
    return $command.Source
}

# Fails the run when Maven succeeded but executed no test at all.
#
# Freshness matters: `test-incr` runs without `clean`, so reports left over from
# a previous run would otherwise satisfy this check while the current run tested
# nothing. $StartedAt must therefore be captured before Maven is invoked.
function Assert-TestsRan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [datetime]$StartedAt,

        [string]$Target = "test run"
    )

    $reportsDir = Join-Path $script:NativeRepoRoot "target\surefire-reports"
    $fresh = @()
    if (Test-Path -LiteralPath $reportsDir) {
        $fresh = @(Get-ChildItem -Path (Join-Path $reportsDir "TEST-*.xml") -ErrorAction SilentlyContinue |
                   Where-Object { $_.LastWriteTime -gt $StartedAt })
    }

    if ($fresh.Count -eq 0) {
        Write-Host ""
        Write-Host "ERROR: no surefire reports were written by $Target." -ForegroundColor Red
        Write-Host ""
        Write-Host "Maven reported success without executing a single test. In this project"
        Write-Host "that happens whenever the surefire profile is missing: the default"
        Write-Host "<configuration> in pom.xml excludes every source file, so"
        Write-Host "  mvn -B clean test          (no -P)  ->  0 tests, BUILD SUCCESS"
        Write-Host "  mvn -B clean test -P fast          ->  tests run"
        Write-Host ""
        Write-Host "Check that the Maven command line actually contains '-P <profile>'."
        exit 1
    }

    Write-Host "Verified: $($fresh.Count) surefire report(s) written by $Target." -ForegroundColor DarkGray
}
