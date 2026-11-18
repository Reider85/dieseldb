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

        [string]$Target = "test run",

        [string]$StorageType = $null
    )

    $reportsDir = Join-Path $script:NativeRepoRoot "target\surefire-reports"
    $fresh = @()
    # reuseForks=true lets the last surefire fork flush its XML reports a moment
    # after the Maven process exits; poll briefly before declaring a false "no tests".
    $deadline = (Get-Date).AddSeconds(30)
    do {
        $fresh = @()
        if (Test-Path -LiteralPath $reportsDir) {
            $fresh = @(Get-ChildItem -Path (Join-Path $reportsDir "TEST-*.xml") -ErrorAction SilentlyContinue |
                       Where-Object { $_.LastWriteTime -gt $StartedAt })
        }
        if ($fresh.Count -gt 0) { break }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)

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

    # Storage type validation (TESTS-19)
    if ($StorageType) {
        $storageReports = @()
        $hasStorageTypeAnnotation = $false
        
        # Check if we have xmllint available (faster)
        if (Get-Command xmllint -ErrorAction SilentlyContinue) {
            foreach ($report in $fresh) {
                try {
                    $className = $report.BaseName -replace "TEST-", ""
                    $xmllintResult = xmllint --xpath "string(//testsuite/@classname)" $report.FullName 2>$null
                    if ($xmllintResult -and $xmllintResult -match ".*$StorageType.*") {
                        $storageReports += $report
                        $hasStorageTypeAnnotation = $true
                    }
                } catch {
                    # Fall back to Python if xmllint fails
                }
            }
        }
        
        # Fallback to Python if xmllint not available or failed
        if ($storageReports.Count -eq 0) {
            foreach ($report in $fresh) {
                try {
                    $pythonScript = @"
import xml.etree.ElementTree as ET
import sys
try:
    tree = ET.parse(sys.argv[1])
    classname = tree.getroot().get('classname', '')
    if '$StorageType' in classname:
        print('found')
except:
    pass
"@
                    $pythonResult = python -c $pythonScript $report.FullName 2>$null
                    if ($pythonResult -eq "found") {
                        $storageReports += $report
                        $hasStorageTypeAnnotation = $true
                    }
                } catch {
                    # Continue to next report
                }
            }
        }
        
        if ($hasStorageTypeAnnotation) {
            $totalCount = $fresh.Count
            $storageCount = $storageReports.Count
            Write-Host "Verified: $($storageCount) storage report(s) for type '$StorageType' (out of $totalCount total)." -ForegroundColor DarkGray
        } else {
            Write-Host ""
            Write-Host "ERROR: no surefire reports found for storage type '$StorageType'." -ForegroundColor Red
            Write-Host ""
            Write-Host "Found $($fresh.Count) total reports, but none match the expected storage type."
            Write-Host "This usually means:"
            Write-Host "  1. The storage type is misspelled (e.g., 'AVRO' instead of 'avro')"
            Write-Host "  2. The storage type is not supported"
            Write-Host "  3. No tests are annotated with @StorageType('$StorageType')"
            Write-Host ""
            Write-Host "Check that diesel.storage.type is set correctly and that tests exist for this type."
            exit 1
        }
    } else {
        Write-Host "Verified: $($fresh.Count) surefire report(s) written by $Target." -ForegroundColor DarkGray
    }
}
