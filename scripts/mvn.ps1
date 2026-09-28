# mvn.ps1 - Maven wrapper with automatic JAVA_HOME setup
# Eliminates the most common agent error: calling mvn without JAVA_HOME
#
# Usage:
#   .\scripts\mvn.ps1 -B compile -q
#   .\scripts\mvn.ps1 -B clean test -P fast
#   .\scripts\mvn.ps1 -B clean test -P core "-Ddiesel.storage.type=avro"
#   .\scripts\mvn.ps1 --version
#
# ARGUMENT PASSTHROUGH - do not reintroduce a param block here.
#
# Arguments arrive in the automatic $args variable, not via param(). With a
# param block PowerShell tries to bind "-P" and "-D..." as parameter names and
# silently drops them, so "mvn.ps1 -B clean test -P fast" would really run
# "mvn -B clean test" with no profile. Verified behaviour of the three variants:
#
#   $args (no param block)              -> -B | clean | test | -P | fast | -D...
#   param(ValueFromRemainingArguments)  -> -B | clean | test          (loses -P)
#   param([string[]])                   -> test                     (loses more)
#
# Dropping "-P" is not a cosmetic error here: the default surefire config in
# pom.xml excludes every source file, so the run executes zero tests and still
# reports BUILD SUCCESS.

. (Join-Path $PSScriptRoot "native.ps1")

$ErrorActionPreference = "Stop"

# --- JAVA_HOME auto-detection (priority order) ---
$candidates = @(
    "C:\Program Files\Axiom\AxiomJDK-21",
    "C:\tools\jdk-21.0.12+8",
    "C:\tools\jdk-21"
)

$javaHome = $null
foreach ($candidate in $candidates) {
    if (Test-Path "$candidate\bin\java.exe") {
        $javaHome = $candidate
        break
    }
}

if (-not $javaHome) {
    # Try system JAVA_HOME
    if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
        $javaHome = $env:JAVA_HOME
    } else {
        Write-Error "Java 21 not found. Searched:`n  $($candidates -join "`n  ")`nSet JAVA_HOME manually or install AxiomJDK-21."
        exit 1
    }
}

$env:JAVA_HOME = $javaHome

# --- Maven path detection ---
$mvnPaths = @(
    "C:\tools\apache-maven-3.9.6\bin\mvn.cmd",
    "C:\tools\apache-maven-3.9.9\bin\mvn.cmd"
)

$mvnCmd = $null
foreach ($p in $mvnPaths) {
    if (Test-Path $p) {
        $mvnCmd = $p
        break
    }
}

if (-not $mvnCmd) {
    # Try system mvn
    $mvnCmd = (Get-Command mvn -ErrorAction SilentlyContinue).Source
    if (-not $mvnCmd) {
        Write-Error "Maven not found. Searched:`n  $($mvnPaths -join "`n  ")`nAlso tried 'mvn' on PATH."
        exit 1
    }
}

# --- Invoke Maven ---
# $args holds every argument the caller passed, unfiltered.
Write-Host "[mvn.ps1] JAVA_HOME=$javaHome" -ForegroundColor DarkGray
Write-Host "[mvn.ps1] args: $($args -join ' ')" -ForegroundColor DarkGray
Invoke-Native -FilePath $mvnCmd -Arguments $args
exit $LASTEXITCODE
