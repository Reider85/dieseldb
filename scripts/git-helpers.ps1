# git-helpers.ps1 - Git operations with retry, lock fix, and safe commit
#
# Usage:
#   .\scripts\git-helpers.ps1 push              # push with retry (origin main)
#   .\scripts\git-helpers.ps1 fix-lock          # remove stale index.lock
#   .\scripts\git-helpers.ps1 commit -m "msg"   # stage all + commit + push
#   .\scripts\git-helpers.ps1 status            # short status

param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("push", "fix-lock", "commit", "status")]
    [string]$Command,

    [string]$Message = "",
    [switch]$NoPush
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

. (Join-Path $PSScriptRoot "native.ps1")

$gitCmd = Get-NativeTool -Name "git"

function Fix-GitLock {
    $lockFiles = @(
        ".git\index.lock",
        ".git\refs\heads\main.lock",
        ".git\refs\heads\master.lock"
    )
    $removed = 0
    foreach ($lock in $lockFiles) {
        if (Test-Path $lock) {
            Remove-Item -LiteralPath $lock -Force
            Write-Host "Removed stale lock: $lock" -ForegroundColor Yellow
            $removed++
        }
    }
    # Also check subdirectories
    Get-ChildItem -Path ".git" -Recurse -Filter "*.lock" -ErrorAction SilentlyContinue | ForEach-Object {
        Remove-Item -LiteralPath $_.FullName -Force
        Write-Host "Removed stale lock: $($_.FullName)" -ForegroundColor Yellow
        $removed++
    }
    if ($removed -eq 0) {
        Write-Host "No stale lock files found." -ForegroundColor Green
    } else {
        Write-Host "Removed $removed lock file(s)." -ForegroundColor Green
    }
    return $removed
}

function Push-WithRetry {
    param(
        [int]$MaxRetries = 3,
        [int]$DelaySeconds = 2
    )

    # Always push to origin main (the correct branch per repo conventions)
    $remote = "origin"
    $branch = "main"

    for ($i = 1; $i -le $MaxRetries; $i++) {
        Write-Host "Push attempt $i/$MaxRetries to $remote $branch..." -ForegroundColor Cyan
        # No 2>&1 here: merging native stderr into the PowerShell stream under
        # $ErrorActionPreference="Stop" turns git's progress output (and CRLF
        # warnings) into a terminating NativeCommandError, so a perfectly normal
        # push would abort the script.
        Invoke-Native -FilePath $gitCmd -Arguments @("push", $remote, $branch)
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Push succeeded." -ForegroundColor Green
            return $true
        }

        Write-Host "Push failed (exit code $LASTEXITCODE)." -ForegroundColor Yellow

        # Check for lock files and fix them
        $hasLock = Get-ChildItem -Path ".git" -Recurse -Filter "*.lock" -ErrorAction SilentlyContinue
        if ($hasLock) {
            Write-Host "Detected stale lock files, removing..." -ForegroundColor Yellow
            Fix-GitLock
        }

        if ($i -lt $MaxRetries) {
            Write-Host "Retrying in ${DelaySeconds}s..." -ForegroundColor Yellow
            Start-Sleep -Seconds $DelaySeconds
            $DelaySeconds = [math]::Min($DelaySeconds * 2, 10)
        }
    }

    Write-Error "Push failed after $MaxRetries attempts."
    return $false
}

switch ($Command) {
    "push" {
        $result = Push-WithRetry
        if (-not $result) { exit 1 }
    }
    "fix-lock" {
        Fix-GitLock
    }
    "commit" {
        if (-not $Message) {
            Write-Error "Usage: .\scripts\git-helpers.ps1 commit -m 'your message'"
            exit 1
        }
        Invoke-Native -FilePath $gitCmd -Arguments @("add", "-A")
        if ($LASTEXITCODE -ne 0) {
            Write-Error "git add failed."
            exit 1
        }
        Invoke-Native -FilePath $gitCmd -Arguments @("commit", "-m", $Message)
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Commit failed."
            exit 1
        }
        Write-Host "Committed: $Message" -ForegroundColor Green
        if (-not $NoPush) {
            $result = Push-WithRetry
            if (-not $result) { exit 1 }
        }
    }
    "status" {
        git status --short
    }
}
