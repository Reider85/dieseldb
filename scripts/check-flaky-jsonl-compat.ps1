# comprehensive-check.ps1 - Check all flaky-jsonl exclusions
param()

$testDir = "src\test\java"
$pomXml = Get-Content "pom.xml" -Raw
$inProfile = $false
$excludedPaths = @()

# Extract the <exclude> paths from the flaky-jsonl profile in pom.xml
$lines = $pomXml -split "`r?`n"
foreach ($line in $lines) {
    if ($line -match '<id>flaky-jsonl</id>') {
        $inProfile = $true
        continue
    }
    if ($inProfile -and $line -match '</profile>') {
        $inProfile = $false
        continue
    }
    if ($inProfile -and $line -match '<exclude>') {
        if ($line -match '<exclude>([^<]+)</exclude>') {
            $excludedPaths += $matches[1]
        }
    }
}

$staleExcludes = @()
foreach ($excl in $excludedPaths) {
    $exclFile = Join-Path $testDir $excl
    if (-not (Test-Path $exclFile)) {
        $staleExcludes += $excl
    }
}

if ($staleExcludes.Count -gt 0) {
    Write-Error "flaky-jsonl profile excludes non-existent test file(s):"
    foreach ($excl in $staleExcludes) {
        Write-Error "  $excl"
    }
    Write-Error "Update pom.xml <profile><id>flaky-jsonl</id> to remove the stale <exclude>."
    exit 1
}

Write-Host "Verified: flaky-jsonl exclusions are up to date ($($excludedPaths -join ', ') in pom.xml)."
exit 0