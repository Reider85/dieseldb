# Simple test script to verify flaky-jsonl exclusions
param()

$testDir = "src\test\java"
$flusherTest = Join-Path $testDir "diesel/FlusherTest.java"

# Check if FlusherTest.java is missing
if (-not (Test-Path $flusherTest)) {
    Write-Error "FlusherTest.java is missing but may be excluded in pom.xml"
    
    # Check pom.xml for flaky-jsonl exclusions
    $pomXml = Get-Content "pom.xml" -Raw
    if ($pomXml -match '<exclude>diesel/FlusherTest.java</exclude>') {
        Write-Error "pom.xml excludes FlusherTest.java but file is missing!"
        Write-Error "This is a stale exclusion that needs to be fixed."
        exit 1
    } else {
        Write-Host "FlusherTest.java is missing but not excluded in pom.xml"
        exit 0
    }
} else {
    Write-Host "FlusherTest.java exists"
    exit 0
}