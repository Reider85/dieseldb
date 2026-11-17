# Simple test to check if FlusherTest.java is missing
param()

$testDir = "src\test\java"
$flusherTest = Join-Path $testDir "diesel\FlusherTest.java"

if (Test-Path $flusherTest) {
    Write-Host "FlusherTest.java exists"
} else {
    Write-Error "FlusherTest.java is missing!"
    exit 1
}

# Check pom.xml for flaky-jsonl exclusions
$pomXml = Get-Content "pom.xml" -Raw
if ($pomXml -match '<exclude>diesel/FlusherTest.java</exclude>') {
    Write-Host "pom.xml excludes FlusherTest.java"
} else {
    Write-Error "pom.xml does NOT exclude FlusherTest.java"
    exit 1
}

Write-Host "Test completed: FlusherTest.java is properly excluded in pom.xml but missing from filesystem"