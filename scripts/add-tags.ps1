# add-tags.ps1 - Adds @Tag("...") annotations to Java test classes
# Usage: .\scripts\add-tags.ps1 <test-dir> <mapping-file>

param(
    [Parameter(Mandatory=$true)]
    [string]$TestDir,
    
    [Parameter(Mandatory=$true)]
    [string]$MappingFile
)

if (-not (Test-Path $TestDir)) {
    Write-Error "Test dir '$TestDir' not found"
    exit 1
}

if (-not (Test-Path $MappingFile)) {
    Write-Error "Mapping file '$MappingFile' not found"
    exit 1
}

$count = 0
$skipped = 0

$lines = Get-Content $MappingFile
foreach ($line in $lines) {
    # Skip comments and empty lines
    if ($line -match '^\s*#' -or $line -match '^\s*$') { continue }
    
    $parts = $line -split "`t"
    if ($parts.Count -lt 2) { continue }
    
    $filename = $parts[0].Trim()
    $tag = $parts[1].Trim()
    
    if ([string]::IsNullOrEmpty($filename) -or [string]::IsNullOrEmpty($tag)) { continue }
    
    $file = Join-Path $TestDir "$filename.java"
    if (-not (Test-Path $file)) {
        Write-Host "  SKIP: $filename.java not found"
        $skipped++
        continue
    }
    
    $content = Get-Content $file -Raw
    
    # Check if already has this @Tag
    if ($content -match "^@Tag\(`"$tag`"\)") {
        Write-Host "  SKIP: $filename already has @Tag(""$tag"")"
        $skipped++
        continue
    }
    
    # Add import org.junit.jupiter.api.Tag if missing
    if ($content -notmatch "^import org\.junit\.jupiter\.api\.Tag;") {
        # Find the last import org.junit.jupiter.api line and insert after it
        if ($content -match "(?m)^(import org\.junit\.jupiter\.api\.[^;]+;)\s*$") {
            $lastImportLine = $matches[0].TrimEnd()
            $content = $content -replace [regex]::Escape($lastImportLine), "$lastImportLine`nimport org.junit.jupiter.api.Tag;"
        } else {
            # No jupiter imports found, insert after package line
            $content = $content -replace "(?m)^(package [^;]+;)", "`$1`n`nimport org.junit.jupiter.api.Tag;"
        }
    }
    
    # Add @Tag("...") before the class declaration
    # Handle both "public class" and "class" declarations
    if ($content -match "(?m)^((public\s+)?class\s+$filename)") {
        $classLine = $matches[0]
        $newClassLine = "@Tag(""$tag"")`n$classLine"
        $content = $content -replace [regex]::Escape($classLine), $newClassLine
    }
    
    Set-Content -Path $file -Value $content -NoNewline
    $count++
    Write-Host "  OK: $filename <- @Tag(""$tag"")"
}

Write-Host ""
Write-Host "Done. Modified $count files, skipped $skipped."
