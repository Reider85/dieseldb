$ErrorActionPreference = 'Stop'
$files = & git status --short | ForEach-Object {
    $line = $_.ToString()
    if ($line.Length -gt 3) { $line.Substring(3).Trim() }
} | Where-Object { $_ -and (Test-Path $_) }
$fixed = 0
foreach ($f in $files) {
    $bytes = [System.IO.File]::ReadAllBytes($f)
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $new = New-Object byte[] ($bytes.Length - 3)
        [Array]::Copy($bytes, 3, $new, 0, $new.Length)
        [System.IO.File]::WriteAllBytes($f, $new)
        $fixed++
    }
}
Write-Host "Stripped BOM from $fixed files"
