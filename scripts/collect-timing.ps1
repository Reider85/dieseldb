# collect-timing.ps1 - collect per-test wall times from surefire XML reports
# Output: timing/timingN.md in the format consumed by compare-timing.ps1/.sh:
#   <test_name> <seconds>
# Usage: powershell -ExecutionPolicy Bypass -File scripts/collect-timing.ps1
#
# PowerShell twin of collect-timing.py. The Makefile keeps calling the .py
# (PY ?= python3); make.ps1 calls this script instead, because on Windows
# `python` may resolve to the non-functional WindowsApps stub (exit 9009),
# which silently produced a stale timingN.md and a vacuous timing gate.

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

$inv = [System.Globalization.CultureInfo]::InvariantCulture
$rows = New-Object System.Collections.Generic.List[object]

$reports = Get-ChildItem -Path (Join-Path $repoRoot "target\surefire-reports\TEST-*.xml") -ErrorAction SilentlyContinue
foreach ($path in $reports) {
    try {
        [xml]$doc = Get-Content -LiteralPath $path.FullName -Raw -ErrorAction Stop
    }
    catch {
        Write-Host "WARN: cannot parse $($path.Name): $($_.Exception.Message)"
        continue
    }
    foreach ($tc in $doc.SelectNodes("//testcase")) {
        $cn = $tc.GetAttribute("classname")
        $cls = ($cn -split "\.")[-1]
        $name = "{0}.{1}" -f $cls, $tc.GetAttribute("name")
        $secs = 0.0
        $raw = $tc.GetAttribute("time")
        if ($raw) {
            [void][double]::TryParse($raw, [System.Globalization.NumberStyles]::Float, $inv, [ref]$secs)
        }
        $rows.Add([pscustomobject]@{ Name = $name; Secs = $secs })
    }
}

$sorted = $rows | Sort-Object -Property Secs -Descending
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("Test Time(s)")
foreach ($r in $sorted) {
    $lines.Add(("{0} {1}" -f $r.Name, $r.Secs.ToString("F3", $inv)))
}

New-Item -ItemType Directory -Path (Join-Path $repoRoot "timing") -Force | Out-Null
[System.IO.File]::WriteAllLines((Join-Path $repoRoot "timing\timingN.md"), $lines)
Write-Host "Wrote $($rows.Count) rows to timing/timingN.md"
