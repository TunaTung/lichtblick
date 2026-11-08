$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$pythonCandidates = @(
  (Join-Path $Root "venv\Scripts\python.exe"),
  (Join-Path $Root "..\dashboard\venv\Scripts\python.exe"),
  "python"
)
$python = $pythonCandidates | Where-Object { $_ -eq "python" -or (Test-Path $_) } | Select-Object -First 1
if (-not $python) { throw "Python not found. Create linggou/venv and pip install foxglove-sdk websockets." }

Write-Output "Remote page: http://127.0.0.1:8088/"
Write-Output "Lichtblick WS: ws://127.0.0.1:8765"
Write-Output "Car TCP: 8090 (startup is STOP; only a new physical press starts motion)"
& $python (Join-Path $Root "controller.py") --lightbrick
