# Fail-stop quality gate (Windows)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== GameStream quality gate ==="
if (-not $env:VIRTUAL_ENV) {
    if (Test-Path ".venv") {
        & .\.venv\Scripts\Activate.ps1
    } elseif (Test-Path "venv") {
        & .\venv\Scripts\Activate.ps1
    } else {
        python -m venv .venv
        & .\.venv\Scripts\Activate.ps1
    }
}
pip install -q -r requirements.txt pytest pyyaml duckdb

Write-Host "--- AST check (pipeline) ---"
python -c "import ast; ast.parse(open('pipeline/local_runner.py').read()); ast.parse(open('pipeline/kafka_io.py').read()); print('AST-ok')"
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "--- pytest ---"
python -m pytest tests/ -v --tb=short
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host "=== quality gate PASSED ==="
