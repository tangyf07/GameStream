# Windows one-click: quality gate (fail-stop) + demo
param(
    [int]$Players = 2000,
    [int]$Events = 20000,
    [int]$Days = 7,
    [int]$Seed = 42
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if ($env:PLAYERS) { $Players = [int]$env:PLAYERS }
if ($env:EVENTS) { $Events = [int]$env:EVENTS }

Write-Host "=== GameStream run_all ==="
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

& .\scripts\quality_gate.ps1
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "--- demo: players=$Players events=$Events days=$Days seed=$Seed ---"
python pipeline/local_runner.py --players $Players --events $Events --days $Days --seed $Seed
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "=== run_all DONE ==="
Write-Host "DuckDB: data/gamestream.duckdb"
Write-Host "ADS parquet: data/ads/"
