"""Ensure metric SQL files exist and reference expected metric_id / tables."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS_DIR = ROOT / "sql" / "metrics"

EXPECTED = [
    "ads_dau_di",
    "ads_retention_nd",
    "ads_online_duration_di",
    "ads_pay_rate_di",
    "ads_arpu_di",
    "ads_dungeon_clear_rate_di",
    "ads_churn_di",
]


def test_metric_sql_files_present():
    for mid in EXPECTED:
        path = METRICS_DIR / f"{mid}.sql"
        assert path.exists(), f"missing {path}"
        text = path.read_text(encoding="utf-8")
        assert mid in text
        assert "SELECT" in text.upper()


def test_ddl_ads_covers_metrics():
    ddl = (ROOT / "sql" / "ddl" / "04_ads.sql").read_text(encoding="utf-8")
    for mid in EXPECTED:
        assert mid in ddl, f"ADS DDL missing {mid}"
