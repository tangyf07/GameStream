"""Smoke test: tiny simulate → DuckDB ADS."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_pipeline_smoke(tmp_path, monkeypatch):
    pytest.importorskip("duckdb")
    from simulator.generate_events import generate
    import pipeline.local_runner as lr

    raw = tmp_path / "events.jsonl"
    generate(players=100, events=500, servers=2, days=5, seed=7, out_path=raw)

    # Point paths into tmp
    paths = {
        "data_root": str(tmp_path),
        "raw_jsonl": str(raw),
        "ods_parquet": str(tmp_path / "ods.parquet"),
        "dwd_parquet": str(tmp_path / "dwd.parquet"),
        "dws_dir": str(tmp_path / "dws"),
        "ads_dir": str(tmp_path / "ads"),
        "duckdb": str(tmp_path / "test.duckdb"),
        "sample_raw": str(tmp_path / "sample.jsonl"),
    }
    Path(paths["dws_dir"]).mkdir()
    Path(paths["ads_dir"]).mkdir()

    con = duckdb.connect(paths["duckdb"])
    try:
        lr.step_ods_dwd(con, paths, raw)
        lr.step_dws(con, paths)
        lr.step_ads(con, paths)
        dau = con.execute("SELECT COUNT(*) FROM ads.ads_dau_di").fetchone()[0]
        assert dau > 0
        for tbl in (
            "ads_retention_nd",
            "ads_online_duration_di",
            "ads_pay_rate_di",
            "ads_arpu_di",
            "ads_dungeon_clear_rate_di",
            "ads_churn_di",
        ):
            n = con.execute(f"SELECT COUNT(*) FROM ads.{tbl}").fetchone()[0]
            assert n >= 0  # churn/retention may be sparse but table must exist
            assert n > 0 or tbl in ("ads_retention_nd",)  # still expect rows
    finally:
        con.close()
