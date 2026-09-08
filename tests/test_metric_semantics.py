"""Hand-crafted deterministic metric semantics (exact DAU / pay_rate / retention).

Not a smoke row-count check: fixture events are fixed so ADS values are asserted
exactly after DuckDB local ODS→DWD→DWS→ADS.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _evt(
    event_id: str,
    event_type: str,
    event_time: str,
    player_id: int,
    *,
    server_id: int = 1,
    role_id: int | None = None,
    session_id: str = "s1",
    payload: dict | None = None,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time,
        "player_id": player_id,
        "role_id": role_id if role_id is not None else player_id * 10,
        "server_id": server_id,
        "session_id": session_id,
        "payload": payload or {},
    }


def _write_fixture(path: Path) -> None:
    """Fixed timeline on server 1 spanning 2024-01-01 .. 2024-01-08.

    Cohort day 2024-01-01 players: 101, 102, 103
      - 101 active on +1, +3, +7
      - 102 active on +1 only
      - 103 recharge on cohort day only
    Duplicate event_id on cohort day must not inflate DAU.
    """
    rows = [
        # 2024-01-01 cohort day — DAU=3, pay_users=1 (103), pay_rate=1/3
        _evt("e101-login", "login", "2024-01-01T10:00:00.000Z", 101),
        _evt("e101-login-dup", "login", "2024-01-01T10:05:00.000Z", 101),  # unique id, same player
        _evt("e102-login", "login", "2024-01-01T11:00:00.000Z", 102),
        _evt(
            "e103-login",
            "login",
            "2024-01-01T12:00:00.000Z",
            103,
        ),
        _evt(
            "e103-recharge",
            "recharge",
            "2024-01-01T12:30:00.000Z",
            103,
            payload={"amount_fen": 10000, "product_id": "com.gs.gem100"},
        ),
        # intentional duplicate event_id (second row dropped in DWD)
        _evt("e103-recharge", "recharge", "2024-01-01T12:31:00.000Z", 103, payload={"amount_fen": 10000}),
        # 2024-01-02 = cohort+1 — 101, 102 retained
        _evt("e101-d1", "login", "2024-01-02T10:00:00.000Z", 101),
        _evt("e102-d1", "login", "2024-01-02T11:00:00.000Z", 102),
        # 2024-01-04 = cohort+3 — 101 retained
        _evt("e101-d3", "login", "2024-01-04T10:00:00.000Z", 101),
        # 2024-01-08 = cohort+7 — 101 retained; also completes n=7 observation window
        _evt("e101-d7", "login", "2024-01-08T10:00:00.000Z", 101),
        # logout with online_sec for duration metric (not asserted exactly beyond presence)
        _evt(
            "e101-logout-d7",
            "logout",
            "2024-01-08T11:00:00.000Z",
            101,
            payload={"online_sec": 3600},
        ),
    ]
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


@pytest.fixture
def semantics_ads(tmp_path):
    pytest.importorskip("duckdb")
    import duckdb
    import pipeline.local_runner as lr

    raw = tmp_path / "semantics_events.jsonl"
    _write_fixture(raw)

    paths = {
        "data_root": str(tmp_path),
        "raw_jsonl": str(raw),
        "ods_parquet": str(tmp_path / "ods.parquet"),
        "dwd_parquet": str(tmp_path / "dwd.parquet"),
        "dws_dir": str(tmp_path / "dws"),
        "ads_dir": str(tmp_path / "ads"),
        "duckdb": str(tmp_path / "semantics.duckdb"),
        "sample_raw": str(tmp_path / "sample.jsonl"),
    }
    Path(paths["dws_dir"]).mkdir()
    Path(paths["ads_dir"]).mkdir()

    con = duckdb.connect(paths["duckdb"])
    try:
        lr.step_ods_dwd(con, paths, raw)
        lr.step_dws(con, paths)
        lr.step_ads(con, paths)
        yield con
    finally:
        con.close()


def test_dau_exact(semantics_ads):
    con = semantics_ads
    row = con.execute(
        """
        SELECT dau, metric_id
        FROM ads.ads_dau_di
        WHERE dt = DATE '2024-01-01' AND server_id = 1
        """
    ).fetchone()
    assert row is not None
    assert row[0] == 3
    assert row[1] == "ads_dau_di"

    # duplicate event_id must not create a 4th player or extra day row inflation
    dwd_n = con.execute(
        "SELECT COUNT(*) FROM dwd.player_events_clean WHERE event_id = 'e103-recharge'"
    ).fetchone()[0]
    assert dwd_n == 1


def test_pay_rate_exact(semantics_ads):
    con = semantics_ads
    row = con.execute(
        """
        SELECT dau, pay_users, pay_rate, metric_id
        FROM ads.ads_pay_rate_di
        WHERE dt = DATE '2024-01-01' AND server_id = 1
        """
    ).fetchone()
    assert row is not None
    dau, pay_users, pay_rate, metric_id = row
    assert dau == 3
    assert pay_users == 1
    assert abs(float(pay_rate) - (1.0 / 3.0)) < 1e-9
    assert metric_id == "ads_pay_rate_di"

    arpu = con.execute(
        """
        SELECT revenue_cny, arpu_cny
        FROM ads.ads_arpu_di
        WHERE dt = DATE '2024-01-01' AND server_id = 1
        """
    ).fetchone()
    assert arpu is not None
    assert abs(float(arpu[0]) - 100.0) < 1e-9  # 10000 fen
    assert abs(float(arpu[1]) - (100.0 / 3.0)) < 1e-9


def test_retention_exact(semantics_ads):
    con = semantics_ads
    rows = con.execute(
        """
        SELECT n_days, cohort_size, retained_cnt, retention_rate, window_complete
        FROM ads.ads_retention_nd
        WHERE cohort_dt = DATE '2024-01-01' AND server_id = 1
        ORDER BY n_days
        """
    ).fetchall()
    by_n = {int(r[0]): r for r in rows}

    # window complete for 1/3/7 because max_dt = 2024-01-08
    assert set(by_n) == {1, 3, 7}

    # cohort 101,102,103
    for n in (1, 3, 7):
        assert by_n[n][1] == 3
        assert by_n[n][4] is True

    # +1: 101,102 → 2/3
    assert by_n[1][2] == 2
    assert abs(float(by_n[1][3]) - (2.0 / 3.0)) < 1e-9

    # +3: 101 → 1/3
    assert by_n[3][2] == 1
    assert abs(float(by_n[3][3]) - (1.0 / 3.0)) < 1e-9

    # +7: 101 → 1/3
    assert by_n[7][2] == 1
    assert abs(float(by_n[7][3]) - (1.0 / 3.0)) < 1e-9

    # Incomplete windows must not emit: no cohort on 2024-01-08 for n=1
    late = con.execute(
        """
        SELECT COUNT(*) FROM ads.ads_retention_nd
        WHERE cohort_dt = DATE '2024-01-08'
        """
    ).fetchone()[0]
    assert late == 0
