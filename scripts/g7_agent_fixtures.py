#!/usr/bin/env python3
"""Deterministic Agent-shaped NL → SQL fixtures for GameStream G7 closed loop.

Maps natural-language prompts to contract-aligned SELECT SQL (metric_id / ads.*).
Used when DataPilot LLM / ChatBI is unavailable; still goes through SQLGuard → Doris ADS.
See docs/datapilot_contract.md + config/metrics.yaml.

Negative paths exercise strict hallucination (allow_unknown_*=false) + DELETE block.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

FIXTURES: list[dict[str, Any]] = [
    {
        "path_id": "dau",
        "prompt": "DAU多少？按日期和服区分组",
        "metric_id": "ads_dau_di",
        "table": "ads.ads_dau_di",
        "sql": (
            "SELECT dt, server_id, dau, metric_id "
            "FROM ads.ads_dau_di "
            "WHERE metric_id = 'ads_dau_di' "
            "ORDER BY dt, server_id"
        ),
        "expect_gate": "EXECUTE",
    },
    {
        "path_id": "pay_rate",
        "prompt": "各服付费率是多少？给出 DAU、付费人数和付费率",
        "metric_id": "ads_pay_rate_di",
        "table": "ads.ads_pay_rate_di",
        "sql": (
            "SELECT dt, server_id, dau, pay_users, pay_rate, metric_id "
            "FROM ads.ads_pay_rate_di "
            "WHERE metric_id = 'ads_pay_rate_di' "
            "ORDER BY dt, server_id"
        ),
        "expect_gate": "EXECUTE",
    },
    {
        "path_id": "block_unknown_column",
        "prompt": "(negative) 查询不存在的列 not_a_real_col",
        "metric_id": "ads_dau_di",
        "table": "ads.ads_dau_di",
        "sql": (
            "SELECT dt, server_id, not_a_real_col "
            "FROM ads.ads_dau_di"
        ),
        "expect_gate": "BLOCK",
    },
    {
        "path_id": "block_unknown_table",
        "prompt": "(negative) 查询未在 allowlist 的表",
        "metric_id": None,
        "table": "ads.ads_not_on_allowlist",
        "sql": "SELECT dt FROM ads.ads_not_on_allowlist",
        "expect_gate": "BLOCK",
    },
    {
        "path_id": "cross_db_same_name",
        "prompt": "(probe) 跨库同名 hive.ads_dau_di — AST 是否保留 schema",
        "metric_id": "ads_dau_di",
        "table": "hive.ads_dau_di",
        "sql": (
            "SELECT dt, server_id, dau, metric_id "
            "FROM hive.ads_dau_di "
            "WHERE metric_id = 'ads_dau_di'"
        ),
        # sql-write-gate strips schema → bare ads_dau_di may ALLOW/EXECUTE.
        # Soft expect: record actual datapilot; closed-loop treats SOFT_* specially.
        "expect_gate": "SOFT_DOCUMENT",
        "note": "SQLGuard/sqlglot Table.name strips schema; hive.ads_dau_di → ads_dau_di",
    },
    {
        "path_id": "block_delete",
        "prompt": "(negative) 清空 DAU 表",
        "metric_id": "ads_dau_di",
        "table": "ads.ads_dau_di",
        "sql": "DELETE FROM ads.ads_dau_di",
        "expect_gate": "BLOCK",
    },
]


def main() -> None:
    ap = argparse.ArgumentParser(description="G7 Agent-shaped fixtures")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--path", choices=[f["path_id"] for f in FIXTURES])
    args = ap.parse_args()
    if args.list:
        for f in FIXTURES:
            print(f["path_id"])
        return
    if args.path:
        print(json.dumps(next(x for x in FIXTURES if x["path_id"] == args.path), ensure_ascii=False))
        return
    print(json.dumps(FIXTURES, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
