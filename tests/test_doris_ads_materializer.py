"""Unit tests for Doris ADS materializer apply path (no Kafka/Doris required)."""
from __future__ import annotations

import json

from pipeline.doris_ads_materializer import DorisAdsMaterializer, MaterializerConfig


class FakeWriter:
    def __init__(self):
        self.ops = []

    def apply_dau(self, dt, server_id, dau, metric_id="ads_dau_di"):
        self.ops.append(("upsert_dau", dt, server_id, dau, metric_id))

    def apply_pay(self, dt, server_id, dau, pay_users, pay_rate, metric_id="ads_pay_rate_di"):
        self.ops.append(("upsert_pay", dt, server_id, dau, pay_users, pay_rate, metric_id))

    def delete_dau(self, dt, server_id):
        self.ops.append(("delete_dau", dt, server_id))

    def delete_pay(self, dt, server_id):
        self.ops.append(("delete_pay", dt, server_id))

    def close(self):
        pass


def test_upsert_dau_and_pay_and_tombstone():
    cfg = MaterializerConfig(topic_dau="t_dau", topic_pay="t_pay")
    w = FakeWriter()
    m = DorisAdsMaterializer(cfg, writer=w)

    key = json.dumps({"dt": "2026-09-07", "server_id": 1}).encode()
    dau_val = json.dumps(
        {"dt": "2026-09-07", "server_id": 1, "dau": 4, "metric_id": "ads_dau_di"}
    ).encode()
    pay_val = json.dumps(
        {
            "dt": "2026-09-07",
            "server_id": 1,
            "dau": 4,
            "pay_users": 1,
            "pay_rate": 0.25,
            "metric_id": "ads_pay_rate_di",
        }
    ).encode()

    assert "UPSERT dau" in m.apply_message("t_dau", key, dau_val)
    assert "UPSERT pay" in m.apply_message("t_pay", key, pay_val)
    assert "DELETE dau" in m.apply_message("t_dau", key, None)
    assert "DELETE pay" in m.apply_message("t_pay", key, None)

    assert w.ops[0][0] == "upsert_dau" and w.ops[0][3] == 4
    assert w.ops[1][0] == "upsert_pay" and w.ops[1][4] == 1
    assert w.ops[2] == ("delete_dau", "2026-09-07", 1)
    assert w.ops[3] == ("delete_pay", "2026-09-07", 1)
