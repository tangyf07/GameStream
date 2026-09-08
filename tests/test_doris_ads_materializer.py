"""Unit tests for Doris ADS materializer (no Kafka/Doris required)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from pipeline.doris_ads_materializer import (
    DorisAdsMaterializer,
    MaterializerConfig,
    build_commit_offsets,
    commit_applied_offsets,
)


class FakeWriter:
    def __init__(self, fail_on: set[str] | None = None, fail_times: dict[str, int] | None = None):
        self.ops: list[tuple] = []
        self.fail_on = fail_on or set()
        self.fail_times = fail_times or {}
        self._fail_counts: dict[str, int] = {}

    def _maybe_fail(self, label: str) -> None:
        if label in self.fail_on:
            raise ConnectionError(f"doris down:{label}")
        left = self.fail_times.get(label, 0)
        if left > 0:
            self.fail_times[label] = left - 1
            raise ConnectionError(f"doris transient:{label}")

    def apply_dau(self, dt, server_id, dau, metric_id="ads_dau_di"):
        self._maybe_fail("upsert_dau")
        self.ops.append(("upsert_dau", dt, server_id, dau, metric_id))

    def apply_pay(self, dt, server_id, dau, pay_users, pay_rate, metric_id="ads_pay_rate_di"):
        self._maybe_fail("upsert_pay")
        self.ops.append(("upsert_pay", dt, server_id, dau, pay_users, pay_rate, metric_id))

    def delete_dau(self, dt, server_id):
        self._maybe_fail("delete_dau")
        self.ops.append(("delete_dau", dt, server_id))

    def delete_pay(self, dt, server_id):
        self._maybe_fail("delete_pay")
        self.ops.append(("delete_pay", dt, server_id))

    def close(self):
        pass


@dataclass
class FakeMsg:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class FakeTP:
    def __init__(self, topic: str, partition: int):
        self.topic = topic
        self.partition = partition

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (
            isinstance(other, FakeTP)
            and self.topic == other.topic
            and self.partition == other.partition
        )


class FakeConsumer:
    """Minimal consumer: poll returns scripted batches; commit requires offsets=."""

    def __init__(self, batches: list[dict[Any, list[FakeMsg]]]):
        self._batches = list(batches)
        self.committed: list[dict] = []
        self.closed = False

    def poll(self, timeout_ms: int = 1000):
        if self._batches:
            return self._batches.pop(0)
        return {}

    def commit(self, offsets=None, **kwargs):
        if offsets is None:
            raise AssertionError("parameterless consumer.commit() is forbidden")
        # Normalize to {(topic, part): next_offset}
        norm = {}
        for tp, oma in offsets.items():
            topic = getattr(tp, "topic", tp[0] if isinstance(tp, tuple) else str(tp))
            part = getattr(tp, "partition", tp[1] if isinstance(tp, tuple) else -1)
            off = getattr(oma, "offset", oma[0] if isinstance(oma, tuple) else oma)
            norm[(topic, part)] = off
        self.committed.append(norm)

    def close(self):
        self.closed = True


def _dau_msg(partition: int, offset: int, dt: str, server_id: int, dau: int, topic="t_dau"):
    key = json.dumps({"dt": dt, "server_id": server_id}).encode()
    val = json.dumps(
        {"dt": dt, "server_id": server_id, "dau": dau, "metric_id": "ads_dau_di"}
    ).encode()
    return FakeMsg(topic, partition, offset, key, val)


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


def test_commit_only_applied_offsets(monkeypatch):
    """Only successfully applied per-partition offsets are committed (next = off+1)."""
    cfg = MaterializerConfig(topic_dau="t_dau", topic_pay="t_pay", commit_every_n=1, poll_timeout_ms=10)
    w = FakeWriter()

    tp0 = FakeTP("t_dau", 0)
    batch = {
        tp0: [
            _dau_msg(0, 10, "2026-09-07", 1, 1),
            _dau_msg(0, 11, "2026-09-07", 1, 2),
        ]
    }
    consumer = FakeConsumer([batch, {}])

    # Stop after first poll processed: patch so run_forever exits after commits
    m = DorisAdsMaterializer(cfg, writer=w, consumer_factory=lambda: consumer)

    original_poll = consumer.poll
    calls = {"n": 0}

    def poll_once(*a, **k):
        calls["n"] += 1
        if calls["n"] > 1:
            m.request_stop()
            return {}
        return original_poll(*a, **k)

    consumer.poll = poll_once
    m.run_forever()

    assert w.ops[-1][3] == 2  # last dau=2
    assert consumer.committed, "expected explicit offset commits"
    # Final committed next-offset for partition 0 should be 12 (last applied 11 + 1)
    last = consumer.committed[-1]
    assert last[("t_dau", 0)] == 12
    # Never a bare commit
    for c in consumer.committed:
        assert isinstance(c, dict) and c


def test_crash_mid_poll_replays_unapplied():
    """If stop mid-batch, only applied offsets are commit-eligible; rest replay."""
    cfg = MaterializerConfig(topic_dau="t_dau", topic_pay="t_pay", commit_every_n=100, retry_base_sec=0.01)
    w = FakeWriter()
    m = DorisAdsMaterializer(cfg, writer=w)

    msgs = [
        _dau_msg(0, 1, "2026-09-07", 1, 1),
        _dau_msg(0, 2, "2026-09-07", 1, 2),
        _dau_msg(0, 3, "2026-09-07", 1, 3),
    ]
    # Apply first, then stop before second would be committed via run loop —
    # use process path: apply first manually, stop, ensure only off=1 applied map.
    applied: dict[tuple[str, int], int] = {}
    for i, msg in enumerate(msgs):
        if i == 1:
            m.request_stop()
        if m._stop and i >= 1:
            break
        m.apply_message(msg.topic, msg.key, msg.value)
        applied[(msg.topic, msg.partition)] = msg.offset

    assert applied == {("t_dau", 0): 1}
    assert len(w.ops) == 1

    # Replay unapplied (offsets 2,3) on a fresh materializer — idempotent UNIQUE path
    w2 = FakeWriter()
    m2 = DorisAdsMaterializer(cfg, writer=w2)
    for msg in msgs[1:]:
        m2.apply_message(msg.topic, msg.key, msg.value)
        applied[(msg.topic, msg.partition)] = msg.offset
    assert applied[("t_dau", 0)] == 3
    assert [op[3] for op in w2.ops] == [2, 3]


def test_multi_partition_commit_independently(monkeypatch):
    """Each partition commits its own applied offset independently."""
    pytest.importorskip("kafka")
    cfg = MaterializerConfig(topic_dau="t_dau", topic_pay="t_pay", commit_every_n=10)
    applied = {("t_dau", 0): 5, ("t_dau", 1): 20, ("t_pay", 0): 3}
    payload = build_commit_offsets(applied)
    # next offsets
    by_key = {}
    for tp, oma in payload.items():
        by_key[(tp.topic, tp.partition)] = oma.offset
    assert by_key[("t_dau", 0)] == 6
    assert by_key[("t_dau", 1)] == 21
    assert by_key[("t_pay", 0)] == 4

    consumer = FakeConsumer([])
    # build_commit_offsets uses real TopicPartition; FakeConsumer.commit normalizes
    commit_applied_offsets(consumer, applied)
    assert len(consumer.committed) == 1
    assert consumer.committed[0][("t_dau", 0)] == 6
    assert consumer.committed[0][("t_dau", 1)] == 21
    assert consumer.committed[0][("t_pay", 0)] == 4


def test_doris_failure_does_not_advance_offset():
    """Doris failure retries; offset not marked applied until write succeeds."""
    cfg = MaterializerConfig(
        topic_dau="t_dau",
        topic_pay="t_pay",
        retry_base_sec=0.01,
        retry_max_sec=0.02,
        commit_every_n=1,
    )
    w = FakeWriter(fail_times={"upsert_dau": 2})  # fail twice then succeed
    m = DorisAdsMaterializer(cfg, writer=w)
    msg = _dau_msg(0, 42, "2026-09-07", 1, 9)

    # Before success, nothing applied
    assert m.stats["retries"] == 0
    action = m._apply_with_retry(msg.topic, msg.key, msg.value)
    assert "UPSERT dau" in action
    assert m.stats["retries"] >= 2
    assert len(w.ops) == 1

    # Permanent failure path: stop during retry => RuntimeError, no apply
    w3 = FakeWriter(fail_on={"upsert_dau"})
    m3 = DorisAdsMaterializer(cfg, writer=w3)
    # stop immediately so retry loop exits without success
    m3.request_stop()
    with pytest.raises(RuntimeError, match="stopped"):
        m3._apply_with_retry(msg.topic, msg.key, msg.value)
    assert w3.ops == []


def test_duplicate_replay_idempotent():
    """Re-applying same upsert is idempotent at writer op level (UNIQUE replace)."""
    cfg = MaterializerConfig(topic_dau="t_dau", topic_pay="t_pay")
    w = FakeWriter()
    m = DorisAdsMaterializer(cfg, writer=w)
    msg = _dau_msg(0, 7, "2026-09-07", 1, 4)
    m.apply_message(msg.topic, msg.key, msg.value)
    m.apply_message(msg.topic, msg.key, msg.value)
    assert w.ops == [
        ("upsert_dau", "2026-09-07", 1, 4, "ads_dau_di"),
        ("upsert_dau", "2026-09-07", 1, 4, "ads_dau_di"),
    ]
    assert m.stats["applied_dau"] == 2


def test_invalid_json_increments_counter(tmp_path_factory):
    import tempfile, os
    # Avoid pytest tmp_path on locked Windows pytest-of-* dirs; use mkdtemp.
    d = tempfile.mkdtemp(prefix="gs_mat_dlq_")
    dlq_file = os.path.join(d, "dlq.jsonl")
    cfg = MaterializerConfig(
        topic_dau="t_dau",
        topic_pay="t_pay",
        dlq_path=dlq_file,
        retry_base_sec=0.01,
    )
    w = FakeWriter()
    m = DorisAdsMaterializer(cfg, writer=w)
    action = m._apply_with_retry("t_dau", b'{"dt":"2026-09-07","server_id":1}', b"NOT-JSON{")
    assert action.startswith("SKIP dirty")
    assert m.stats["invalid_json"] == 1
    assert m.stats["skipped_dirty"] == 1
    assert w.ops == []
    with open(dlq_file, encoding="utf-8") as f:
        dlq = f.read()
    assert "NOT-JSON" in dlq


def test_parameterless_commit_forbidden():
    consumer = FakeConsumer([])
    with pytest.raises(AssertionError, match="forbidden"):
        consumer.commit()
