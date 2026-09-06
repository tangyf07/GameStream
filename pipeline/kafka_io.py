#!/usr/bin/env python3
"""
Kafka I/O helpers.

Lite path: file-based topic simulation under data/topics/ (no Docker / no broker).
Optional: confluent-kafka if installed (production-shaped).
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Iterator


class FileTopic:
    """Append-only JSONL folder mimicking a Kafka topic partition."""

    def __init__(self, base_dir: str | Path, topic: str, partition: int = 0):
        self.dir = Path(base_dir) / topic / f"p{partition}"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._seq = 0
        existing = sorted(self.dir.glob("*.jsonl"))
        if existing:
            # continue sequence after last file index
            try:
                self._seq = int(existing[-1].stem) + 1
            except ValueError:
                self._seq = len(existing)

    def produce(self, records: list[dict[str, Any]], batch_size: int = 10000) -> int:
        written = 0
        buf: list[str] = []
        for rec in records:
            buf.append(json.dumps(rec, ensure_ascii=False))
            if len(buf) >= batch_size:
                self._flush(buf)
                written += len(buf)
                buf = []
        if buf:
            self._flush(buf)
            written += len(buf)
        return written

    def produce_file(self, jsonl_path: str | Path) -> int:
        """Copy / split an existing JSONL into the topic folder as one segment."""
        src = Path(jsonl_path)
        dest = self.dir / f"{self._seq:06d}.jsonl"
        shutil.copyfile(src, dest)
        self._seq += 1
        # count lines
        n = 0
        with src.open("r", encoding="utf-8") as f:
            for _ in f:
                n += 1
        return n

    def _flush(self, lines: list[str]) -> None:
        dest = self.dir / f"{self._seq:06d}.jsonl"
        with dest.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        self._seq += 1

    def consume(self) -> Iterator[dict[str, Any]]:
        for path in sorted(self.dir.glob("*.jsonl")):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

    def clear(self) -> None:
        if self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._seq = 0


def try_confluent_producer(bootstrap: str, topic: str):
    """Return a confluent Producer if available, else None."""
    try:
        from confluent_kafka import Producer  # type: ignore
    except ImportError:
        return None
    return Producer({"bootstrap.servers": bootstrap})


def publish_jsonl_to_kafka(jsonl_path: str, bootstrap: str, topic: str) -> int:
    """Optional real Kafka publish; raises if confluent-kafka missing."""
    producer = try_confluent_producer(bootstrap, topic)
    if producer is None:
        raise RuntimeError(
            "confluent-kafka not installed; use FileTopic for local lite path"
        )
    n = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            producer.produce(topic, line.encode("utf-8"))
            n += 1
            if n % 1000 == 0:
                producer.poll(0)
    producer.flush()
    return n
