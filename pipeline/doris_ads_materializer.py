"""Resident Doris ADS materializer for GameStream G8.

Consumes Flink upsert-kafka changelog topics (ALS + PK) and applies rows to
Doris UNIQUE KEY tables via plain INSERT / DELETE.

Semantics (honest):
  - at-least-once: Kafka offsets commit ONLY after Doris write confirmed
  - idempotent upserts: UNIQUE KEY replace (duplicate upserts OK)
  - tombstone (null value) => DELETE by (dt, server_id)
  - Doris brief outage: retry without committing => no silent skip
  - NOT end-to-end EO-2PC; NOT Flink JDBC upsert (Doris FE rejects that dialect)
"""
from __future__ import annotations

import json
import logging
import signal
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

log = logging.getLogger("g8.doris_materializer")

TOPIC_DAU_DEFAULT = "gamestream.g8.ads_dau"
TOPIC_PAY_DEFAULT = "gamestream.g8.ads_pay_rate"
GROUP_DEFAULT = "gamestream-g8-doris-materializer"


@dataclass
class MaterializerConfig:
    bootstrap: str = "localhost:19092"
    group_id: str = GROUP_DEFAULT
    topic_dau: str = TOPIC_DAU_DEFAULT
    topic_pay: str = TOPIC_PAY_DEFAULT
    doris_host: str = "127.0.0.1"
    doris_port: int = 9030
    doris_user: str = "root"
    doris_password: str = ""
    doris_db: str = "ads"
    poll_timeout_ms: int = 1000
    retry_base_sec: float = 1.0
    retry_max_sec: float = 30.0
    commit_every_n: int = 1  # commit after each successful apply (safest ALS)
    auto_offset_reset: str = "earliest"


class DorisWriter:
    """Thin PyMySQL writer with reconnect + retry."""

    def __init__(self, cfg: MaterializerConfig):
        self.cfg = cfg
        self._conn = None

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _connect(self):
        import pymysql

        self.close()
        self._conn = pymysql.connect(
            host=self.cfg.doris_host,
            port=self.cfg.doris_port,
            user=self.cfg.doris_user,
            password=self.cfg.doris_password,
            database=self.cfg.doris_db,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            autocommit=True,
            charset="utf8mb4",
        )
        return self._conn

    def conn(self):
        if self._conn is None:
            return self._connect()
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            return self._connect()
        return self._conn

    def apply_dau(self, dt: str, server_id: int, dau: int, metric_id: str = "ads_dau_di") -> None:
        sql = (
            "INSERT INTO ads_dau_di (dt, server_id, dau, metric_id) "
            "VALUES (%s, %s, %s, %s)"
        )
        c = self.conn()
        with c.cursor() as cur:
            cur.execute(sql, (dt, server_id, dau, metric_id or "ads_dau_di"))

    def apply_pay(
        self,
        dt: str,
        server_id: int,
        dau: int,
        pay_users: int,
        pay_rate: float,
        metric_id: str = "ads_pay_rate_di",
    ) -> None:
        sql = (
            "INSERT INTO ads_pay_rate_di (dt, server_id, dau, pay_users, pay_rate, metric_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        c = self.conn()
        with c.cursor() as cur:
            cur.execute(
                sql,
                (dt, server_id, dau, pay_users, pay_rate, metric_id or "ads_pay_rate_di"),
            )

    def delete_dau(self, dt: str, server_id: int) -> None:
        c = self.conn()
        with c.cursor() as cur:
            cur.execute(
                "DELETE FROM ads_dau_di WHERE dt=%s AND server_id=%s",
                (dt, server_id),
            )

    def delete_pay(self, dt: str, server_id: int) -> None:
        c = self.conn()
        with c.cursor() as cur:
            cur.execute(
                "DELETE FROM ads_pay_rate_di WHERE dt=%s AND server_id=%s",
                (dt, server_id),
            )


def _norm_dt(v: Any) -> str:
    if v is None:
        raise ValueError("dt is required")
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    if len(s) >= 10:
        return s[:10]
    raise ValueError(f"bad dt: {v!r}")


def _norm_int(v: Any, name: str) -> int:
    if v is None:
        raise ValueError(f"{name} is required")
    return int(v)


def _decode_json(raw: bytes | None) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        if len(raw) == 0:
            return None
        return json.loads(raw.decode("utf-8"))
    return raw


def _key_parts(key_obj: Any, val_obj: Any) -> tuple[str, int]:
    src = val_obj if isinstance(val_obj, dict) else None
    if src is None and isinstance(key_obj, dict):
        src = key_obj
    if not isinstance(src, dict):
        raise ValueError(f"cannot derive key from key={key_obj!r} value={val_obj!r}")
    return _norm_dt(src.get("dt")), _norm_int(src.get("server_id"), "server_id")


class DorisAdsMaterializer:
    def __init__(
        self,
        cfg: MaterializerConfig | None = None,
        writer: DorisWriter | None = None,
        consumer_factory: Callable[..., Any] | None = None,
    ):
        self.cfg = cfg or MaterializerConfig()
        self.writer = writer or DorisWriter(self.cfg)
        self._consumer_factory = consumer_factory
        self._stop = False
        self.stats = {
            "applied_dau": 0,
            "applied_pay": 0,
            "tombstone_dau": 0,
            "tombstone_pay": 0,
            "commits": 0,
            "retries": 0,
            "errors": 0,
        }

    def request_stop(self, *_args) -> None:
        log.info("stop requested")
        self._stop = True

    def _make_consumer(self):
        if self._consumer_factory is not None:
            return self._consumer_factory()
        from kafka import KafkaConsumer

        topics = [self.cfg.topic_dau, self.cfg.topic_pay]
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.cfg.bootstrap.split(","),
            group_id=self.cfg.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.cfg.auto_offset_reset,
            key_deserializer=lambda b: b,
            value_deserializer=lambda b: b,
            max_poll_records=50,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            request_timeout_ms=30000,
        )

    def apply_message(self, topic: str, key_raw: bytes | None, value_raw: bytes | None) -> str:
        """Apply one upsert-kafka record. Returns action label. Raises on Doris failure."""
        key_obj = _decode_json(key_raw) if key_raw else None
        val_obj = _decode_json(value_raw)

        if val_obj is None:
            # tombstone: need key
            dt, sid = _key_parts(key_obj, None)
            if topic == self.cfg.topic_dau:
                self.writer.delete_dau(dt, sid)
                self.stats["tombstone_dau"] += 1
                return f"DELETE dau {dt}/{sid}"
            if topic == self.cfg.topic_pay:
                self.writer.delete_pay(dt, sid)
                self.stats["tombstone_pay"] += 1
                return f"DELETE pay {dt}/{sid}"
            raise ValueError(f"unknown topic for tombstone: {topic}")

        if not isinstance(val_obj, dict):
            raise ValueError(f"value must be object, got {type(val_obj)}")

        dt, sid = _key_parts(key_obj, val_obj)

        if topic == self.cfg.topic_dau:
            dau = _norm_int(val_obj.get("dau"), "dau")
            metric = str(val_obj.get("metric_id") or "ads_dau_di")
            self.writer.apply_dau(dt, sid, dau, metric)
            self.stats["applied_dau"] += 1
            return f"UPSERT dau {dt}/{sid} dau={dau}"

        if topic == self.cfg.topic_pay:
            dau = _norm_int(val_obj.get("dau"), "dau")
            pay = _norm_int(val_obj.get("pay_users"), "pay_users")
            rate_raw = val_obj.get("pay_rate")
            if rate_raw is None:
                raise ValueError("pay_rate is required")
            rate = float(rate_raw)
            metric = str(val_obj.get("metric_id") or "ads_pay_rate_di")
            self.writer.apply_pay(dt, sid, dau, pay, rate, metric)
            self.stats["applied_pay"] += 1
            return f"UPSERT pay {dt}/{sid} dau={dau} pay={pay} rate={rate}"

        raise ValueError(f"unknown topic: {topic}")

    def _apply_with_retry(self, topic: str, key_raw, value_raw) -> str:
        delay = self.cfg.retry_base_sec
        while not self._stop:
            try:
                return self.apply_message(topic, key_raw, value_raw)
            except Exception as e:
                # parse / schema errors should not spin forever on same poison? 
                # For honesty: still don't commit; log and re-raise after classifying.
                msg = str(e).lower()
                transient = any(
                    x in msg
                    for x in (
                        "can't connect",
                        "connection",
                        "timed out",
                        "timeout",
                        "gone away",
                        "not available",
                        "refused",
                        "broken pipe",
                        "errno",
                        "packet sequence",
                        "lost connection",
                        "is not alive",
                        "backend",
                    )
                ) or e.__class__.__name__ in (
                    "OperationalError",
                    "InterfaceError",
                    "InternalError",
                )
                self.stats["retries"] += 1
                self.stats["errors"] += 1
                if not transient and isinstance(e, (ValueError, TypeError, json.JSONDecodeError, KeyError)):
                    log.error("non-transient apply error topic=%s err=%s", topic, e)
                    raise
                log.warning(
                    "Doris apply failed (will retry, offset NOT committed): %s; sleep=%.1fs",
                    e,
                    delay,
                )
                self.writer.close()
                time.sleep(delay)
                delay = min(self.cfg.retry_max_sec, delay * 2)
        raise RuntimeError("stopped during retry")

    def run_forever(self) -> None:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

        log.info(
            "starting materializer bootstrap=%s group=%s topics=%s,%s doris=%s:%s/%s",
            self.cfg.bootstrap,
            self.cfg.group_id,
            self.cfg.topic_dau,
            self.cfg.topic_pay,
            self.cfg.doris_host,
            self.cfg.doris_port,
            self.cfg.doris_db,
        )

        consumer = self._make_consumer()
        pending = 0
        try:
            while not self._stop:
                try:
                    batch = consumer.poll(timeout_ms=self.cfg.poll_timeout_ms)
                except Exception as e:
                    if self._stop:
                        break
                    log.warning("poll error: %s", e)
                    time.sleep(1)
                    continue

                if not batch:
                    continue

                # Apply all records; on failure retry the failing record without committing.
                for _tp, records in batch.items():
                    for msg in records:
                        if self._stop:
                            break
                        action = self._apply_with_retry(msg.topic, msg.key, msg.value)
                        pending += 1
                        log.info(
                            "applied %s topic=%s part=%s off=%s",
                            action,
                            msg.topic,
                            msg.partition,
                            msg.offset,
                        )
                        if pending >= self.cfg.commit_every_n:
                            consumer.commit()
                            self.stats["commits"] += 1
                            pending = 0
                if pending > 0:
                    consumer.commit()
                    self.stats["commits"] += 1
                    pending = 0
        finally:
            try:
                if pending > 0:
                    consumer.commit()
                    self.stats["commits"] += 1
            except Exception as e:
                log.warning("final commit skipped: %s", e)
            try:
                consumer.close()
            except Exception:
                pass
            self.writer.close()
            log.info("materializer stopped stats=%s", self.stats)


def config_from_env(env: dict[str, str] | None = None) -> MaterializerConfig:
    import os

    e = env or os.environ
    return MaterializerConfig(
        bootstrap=e.get("G8_MAT_BOOTSTRAP", e.get("KAFKA_BOOTSTRAP", "localhost:19092")),
        group_id=e.get("G8_MAT_GROUP", GROUP_DEFAULT),
        topic_dau=e.get("G8_MAT_TOPIC_DAU", TOPIC_DAU_DEFAULT),
        topic_pay=e.get("G8_MAT_TOPIC_PAY", TOPIC_PAY_DEFAULT),
        doris_host=e.get("G8_MAT_DORIS_HOST", "127.0.0.1"),
        doris_port=int(e.get("G8_MAT_DORIS_PORT", "9030")),
        doris_user=e.get("G8_MAT_DORIS_USER", "root"),
        doris_password=e.get("G8_MAT_DORIS_PASSWORD", ""),
        doris_db=e.get("G8_MAT_DORIS_DB", "ads"),
        poll_timeout_ms=int(e.get("G8_MAT_POLL_MS", "1000")),
        retry_base_sec=float(e.get("G8_MAT_RETRY_BASE", "1")),
        retry_max_sec=float(e.get("G8_MAT_RETRY_MAX", "30")),
        commit_every_n=int(e.get("G8_MAT_COMMIT_EVERY", "1")),
        auto_offset_reset=e.get("G8_MAT_OFFSET_RESET", "earliest"),
    )
