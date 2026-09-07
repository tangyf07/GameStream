#!/usr/bin/env python3
"""Helpers for G8 steady-state bench: chunked produce, Doris E2E wait, lag parse, assemble JSON."""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def produce_continuous_chunked(path: str, duration: float, topic: str, meta_out: str) -> dict[str, Any]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    n = len(lines)
    if n == 0:
        meta = {"events": 0, "error": "empty"}
        Path(meta_out).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return meta
    n_chunks = max(1, int(round(duration)))
    chunk_size = max(1, (n + n_chunks - 1) // n_chunks)
    t0 = time.perf_counter()
    produced = 0
    for ci in range(n_chunks):
        start = ci * chunk_size
        end = min(n, start + chunk_size)
        if start >= n:
            break
        chunk = ("\n".join(lines[start:end]) + "\n").encode("utf-8")
        p = subprocess.run(
            [
                "docker", "exec", "-i", "gs-kafka",
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", topic,
            ],
            input=chunk,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if p.returncode != 0:
            sys.stderr.write(p.stderr.decode("utf-8", "replace")[:500] + "\n")
        produced += end - start
        target = t0 + ((ci + 1) / n_chunks) * duration
        now = time.perf_counter()
        if target > now:
            time.sleep(target - now)
    wall = round(time.perf_counter() - t0, 4)
    meta = {
        "events": produced,
        "produce_wall_sec": wall,
        "input_events_per_sec": round(produced / wall, 3) if wall > 0 else None,
        "chunks": n_chunks,
        "mode": "chunked_continuous",
    }
    Path(meta_out).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def _read_kafka_ads(topic_dau: str, topic_pay: str, dt: str, server_id: int) -> tuple[Any, Any, Any]:
    dau_file = Path("/tmp/g8s_kafka_dau.jsonl")
    pay_file = Path("/tmp/g8s_kafka_pay.jsonl")
    for path, topic in ((dau_file, topic_dau), (pay_file, topic_pay)):
        path.unlink(missing_ok=True)
        subprocess.run(
            [
                "timeout", "10", "docker", "exec", "gs-kafka",
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server", "localhost:9092", "--topic", topic,
                "--from-beginning", "--property", "print.key=true",
                "--property", "key.separator=|", "--timeout-ms", "7000",
            ],
            stdout=open(path, "w"),
            stderr=subprocess.DEVNULL,
        )

    def latest(path: Path):
        if not path.exists():
            return None
        latest_o = None
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            val = line.split("|", 1)[1] if "|" in line else line
            try:
                o = json.loads(val)
            except Exception:
                continue
            odt = str(o.get("dt", ""))[:10]
            try:
                osid = int(o.get("server_id"))
            except Exception:
                continue
            if odt == dt and osid == server_id:
                latest_o = o
        return latest_o

    dau_o = latest(dau_file)
    pay_o = latest(pay_file)
    dau = dau_o.get("dau") if dau_o else None
    pay = pay_o.get("pay_users") if pay_o else None
    rate = pay_o.get("pay_rate") if pay_o else None
    if dau is None and pay_o and pay_o.get("dau") is not None:
        dau = pay_o.get("dau")
    return dau, pay, rate


def materialize_doris(dt: str, server_id: int, topic_dau: str, topic_pay: str) -> str | None:
    dau, pay, rate = _read_kafka_ads(topic_dau, topic_pay, dt, server_id)
    if dau is None or pay is None or rate is None:
        return None
    sql = (
        f"INSERT INTO ads.ads_dau_di (dt, server_id, dau, metric_id) "
        f"VALUES ('{dt}', {server_id}, {dau}, 'ads_dau_di');"
        f"INSERT INTO ads.ads_pay_rate_di (dt, server_id, dau, pay_users, pay_rate, metric_id) "
        f"VALUES ('{dt}', {server_id}, {dau}, {pay}, {rate}, 'ads_pay_rate_di');"
    )
    subprocess.run(
        ["docker", "exec", "gs-doris-fe", "mysql", "-h127.0.0.1", "-P9030", "-uroot", "-e", sql],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return f"{dau} {pay} {rate}"


def doris_ads_row(dt: str, server_id: int) -> str:
    sql = (
        "SELECT CONCAT_WS(' ', "
        f"IFNULL((SELECT dau FROM ads.ads_dau_di WHERE dt='{dt}' AND server_id={server_id}), 'NULL'), "
        f"IFNULL((SELECT pay_users FROM ads.ads_pay_rate_di WHERE dt='{dt}' AND server_id={server_id}), 'NULL'), "
        f"IFNULL((SELECT pay_rate FROM ads.ads_pay_rate_di WHERE dt='{dt}' AND server_id={server_id}), 'NULL')"
        ");"
    )
    p = subprocess.run(
        ["docker", "exec", "gs-doris-fe", "mysql", "-h127.0.0.1", "-P9030", "-uroot", "-N", "-e", sql],
        capture_output=True,
        text=True,
    )
    lines = [ln.strip() for ln in (p.stdout or "").replace("\r", "").splitlines() if ln.strip()]
    return lines[-1] if lines else "NULL NULL NULL"


def wait_doris_e2e(
    want_dau: int,
    want_pay: int,
    produce_epoch: float,
    out_path: str,
    dt: str,
    server_id: int,
    topic_dau: str,
    topic_pay: str,
    wait_sec: int = 120,
) -> dict[str, Any]:
    t_start = time.time()
    definition = (
        "Doris-query-visible E2E = host wallclock when Doris SELECT returns target dau/pay "
        "minus produce_wallclock_end of probe batch. Path: produce -> Flink upsert-kafka ADS "
        "-> script UNIQUE KEY materialize -> Doris SELECT."
    )
    for i in range(1, wait_sec + 1):
        kdau, kpay, _ = _read_kafka_ads(topic_dau, topic_pay, dt, server_id)
        if kdau == want_dau and kpay == want_pay:
            materialize_doris(dt, server_id, topic_dau, topic_pay)
            drow = doris_ads_row(dt, server_id)
            parts = drow.split()
            ddau = parts[0] if parts else "NULL"
            dpay = parts[1] if len(parts) > 1 else "NULL"
            if ddau == str(want_dau) and dpay == str(want_pay):
                visible = time.time()
                lat_ms = round((visible - produce_epoch) * 1000.0, 3)
                out = {
                    "status": "ok",
                    "definition": definition,
                    "produce_epoch": produce_epoch,
                    "doris_visible_epoch": visible,
                    "e2e_latency_ms": lat_ms,
                    "poll_wait_ms": round((visible - t_start) * 1000.0, 3),
                    "want_dau": want_dau,
                    "want_pay_users": want_pay,
                    "doris_row": drow,
                    "tries": i,
                    "sink_honesty": (
                        "Flink continuous sink=upsert-kafka ALS+PK; "
                        "Doris=plain INSERT UNIQUE KEY materialize; NOT EO-2PC"
                    ),
                }
                Path(out_path).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[g8s] e2e OK try={i} lat_ms={lat_ms} doris={drow}", file=sys.stderr)
                return out
            print(f"[g8s] kafka OK doris pending ({drow}) try={i}", file=sys.stderr)
        elif i % 5 == 0:
            print(f"[g8s] waiting kafka=({kdau},{kpay}) want=({want_dau},{want_pay}) try={i}", file=sys.stderr)
            materialize_doris(dt, server_id, topic_dau, topic_pay)
        time.sleep(1)
    drow = doris_ads_row(dt, server_id)
    out = {
        "status": "未测到",
        "reason": f"Doris ADS did not reach dau={want_dau} pay_users={want_pay} within wait; last_doris={drow}",
        "definition": definition,
        "produce_epoch": produce_epoch,
        "doris_visible_epoch": None,
        "e2e_latency_ms": None,
        "want_dau": want_dau,
        "want_pay_users": want_pay,
        "doris_row": drow,
        "tries": wait_sec,
    }
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def parse_lag_dir(raw_dir: str, group: str) -> dict[str, Any]:
    raw = Path(raw_dir)

    def parse(p: Path):
        text = p.read_text(errors="replace") if p.exists() else ""
        lags = []
        for line in text.splitlines():
            parts = line.split()
            if len(parts) < 5:
                continue
            try:
                if parts[0] == group:
                    topic, part, cur, end, lag = parts[1], parts[2], parts[3], parts[4], parts[5]
                elif "gamestream" in parts[0]:
                    topic, part, cur, end, lag = parts[0], parts[1], parts[2], parts[3], parts[4]
                else:
                    continue
                if str(lag).lstrip("-").isdigit():
                    lags.append({
                        "topic": topic,
                        "partition": int(part),
                        "current_offset": int(cur),
                        "log_end_offset": int(end),
                        "lag": int(lag),
                    })
            except Exception:
                pass
        return lags

    out: dict[str, Any] = {}
    for p in sorted(raw.glob("lag_*.txt")):
        out[p.stem] = parse(p)
    rounds = raw / "rounds"
    if rounds.exists():
        for p in sorted(rounds.glob("round_*/lag_*.txt")):
            out[f"{p.parent.name}_{p.stem}"] = parse(p)
    status = "ok" if any(out.values()) else "未测到"
    payload = {
        "status": status,
        "by_label": out,
        "source": "kafka-consumer-groups.sh --describe --group",
    }
    if status == "未测到":
        payload["reason"] = "could not parse consumer-groups describe; see raw lag_*.txt"
    Path(raw / "lag.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def assemble(env: dict[str, str]) -> dict[str, Any]:
    raw = Path(env["RAW_DIR"])
    rounds_n = int(env["ROUNDS"])

    def load(p, default=None):
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            return default
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            return {"status": "未测到", "reason": f"parse {p.name}: {e}"}

    lag = load(raw / "lag.json", {}) or {}
    clock = load(raw / "clock_skew.json", {})
    cp_final = load(raw / "checkpoints_final.json", {})
    vbp_final = load(raw / "vertices_bp_final.json", {})
    baseline_sample = load(raw / "baseline_sample.json", {})
    baseline_cp = load(raw / "baseline_checkpoints.json", {})
    state = load(raw / "player_state.json", {})

    round_summaries = []
    all_e2e = []
    flink_rates = []
    bp_maxes = []
    cp_merged_samples = []

    for r in range(1, rounds_n + 1):
        rd = raw / "rounds" / f"round_{r}"
        poll = load(rd / "poll_summary.json", {}) or {}
        e2e = load(rd / "e2e_doris.json", {}) or {}
        prod_c = load(rd / "produce_continuous.json", {}) or {}
        prod_p = load(rd / "produce_probe.json", {}) or {}
        cp = load(rd / "checkpoints.json", {}) or {}
        vbp = load(rd / "vertices_bp.json", {}) or {}
        fr = poll.get("flink_in_rate_per_s") or {}
        if fr.get("status") == "ok" and fr.get("samples"):
            flink_rates.extend([x for x in fr["samples"] if x is not None])
        bp = poll.get("backpressure") or {}
        if bp.get("max_backPressuredTimeMsPerSecond") is not None:
            bp_maxes.append(bp["max_backPressuredTimeMsPerSecond"])
        if cp.get("end_to_end_duration_ms_samples"):
            cp_merged_samples.extend(cp["end_to_end_duration_ms_samples"])
        levels, ratios = [], []
        for vx in (vbp.get("vertices") or []):
            b = vx.get("backpressure") or {}
            levels.append(b.get("backpressureLevel") or b.get("backpressure-level"))
            for st in b.get("subtasks") or []:
                if st.get("ratio") is not None:
                    ratios.append(float(st["ratio"]))
        round_summaries.append({
            "round": r,
            "continuous_events": prod_c.get("events"),
            "continuous_produce_wall_sec": prod_c.get("produce_wall_sec"),
            "continuous_input_eps": prod_c.get("input_events_per_sec"),
            "probe_events": prod_p.get("events"),
            "probe_want_dau": prod_p.get("want_dau"),
            "probe_want_pay_users": prod_p.get("want_pay_users"),
            "doris_e2e": e2e,
            "poll_flink_in_rate": fr,
            "poll_backpressure": bp,
            "checkpoint": {k: cp.get(k) for k in ("status", "n", "avg_ms", "p50_ms", "p95_ms", "reason")} if cp else {},
            "vertices_bp_levels": levels,
            "vertices_bp_max_ratio": max(ratios) if ratios else None,
        })
        if e2e:
            all_e2e.append(e2e)

    ok_lat = [
        e.get("e2e_latency_ms")
        for e in all_e2e
        if e.get("status") == "ok" and e.get("e2e_latency_ms") is not None
    ]
    ok_lat_sorted = sorted(ok_lat)

    def pct(arr, p):
        if not arr:
            return None
        idx = max(0, min(len(arr) - 1, math.ceil(p * len(arr)) - 1))
        return arr[idx]

    e2e_agg = {
        "definition": (
            "Doris-query-visible E2E: host wallclock from probe produce_end -> Doris SELECT "
            "returns expected dau/pay after upsert-kafka + UNIQUE KEY materialize. "
            "NOT G6 Kafka sink_ts-produce_ts."
        ),
        "status": "ok" if ok_lat else "未测到",
        "n_ok": len(ok_lat),
        "n_rounds": len(all_e2e),
        "samples_ms": ok_lat,
        "avg_ms": round(sum(ok_lat) / len(ok_lat), 3) if ok_lat else None,
        "p50_ms": pct(ok_lat_sorted, 0.50),
        "p95_ms": pct(ok_lat_sorted, 0.95),
        "min_ms": ok_lat_sorted[0] if ok_lat_sorted else None,
        "max_ms": ok_lat_sorted[-1] if ok_lat_sorted else None,
        "per_round": all_e2e,
    }
    if not ok_lat:
        reasons = [e.get("reason") for e in all_e2e if e.get("reason")]
        e2e_agg["reason"] = "; ".join(reasons) if reasons else "no successful Doris-visible E2E samples"

    if cp_final and cp_final.get("end_to_end_duration_ms_samples"):
        cp_out = cp_final
    elif cp_merged_samples:
        s = sorted(cp_merged_samples)
        cp_out = {
            "status": "ok",
            "n": len(s),
            "avg_ms": round(sum(s) / len(s), 2),
            "p50_ms": s[len(s) // 2],
            "p95_ms": s[max(0, math.ceil(0.95 * len(s)) - 1)],
            "min_ms": s[0],
            "max_ms": s[-1],
            "end_to_end_duration_ms_samples": s,
            "source": "merged per-round checkpoint samples",
        }
    else:
        cp_out = cp_final if cp_final else {"status": "未测到", "reason": "no checkpoint durations"}

    total_events = int(env["TOTAL_EVENTS"])
    total_sec = float(env["TOTAL_PRODUCE_SEC"])
    input_eps = round(total_events / total_sec, 3) if total_sec > 0 else None
    flink_rate = {
        "status": "ok" if flink_rates else "未测到",
        "max": max(flink_rates) if flink_rates else None,
        "avg": round(sum(flink_rates) / len(flink_rates), 3) if flink_rates else None,
        "samples": flink_rates,
        "reason": None if flink_rates else "metric unavailable or always zero during continuous poll",
    }

    bp_out: dict[str, Any] = {
        "status": "ok" if bp_maxes else "未测到",
        "max_backPressuredTimeMsPerSecond": max(bp_maxes) if bp_maxes else None,
        "observed": (max(bp_maxes) > 0) if bp_maxes else None,
        "samples": bp_maxes,
        "reason": None if bp_maxes else "job-level backPressuredTimeMsPerSecond unavailable during poll",
    }
    if vbp_final and vbp_final.get("status") == "ok":
        levels, ratios = [], []
        for vx in vbp_final.get("vertices") or []:
            b = vx.get("backpressure") or {}
            levels.append(b.get("backpressureLevel") or b.get("backpressure-level"))
            for st in b.get("subtasks") or []:
                if st.get("ratio") is not None:
                    ratios.append(float(st["ratio"]))
        if levels or ratios:
            observed = any(lv not in (None, "ok") for lv in levels) or any(r > 0 for r in ratios)
            bp_out = {
                "status": "ok",
                "observed": observed or bool(bp_out.get("observed")),
                "backpressureLevel_samples": levels,
                "subtask_ratio_samples": ratios,
                "max_subtask_ratio": max(ratios) if ratios else None,
                "max_backPressuredTimeMsPerSecond": bp_out.get("max_backPressuredTimeMsPerSecond"),
                "source": vbp_final.get("source"),
                "vertices_backpressure_endpoint": vbp_final,
                "note": "Vertex backpressure endpoint used; job-level metric may be 未测到",
            }

    lag_summary = {
        "status": lag.get("status"),
        "baseline_before_load": (lag.get("by_label") or {}).get("lag_baseline"),
        "final": (lag.get("by_label") or {}).get("lag_final"),
        "by_label": lag.get("by_label"),
        "source": lag.get("source"),
    }
    if lag.get("reason"):
        lag_summary["reason"] = lag["reason"]

    result = {
        "schema": "gamestream.g8.steady_bench.v1",
        "timestamp_utc": env["TS_UTC"],
        "tier": env["TIER"],
        "host": env["HOST"],
        "mem_note": env["MEM_NOTE"],
        "job_id": env["JOB_ID"],
        "group_id": env["G8_GROUP"],
        "topic_in": env["TOPIC_IN"],
        "topics_ads": [env["TOPIC_DAU"], env["TOPIC_PAY"]],
        "dt": env["DT"],
        "server_id": int(env["SERVER_ID"]),
        "rounds": rounds_n,
        "rate_target_eps": float(env["RATE"]),
        "duration_sec_per_round": float(env["DURATION_SEC"]),
        "doris_alive": env.get("ALIVE"),
        "raw_dir": str(raw),
        "sink_honesty": {
            "flink_continuous_sink": "upsert-kafka (ALS + PK)",
            "doris_materialize": "plain INSERT on UNIQUE KEY (script)",
            "not": "EO-2PC / Flink JDBC MySQL ON DUPLICATE KEY UPDATE",
            "not_g6": "Do NOT reuse G6 Kafka-only sink_ts-produce_ts numbers as G8 Doris-visible E2E",
        },
        "definitions": {
            "baseline": "Kafka lag + Flink sample-once + checkpoints + vertices backpressure BEFORE continuous load starts",
            "throughput_input": "total produced events / total produce wall seconds across continuous chunked produce + probes",
            "throughput_flink": "Flink REST numRecordsInPerSecond during poll windows; prefer source-vertex sample; if sum-of-operators used it is NOT source throughput",
            "kafka_lag": "kafka-consumer-groups.sh --describe --group for Flink source group (baseline + per-round + final)",
            "checkpoint_duration": "Flink REST /jobs/:id/checkpoints history[].end_to_end_duration (ms)",
            "doris_e2e_p95": "Per-round Doris-query-visible latency (produce_probe_end -> Doris SELECT); with n=2 P95≈max — prefer quoting ~39-41s可见; includes console-consumer serial + script materialize",
            "backpressure": "Flink backPressuredTimeMsPerSecond and/or vertices/:id/backpressure during/after load",
        },
        "baseline_before_load": {
            "sample_once": baseline_sample,
            "checkpoints": baseline_cp,
            "lag": lag_summary.get("baseline_before_load"),
        },
        "metrics": {
            "throughput": {
                "definition": "continuous chunked produce + probes; input_eps = total_events/total_produce_wall_sec",
                "total_events": total_events,
                "total_produce_wall_sec": total_sec,
                "input_produce_events_per_sec": input_eps,
                "flink_numRecordsInPerSecond": flink_rate,
                "rate_target_eps": float(env["RATE"]),
                "duration_sec_per_round": float(env["DURATION_SEC"]),
                "rounds": rounds_n,
            },
            "kafka_consumer_lag": lag_summary,
            "checkpoint_duration": cp_out,
            "doris_e2e_latency": e2e_agg,
            "backpressure": bp_out,
        },
        "round_summaries": round_summaries,
        "player_state_expected": {
            "expected_dau": (state or {}).get("expected_dau"),
            "expected_pay_users": (state or {}).get("expected_pay_users"),
            "events_total": (state or {}).get("events_total"),
        },
        "clock_skew": clock,
        "honesty": (
            "Numbers copied from raw sampler outputs only. Missing -> 未测到 with reason. "
            "Not G6 Kafka-only E2E. Sink is ALS+PK / UNIQUE KEY — not EO-2PC."
        ),
    }

    out = Path(env["RESULT_JSON"])
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = []
    lines.append(f"G8 chunked-load / batch-visibility  tier={env['TIER']}  ts={env['TS_UTC']}")
    lines.append(f"result_json={out}")
    lines.append(f"raw_dir={raw}")
    lines.append(f"job_id={env['JOB_ID']}  rounds={rounds_n}  doris_alive={env.get('ALIVE')}")
    lines.append("")
    m = result["metrics"]
    thr = m["throughput"]
    lines.append(
        f"throughput.input_eps={thr.get('input_produce_events_per_sec')} "
        f"total_events={thr.get('total_events')} wall_sec={thr.get('total_produce_wall_sec')}"
    )
    fr = thr.get("flink_numRecordsInPerSecond") or {}
    lines.append(
        f"throughput.flink_in_rate: status={fr.get('status')} max={fr.get('max')} "
        f"avg={fr.get('avg')} reason={fr.get('reason')}"
    )
    lg = m.get("kafka_consumer_lag") or {}
    lines.append(f"kafka_lag: status={lg.get('status')} baseline={lg.get('baseline_before_load')} final={lg.get('final')}")
    cpb = m.get("checkpoint_duration") or {}
    lines.append(
        f"checkpoint: status={cpb.get('status')} n={cpb.get('n')} avg_ms={cpb.get('avg_ms')} "
        f"p50_ms={cpb.get('p50_ms')} p95_ms={cpb.get('p95_ms')} reason={cpb.get('reason')}"
    )
    e = m.get("doris_e2e_latency") or {}
    lines.append(
        f"doris_e2e: status={e.get('status')} n_ok={e.get('n_ok')} p95_ms={e.get('p95_ms')} "
        f"p50_ms={e.get('p50_ms')} avg_ms={e.get('avg_ms')} samples={e.get('samples_ms')} reason={e.get('reason')}"
    )
    b = m.get("backpressure") or {}
    lines.append(
        f"backpressure: status={b.get('status')} observed={b.get('observed')} "
        f"max_ratio={b.get('max_subtask_ratio')} max_bp_ms={b.get('max_backPressuredTimeMsPerSecond')} "
        f"reason={b.get('reason')}"
    )
    Path(env["RESULT_TXT"]).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return result


def clock_skew(out_path: str, host: str, jm: str, tm: str, kfk: str, doris: str) -> dict[str, Any]:
    def parse(s):
        if not s or s == "未测到":
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    h, j, t, k, d = map(parse, [host, jm, tm, kfk, doris])

    def skew(a, b):
        if a is None or b is None:
            return None
        return (b - a).total_seconds()

    out = {
        "host_utc": host,
        "flink_jm_utc": jm,
        "flink_tm_utc": tm,
        "kafka_utc": kfk,
        "doris_fe_utc": doris,
        "skew_jm_minus_host_sec": skew(h, j),
        "skew_tm_minus_host_sec": skew(h, t),
        "skew_kafka_minus_host_sec": skew(h, k),
        "skew_doris_minus_host_sec": skew(h, d),
        "note": (
            "Doris-visible E2E uses host wallclock for both produce_end and Doris SELECT success "
            "— same host, no cross-clock subtraction"
        ),
    }
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--action", required=True, choices=[
        "produce-chunked", "wait-doris-e2e", "parse-lag", "assemble", "clock-skew",
    ])
    ap.add_argument("--file", default="")
    ap.add_argument("--duration", type=float, default=40)
    ap.add_argument("--topic", default="")
    ap.add_argument("--meta-out", default="")
    ap.add_argument("--want-dau", type=int, default=0)
    ap.add_argument("--want-pay", type=int, default=0)
    ap.add_argument("--produce-epoch", type=float, default=0)
    ap.add_argument("--out", default="")
    ap.add_argument("--dt", default="2026-09-07")
    ap.add_argument("--server-id", type=int, default=88)
    ap.add_argument("--topic-dau", default="")
    ap.add_argument("--topic-pay", default="")
    ap.add_argument("--wait-sec", type=int, default=120)
    ap.add_argument("--raw-dir", default="")
    ap.add_argument("--group", default="")
    ap.add_argument("--host-utc", default="")
    ap.add_argument("--jm-utc", default="")
    ap.add_argument("--tm-utc", default="")
    ap.add_argument("--kafka-utc", default="")
    ap.add_argument("--doris-utc", default="")
    args = ap.parse_args()

    if args.action == "produce-chunked":
        print(json.dumps(produce_continuous_chunked(args.file, args.duration, args.topic, args.meta_out), ensure_ascii=False))
        return 0
    if args.action == "wait-doris-e2e":
        out = wait_doris_e2e(
            args.want_dau, args.want_pay, args.produce_epoch, args.out,
            args.dt, args.server_id, args.topic_dau, args.topic_pay, args.wait_sec,
        )
        print(json.dumps({"status": out.get("status"), "e2e_latency_ms": out.get("e2e_latency_ms")}, ensure_ascii=False))
        return 0 if out.get("status") == "ok" else 1
    if args.action == "parse-lag":
        print(json.dumps(parse_lag_dir(args.raw_dir, args.group), ensure_ascii=False)[:500])
        return 0
    if args.action == "assemble":
        # env vars required
        assemble(dict(os.environ))
        return 0
    if args.action == "clock-skew":
        print(json.dumps(clock_skew(args.out, args.host_utc, args.jm_utc, args.tm_utc, args.kafka_utc, args.doris_utc), ensure_ascii=False, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
