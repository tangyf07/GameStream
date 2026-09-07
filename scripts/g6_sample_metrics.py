#!/usr/bin/env python3
"""G6 Flink/Kafka metric sampler — only records what REST/CLI returns; never invents."""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


FLINK = "http://127.0.0.1:8081"


def _get_json(url: str, timeout: float = 8.0) -> Any | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_error": str(e), "_url": url}


def _get_text(url: str, timeout: float = 8.0) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode("utf-8")
    except Exception:
        return None


def find_job(name_substr: str = "g6-bench") -> dict[str, Any]:
    data = _get_json(f"{FLINK}/jobs/overview") or {}
    jobs = data.get("jobs") or []
    for j in jobs:
        if name_substr in (j.get("name") or "") and j.get("state") == "RUNNING":
            return {"jid": j["jid"], "name": j.get("name"), "state": j.get("state")}
    running = [j for j in jobs if j.get("state") == "RUNNING"]
    if running:
        j = running[0]
        return {"jid": j["jid"], "name": j.get("name"), "state": j.get("state"), "note": "first RUNNING"}
    return {"jid": None, "jobs_overview": jobs, "error": "no RUNNING g6 job"}


def checkpoint_stats(jid: str) -> dict[str, Any]:
    d = _get_json(f"{FLINK}/jobs/{jid}/checkpoints")
    if not d or d.get("_error"):
        return {"status": "未测到", "reason": f"REST /checkpoints failed: {d}", "samples": []}
    hist = d.get("history") or []
    completed = [h for h in hist if (h.get("status") == "COMPLETED")]
    durs = []
    for h in completed:
        # end_to_end_duration is ms
        v = h.get("end_to_end_duration")
        if v is None:
            v = h.get("duration")
        if v is not None:
            try:
                durs.append(int(v))
            except Exception:
                pass
    counts = d.get("counts") or {}
    out: dict[str, Any] = {
        "status": "ok" if durs else "未测到",
        "counts": counts,
        "completed_in_history": len(completed),
        "end_to_end_duration_ms_samples": durs,
        "source": f"GET {FLINK}/jobs/{jid}/checkpoints → history[].end_to_end_duration",
    }
    if not durs:
        out["reason"] = "no COMPLETED checkpoints with duration in history yet"
        latest = (d.get("latest") or {})
        out["latest_keys"] = list(latest.keys()) if isinstance(latest, dict) else None
        return out
    durs_sorted = sorted(durs)
    out["avg_ms"] = round(sum(durs) / len(durs), 2)
    out["p50_ms"] = durs_sorted[len(durs_sorted) // 2]
    out["p95_ms"] = durs_sorted[max(0, math.ceil(0.95 * len(durs_sorted)) - 1)]
    out["min_ms"] = durs_sorted[0]
    out["max_ms"] = durs_sorted[-1]
    out["n"] = len(durs)
    return out


def _metric_values(jid: str, metric_ids: list[str]) -> dict[str, Any]:
    if not metric_ids:
        return {}
    q = urllib.parse.urlencode({"get": ",".join(metric_ids)})
    d = _get_json(f"{FLINK}/jobs/{jid}/metrics?{q}")
    if not isinstance(d, list):
        return {"_raw": d}
    return {item.get("id"): item.get("value") for item in d if isinstance(item, dict)}


def list_metric_ids(jid: str, substrings: list[str]) -> list[str]:
    d = _get_json(f"{FLINK}/jobs/{jid}/metrics")
    if not isinstance(d, list):
        return []
    ids = [x.get("id") for x in d if isinstance(x, dict) and x.get("id")]
    out = []
    for mid in ids:
        low = mid.lower()
        if any(s.lower() in low for s in substrings):
            out.append(mid)
    return out


def sample_throughput_and_bp(jid: str) -> dict[str, Any]:
    want = [
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "numRecordsIn",
        "numRecordsOut",
        "busyTimeMsPerSecond",
        "backPressuredTimeMsPerSecond",
        "idleTimeMsPerSecond",
    ]
    available = list_metric_ids(jid, want)
    # Prefer aggregated / job-level ids if present; else take a few vertex-scoped
    picked: list[str] = []
    for w in want:
        exact = [a for a in available if a == w or a.endswith("." + w) or a.endswith(w)]
        picked.extend(exact[:6])
    picked = list(dict.fromkeys(picked))[:40]
    values = _metric_values(jid, picked) if picked else {}
    bp_ids = [i for i in picked if "backPressuredTimeMsPerSecond" in i]
    bp_vals = []
    for i in bp_ids:
        try:
            bp_vals.append(float(values.get(i) or 0))
        except Exception:
            pass
    in_ids = [i for i in picked if i.endswith("numRecordsInPerSecond") or i == "numRecordsInPerSecond"]
    in_vals = []
    for i in in_ids:
        try:
            in_vals.append(float(values.get(i) or 0))
        except Exception:
            pass
    out = {
        "metric_ids_sampled": picked,
        "values": values,
        "numRecordsInPerSecond_samples": in_vals,
        "numRecordsInPerSecond_max": max(in_vals) if in_vals else None,
        "numRecordsInPerSecond_sum": round(sum(in_vals), 3) if in_vals else None,
        "backPressuredTimeMsPerSecond_samples": bp_vals,
        "backPressuredTimeMsPerSecond_max": max(bp_vals) if bp_vals else None,
        "backpressure_observed": (max(bp_vals) > 0) if bp_vals else None,
        "status": "ok" if picked else "未测到",
        "reason": None if picked else "no matching metric ids on /jobs/:id/metrics",
        "source": f"GET {FLINK}/jobs/{jid}/metrics",
    }
    if not picked or not in_vals:
        vert = sample_vertex_metrics(jid)
        out["vertex_fallback"] = {k: vert.get(k) for k in (
            "status","reason","source","numRecordsInPerSecond_max","numRecordsInPerSecond_sum",
            "numRecordsInPerSecond_samples","backPressuredTimeMsPerSecond_max","backpressure_observed",
            "backPressuredTimeMsPerSecond_samples")}
        if vert.get("numRecordsInPerSecond_samples"):
            out["numRecordsInPerSecond_samples"] = vert["numRecordsInPerSecond_samples"]
            out["numRecordsInPerSecond_max"] = vert.get("numRecordsInPerSecond_max")
            out["numRecordsInPerSecond_sum"] = vert.get("numRecordsInPerSecond_sum")
            out["numRecordsInPerSecond_sum_across_operators"] = vert.get(
                "numRecordsInPerSecond_sum_across_operators", vert.get("numRecordsInPerSecond_sum")
            )
            out["numRecordsInPerSecond_source_samples"] = vert.get("numRecordsInPerSecond_source_samples")
            out["numRecordsInPerSecond_source_max"] = vert.get("numRecordsInPerSecond_source_max")
            out["sampling_scope"] = vert.get("sampling_scope")
            out["source_vertex_names"] = vert.get("source_vertex_names")
            out["note"] = vert.get("note")
            out["status"] = "ok"
            out["reason"] = None
            out["source"] = vert.get("source")
        if vert.get("backPressuredTimeMsPerSecond_samples") is not None and vert.get("backPressuredTimeMsPerSecond_max") is not None:
            out["backPressuredTimeMsPerSecond_samples"] = vert["backPressuredTimeMsPerSecond_samples"]
            out["backPressuredTimeMsPerSecond_max"] = vert.get("backPressuredTimeMsPerSecond_max")
            out["backpressure_observed"] = vert.get("backpressure_observed")
            if out["status"] != "ok":
                out["status"] = "ok"
                out["reason"] = None
    return out



def _is_source_vertex(name: str | None) -> bool:
    """Heuristic: Flink SQL/DataStream source operators."""
    n = (name or "").lower()
    return (
        "source:" in n
        or n.startswith("source")
        or "kafka" in n and "source" in n
        or "tableSource" in (name or "")
        or "streamscansource" in n
        or "sourcesoperator" in n.replace(" ", "")
    )


def sample_vertex_metrics(jid: str) -> dict[str, Any]:
    """Sample vertex metrics. Prefer Source vertex numRecordsInPerSecond as throughput;
    also record sum-across-operators (NOT source throughput — do not misuse)."""
    job = _get_json(f"{FLINK}/jobs/{jid}")
    if not job or job.get("_error"):
        return {"status": "未测到", "reason": f"job detail failed: {job}"}
    want = [
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "numRecordsIn",
        "numRecordsOut",
        "busyTimeMsPerSecond",
        "backPressuredTimeMsPerSecond",
        "idleTimeMsPerSecond",
    ]
    all_values = {}
    in_rates_all: list[float] = []
    in_rates_source: list[float] = []
    source_names: list[str] = []
    bp_vals = []
    for v in job.get("vertices") or []:
        vid = v.get("id")
        vname = v.get("name") or ""
        if not vid:
            continue
        is_src = _is_source_vertex(vname)
        listed = _get_json(f"{FLINK}/jobs/{jid}/vertices/{vid}/metrics")
        ids = []
        if isinstance(listed, list):
            for item in listed:
                mid = item.get("id") if isinstance(item, dict) else None
                if mid and any(w in mid for w in want):
                    ids.append(mid)
        ids = list(dict.fromkeys(ids))[:30]
        if not ids:
            continue
        q = urllib.parse.urlencode({"get": ",".join(ids)})
        vals = _get_json(f"{FLINK}/jobs/{jid}/vertices/{vid}/metrics?{q}")
        if isinstance(vals, list):
            for item in vals:
                if not isinstance(item, dict):
                    continue
                mid = item.get("id")
                val = item.get("value")
                all_values[f"{vid}:{mid}"] = val
                try:
                    fv = float(val)
                except Exception:
                    continue
                if mid and (mid.endswith("numRecordsInPerSecond") or mid == "numRecordsInPerSecond"):
                    in_rates_all.append(fv)
                    if is_src:
                        in_rates_source.append(fv)
                        if vname not in source_names:
                            source_names.append(vname)
                if mid and "backPressuredTimeMsPerSecond" in str(mid):
                    bp_vals.append(fv)
    # Prefer source-vertex samples for the primary "samples/max" used by poll aggregates.
    prefer_source = bool(in_rates_source)
    primary = in_rates_source if prefer_source else in_rates_all
    return {
        "status": "ok" if all_values else "未测到",
        "values": all_values,
        "numRecordsInPerSecond_samples": primary,
        "numRecordsInPerSecond_max": max(primary) if primary else None,
        "numRecordsInPerSecond_sum": round(sum(in_rates_all), 3) if in_rates_all else None,
        "numRecordsInPerSecond_sum_across_operators": round(sum(in_rates_all), 3) if in_rates_all else None,
        "numRecordsInPerSecond_source_samples": in_rates_source,
        "numRecordsInPerSecond_source_max": max(in_rates_source) if in_rates_source else None,
        "sampling_scope": "source_vertex" if prefer_source else "all_operators_fallback",
        "source_vertex_names": source_names,
        "note": (
            "Primary samples prefer Source vertex. "
            "numRecordsInPerSecond_sum(_across_operators) is SUM across operators — NOT source throughput."
        ),
        "backPressuredTimeMsPerSecond_samples": bp_vals,
        "backPressuredTimeMsPerSecond_max": max(bp_vals) if bp_vals else None,
        "backpressure_observed": (max(bp_vals) > 0) if bp_vals else None,
        "reason": None if all_values else "no vertex metric ids matched",
        "source": f"GET {FLINK}/jobs/{jid}/vertices/:vid/metrics",
    }


def vertices_bp(jid: str) -> dict[str, Any]:
    """Fallback: job vertex backpressure endpoint if available (Flink 1.13+)."""
    job = _get_json(f"{FLINK}/jobs/{jid}")
    if not job or job.get("_error"):
        return {"status": "未测到", "reason": f"job detail failed: {job}"}
    verts = job.get("vertices") or []
    samples = []
    for v in verts:
        vid = v.get("id")
        if not vid:
            continue
        bp = _get_json(f"{FLINK}/jobs/{jid}/vertices/{vid}/backpressure")
        samples.append({"vertex_id": vid, "name": v.get("name"), "backpressure": bp})
    return {
        "status": "ok" if samples else "未测到",
        "vertices": samples,
        "source": "GET /jobs/:id/vertices/:vid/backpressure",
    }


def e2e_from_sink_jsonl(path: Path) -> dict[str, Any]:
    """
    E2E definition (document in docs/g6-bench.md):
      latency_ms = sink_ts - produce_ts
      produce_ts = wallclock embedded at Kafka produce time (host clock)
      sink_ts    = Flink CURRENT_TIMESTAMP at sink SELECT (JM/TM processing clock)
    Clocks are not NTP-synced across containers; treat as approximate same-host Docker.
    """
    if not path.exists():
        return {"status": "未测到", "reason": f"missing sink file {path}", "n": 0}
    latencies: list[float] = []
    parse_errors = 0
    rows = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        # console-consumer may print key|value
        if "|" in line and line.split("|", 1)[0].startswith("{"):
            pass
        if "|" in line:
            # value after first | if key present; else whole line
            parts = line.split("|", 1)
            if len(parts) == 2 and parts[1].lstrip().startswith("{"):
                line = parts[1]
        try:
            o = json.loads(line)
        except Exception:
            parse_errors += 1
            continue
        rows += 1
        pt = o.get("produce_ts")
        st = o.get("sink_ts")
        if not pt or not st:
            continue
        try:
            # SQL format: 'YYYY-MM-DD HH:MM:SS[.fff]'
            def parse_ts(s: str) -> datetime:
                s = str(s).replace("T", " ").replace("Z", "")
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return datetime.strptime(s, fmt)
                    except ValueError:
                        continue
                raise ValueError(s)

            a = parse_ts(pt)
            b = parse_ts(st)
            latencies.append((b - a).total_seconds() * 1000.0)
        except Exception:
            parse_errors += 1
    if not latencies:
        return {
            "status": "未测到",
            "reason": "no parseable produce_ts/sink_ts pairs",
            "rows_seen": rows,
            "parse_errors": parse_errors,
            "definition": "sink_ts - produce_ts (ms)",
        }
    latencies.sort()
    n = len(latencies)

    def pct(p: float) -> float:
        idx = max(0, min(n - 1, math.ceil(p * n) - 1))
        return round(latencies[idx], 3)

    return {
        "status": "ok",
        "definition": "E2E_ms = sink_ts - produce_ts; produce_ts=host publish wallclock; sink_ts=Flink CURRENT_TIMESTAMP at sink projection",
        "n": n,
        "rows_seen": rows,
        "parse_errors": parse_errors,
        "avg_ms": round(sum(latencies) / n, 3),
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "min_ms": round(latencies[0], 3),
        "max_ms": round(latencies[-1], 3),
        "negative_count": sum(1 for x in latencies if x < 0),
        "note": "negative latency possible if container clocks skew vs host produce_ts",
    }


def poll_loop(jid: str, seconds: int, interval: float, out_path: Path) -> dict[str, Any]:
    samples = []
    t_end = time.time() + seconds
    while time.time() < t_end:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        thr = sample_throughput_and_bp(jid)
        cp = checkpoint_stats(jid)
        samples.append({"ts_utc": ts, "throughput_bp": thr, "checkpoints": cp})
        time.sleep(interval)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "jid": jid,
        "poll_seconds": seconds,
        "interval_sec": interval,
        "samples": samples,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    # aggregate — prefer source-vertex rate; sum-across-operators is NOT source throughput
    in_rates = []
    sum_rates = []
    bp_maxes = []
    scopes = []
    for s in samples:
        thr = s.get("throughput_bp") or {}
        vf = thr.get("vertex_fallback") or {}
        scope = thr.get("sampling_scope") or vf.get("sampling_scope")
        if scope:
            scopes.append(scope)
        # Prefer explicit source samples / max
        src_max = thr.get("numRecordsInPerSecond_source_max")
        if src_max is None:
            src_max = vf.get("numRecordsInPerSecond_source_max")
        if src_max is not None:
            in_rates.append(src_max)
        elif thr.get("sampling_scope") == "source_vertex" and thr.get("numRecordsInPerSecond_max") is not None:
            in_rates.append(thr["numRecordsInPerSecond_max"])
        elif thr.get("numRecordsInPerSecond_max") is not None:
            in_rates.append(thr["numRecordsInPerSecond_max"])
        ssum = thr.get("numRecordsInPerSecond_sum_across_operators")
        if ssum is None:
            ssum = thr.get("numRecordsInPerSecond_sum")
        if ssum is None:
            ssum = vf.get("numRecordsInPerSecond_sum_across_operators") or vf.get("numRecordsInPerSecond_sum")
        if ssum is not None:
            sum_rates.append(ssum)
        if thr.get("backPressuredTimeMsPerSecond_max") is not None:
            bp_maxes.append(thr["backPressuredTimeMsPerSecond_max"])
    last_cp = samples[-1]["checkpoints"] if samples else {}
    return {
        "poll_file": str(out_path),
        "n_samples": len(samples),
        "flink_in_rate_per_s": {
            "status": "ok" if in_rates else "未测到",
            "max": max(in_rates) if in_rates else None,
            "avg": round(sum(in_rates) / len(in_rates), 3) if in_rates else None,
            "samples": in_rates,
            "sampling_scope": (
                "source_vertex" if scopes and all(s == "source_vertex" for s in scopes)
                else ("mixed" if scopes else "legacy_or_unknown")
            ),
            "sum_across_operators_samples": sum_rates,
            "sum_across_operators_max": max(sum_rates) if sum_rates else None,
            "definition": (
                "Primary = prefer Source vertex numRecordsInPerSecond. "
                "sum_across_operators = SUM across operators — NOT source throughput; do not misuse."
            ),
            "reason": None if in_rates else "metric unavailable or always zero",
        },
        "backpressure": {
            "status": "ok" if bp_maxes else "未测到",
            "max_backPressuredTimeMsPerSecond": max(bp_maxes) if bp_maxes else None,
            "observed": (max(bp_maxes) > 0) if bp_maxes else None,
            "samples": bp_maxes,
            "reason": None if bp_maxes else "metric unavailable",
        },
        "checkpoints_final": last_cp,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--action", required=True, choices=[
        "find-job", "checkpoints", "sample-once", "poll", "e2e", "vertices-bp"
    ])
    ap.add_argument("--jid", default="")
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--out", default="")
    ap.add_argument("--sink-jsonl", default="")
    args = ap.parse_args()

    if args.action == "find-job":
        print(json.dumps(find_job(), ensure_ascii=False))
        return 0

    jid = args.jid
    if not jid and args.action != "e2e":
        jid = find_job().get("jid") or ""
        if not jid:
            print(json.dumps({"status": "未测到", "reason": "no jid"}, ensure_ascii=False))
            return 2

    if args.action == "checkpoints":
        print(json.dumps(checkpoint_stats(jid), indent=2, ensure_ascii=False))
        return 0
    if args.action == "sample-once":
        print(json.dumps({
            "throughput_bp": sample_throughput_and_bp(jid),
            "checkpoints": checkpoint_stats(jid),
        }, indent=2, ensure_ascii=False))
        return 0
    if args.action == "vertices-bp":
        print(json.dumps(vertices_bp(jid), indent=2, ensure_ascii=False))
        return 0
    if args.action == "poll":
        out = Path(args.out or "/tmp/g6_poll.json")
        print(json.dumps(poll_loop(jid, args.seconds, args.interval, out), indent=2, ensure_ascii=False))
        return 0
    if args.action == "e2e":
        print(json.dumps(e2e_from_sink_jsonl(Path(args.sink_jsonl)), indent=2, ensure_ascii=False))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
