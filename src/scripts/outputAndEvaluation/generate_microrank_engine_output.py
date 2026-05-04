#!/usr/bin/env python3
"""Generate per-scenario RCA rankings using MicroRank core algorithm in offline mode.

This script adapts TraStrainer trace CSV files into MicroRank-compatible structures,
executes the PageRank + spectrum pipeline, and writes one raw engine output file
per scenario for downstream adapter/evaluator scripts.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_id: str
    service: str
    operation: str
    start_ns: int
    end_ns: int
    duration_us: int


@dataclass
class TraceRecord:
    trace_id: str
    start_ts_s: int
    end_ts_s: int
    spans: list[Span]


def _parse_iso_utc_to_s(value: str) -> int:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    return int(dt.astimezone(timezone.utc).timestamp())


def _service_from_pod(pod_name: str) -> str:
    parts = pod_name.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return pod_name


def _safe_int(text: str, default: int = 0) -> int:
    try:
        return int(str(text).strip())
    except Exception:
        return default


def _load_scenarios(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _load_traces(trace_files: list[str]) -> dict[str, TraceRecord]:
    traces: dict[str, TraceRecord] = {}

    for tf in trace_files:
        with open(tf, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trace_id = str(row.get("TraceID", "")).strip()
                if not trace_id:
                    continue

                span_id = str(row.get("SpanID", "")).strip()
                parent_id = str(row.get("ParentID", "")).strip()
                pod_name = str(row.get("PodName", "")).strip()
                operation_name = str(row.get("OperationName", "")).strip()
                start_ns = _safe_int(str(row.get("StartTimeUnixNano", "0")), 0)
                end_ns = _safe_int(str(row.get("EndTimeUnixNano", "0")), 0)
                duration_us = _safe_int(str(row.get("Duration", "0")), 0)

                if start_ns <= 0:
                    continue
                if end_ns <= 0:
                    end_ns = start_ns
                if duration_us <= 0:
                    duration_us = max(0, (end_ns - start_ns) // 1000)

                service = _service_from_pod(pod_name) if pod_name else "unknown-service"
                operation = operation_name.split("/")[-1] if operation_name else "unknown-operation"

                span = Span(
                    trace_id=trace_id,
                    span_id=span_id,
                    parent_id=parent_id,
                    service=service,
                    operation=operation,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    duration_us=duration_us,
                )

                rec = traces.get(trace_id)
                if rec is None:
                    traces[trace_id] = TraceRecord(
                        trace_id=trace_id,
                        start_ts_s=start_ns // 1_000_000_000,
                        end_ts_s=end_ns // 1_000_000_000,
                        spans=[span],
                    )
                else:
                    rec.spans.append(span)
                    if start_ns // 1_000_000_000 < rec.start_ts_s:
                        rec.start_ts_s = start_ns // 1_000_000_000
                    if end_ns // 1_000_000_000 > rec.end_ts_s:
                        rec.end_ts_s = end_ns // 1_000_000_000

    for rec in traces.values():
        rec.spans.sort(key=lambda x: (x.start_ns, x.span_id))

    return traces


def _operation_name(span: Span) -> str:
    return f"{span.service}_{span.operation}"


def _trace_duration_ms(rec: TraceRecord) -> float:
    if not rec.spans:
        return 0.0
    start_ns = min(s.start_ns for s in rec.spans)
    end_ns = max(s.end_ns for s in rec.spans)
    return max(0.0, (end_ns - start_ns) / 1_000_000.0)


def _build_operation_count(traces: dict[str, TraceRecord], trace_ids: list[str], operation_list: list[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for tid in trace_ids:
        rec = traces.get(tid)
        if rec is None:
            continue

        counts: dict[str, float] = {op: 0.0 for op in operation_list}
        for span in rec.spans:
            op = _operation_name(span)
            if op in counts:
                counts[op] += 1.0
        counts["duration"] = _trace_duration_ms(rec)
        out[tid] = counts
    return out


def _build_operation_slo(traces: dict[str, TraceRecord], trace_ids: list[str], operation_list: list[str]) -> dict[str, list[float]]:
    duration_map: dict[str, list[float]] = {op: [] for op in operation_list}

    for tid in trace_ids:
        rec = traces.get(tid)
        if rec is None:
            continue
        for span in rec.spans:
            op = _operation_name(span)
            if op in duration_map:
                duration_map[op].append(span.duration_us / 1000.0)

    slo: dict[str, list[float]] = {}
    for op in operation_list:
        vals = duration_map[op]
        if not vals:
            slo[op] = [1.0, 0.1]
            continue
        mean_ms = statistics.fmean(vals)
        std_ms = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        slo[op] = [round(mean_ms, 4), round(std_ms, 4)]
    return slo


def _trace_anomaly_detect(op_counts: dict[str, float], slo: dict[str, list[float]]) -> bool:
    expect_duration = 0.0
    real_duration = float(op_counts.get("duration", 0.0))
    for op, cnt in op_counts.items():
        if op == "duration":
            continue
        if cnt <= 0:
            continue
        op_slo = slo.get(op, [1.0, 0.1])
        expect_duration += cnt * (op_slo[0] + op_slo[1])
    return real_duration > expect_duration + 50.0


def _partition_traces(operation_count: dict[str, dict[str, float]], slo: dict[str, list[float]]) -> tuple[list[str], list[str]]:
    abnormal: list[str] = []
    normal: list[str] = []
    for tid, counts in operation_count.items():
        if _trace_anomaly_detect(counts, slo):
            abnormal.append(tid)
        else:
            normal.append(tid)
    return abnormal, normal


def _build_pagerank_graph(traces: dict[str, TraceRecord], trace_ids: list[str]) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
    operation_operation: dict[str, list[str]] = {}
    operation_trace: dict[str, list[str]] = {}
    trace_operation: dict[str, list[str]] = {}
    pr_trace: dict[str, list[str]] = {}

    target = set(trace_ids)
    for tid in trace_ids:
        rec = traces.get(tid)
        if rec is None:
            continue

        by_span_id: dict[str, Span] = {s.span_id: s for s in rec.spans if s.span_id}
        operation_trace.setdefault(tid, [])
        pr_trace.setdefault(tid, [])

        for span in rec.spans:
            op = _operation_name(span)
            operation_operation.setdefault(op, [])
            trace_operation.setdefault(op, [])

            pr_trace[tid].append(op)
            if op not in operation_trace[tid]:
                operation_trace[tid].append(op)
            if tid not in trace_operation[op]:
                trace_operation[op].append(tid)

            parent_id = span.parent_id
            if parent_id and parent_id.lower() != "root" and parent_id in by_span_id:
                parent_op = _operation_name(by_span_id[parent_id])
                operation_operation.setdefault(parent_op, [])
                if op not in operation_operation[parent_op]:
                    operation_operation[parent_op].append(op)

    # Keep dicts aligned to traces that exist in this window.
    operation_trace = {k: v for k, v in operation_trace.items() if k in target and v}
    pr_trace = {k: v for k, v in pr_trace.items() if k in target and v}

    return operation_operation, operation_trace, trace_operation, pr_trace


def _calculate_spectrum(
    anomaly_result: dict[str, float],
    normal_result: dict[str, float],
    anomaly_list_len: int,
    normal_list_len: int,
    normal_num_list: dict[str, int],
    anomaly_num_list: dict[str, int],
) -> list[tuple[str, float]]:
    spectrum: dict[str, dict[str, float]] = {}

    for node in anomaly_result:
        spectrum[node] = {}
        spectrum[node]["ef"] = anomaly_result[node] * float(anomaly_num_list.get(node, 0))
        spectrum[node]["nf"] = anomaly_result[node] * float(anomaly_list_len - anomaly_num_list.get(node, 0))
        if node in normal_result:
            spectrum[node]["ep"] = normal_result[node] * float(normal_num_list.get(node, 0))
            spectrum[node]["np"] = normal_result[node] * float(normal_list_len - normal_num_list.get(node, 0))
        else:
            spectrum[node]["ep"] = 1e-7
            spectrum[node]["np"] = 1e-7

    for node in normal_result:
        if node not in spectrum:
            spectrum[node] = {}
            spectrum[node]["ep"] = (1.0 + normal_result[node]) * float(normal_num_list.get(node, 0))
            spectrum[node]["np"] = float(normal_list_len - normal_num_list.get(node, 0))
            spectrum[node]["ef"] = 1e-7
            spectrum[node]["nf"] = 1e-7

    result: dict[str, float] = {}
    for node, s in spectrum.items():
        denom = s["ep"] + s["nf"]
        if denom <= 0:
            result[node] = 0.0
        else:
            result[node] = (s["ef"] * s["ef"]) / denom

    return sorted(result.items(), key=lambda x: x[1], reverse=True)


def _aggregate_service_scores(op_scores: list[tuple[str, float]]) -> list[tuple[str, float]]:
    agg: dict[str, float] = defaultdict(float)
    for op_name, score in op_scores:
        service = op_name.split("_", 1)[0]
        agg[service] += float(score)
    return sorted(agg.items(), key=lambda x: x[1], reverse=True)


def _rank_from_latency_fallback(traces: dict[str, TraceRecord], trace_ids: list[str]) -> list[tuple[str, float]]:
    agg: dict[str, float] = defaultdict(float)
    for tid in trace_ids:
        rec = traces.get(tid)
        if rec is None:
            continue
        for span in rec.spans:
            agg[span.service] += span.duration_us / 1000.0
    return sorted(agg.items(), key=lambda x: x[1], reverse=True)


def _run_microrank_for_scenario(
    traces: dict[str, TraceRecord],
    start_ts_s: int,
    end_ts_s: int,
    normal_window_s: int,
    trace_pagerank_fn: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if end_ts_s < start_ts_s:
        end_ts_s = start_ts_s

    incident_trace_ids = [
        tid
        for tid, rec in traces.items()
        if rec.start_ts_s >= start_ts_s and rec.start_ts_s <= end_ts_s
    ]

    normal_end = start_ts_s - 1
    normal_start = max(0, normal_end - normal_window_s + 1)
    normal_trace_ids = [
        tid
        for tid, rec in traces.items()
        if rec.start_ts_s >= normal_start and rec.start_ts_s <= normal_end
    ]

    debug = {
        "incident_trace_count": len(incident_trace_ids),
        "normal_trace_count": len(normal_trace_ids),
        "normal_window_start": datetime.fromtimestamp(normal_start, tz=timezone.utc).isoformat(),
        "normal_window_end": datetime.fromtimestamp(normal_end, tz=timezone.utc).isoformat(),
    }

    if not incident_trace_ids:
        return [], debug

    op_set = set()
    for tid in incident_trace_ids + normal_trace_ids:
        rec = traces.get(tid)
        if rec is None:
            continue
        for span in rec.spans:
            op_set.add(_operation_name(span))

    operation_list = sorted(op_set)
    if not operation_list:
        return [], debug

    if normal_trace_ids:
        slo = _build_operation_slo(traces, normal_trace_ids, operation_list)
    else:
        slo = _build_operation_slo(traces, incident_trace_ids, operation_list)

    incident_op_counts = _build_operation_count(traces, incident_trace_ids, operation_list)
    anomaly_ids, normal_ids = _partition_traces(incident_op_counts, slo)

    # Keep MicroRank stable when partition is degenerate.
    if not anomaly_ids and incident_trace_ids:
        anomaly_ids = incident_trace_ids[: max(1, len(incident_trace_ids) // 3)]
    if not normal_ids and incident_trace_ids:
        normal_ids = incident_trace_ids[max(1, len(incident_trace_ids) // 3) :]
    if not normal_ids and normal_trace_ids:
        normal_ids = normal_trace_ids

    if not anomaly_ids or not normal_ids:
        fallback = _rank_from_latency_fallback(traces, incident_trace_ids)
        ranked = [
            {"candidate_id": svc, "rank": i, "score": round(score, 6)}
            for i, (svc, score) in enumerate(fallback, start=1)
        ]
        debug["fallback"] = "latency"
        debug["partition_anomaly_count"] = len(anomaly_ids)
        debug["partition_normal_count"] = len(normal_ids)
        return ranked, debug

    normal_graph = _build_pagerank_graph(traces, normal_ids)
    anomaly_graph = _build_pagerank_graph(traces, anomaly_ids)

    if not normal_graph[0] or not normal_graph[1] or not anomaly_graph[0] or not anomaly_graph[1]:
        fallback = _rank_from_latency_fallback(traces, incident_trace_ids)
        ranked = [
            {"candidate_id": svc, "rank": i, "score": round(score, 6)}
            for i, (svc, score) in enumerate(fallback, start=1)
        ]
        debug["fallback"] = "latency_graph_empty"
        debug["partition_anomaly_count"] = len(anomaly_ids)
        debug["partition_normal_count"] = len(normal_ids)
        return ranked, debug

    normal_result, normal_num = trace_pagerank_fn(*normal_graph, False)
    anomaly_result, anomaly_num = trace_pagerank_fn(*anomaly_graph, True)

    op_scores = _calculate_spectrum(
        anomaly_result=anomaly_result,
        normal_result=normal_result,
        anomaly_list_len=len(anomaly_ids),
        normal_list_len=len(normal_ids),
        normal_num_list=normal_num,
        anomaly_num_list=anomaly_num,
    )
    svc_scores = _aggregate_service_scores(op_scores)

    ranked = [
        {"candidate_id": svc, "rank": i, "score": round(score, 6)}
        for i, (svc, score) in enumerate(svc_scores, start=1)
    ]

    debug["partition_anomaly_count"] = len(anomaly_ids)
    debug["partition_normal_count"] = len(normal_ids)
    return ranked, debug


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate raw RCA outputs with MicroRank (offline)")
    ap.add_argument("--scenario-file", required=True, help="Scenario .jsonl file")
    ap.add_argument("--out-dir", required=True, help="Output dir for raw engine JSON files")
    ap.add_argument(
        "--microrank-dir",
        default="external/rca-engines/MicroRank",
        help="Path to cloned MicroRank repository",
    )
    ap.add_argument(
        "--normal-window-minutes",
        type=int,
        default=30,
        help="Normal baseline window length before incident start",
    )
    args = ap.parse_args()

    scenario_file = Path(args.scenario_file).resolve()
    out_dir = Path(args.out_dir).resolve()
    microrank_dir = Path(args.microrank_dir).resolve()

    if not scenario_file.exists():
        raise SystemExit(f"Scenario file not found: {scenario_file}")
    if not microrank_dir.exists():
        raise SystemExit(f"MicroRank dir not found: {microrank_dir}")

    sys.path.insert(0, str(microrank_dir))
    try:
        from pagerank import trace_pagerank
    except Exception as e:
        raise SystemExit(f"Failed to import MicroRank pagerank.py from {microrank_dir}: {e}")

    out_dir.mkdir(parents=True, exist_ok=True)
    scenarios = _load_scenarios(scenario_file)

    traces_cache: dict[str, dict[str, TraceRecord]] = {}

    ok = 0
    empty = 0
    failures = 0

    for row in scenarios:
        sid = str(row.get("scenario_id", "")).strip()
        if not sid:
            continue

        trace_glob = str(row.get("trace_glob", "")).strip()
        if not trace_glob:
            failures += 1
            continue

        traces = traces_cache.get(trace_glob)
        if traces is None:
            files = sorted(glob.glob(trace_glob))
            traces = _load_traces(files)
            traces_cache[trace_glob] = traces

        start_s = _parse_iso_utc_to_s(str(row.get("incident_start_ts", "")))
        end_s = _parse_iso_utc_to_s(str(row.get("incident_end_ts", "")))

        t0 = time.time()
        ranked, debug = _run_microrank_for_scenario(
            traces=traces,
            start_ts_s=start_s,
            end_ts_s=end_s,
            normal_window_s=max(60, args.normal_window_minutes * 60),
            trace_pagerank_fn=trace_pagerank,
        )
        infer_ms = (time.time() - t0) * 1000.0

        payload = {
            "method": "microrank-offline",
            "scenario_id": sid,
            "inference_time_ms": round(infer_ms, 3),
            "ranked_candidates": ranked,
            "debug": debug,
        }
        (out_dir / f"{sid}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        if ranked:
            ok += 1
        else:
            empty += 1

    print(f"scenario_count={len(scenarios)}")
    print(f"ok_count={ok}")
    print(f"empty_count={empty}")
    print(f"failure_count={failures}")
    print(f"out_dir={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
