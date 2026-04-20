import csv
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

import streamv3_composite_strictcap as sv35comp


@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: float
    span_count: int
    has_error: bool
    services: FrozenSet[str]


MetricVector = Dict[str, float]
PreferenceVector = Dict[str, MetricVector]
MetricStats = Dict[str, Dict[str, Any]]
MetricsSnapshot = Dict[str, Dict[str, float]]


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _std(values: List[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mu = _mean(values)
    var = sum((value - mu) * (value - mu) for value in values) / float(len(values))
    return math.sqrt(max(0.0, var))


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _strip_pod_to_service(pod_name: str) -> str:
    parts = str(pod_name).strip().split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return str(pod_name).strip()


def build_real_metrics_stream(metric_dir: str) -> Dict[int, MetricsSnapshot]:
    if not metric_dir:
        return {}
    path = os.path.abspath(metric_dir)
    if not os.path.isdir(path):
        return {}

    rows_by_service: Dict[str, List[Tuple[int, float, float, float, float, float]]] = defaultdict(list)

    for name in sorted(os.listdir(path)):
        if not name.endswith(".csv"):
            continue
        if name in {"dependency.csv", "front_service.csv", "source_50.csv", "destination_50.csv"}:
            continue

        file_path = os.path.join(path, name)
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    sec = int(float(str(row.get("TimeStamp", "0")).strip()))
                except Exception:
                    continue
                if sec <= 0:
                    continue

                pod = str(row.get("PodName", "")).strip()
                if not pod:
                    continue
                service = _strip_pod_to_service(pod)

                cpu = _clamp(float(row.get("CpuUsageRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                mem = _clamp(float(row.get("MemoryUsageRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                client_p95 = float(row.get("PodClientLatencyP95(s)", 0.0))
                server_p95 = float(row.get("PodServerLatencyP95(s)", 0.0))
                latency = max(client_p95, server_p95)
                success_rate = _clamp(float(row.get("PodSuccessRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                workload = max(0.0, float(row.get("PodWorkload(Ops)", 0.0)))
                rows_by_service[service].append((sec, cpu, mem, latency, success_rate, workload))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        acc_by_sec: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {
                "cpu": [],
                "memory": [],
                "latency": [],
                "success": [],
                "workload": [],
            }
        )

        for sec, cpu, mem, latency, success_rate, workload in rows:
            acc = acc_by_sec[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(latency)
            acc["success"].append(success_rate)
            acc["workload"].append(workload)

        for sec, acc in acc_by_sec.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "success": _mean(acc["success"]),
                "workload": _mean(acc["workload"]),
            }

    return dict(out)


def get_aligned_metrics(
    trace_sec: int,
    service: str,
    metrics_stream_by_sec: Dict[int, MetricsSnapshot],
    lookback_sec: int = 20,
) -> Optional[Dict[str, float]]:
    for sec in range(trace_sec, trace_sec - max(1, lookback_sec) - 1, -1):
        snap = metrics_stream_by_sec.get(sec)
        if not snap:
            continue
        metric = snap.get(service)
        if metric is not None:
            return metric
    return None


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    if not points:
        return {}, {}
    return {}, {}


def _composite_v49_alignment_golden_sampler(
    points: List[TracePoint],
    budget_pct: float,
    preference_vector: PreferenceVector,
    metric_stats: MetricStats,
    scenario_windows: List[Tuple[str, int, int]],
    incident_anchor_sec: Optional[int],
    seed: int,
    incident_services: Optional[Set[str]] = None,
    min_incident_traces_per_scenario: int = 1,
    online_soft_cap: bool = False,
    metrics_stream_by_sec: Optional[Dict[int, MetricsSnapshot]] = None,
    metric_lookback_sec: int = 20,
    golden_ratio: float = 0.15,
    alignment_percentile: float = 0.80,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    total = len(traces)
    if total == 0:
        return set()

    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}

    # Stage A: frozen v3.5 membership.
    base_kept = sv35comp._composite_v3_metrics_strictcap(
        points=traces,
        budget_pct=budget_pct,
        preference_vector=preference_vector,
        metric_stats=metric_stats,
        scenario_windows=scenario_windows,
        incident_anchor_sec=incident_anchor_sec,
        seed=seed,
        incident_services=incident_services,
        min_incident_traces_per_scenario=min_incident_traces_per_scenario,
        online_soft_cap=online_soft_cap,
    )

    if not base_kept:
        return set()

    target_count = len(base_kept)
    golden_quota = max(1, int(round(target_count * _clamp(golden_ratio, 0.0, 0.5))))

    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    service_history: Dict[str, Dict[str, Deque[float]]] = defaultdict(
        lambda: {
            "cpu": deque(maxlen=10),
            "memory": deque(maxlen=10),
            "latency": deque(maxlen=10),
            "success": deque(maxlen=10),
            "workload": deque(maxlen=10),
        }
    )

    alignment_rows: List[Tuple[str, float, float, float, float, float]] = []

    for p in traces:
        trace_sec = int(p.start_ns // 1_000_000_000)
        lat_norm = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        span_norm = _norm(float(p.span_count), float(span_lo), float(span_hi))
        err_norm = 1.0 if p.has_error else 0.0

        red_scalar = _clamp(0.55 * lat_norm + 0.25 * err_norm + 0.20 * span_norm, 0.0, 1.0)

        svc_scores: List[float] = []
        for svc in sorted(p.services):
            aligned = get_aligned_metrics(trace_sec, svc, metrics_stream_by_sec, lookback_sec=metric_lookback_sec)
            if aligned is None:
                continue

            h = service_history[svc]
            x_cpu = _clamp(float(aligned.get("cpu", 0.0)), 0.0, 1.0)
            x_mem = _clamp(float(aligned.get("memory", 0.0)), 0.0, 1.0)
            x_lat = max(0.0, float(aligned.get("latency", 0.0)))
            x_success = _clamp(float(aligned.get("success", 1.0)), 0.0, 1.0)
            x_work = max(0.0, float(aligned.get("workload", 0.0)))

            z_lat = (x_lat - _mean(list(h["latency"]))) / (_std(list(h["latency"])) + 1e-3) if len(h["latency"]) >= 5 else 0.0
            z_cpu = (x_cpu - _mean(list(h["cpu"]))) / (_std(list(h["cpu"])) + 1e-3) if len(h["cpu"]) >= 5 else 0.0
            z_mem = (x_mem - _mean(list(h["memory"]))) / (_std(list(h["memory"])) + 1e-3) if len(h["memory"]) >= 5 else 0.0
            z_work = (x_work - _mean(list(h["workload"]))) / (_std(list(h["workload"])) + 1e-3) if len(h["workload"]) >= 5 else 0.0

            metric_projection = (
                0.35 * max(0.0, z_lat)
                + 0.20 * max(0.0, z_cpu)
                + 0.15 * max(0.0, z_mem)
                + 0.15 * max(0.0, z_work)
                + 0.15 * (1.0 - x_success)
            )
            svc_scores.append(red_scalar * metric_projection)

            h["cpu"].append(x_cpu)
            h["memory"].append(x_mem)
            h["latency"].append(x_lat)
            h["success"].append(x_success)
            h["workload"].append(x_work)

        align_max = max(svc_scores) if svc_scores else 0.0
        align_mean = _mean(svc_scores) if svc_scores else 0.0
        incident_hit = 1.0 if (set(p.services) & incident_services) else 0.0

        raw_alignment = 0.7 * align_max + 0.3 * align_mean
        alignment = _clamp(_sigmoid(2.5 * (raw_alignment - 0.25)), 0.0, 1.0)

        score = _clamp(
            0.70 * alignment + 0.20 * incident_hit + 0.10 * err_norm,
            0.0,
            1.0,
        )
        alignment_rows.append((p.trace_id, alignment, incident_hit, err_norm, red_scalar, score))

    if not alignment_rows:
        return set(base_kept)

    sorted_scores = sorted(row[5] for row in alignment_rows)
    threshold_idx = int(_clamp(alignment_percentile, 0.0, 1.0) * (len(sorted_scores) - 1))
    score_threshold = sorted_scores[threshold_idx]

    row_by_id: Dict[str, Tuple[str, float, float, float, float, float]] = {row[0]: row for row in alignment_rows}

    candidate_add = [
        row
        for row in alignment_rows
        if row[0] not in base_kept and row[5] >= score_threshold
    ]
    candidate_add.sort(key=lambda row: (row[5], row[2], row[3], row[1], row[0]), reverse=True)

    candidate_drop = [
        row_by_id[tid]
        for tid in base_kept
        if tid in row_by_id and row_by_id[tid][2] <= 0.0
    ]
    candidate_drop.sort(key=lambda row: (row[3], row[2], row[5], row[1], row[0]))

    replace_n = min(golden_quota, len(candidate_add), len(candidate_drop))

    kept_ids = set(base_kept)
    replaced_pairs: List[Tuple[str, str]] = []
    for i in range(replace_n):
        add_tid = candidate_add[i][0]
        drop_tid = candidate_drop[i][0]
        if add_tid in kept_ids or drop_tid not in kept_ids:
            continue
        kept_ids.discard(drop_tid)
        kept_ids.add(add_tid)
        replaced_pairs.append((drop_tid, add_tid))

    # Hard guard to keep output cardinality stable.
    if len(kept_ids) > target_count:
        ranked_keep = sorted(
            kept_ids,
            key=lambda tid: row_by_id.get(tid, (tid, 0.0, 0.0, 0.0, 0.0, 0.0))[5],
            reverse=True,
        )
        kept_ids = set(ranked_keep[:target_count])
    elif len(kept_ids) < target_count:
        fillers = [row[0] for row in alignment_rows if row[0] not in kept_ids]
        fillers.sort(key=lambda tid: row_by_id[tid][5], reverse=True)
        for tid in fillers:
            kept_ids.add(tid)
            if len(kept_ids) >= target_count:
                break

    if debug_trace_logs is not None:
        replaced_add_ids = {add_tid for _drop_tid, add_tid in replaced_pairs}
        replaced_drop_ids = {drop_tid for drop_tid, _add_tid in replaced_pairs}
        for tid, alignment, incident_hit, err_norm, red_scalar, score in sorted(
            alignment_rows,
            key=lambda row: (row[5], row[1], row[0]),
            reverse=True,
        ):
            debug_trace_logs.append(
                {
                    "trace_id": tid,
                    "alignment": round(float(alignment), 6),
                    "incident_hit": round(float(incident_hit), 6),
                    "error_flag": round(float(err_norm), 6),
                    "red_scalar": round(float(red_scalar), 6),
                    "golden_score": round(float(score), 6),
                    "score_threshold": round(float(score_threshold), 6),
                    "in_base_kept": tid in base_kept,
                    "in_final_kept": tid in kept_ids,
                    "replaced_in": tid in replaced_add_ids,
                    "replaced_out": tid in replaced_drop_ids,
                }
            )

    return kept_ids
