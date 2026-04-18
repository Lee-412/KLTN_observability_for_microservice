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

    rows_by_service: Dict[str, List[Tuple[int, float, float, float, float, float, float]]] = defaultdict(list)

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
                rows_by_service[service].append((sec, cpu, mem, latency, success_rate, workload, float(sec)))

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
                "ts": [],
            }
        )

        for sec, cpu, mem, latency, success_rate, workload, ts in rows:
            acc = acc_by_sec[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(latency)
            acc["success"].append(success_rate)
            acc["workload"].append(workload)
            acc["ts"].append(ts)

        for sec, acc in acc_by_sec.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "success": _mean(acc["success"]),
                "workload": _mean(acc["workload"]),
                "ts": _mean(acc["ts"]),
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


def _composite_v45_hybrid_rerank(
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
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    if not traces:
        return []

    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}

    # Stage A: frozen v3.5 sampling.
    sampled_ids = sv35comp._composite_v3_metrics_strictcap(
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

    sampled_traces = [p for p in traces if p.trace_id in sampled_ids]
    if not sampled_traces:
        return []

    dur_lo = min(p.duration_ms for p in sampled_traces)
    dur_hi = max(p.duration_ms for p in sampled_traces)

    def _latency_norm(v: float) -> float:
        if dur_hi <= dur_lo:
            return 0.0
        return _clamp((v - dur_lo) / (dur_hi - dur_lo), 0.0, 1.0)

    # Stage B: metric-aware rerank signal construction.
    service_history: Dict[str, Dict[str, Deque[float]]] = defaultdict(
        lambda: {
            "latency": deque(maxlen=10),
            "cpu": deque(maxlen=10),
            "mem": deque(maxlen=10),
            "success": deque(maxlen=10),
            "slope": deque(maxlen=10),
            "ts": deque(maxlen=10),
        }
    )

    last_incident_t: Optional[int] = None
    score_rows: List[Tuple[str, float, float, float, float, float]] = []

    for p in sampled_traces:
        trace_sec = int(p.start_ns // 1_000_000_000)
        service_risk = 0.0
        trend_score = 0.0

        for svc in sorted(p.services):
            aligned = get_aligned_metrics(trace_sec, svc, metrics_stream_by_sec, lookback_sec=metric_lookback_sec)
            if aligned is None:
                continue

            h = service_history[svc]

            x_cpu = _clamp(float(aligned.get("cpu", 0.0)), 0.0, 1.0)
            x_mem = _clamp(float(aligned.get("memory", 0.0)), 0.0, 1.0)
            x_lat = max(0.0, float(aligned.get("latency", 0.0)))
            x_success = _clamp(float(aligned.get("success", 1.0)), 0.0, 1.0)

            if len(h["latency"]) < 5:
                z_latency = 0.0
            else:
                z_latency = (x_lat - _mean(list(h["latency"]))) / (_std(list(h["latency"])) + 0.001)

            if len(h["cpu"]) < 5:
                z_cpu = 0.0
            else:
                z_cpu = (x_cpu - _mean(list(h["cpu"]))) / (_std(list(h["cpu"])) + 0.001)

            if len(h["mem"]) < 5:
                z_mem = 0.0
            else:
                z_mem = (x_mem - _mean(list(h["mem"]))) / (_std(list(h["mem"])) + 0.001)

            error_penalty = 1.0 if x_success < 0.99 else 0.0
            raw_risk = 0.4 * z_latency + 0.3 * z_cpu + 0.2 * z_mem + 0.1 * error_penalty
            svc_risk = _clamp(_sigmoid(raw_risk), 0.0, 1.0)
            service_risk = max(service_risk, svc_risk)

            if len(h["latency"]) >= 3:
                slope = x_lat - list(h["latency"])[-3]
            else:
                slope = 0.0

            if len(h["slope"]) < 5:
                trend_z = 0.0
            else:
                trend_z = (slope - _mean(list(h["slope"]))) / (_std(list(h["slope"])) + 0.001)

            svc_trend = _clamp(_sigmoid(max(0.0, trend_z)), 0.0, 1.0)
            trend_score = max(trend_score, svc_trend)

            h["latency"].append(x_lat)
            h["cpu"].append(x_cpu)
            h["mem"].append(x_mem)
            h["success"].append(x_success)
            h["slope"].append(slope)
            h["ts"].append(float(trace_sec))

        if service_risk > 0.85:
            last_incident_t = trace_sec

        if last_incident_t is None or trace_sec < last_incident_t:
            time_score = 0.0
        else:
            time_score = _clamp(math.exp(-(trace_sec - last_incident_t) / 15.0), 0.0, 1.0)

        incident_hit = 1.0 if (set(p.services) & incident_services) else 0.0
        microrank_prior = _clamp(0.6 * _latency_norm(float(p.duration_ms)) + 0.4 * incident_hit, 0.0, 1.0)

        rerank_score = _clamp(
            0.5 * microrank_prior
            + 0.3 * service_risk
            + 0.15 * trend_score
            + 0.05 * time_score,
            0.0,
            1.0,
        )

        score_rows.append((p.trace_id, microrank_prior, service_risk, trend_score, time_score, rerank_score))

    # Stage C: deterministic rerank only (no resampling / no size change).
    score_rows.sort(key=lambda x: (x[5], x[2], x[1], x[0]), reverse=True)

    if debug_trace_logs is not None:
        for tid, base_score, svc_risk, tr_score, tm_score, rr_score in score_rows:
            debug_trace_logs.append(
                {
                    "trace_id": tid,
                    "base_score": round(float(base_score), 6),
                    "service_risk": round(float(svc_risk), 6),
                    "trend_score": round(float(tr_score), 6),
                    "time_score": round(float(tm_score), 6),
                    "rerank_score": round(float(rr_score), 6),
                }
            )

    return [row[0] for row in score_rows]
