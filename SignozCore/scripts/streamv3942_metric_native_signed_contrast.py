"""
Stream v3.94.2 -- self-contained metric-native signed-contrast sampler.

This module is intentionally self-contained:
- No imports from internal sampler modules.
- All sampling, consensus, contrast-guard, and telemetry logic is in this file.
"""

from __future__ import annotations

import bisect
import math
import random
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple


@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: float
    span_count: int
    has_error: bool
    services: FrozenSet[str]

@dataclass
class V3942MetricContext:
    metric_evidence_by_id: Dict[str, float]
    metric_intensity_by_id: Dict[str, float]
    metric_coverage_by_id: Dict[str, float]
    mapping_reliability_by_id: Dict[str, float]
    mismatch_count: int

MetricVector = Dict[str, float]
PreferenceVector = Dict[str, MetricVector]
MetricStats = Dict[str, Dict[str, Any]]
MetricsStreamBySec = Dict[int, Dict[str, Dict[str, float]]]


_ENSEMBLE_SEEDS = [11, 22,33]

# _ENSEMBLE_SEEDS = [11]
# _ENSEMBLE_SEEDS = [11,22,33, 44]

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
    var = sum((x - mu) * (x - mu) for x in values) / float(len(values))
    return math.sqrt(max(0.0, var))


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    mid = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[mid])
    return 0.5 * (float(xs[mid - 1]) + float(xs[mid]))


def _mad(values: List[float]) -> float:
    if not values:
        return 0.0
    med = _median(values)
    return _median([abs(v - med) for v in values])


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    p = _clamp(float(pct), 0.0, 100.0)
    xs = sorted(values)
    if len(xs) == 1:
        return float(xs[0])
    rank = (len(xs) - 1) * (p / 100.0)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(xs[lo])
    frac = rank - lo
    return float(xs[lo]) * (1.0 - frac) + float(xs[hi]) * frac


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _logit(p: float) -> float:
    q = _clamp(float(p), 1e-6, 1.0 - 1e-6)
    return math.log(q / (1.0 - q))


def _topk_projection(values: List[float], weights: Optional[List[float]] = None) -> float:
    if not values:
        return 0.0
    w = weights if weights is not None else [0.5, 0.3, 0.2]
    ranked = sorted(values, reverse=True)
    out = 0.0
    for ww, vv in zip(w, ranked):
        out += float(ww) * float(vv)
    return out


def _robust_sigma(values: List[float], floor: float = 1e-6) -> float:
    if len(values) <= 1:
        return floor
    std = _std(values)
    mad_sigma = 1.4826 * _mad(values)
    return max(floor, std, mad_sigma)


_K8S_HASH_RE = re.compile(r"^[a-f0-9]{8,10}$")
_K8S_POD_TAIL_RE = re.compile(r"^[a-z0-9]{5}$")


def _normalize_name(raw: str) -> str:
    name = str(raw or "").strip().lower().replace("_", "-")
    name = re.sub(r"[^a-z0-9-]", "", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name


def _strip_pod_to_service(pod_name: str) -> str:
    name = _normalize_name(pod_name)
    if not name:
        return ""

    parts = [p for p in name.split("-") if p]
    if len(parts) >= 3 and _K8S_HASH_RE.match(parts[-2]) and _K8S_POD_TAIL_RE.match(parts[-1]):
        return "-".join(parts[:-2])
    if len(parts) >= 2 and _K8S_POD_TAIL_RE.match(parts[-1]):
        return "-".join(parts[:-1])
    return name


def _name_tokens(raw: str) -> Set[str]:
    name = _normalize_name(raw)
    if not name:
        return set()
    return {tok for tok in name.split("-") if tok}

import os

MetricsSnapshot = Dict[str, Dict[str, float]]
import csv

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
        with open(file_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
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
                workload_ops = max(0.0, float(row.get("PodWorkload(Ops)", 0.0)))
                success_rate = _clamp(float(row.get("PodSuccessRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                rows_by_service[service].append((sec, cpu, mem, latency, workload_ops, success_rate))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda item: item[0])
        lat_hist: deque[float] = deque(maxlen=120)
        ops_hist: deque[float] = deque(maxlen=120)
        per_sec_acc: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {
                "cpu": [],
                "memory": [],
                "latency": [],
                "workload": [],
                "success_rate": [],
            }
        )

        for sec, cpu, mem, latency, ops, success in rows:
            lat_hist.append(latency)
            ops_hist.append(ops)

            sorted_lat = sorted(lat_hist)
            sorted_ops = sorted(ops_hist)
            lat_p95 = sorted_lat[int(0.95 * (len(sorted_lat) - 1))] if sorted_lat else 1.0
            ops_p95 = sorted_ops[int(0.95 * (len(sorted_ops) - 1))] if sorted_ops else 1.0
            lat_p95 = max(1e-6, float(lat_p95))
            ops_p95 = max(1e-6, float(ops_p95))

            norm_latency = _clamp(latency / lat_p95, 0.0, 1.0)
            norm_workload = _clamp(ops / ops_p95, 0.0, 1.0)

            acc = per_sec_acc[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(norm_latency)
            acc["workload"].append(norm_workload)
            acc["success_rate"].append(success)

        for sec, acc in per_sec_acc.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "workload": _mean(acc["workload"]),
                "success_rate": _mean(acc["success_rate"]),
            }

    return dict(out)



def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    """Build service-keyed metric inputs for runner compatibility.

    Contract:
    - preference_vector[service] = {latency, error, throughput, burst}
    - metric_stats[service] = {history_count, mean_duration_ms, error_rate, qps_mean, burst}
    """
    if not points:
        return {}, {}

    traces = sorted(points, key=lambda p: (int(p.start_ns), str(p.trace_id)))

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_count: Dict[str, int] = defaultdict(int)
    svc_duration_sum: Dict[str, float] = defaultdict(float)
    sec_svc_count: Dict[int, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000)
        for svc in p.services:
            svc_norm = _normalize_name(str(svc))
            if not svc_norm:
                continue
            svc_trace_count[svc_norm] += 1
            svc_duration_sum[svc_norm] += float(p.duration_ms)
            if p.has_error:
                svc_error_count[svc_norm] += 1
            sec_svc_count[sec][svc_norm] += 1

    per_service_qps: Dict[str, List[float]] = defaultdict(list)
    for sec in sorted(sec_svc_count.keys()):
        snap = sec_svc_count[sec]
        for svc, cnt in snap.items():
            per_service_qps[svc].append(float(cnt))

    latency_series: List[float] = []
    throughput_series: List[float] = []
    burst_series: List[float] = []

    raw_stats: Dict[str, Dict[str, float]] = {}
    for svc in sorted(svc_trace_count.keys()):
        history_count = int(svc_trace_count.get(svc, 0))
        mean_duration_ms = float(svc_duration_sum.get(svc, 0.0)) / float(max(1, history_count))
        error_rate = float(svc_error_count.get(svc, 0)) / float(max(1, history_count))

        qps_values = per_service_qps.get(svc, [])
        qps_mean = _mean(qps_values)
        qps_p95 = _percentile(qps_values, 95.0)
        burst = max(0.0, qps_p95 - qps_mean)

        raw_stats[svc] = {
            "history_count": float(history_count),
            "mean_duration_ms": mean_duration_ms,
            "error_rate": error_rate,
            "qps_mean": qps_mean,
            "burst": burst,
        }

        latency_series.append(mean_duration_ms)
        throughput_series.append(qps_mean)
        burst_series.append(burst)

    lat_tau = _median(latency_series)
    lat_scale = _resolve_scale(latency_series, mode="robust")
    qps_tau = _median(throughput_series)
    qps_scale = _resolve_scale(throughput_series, mode="robust")
    burst_tau = _median(burst_series)
    burst_scale = _resolve_scale(burst_series, mode="robust")

    preference_vector: PreferenceVector = {}
    metric_stats: MetricStats = {}

    for svc, stats in raw_stats.items():
        latency_score = _sigmoid((stats["mean_duration_ms"] - lat_tau) / max(1e-6, lat_scale))
        throughput_score = _sigmoid((stats["qps_mean"] - qps_tau) / max(1e-6, qps_scale))
        burst_score = _sigmoid((stats["burst"] - burst_tau) / max(1e-6, burst_scale))

        preference_vector[svc] = {
            "latency": float(_clamp(latency_score, 0.0, 1.0)),
            "error": float(_clamp(stats["error_rate"], 0.0, 1.0)),
            "throughput": float(_clamp(throughput_score, 0.0, 1.0)),
            "burst": float(_clamp(burst_score, 0.0, 1.0)),
        }
        metric_stats[svc] = {
            "history_count": int(stats["history_count"]),
            "mean_duration_ms": float(stats["mean_duration_ms"]),
            "error_rate": float(_clamp(stats["error_rate"], 0.0, 1.0)),
            "qps_mean": float(max(0.0, stats["qps_mean"])),
            "burst": float(max(0.0, stats["burst"])),
        }

    return preference_vector, metric_stats

# Legacy default dual floors based on budget percentage. => A@1 first =>  36.91
# def _default_dual_floors(budget_pct: float) -> Tuple[float, float]:
#     # 0.1 budget: 30% normal, 15% error 
#     if budget_pct <= 0.1:
#         return 0.30, 0.15
#     if budget_pct <= 1.0:
#         return 0.35, 0.20
#     return 0.40, 0.20

#  34.35 | 17.93 | 14.81 | 46.73 | 44.53 | 37.13 | 0.4682 | 0.3650 | 0.3310 | - 5seed
#  39.91 | 20.36 | 16.20 | 50.20 | 41.18 | 32.74 | 0.5145 | 0.3795 | 0.3328 | - 1seed
def _default_dual_floors(budget_pct: float) -> Tuple[float, float]:
    if budget_pct <= 0.1:
        return 0.30, 0.15
    if budget_pct <= 1.0:
        return 0.38, 0.18
    return 0.44, 0.16

# Legacy default dual floors based on budget percentage. => balanced normal/error in tail after head-k
# 28.23 | 17.93 | 17.24 | 42.69 | 40.60 | 35.74 | 0.4220 | 0.3630 | 0.3418
# def _default_dual_floors(budget_pct: float) -> Tuple[float, float]:
#     if budget_pct <= 0.1:
#         return 0.25, 0.15
#     if budget_pct <= 1.0:
#         return 0.42, 0.15
#     return 0.50, 0.12

# 32.39 | 16.89 | 13.89 | 45.34 | 46.83 | 39.44 | 0.4587 | 0.3590 | 0.3309 |
# def _default_dual_floors(budget_pct: float) -> Tuple[float, float]:
#     if budget_pct <= 0.1:
#         return 0.25, 0.15
#     if budget_pct <= 1.0:
#         return 0.35, 0.20
#     return 0.40, 0.20

def _resolve_tau(values: List[float], budget_pct: float, mode: str) -> float:
    if not values:
        return 0.0
    mode_norm = str(mode or "").strip().lower()
    if mode_norm == "budget-percentile":
        if budget_pct <= 0.1:
            return _percentile(values, 60.0)
        if budget_pct <= 1.0:
            return _percentile(values, 55.0)
        return _percentile(values, 50.0)
    if mode_norm == "median":
        return _median(values)
    if mode_norm.startswith("p"):
        try:
            return _percentile(values, float(mode_norm[1:]))
        except Exception:
            return _median(values)
    return _median(values)


def _resolve_scale(values: List[float], mode: str) -> float:
    if not values:
        return 1.0
    mode_norm = str(mode or "").strip().lower()
    mad_sigma = 1.4826 * _mad(values)
    iqr = max(0.0, _percentile(values, 75.0) - _percentile(values, 25.0))
    iqr_sigma = iqr / 1.349 if iqr > 0.0 else 0.0
    std_sigma = _std(values)
    if mode_norm == "mad":
        scale = mad_sigma
    elif mode_norm == "iqr":
        scale = iqr_sigma
    elif mode_norm == "std":
        scale = std_sigma
    else:
        scale = max(mad_sigma, iqr_sigma, std_sigma)
    return max(1e-6, scale)


def _signed_metric_value(metric_evidence: float, tau: float, scale: float) -> float:
    e_center = (float(metric_evidence) - float(tau)) / max(1e-6, float(scale))
    return _clamp(2.0 * _sigmoid(e_center) - 1.0, -1.0, 1.0)


def _normalize_metric_stream(metrics_stream_by_sec: Optional[MetricsStreamBySec]) -> MetricsStreamBySec:
    """Normalize raw pod/service metrics into robust z-like snapshots."""
    if not metrics_stream_by_sec:
        return {}

    raw: Dict[int, Dict[str, Dict[str, float]]] = {}
    per_pod_hist: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for sec, snap in metrics_stream_by_sec.items():
        try:
            sec_i = int(sec)
        except Exception:
            continue
        if sec_i <= 0 or not isinstance(snap, dict):
            continue

        out_snap: Dict[str, Dict[str, float]] = {}
        for pod_id, metrics in snap.items():
            if not isinstance(metrics, dict):
                continue
            pod = str(pod_id or "").strip()
            if not pod:
                continue

            cpu = _clamp(float(metrics.get("cpu", metrics.get("cpu_usage", 0.0))), 0.0, 1.0)
            memory = _clamp(float(metrics.get("memory", metrics.get("memory_usage", 0.0))), 0.0, 1.0)
            latency_p99 = max(
                0.0,
                float(metrics.get("latency_p99", metrics.get("latency", 0.0))),
            )
            workload = max(0.0, float(metrics.get("workload", metrics.get("qps", 0.0))))
            node_pressure = _clamp(
                float(metrics.get("node_pressure", metrics.get("node", 0.0))),
                0.0,
                1.0,
            )
            success_rate = _clamp(
                float(metrics.get("success_rate", metrics.get("success_proxy", 1.0))),
                0.0,
                1.0,
            )
            error_proxy = _clamp(
                float(metrics.get("error_proxy", 1.0 - success_rate)),
                0.0,
                1.0,
            )

            out_snap[pod] = {
                "cpu": cpu,
                "memory": memory,
                "latency_p99": latency_p99,
                "workload": workload,
                "node_pressure": node_pressure,
                "success_rate": success_rate,
                "error_proxy": error_proxy,
            }

            per_pod_hist[pod]["cpu"].append(cpu)
            per_pod_hist[pod]["memory"].append(memory)
            per_pod_hist[pod]["latency_p99"].append(latency_p99)
            per_pod_hist[pod]["workload"].append(workload)
            per_pod_hist[pod]["node_pressure"].append(node_pressure)

        if out_snap:
            raw[sec_i] = out_snap

    if not raw:
        return {}

    pod_stats: Dict[str, Dict[str, Tuple[float, float]]] = defaultdict(dict)
    for pod, fields in per_pod_hist.items():
        for key, vals in fields.items():
            med = _median(vals)
            sigma = _robust_sigma(vals)
            pod_stats[pod][key] = (med, sigma)

    normalized: Dict[int, Dict[str, Dict[str, float]]] = {}
    for sec in sorted(raw.keys()):
        out_snap: Dict[str, Dict[str, float]] = {}
        for pod, metrics in raw[sec].items():
            stats = pod_stats.get(pod, {})

            def _z(metric_key: str) -> float:
                val = float(metrics.get(metric_key, 0.0))
                med, sig = stats.get(metric_key, (0.0, 1.0))
                return _clamp((val - med) / max(1e-6, sig), 0.0, 10.0)

            out_snap[pod] = {
                "cpu_z": _z("cpu"),
                "memory_z": _z("memory"),
                "latency_p99_z": _z("latency_p99"),
                "workload_z": _z("workload"),
                "node_pressure_z": _z("node_pressure"),
                "error_proxy": float(metrics.get("error_proxy", 0.0)),
                "success_rate": float(metrics.get("success_rate", 1.0)),
            }
        if out_snap:
            normalized[sec] = out_snap

    return normalized


def _build_metric_index(
    normalized_stream: MetricsStreamBySec,
) -> Tuple[Dict[str, List[int]], Dict[str, List[Dict[str, float]]]]:
    sec_index: Dict[str, List[int]] = defaultdict(list)
    snap_index: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for sec in sorted(normalized_stream.keys()):
        snap = normalized_stream[sec]
        for pod_id, metrics in snap.items():
            sec_index[pod_id].append(int(sec))
            snap_index[pod_id].append(metrics)
    return dict(sec_index), dict(snap_index)


def _lookup_nearest_snapshot(
    pod_id: str,
    trace_sec: int,
    sec_index: Dict[str, List[int]],
    snap_index: Dict[str, List[Dict[str, float]]],
    lookback_sec: int,
) -> Tuple[Optional[Dict[str, float]], float]:
    secs = sec_index.get(pod_id)
    snaps = snap_index.get(pod_id)
    if not secs or not snaps:
        return None, 0.0

    idx = bisect.bisect_right(secs, int(trace_sec)) - 1
    if idx < 0:
        return None, 0.0

    min_sec = int(trace_sec) - max(1, int(lookback_sec))
    while idx >= 0:
        sec = secs[idx]
        if sec < min_sec:
            break
        if sec <= int(trace_sec):
            dt = int(trace_sec) - sec
            align = math.exp(-float(dt) / float(max(1, lookback_sec)))
            return snaps[idx], _clamp(align, 0.0, 1.0)
        idx -= 1
    return None, 0.0


def _build_service_pod_registry(
    points: List[TracePoint],
    sec_index: Dict[str, List[int]],
) -> Dict[str, List[Dict[str, Any]]]:
    all_services: Set[str] = set()
    service_secs: Dict[str, Set[int]] = defaultdict(set)
    for p in points:
        sec = int(p.start_ns // 1_000_000_000)
        for svc in p.services:
            svc_norm = _normalize_name(str(svc))
            if not svc_norm:
                continue
            all_services.add(svc_norm)
            service_secs[svc_norm].add(sec)

    all_pods = sorted(str(pod) for pod in sec_index.keys())
    if not all_pods:
        return {svc: [] for svc in sorted(all_services)}

    pod_base: Dict[str, str] = {pod: _strip_pod_to_service(pod) for pod in all_pods}
    pod_tokens: Dict[str, Set[str]] = {pod: _name_tokens(pod_base[pod]) | _name_tokens(pod) for pod in all_pods}
    pod_secs: Dict[str, Set[int]] = {pod: set(int(sec) for sec in sec_index.get(pod, [])) for pod in all_pods}
    max_cov = max([len(v) for v in sec_index.values()] + [1])

    registry: Dict[str, List[Dict[str, Any]]] = {}
    for service in sorted(all_services):
        svc_tokens = _name_tokens(service)
        svc_secs = service_secs.get(service, set())
        candidates: List[Dict[str, Any]] = []

        for pod in all_pods:
            base = pod_base.get(pod, "")
            name_score = 0.0
            if base == service or _normalize_name(pod) == service:
                name_score = 1.0
            else:
                toks = pod_tokens.get(pod, set())
                union = len(svc_tokens | toks)
                token_overlap = (len(svc_tokens & toks) / float(union)) if union > 0 else 0.0
                substring = 1.0 if (service in base or base in service) and base else 0.0
                name_score = max(token_overlap, 0.75 * substring)

            psecs = pod_secs.get(pod, set())
            if svc_secs and psecs:
                overlap = len(svc_secs & psecs) / float(max(1, min(len(svc_secs), len(psecs))))
            else:
                overlap = 0.0

            coverage = len(sec_index.get(pod, [])) / float(max_cov)
            confidence = _clamp(0.45 * name_score + 0.35 * overlap + 0.20 * coverage, 0.0, 1.0)
            if (name_score > 0.05) or (overlap > 0.05):
                candidates.append(
                    {
                        "pod_id": pod,
                        "confidence_score": confidence,
                        "name_score": _clamp(name_score, 0.0, 1.0),
                        "temporal_overlap": _clamp(overlap, 0.0, 1.0),
                        "coverage": _clamp(coverage, 0.0, 1.0),
                    }
                )

        candidates.sort(
            key=lambda x: (
                float(x.get("confidence_score", 0.0)),
                float(x.get("name_score", 0.0)),
                float(x.get("temporal_overlap", 0.0)),
                float(x.get("coverage", 0.0)),
                str(x.get("pod_id", "")),
            ),
            reverse=True,
        )
        registry[service] = candidates[:8]

    return registry


def _compute_trace_metric_components(
    trace: TracePoint,
    trace_sec: int,
    service_pod_registry: Dict[str, List[Dict[str, Any]]],
    sec_index: Dict[str, List[int]],
    snap_index: Dict[str, List[Dict[str, float]]],
    lookback_sec: int,
) -> Tuple[float, float, float, float, int, int]:
    """Return (evidence, intensity, coverage, reliability, mapped_services, mismatch_count)."""
    if not trace.services:
        return 0.0, 0.0, 0.0, 0.0, 0, 0

    mapped = 0
    mismatch = 0
    svc_intensity: List[float] = []
    svc_reliability: List[float] = []

    for service in trace.services:
        service_norm = _normalize_name(str(service))
        candidates = service_pod_registry.get(service_norm, [])
        if not candidates:
            mismatch += 1
            continue

        pod_scores: List[float] = []
        pod_reliability: List[float] = []
        for cand in candidates:
            pod_id = str(cand.get("pod_id", ""))
            confidence = _clamp(float(cand.get("confidence_score", 0.0)), 0.0, 1.0)
            snap, temporal = _lookup_nearest_snapshot(
                pod_id=pod_id,
                trace_sec=trace_sec,
                sec_index=sec_index,
                snap_index=snap_index,
                lookback_sec=lookback_sec,
            )
            if snap is None:
                continue

            intensity = (
                0.24 * float(snap.get("cpu_z", 0.0))
                + 0.20 * float(snap.get("memory_z", 0.0))
                + 0.24 * float(snap.get("latency_p99_z", 0.0))
                + 0.12 * float(snap.get("workload_z", 0.0))
                + 0.12 * float(snap.get("node_pressure_z", 0.0))
                + 0.08 * float(snap.get("error_proxy", 0.0))
            )
            intensity = max(0.0, intensity)
            reliability = _clamp(confidence * temporal, 0.0, 1.0)
            if reliability > 0.0:
                pod_scores.append(intensity)
                pod_reliability.append(reliability)

        if not pod_scores:
            mismatch += 1
            continue

        mapped += 1
        svc_intensity.append(_topk_projection(pod_scores))
        svc_reliability.append(_topk_projection(pod_reliability))

    if mapped <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0, mismatch

    metric_intensity = _topk_projection(svc_intensity)
    metric_coverage = mapped / float(max(1, len(trace.services)))
    mapping_reliability = _mean(svc_reliability)
    metric_evidence = max(0.0, metric_intensity * metric_coverage)
    return (
        metric_evidence,
        metric_intensity,
        metric_coverage,
        mapping_reliability,
        mapped,
        mismatch,
    )


def _compute_signed_modulation(
    metric_evidence: float,
    metric_coverage: float,
    mapping_reliability: float,
    gain: float,
    tau: float,
    scale: float,
    suspicious_score: float,
    suspicious_threshold: float,
) -> Tuple[float, float]:
    signed_metric = _signed_metric_value(metric_evidence, tau, scale)
    delta_logit = (
        float(gain)
        * float(signed_metric)
        * _clamp(float(metric_coverage), 0.0, 1.0)
        * _clamp(float(mapping_reliability), 0.0, 1.0)
    )
    if float(suspicious_score) < float(suspicious_threshold):
        delta_logit *= 0.5
    delta_logit = _clamp(delta_logit, -0.75, 0.75)
    return signed_metric, delta_logit


def _trace_tokens(trace: TracePoint) -> FrozenSet[str]:
    def _dur_bucket(v: float) -> str:
        if v < 100.0:
            return "d0"
        if v < 500.0:
            return "d1"
        if v < 2000.0:
            return "d2"
        return "d3"

    def _span_bucket(v: int) -> str:
        if v <= 3:
            return "s0"
        if v <= 8:
            return "s1"
        if v <= 20:
            return "s2"
        return "s3"

    out = {"dur:" + _dur_bucket(float(trace.duration_ms)), "span:" + _span_bucket(int(trace.span_count))}
    for svc in trace.services:
        out.add("svc:" + str(svc))
    return frozenset(out)


def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    if union <= 0:
        return 0.0
    return len(a & b) / float(union)


def _rca_quadrant_value(has_error: bool, suspicious_hit: int, suspicious_ratio: float) -> float:
    if has_error and suspicious_hit > 0:
        return 0.80 + 0.10 * suspicious_ratio
    if (not has_error) and suspicious_hit == 0:
        return 0.50
    if has_error and suspicious_hit == 0:
        return 0.28
    return 0.40


def _compute_online_rca_state(
    points: List[TracePoint],
    budget_pct: float,
    incident_services: Set[str],
    high_error_service_threshold: float,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float], float]:
    traces = sorted(points, key=lambda p: (int(p.start_ns), str(p.trace_id)))
    svc_count: Dict[str, int] = defaultdict(int)
    svc_err: Dict[str, int] = defaultdict(int)

    min_hist = 4 if budget_pct <= 0.15 else 8
    if budget_pct <= 0.1:
        s_pct = 65.0
    elif budget_pct <= 1.0:
        s_pct = 55.0
    else:
        s_pct = 50.0

    seen_error_flags: List[float] = []
    suspicious_history: List[float] = []

    rca_value_by_id: Dict[str, float] = {}
    suspicious_score_by_id: Dict[str, float] = {}
    structural_rarity_by_id: Dict[str, float] = {}
    contrast_term_by_id: Dict[str, float] = {}

    suspicious_threshold = 0.0
    for p in traces:
        trace_services = {_normalize_name(str(svc)) for svc in p.services if _normalize_name(str(svc))}

        suspicious_services = set(_normalize_name(svc) for svc in incident_services if _normalize_name(svc))
        for svc, cnt in svc_count.items():
            if cnt > min_hist:
                err_rate = svc_err.get(svc, 0) / float(max(1, cnt))
                if err_rate > high_error_service_threshold:
                    suspicious_services.add(svc)

        hit = len(trace_services & suspicious_services)
        ratio = hit / float(max(1, len(trace_services)))
        suspicious_score_by_id[p.trace_id] = ratio
        rca_value_by_id[p.trace_id] = _rca_quadrant_value(bool(p.has_error), hit, ratio)

        rarity_vals = [1.0 / math.sqrt(float(max(1, svc_count.get(svc, 0) + 1))) for svc in trace_services]
        structural_rarity_by_id[p.trace_id] = _mean(rarity_vals)

        hist_err = _mean(seen_error_flags) if seen_error_flags else 0.0
        incident_hit = 1.0 if (trace_services & suspicious_services) else 0.0
        label_dist = abs((1.0 if p.has_error else 0.0) - hist_err)
        contrast_term_by_id[p.trace_id] = _clamp(0.65 * incident_hit + 0.35 * label_dist, 0.0, 1.0)

        suspicious_history.append(ratio)
        suspicious_threshold = _percentile(suspicious_history, s_pct)

        for svc in trace_services:
            svc_count[svc] += 1
            if p.has_error:
                svc_err[svc] += 1
        seen_error_flags.append(1.0 if p.has_error else 0.0)

    return (
        rca_value_by_id,
        suspicious_score_by_id,
        structural_rarity_by_id,
        contrast_term_by_id,
        suspicious_threshold,
    )


def _infer_incident_anchor_sec(points: List[TracePoint], incident_services: Set[str]) -> Optional[int]:
    if not points:
        return None
    candidates = [
        int(p.start_ns // 1_000_000_000)
        for p in points
        if p.has_error and bool(set(p.services) & incident_services)
    ]
    if not candidates:
        return None
    return int(min(candidates))


def _enforce_scenario_floor(
    selected_ids: Set[str],
    points: List[TracePoint],
    scenario_windows: List[Tuple[str, int, int]],
    target_count: int,
    floor_each: int,
    priority_key: Any,
) -> Set[str]:
    if not scenario_windows or floor_each <= 0:
        return set(selected_ids)

    by_id = {p.trace_id: p for p in points}
    start_sec = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}

    scenario_ids: List[Set[str]] = []
    membership: Dict[str, Set[int]] = defaultdict(set)
    non_empty = 0
    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, sec in start_sec.items() if int(st) <= sec <= int(ed)}
        scenario_ids.append(ids)
        if ids:
            non_empty += 1
            for tid in ids:
                membership[tid].add(idx)

    if non_empty <= 0:
        return set(selected_ids)

    if target_count < non_empty * floor_each:
        floor_each = 0
        if floor_each <= 0:
            return set(selected_ids)

    final_ids = set(selected_ids)

    for idx, ids in enumerate(scenario_ids):
        if not ids:
            continue
        present = [tid for tid in final_ids if tid in ids]
        if len(present) >= floor_each:
            continue
        need = floor_each - len(present)
        cand = [tid for tid in ids if tid not in final_ids and tid in by_id]
        cand.sort(key=priority_key, reverse=True)
        for tid in cand[:need]:
            final_ids.add(tid)

    if len(final_ids) <= target_count:
        return final_ids

    counts = [0] * len(scenario_ids)
    for tid in final_ids:
        for idx in membership.get(tid, set()):
            counts[idx] += 1

    def _can_remove(tid: str) -> bool:
        for idx in membership.get(tid, set()):
            if counts[idx] <= floor_each:
                return False
        return True

    removable = sorted(final_ids, key=priority_key)
    for tid in removable:
        if len(final_ids) <= target_count:
            break
        if not _can_remove(tid):
            continue
        final_ids.remove(tid)
        for idx in membership.get(tid, set()):
            counts[idx] -= 1

    if len(final_ids) > target_count:
        ordered = sorted(final_ids, key=priority_key, reverse=True)
        final_ids = set(ordered[:target_count])
    return final_ids


def _run_member_sampler(
    points: List[TracePoint],
    budget_pct: float,
    incident_services: Set[str],
    scenario_windows: List[Tuple[str, int, int]],
    min_incident_traces_per_scenario: int,
    online_soft_cap: bool,
    incident_anchor_sec: Optional[int],
    seed: int,
    lookback_sec: int,
    alpha: float,
    gamma: float,
    rca_weight: float,
    high_error_service_threshold: float,
    delta_logit_by_id: Dict[str, float],
    inner_metric_distress_by_id: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    total = len(points)
    if total <= 0:
        return {
            "selected_ids": set(),
            "score_by_id": {},
            "rca_value_by_id": {},
            "suspicious_score_by_id": {},
            "structural_rarity_by_id": {},
        }

    beta = _clamp(float(budget_pct) / 100.0, 0.0, 1.0)
    target_count = max(1, min(total, int(round(total * beta))))
    traces = sorted(points, key=lambda p: (int(p.start_ns), str(p.trace_id)))

    t0_sec = int(min(p.start_ns for p in traces) // 1_000_000_000)
    dur_lo = min(float(p.duration_ms) for p in traces)
    dur_hi = max(float(p.duration_ms) for p in traces)
    span_lo = min(int(p.span_count) for p in traces)
    span_hi = max(int(p.span_count) for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    rng = random.Random(int(seed))
    seen = 0
    kept_ids: Set[str] = set()

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_count: Dict[str, int] = defaultdict(int)
    min_susp_hist = 4 if beta <= 0.0015 else 8

    seen_win: Deque[Tuple[int, float, float, float]] = deque()
    kept_win: Deque[Tuple[int, bool]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: Deque[Tuple[int, Tuple[str, ...]]] = deque()

    score_by_id: Dict[str, float] = {}
    p_s_final_by_id: Dict[str, float] = {}
    rca_value_by_id: Dict[str, float] = {}
    suspicious_score_by_id: Dict[str, float] = {}
    structural_rarity_by_id: Dict[str, float] = {}

    service_occurrence: Dict[str, int] = defaultdict(int)
    for p in traces:
        for svc in p.services:
            service_occurrence[svc] += 1

    for p in traces:
        seen += 1
        sec_rel = int(p.start_ns // 1_000_000_000) - t0_sec
        while seen_win and seen_win[0][0] < sec_rel - lookback_sec + 1:
            seen_win.popleft()
        while kept_win and kept_win[0][0] < sec_rel - lookback_sec + 1:
            kept_win.popleft()
        while cluster_win and cluster_win[0][0] < sec_rel - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        lat_pressure = 0.7 * _norm(float(p.duration_ms), dur_lo, dur_hi) + 0.3 * _norm(
            float(p.span_count), float(span_lo), float(span_hi)
        )
        incident_hit = 1.0 if bool(set(p.services) & incident_services) else 0.0
        err_flag = 1.0 if p.has_error else 0.0

        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            sec_abs = int(p.start_ns // 1_000_000_000)
            time_prox = math.exp(-abs(float(sec_abs - incident_anchor_sec)) / 60.0)

        dynamic_suspicious = set(incident_services)
        for svc in p.services:
            if svc_trace_count[svc] > min_susp_hist:
                err_rate = svc_error_count[svc] / float(max(1, svc_trace_count[svc]))
                if err_rate > high_error_service_threshold:
                    dynamic_suspicious.add(svc)

        suspicious_hit = len(set(p.services) & dynamic_suspicious)
        suspicious_ratio = suspicious_hit / float(max(1, len(p.services)))
        suspicious_score_by_id[p.trace_id] = suspicious_ratio
        rca_value = _rca_quadrant_value(bool(p.has_error), suspicious_hit, suspicious_ratio)
        rca_value_by_id[p.trace_id] = rca_value

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        z_err = abs(err_flag - _mean(hist_err)) / _robust_sigma(hist_err)
        z_lat = abs(lat_pressure - _mean(hist_lat)) / _robust_sigma(hist_lat)
        z_inc = abs(incident_hit - _mean(hist_inc)) / _robust_sigma(hist_inc)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc

        alpha_eff = float(alpha) * (0.90 if beta >= 0.01 else 1.00)
        base_sys = 0.45 * incident_hit + 0.25 * lat_pressure + 0.15 * err_flag + 0.15 * time_prox
        shock = math.tanh(alpha_eff * z_mix)
        p_s = _clamp(0.55 * base_sys + 0.45 * shock, 0.02, 0.98)

        if inner_metric_distress_by_id is not None:
            distress = _clamp(float(inner_metric_distress_by_id.get(p.trace_id, 0.0)), 0.0, 1.0)
            p_s = _clamp(p_s + 0.35 * distress * (1.0 - p_s), 0.02, 0.98)
            if distress > 0.75:
                p_s = max(p_s, 0.75)

        rca_boost = _clamp(float(rca_weight) * (rca_value - 0.45), -0.12, 0.20)
        p_s = _clamp(p_s + rca_boost, 0.02, 0.98)

        delta_logit = float(delta_logit_by_id.get(p.trace_id, 0.0))
        p_s = _clamp(_sigmoid(_logit(p_s) + delta_logit), 0.02, 0.98)

        incident_floor = 0.0
        if incident_hit > 0.0:
            if (err_flag > 0.0) and (time_prox >= 0.55):
                incident_floor = 0.62
            elif (time_prox >= 0.78) and (lat_pressure >= 0.58):
                incident_floor = 0.56
            elif (time_prox >= 0.88) and (lat_pressure >= 0.50):
                incident_floor = 0.50
        p_s = max(p_s, incident_floor)

        toks = _trace_tokens(p)
        best_key = None
        best_sim = 0.0
        for key, tmpl in cluster_templates.items():
            sim = _jaccard(toks, tmpl)
            if sim > best_sim:
                best_sim = sim
                best_key = key

        if best_key is not None and best_sim >= 0.5:
            assigned_key = best_key
            mass = float(cluster_counts.get(assigned_key, 0)) * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            mass = 0.0

        if incident_hit > 0.0:
            mass *= 0.35
            gamma_eff = max(0.35, float(gamma) * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = float(gamma)
            pd_floor = 0.05

        p_d = _clamp((1.0 + mass) ** (-gamma_eff), pd_floor, 1.0)

        theta = len(kept_ids) / float(max(1, seen))
        if beta <= 0.0015:
            ps_or_floor = 0.12
        elif beta <= 0.01:
            ps_or_floor = 0.20
        else:
            ps_or_floor = 0.30
        if incident_hit > 0.0 and time_prox >= 0.55:
            ps_or_floor = max(0.08, ps_or_floor - 0.06)

        kept_err_hist = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio = _mean(kept_err_hist)
        kept_norm_ratio = 1.0 - kept_err_ratio

        is_high_conf = (rca_value >= 0.80) and (p_s >= 0.55)
        u = rng.random()
        if theta > beta:
            if is_high_conf:
                keep = u < p_s
            else:
                keep = (u < p_s) and (u < p_d)
        else:
            rare_noise = (p_d >= 0.72) and (p_s < ps_or_floor)
            keep = False if rare_noise else (u < p_s)

            is_clean_normal = (
                (not p.has_error)
                and incident_hit <= 0.0
                and (p_s < ps_or_floor)
                and (p_d <= 0.45)
            )
            if (not keep) and is_clean_normal and (kept_norm_ratio < 0.30):
                if rng.random() < beta:
                    keep = True
            if (not keep) and online_soft_cap and is_clean_normal:
                gap = _clamp(beta - theta, 0.0, 0.20)
                if rng.random() < gap:
                    keep = True

        for svc in p.services:
            svc_trace_count[svc] += 1
            if p.has_error:
                svc_error_count[svc] += 1

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec_rel, bool(p.has_error)))

        p_s_final_by_id[p.trace_id] = p_s
        score_by_id[p.trace_id] = 0.50 * p_s + 0.20 * p_d + 0.30 * rca_value
        structural_rarity_by_id[p.trace_id] = _mean(
            [1.0 / math.sqrt(float(max(1, service_occurrence.get(svc, 1)))) for svc in p.services]
        )

        seen_win.append((sec_rel, err_flag, lat_pressure, incident_hit))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec_rel, assigned_key))

    by_id = {p.trace_id: p for p in traces}

    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / float(max(1, total))
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.4), 0.08, 0.40)
        need_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < need_err:
            deficit = need_err - len(kept_err)
            cand_add = [tid for tid, tr in by_id.items() if (tid not in kept_ids) and tr.has_error]
            cand_add.sort(
                key=lambda tid: (rca_value_by_id.get(tid, 0.0), score_by_id.get(tid, 0.0)),
                reverse=True,
            )
            add_ids = cand_add[:deficit]
            if add_ids:
                cand_drop = [tid for tid in kept_ids if by_id.get(tid) and (not by_id[tid].has_error)]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    kept_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    kept_ids.add(tid)

    def _priority(tid: str) -> Tuple[float, float, float, float, str]:
        tr = by_id.get(tid)
        if tr is None:
            return (0.0, 0.0, 0.0, 0.0, tid)
        return (
            rca_value_by_id.get(tid, 0.0),
            1.0 if tr.has_error else 0.0,
            float(tr.duration_ms),
            float(tr.span_count),
            tid,
        )

    if len(kept_ids) < target_count:
        outside = [tid for tid in by_id.keys() if tid not in kept_ids]
        outside.sort(key=_priority, reverse=True)
        for tid in outside:
            if len(kept_ids) >= target_count:
                break
            kept_ids.add(tid)

    if len(kept_ids) > target_count:
        ranked = sorted(kept_ids, key=lambda tid: (
            -rca_value_by_id.get(tid, 0.0),
            -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
            -p_s_final_by_id.get(tid, 0.0),
            tid,
        ))
        kept_ids = set(ranked[:target_count])

    kept_ids = _enforce_scenario_floor(
        selected_ids=kept_ids,
        points=traces,
        scenario_windows=scenario_windows,
        target_count=target_count,
        floor_each=max(0, int(min_incident_traces_per_scenario)),
        priority_key=_priority,
    )

    if len(kept_ids) < target_count:
        outside = [tid for tid in by_id.keys() if tid not in kept_ids]
        outside.sort(key=_priority, reverse=True)
        for tid in outside:
            if len(kept_ids) >= target_count:
                break
            kept_ids.add(tid)
    elif len(kept_ids) > target_count:
        ranked = sorted(kept_ids, key=_priority, reverse=True)
        kept_ids = set(ranked[:target_count])

    return {
        "selected_ids": kept_ids,
        "score_by_id": score_by_id,
        "rca_value_by_id": rca_value_by_id,
        "suspicious_score_by_id": suspicious_score_by_id,
        "structural_rarity_by_id": structural_rarity_by_id,
    }


def _default_signed_metric_gain_for_budget(budget_pct: float) -> float:
    if budget_pct <= 0.1:
        return 0.45
    if budget_pct <= 1.0:
        return 0.60
    return 0.75


def _resolve_signed_metric_gain(signed_metric_gain: Optional[float], budget_pct: float) -> float:
    if signed_metric_gain is None:
        return _default_signed_metric_gain_for_budget(budget_pct)
    return float(signed_metric_gain)


def _default_head_k_for_budget(budget_pct: float, target_count: int) -> int:
    if budget_pct <= 1.0:
        return min(target_count, 1)
    return min(target_count, 2)


def _build_head_score(
    vote_count: int,
    member_count: int,
    rca_value: float,
    signed_metric: float,
    incident_alignment: float,
) -> float:
    vote_frac = float(vote_count) / float(max(1, member_count))
    return (
        0.40 * vote_frac
        + 0.30 * float(rca_value)
        + 0.20 * max(0.0, float(signed_metric))
        + 0.10 * float(_clamp(incident_alignment, 0.0, 1.0))
    )


def _build_consensus_score(
    trace_id: str,
    budget_pct: float,
    vote_count: int,
    member_count: int,
    member_score: float,
    rca_value: float,
    metric_evidence: float,
    structural_rarity: float,
    contrast_term: float,
) -> float:

    if budget_pct <= 0.1:
        w_vote, w_rca, w_metric, w_div = 0.46, 0.28, 0.18, 0.08
    elif budget_pct <= 1.0:
        w_vote, w_rca, w_metric, w_div = 0.42, 0.28, 0.18, 0.12
    else:
        w_vote, w_rca, w_metric, w_div = 0.38, 0.26, 0.18, 0.18

    vote_frac = float(vote_count) / float(max(1, member_count))
    rca_mix = 0.65 * float(rca_value) + 0.35 * _clamp(float(member_score), 0.0, 1.0)
    diversity_mix = 0.60 * float(structural_rarity) + 0.40 * float(contrast_term)
    metric_norm = _clamp(_sigmoid(float(metric_evidence) - 1.0), 0.0, 1.0)

    return (
        w_vote * vote_frac
        + w_rca * rca_mix
        + w_metric * metric_norm
        + w_div * diversity_mix
    )

def _apply_dual_contrast_guard(
    global_ranked_ids: List[str],
    by_id: Dict[str, TracePoint],
    target_count: int,
    min_normal_ratio: float,
    min_error_ratio: float,
) -> List[str]:
    if target_count <= 0:
        return []

    normal_ranked = [tid for tid in global_ranked_ids if tid in by_id and (not by_id[tid].has_error)]
    error_ranked = [tid for tid in global_ranked_ids if tid in by_id and by_id[tid].has_error]

    min_normal = min(len(normal_ranked), int(round(target_count * _clamp(min_normal_ratio, 0.0, 1.0))))
    min_error = min(len(error_ranked), int(round(target_count * _clamp(min_error_ratio, 0.0, 1.0))))

    if min_normal + min_error > target_count:
        overflow = min_normal + min_error - target_count
        drop_err = min(overflow, min_error)
        min_error -= drop_err
        overflow -= drop_err
        if overflow > 0:
            min_normal = max(0, min_normal - overflow)

    guaranteed: List[str] = []
    guaranteed_set: Set[str] = set()

    for tid in normal_ranked[:min_normal]:
        if tid not in guaranteed_set:
            guaranteed.append(tid)
            guaranteed_set.add(tid)

    for tid in error_ranked[:min_error]:
        if tid not in guaranteed_set:
            guaranteed.append(tid)
            guaranteed_set.add(tid)

    selected: List[str] = []
    selected_set: Set[str] = set()

    for tid in global_ranked_ids:
        if tid in guaranteed_set and tid not in selected_set:
            selected.append(tid)
            selected_set.add(tid)

    for tid in global_ranked_ids:
        if len(selected) >= target_count:
            break
        if tid in selected_set:
            continue
        selected.append(tid)
        selected_set.add(tid)

    return selected[:target_count]


def _emit_telemetry(
    debug_trace_logs: Optional[List[Dict[str, Any]]],
    payload: Dict[str, Any],
) -> None:
    if debug_trace_logs is None:
        return
    debug_trace_logs.append(payload)

def build_v3942_metric_context(
    points: List[TracePoint],
    metrics_stream_by_sec: Optional[MetricsStreamBySec],
    metric_lookback_sec: int = 120,
) -> V3942MetricContext:
    normalized_metrics = _normalize_metric_stream(metrics_stream_by_sec)
    sec_index, snap_index = _build_metric_index(normalized_metrics)
    registry = _build_service_pod_registry(points, sec_index)

    metric_evidence_by_id: Dict[str, float] = {}
    metric_intensity_by_id: Dict[str, float] = {}
    metric_coverage_by_id: Dict[str, float] = {}
    mapping_reliability_by_id: Dict[str, float] = {}
    mismatch_count = 0

    for p in points:
        trace_sec = int(p.start_ns // 1_000_000_000)
        evidence, intensity, coverage, reliability, _mapped, mismatches = _compute_trace_metric_components(
            trace=p,
            trace_sec=trace_sec,
            service_pod_registry=registry,
            sec_index=sec_index,
            snap_index=snap_index,
            lookback_sec=metric_lookback_sec,
        )
        metric_evidence_by_id[p.trace_id] = evidence
        metric_intensity_by_id[p.trace_id] = intensity
        metric_coverage_by_id[p.trace_id] = coverage
        mapping_reliability_by_id[p.trace_id] = reliability
        mismatch_count += mismatches

    return V3942MetricContext(
        metric_evidence_by_id=metric_evidence_by_id,
        metric_intensity_by_id=metric_intensity_by_id,
        metric_coverage_by_id=metric_coverage_by_id,
        mapping_reliability_by_id=mapping_reliability_by_id,
        mismatch_count=mismatch_count,
    )
def _run_v3942_pipeline(
    points: List[TracePoint],
    budget_pct: float,
    incident_services: Optional[Set[str]],
    metrics_stream_by_sec: Optional[Dict[int, Dict[str, Dict[str, float]]]],
    seed: int,
    debug_trace_logs: Optional[List[Dict[str, Any]]],
    scenario_windows: Optional[List[Tuple[str, int, int]]] = None,
    min_incident_traces_per_scenario: int = 1,
    online_soft_cap: bool = False,
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
    rca_weight: float = 0.40,
    high_error_service_threshold: float = 0.15,
    metric_lookback_sec: int = 120,
    min_consensus: int = 2,
    enable_swap_refine: bool = False,
    enable_inner_real_metrics: bool = False,
    min_normal_ratio_override: Optional[float] = None,
    min_error_ratio_override: Optional[float] = None,
    signed_metric_gain: Optional[float] = None,
    metric_tau_mode: str = "budget-percentile",
    metric_scale_mode: str = "robust",
    head_k_override: Optional[int] = None,
    precomputed_metric_context: Optional[V3942MetricContext] = None,
) -> Set[str]:
    if not points:
        return set()

    total = len(points)
    beta = _clamp(float(budget_pct) / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    target_count = max(1, min(total, int(round(total * beta))))

    by_id = {p.trace_id: p for p in points}
    incident_raw = set(incident_services or set())

    if precomputed_metric_context is None:
        metric_context = build_v3942_metric_context(
            points=points,
            metrics_stream_by_sec=metrics_stream_by_sec,
            metric_lookback_sec=metric_lookback_sec,
        )
    else:
        metric_context = precomputed_metric_context

    metric_evidence_by_id = metric_context.metric_evidence_by_id
    metric_intensity_by_id = metric_context.metric_intensity_by_id
    metric_coverage_by_id = metric_context.metric_coverage_by_id
    mapping_reliability_by_id = metric_context.mapping_reliability_by_id
    mismatch_count = metric_context.mismatch_count

    (
        online_rca_value_by_id,
        online_suspicious_score_by_id,
        online_structural_rarity_by_id,
        online_contrast_term_by_id,
        suspicious_threshold,
    ) = _compute_online_rca_state(
        points=points,
        budget_pct=float(budget_pct),
        incident_services=incident_raw,
        high_error_service_threshold=float(high_error_service_threshold),
    )
 
    evidence_values = list(metric_evidence_by_id.values())
    tau = _resolve_tau(evidence_values, budget_pct=float(budget_pct), mode=metric_tau_mode)
    scale = _resolve_scale(evidence_values, mode=metric_scale_mode)
    signed_gain = _resolve_signed_metric_gain(signed_metric_gain, float(budget_pct))

    signed_metric_by_id: Dict[str, float] = {}
    delta_logit_by_id: Dict[str, float] = {}
    for p in points:
        tid = p.trace_id
        signed_metric, delta_logit = _compute_signed_modulation(
            metric_evidence=float(metric_evidence_by_id.get(tid, 0.0)),
            metric_coverage=float(metric_coverage_by_id.get(tid, 0.0)),
            mapping_reliability=float(mapping_reliability_by_id.get(tid, 0.0)),
            gain=float(signed_gain),
            tau=float(tau),
            scale=float(scale),
            suspicious_score=float(online_suspicious_score_by_id.get(tid, 0.0)),
            suspicious_threshold=float(suspicious_threshold),
        )
        signed_metric_by_id[tid] = signed_metric
        delta_logit_by_id[tid] = delta_logit

    inner_metric_distress_by_id: Optional[Dict[str, float]] = None
    if enable_inner_real_metrics:
        inner_metric_distress_by_id = {
            tid: _clamp(_sigmoid(float(metric_intensity_by_id.get(tid, 0.0)) - 1.0), 0.0, 1.0)
            for tid in by_id.keys()
        }

    if scenario_windows is None:
        scenario_windows = []
    incident_anchor_sec = _infer_incident_anchor_sec(points, incident_raw)

    rng = random.Random(int(seed))
    member_seeds = [s + int(rng.randint(0, 10_000)) for s in _ENSEMBLE_SEEDS]

    vote_count_by_id: Dict[str, int] = defaultdict(int)
    member_score_acc: Dict[str, List[float]] = defaultdict(list)
    member_sizes: List[int] = []

    for run_seed in member_seeds:
        out = _run_member_sampler(
            points=points,
            budget_pct=float(budget_pct),
            incident_services=incident_raw,
            scenario_windows=scenario_windows,
            min_incident_traces_per_scenario=int(min_incident_traces_per_scenario),
            online_soft_cap=bool(online_soft_cap),
            incident_anchor_sec=incident_anchor_sec,
            seed=int(run_seed),
            lookback_sec=int(lookback_sec),
            alpha=float(alpha),
            gamma=float(gamma),
            rca_weight=float(rca_weight),
            high_error_service_threshold=float(high_error_service_threshold),
            delta_logit_by_id=delta_logit_by_id,
            inner_metric_distress_by_id=inner_metric_distress_by_id,
        )
        selected_ids = set(out.get("selected_ids", set()))
        score_by_id = dict(out.get("score_by_id", {}))
        member_sizes.append(len(selected_ids))
        for tid in selected_ids:
            vote_count_by_id[tid] += 1
        for tid, score in score_by_id.items():
            member_score_acc[tid].append(float(score))

    member_score_mean = {tid: _mean(vals) for tid, vals in member_score_acc.items()}

    consensus_scores: Dict[str, float] = {}
    all_trace_ids = [p.trace_id for p in points]
    member_count = max(1, len(member_seeds))
    for tid in all_trace_ids:
        consensus_scores[tid] = _build_consensus_score(
            trace_id=tid,
            budget_pct=float(budget_pct),
            vote_count=int(vote_count_by_id.get(tid, 0)),
            member_count=member_count,
            member_score=float(member_score_mean.get(tid, 0.0)),
            rca_value=float(online_rca_value_by_id.get(tid, 0.0)),
            metric_evidence=float(metric_evidence_by_id.get(tid, 0.0)),
            structural_rarity=float(online_structural_rarity_by_id.get(tid, 0.0)),
            contrast_term=float(online_contrast_term_by_id.get(tid, 0.0)),
        )

    min_consensus_eff = max(1, int(min_consensus))
    vote_pool = [tid for tid in all_trace_ids if int(vote_count_by_id.get(tid, 0)) > 0]
    consensus_pool = [tid for tid in all_trace_ids if int(vote_count_by_id.get(tid, 0)) >= min_consensus_eff]
    remainder_pool = [tid for tid in all_trace_ids if 0 < int(vote_count_by_id.get(tid, 0)) < min_consensus_eff]
    outside_pool = [tid for tid in all_trace_ids if int(vote_count_by_id.get(tid, 0)) <= 0]

    incident_alignment_by_id: Dict[str, float] = {}
    for tid, tr in by_id.items():
        incident_alignment_by_id[tid] = 1.0 if bool(set(tr.services) & incident_raw) else 0.0

    head_scores: Dict[str, float] = {}
    for tid in all_trace_ids:
        head_scores[tid] = _build_head_score(
            vote_count=int(vote_count_by_id.get(tid, 0)),
            member_count=member_count,
            rca_value=float(online_rca_value_by_id.get(tid, 0.0)),
            signed_metric=float(signed_metric_by_id.get(tid, 0.0)),
            incident_alignment=float(incident_alignment_by_id.get(tid, 0.0)),
        )

    def _head_key(tid: str) -> Tuple[float, float, float, str]:
        return (
            float(head_scores.get(tid, 0.0)),
            float(consensus_scores.get(tid, 0.0)),
            float(vote_count_by_id.get(tid, 0)),
            tid,
        )

    def _tail_key(tid: str) -> Tuple[float, float, float, str]:
        return (
            float(consensus_scores.get(tid, 0.0)),
            float(vote_count_by_id.get(tid, 0)),
            float(head_scores.get(tid, 0.0)),
            tid,
        )

    consensus_head_ranked = sorted(consensus_pool, key=_head_key, reverse=True)
    remainder_head_ranked = sorted(remainder_pool, key=_head_key, reverse=True)
    outside_head_ranked = sorted(outside_pool, key=_head_key, reverse=True)

    if head_k_override is None:
        head_k = _default_head_k_for_budget(float(budget_pct), target_count)
    else:
        head_k = max(0, min(target_count, int(head_k_override)))

    head_selected: List[str] = []
    head_selected_set: Set[str] = set()
    head_from_consensus_count = 0
    head_from_remainder_count = 0
    head_from_outside_count = 0

    def _append_head(items: List[str], src: str) -> None:
        nonlocal head_from_consensus_count, head_from_remainder_count, head_from_outside_count
        for tid in items:
            if len(head_selected) >= head_k:
                break
            if tid in head_selected_set:
                continue
            head_selected.append(tid)
            head_selected_set.add(tid)
            if src == "consensus":
                head_from_consensus_count += 1
            elif src == "remainder":
                head_from_remainder_count += 1
            else:
                head_from_outside_count += 1

    _append_head(consensus_head_ranked, "consensus")
    if len(head_selected) < head_k:
        _append_head(remainder_head_ranked, "remainder")
    if len(head_selected) < head_k:
        _append_head(outside_head_ranked, "outside")

    def_normal, def_error = _default_dual_floors(float(budget_pct))
    min_normal_ratio = def_normal if min_normal_ratio_override is None else float(min_normal_ratio_override)
    min_error_ratio = def_error if min_error_ratio_override is None else float(min_error_ratio_override)
    min_normal_ratio = _clamp(min_normal_ratio, 0.0, 1.0)
    min_error_ratio = _clamp(min_error_ratio, 0.0, 1.0)
    if min_normal_ratio + min_error_ratio > 1.0:
        s = max(1e-6, min_normal_ratio + min_error_ratio)
        min_normal_ratio /= s
        min_error_ratio /= s

    tail_target = max(0, target_count - len(head_selected))
    remaining_consensus = [tid for tid in consensus_pool if tid not in head_selected_set]
    remaining_remainder = [tid for tid in remainder_pool if tid not in head_selected_set]
    remaining_outside = [tid for tid in outside_pool if tid not in head_selected_set]

    consensus_tail_ranked = sorted(remaining_consensus, key=_tail_key, reverse=True)
    remainder_tail_ranked = sorted(remaining_remainder, key=_tail_key, reverse=True)
    outside_tail_ranked = sorted(remaining_outside, key=_tail_key, reverse=True)

    tail_candidates = consensus_tail_ranked + remainder_tail_ranked + outside_tail_ranked
    pre_tail = tail_candidates[:tail_target]
    pre_guard_normal = sum(1 for tid in pre_tail if tid in by_id and (not by_id[tid].has_error))
    pre_guard_error = len(pre_tail) - pre_guard_normal

    tail_selected = _apply_dual_contrast_guard(
        global_ranked_ids=tail_candidates,
        by_id=by_id,
        target_count=tail_target,
        min_normal_ratio=min_normal_ratio,
        min_error_ratio=min_error_ratio,
    )

    tail_set = set(tail_selected)
    if enable_swap_refine and tail_target > 0:
        swap_quota = min(int(round(tail_target * 0.15)), int(round(tail_target * 0.10)))
        if swap_quota > 0:
            inside_low = sorted(tail_set, key=_tail_key)
            outside_high = [tid for tid in tail_candidates if tid not in tail_set][:swap_quota]
            for drop_tid, add_tid in zip(inside_low[:swap_quota], outside_high):
                if _tail_key(add_tid) > _tail_key(drop_tid):
                    tail_set.discard(drop_tid)
                    tail_set.add(add_tid)
            tail_selected = [tid for tid in tail_candidates if tid in tail_set]

    # Safety: keep head frozen, then re-apply tail guard after potential refine/backfill.
    tail_reapply_order = tail_selected + [tid for tid in tail_candidates if tid not in set(tail_selected)]
    tail_selected = _apply_dual_contrast_guard(
        global_ranked_ids=tail_reapply_order,
        by_id=by_id,
        target_count=tail_target,
        min_normal_ratio=min_normal_ratio,
        min_error_ratio=min_error_ratio,
    )

    tail_set = set(tail_selected)
    if len(tail_set) > tail_target:
        tail_selected = sorted(tail_set, key=_tail_key, reverse=True)[:tail_target]
        tail_set = set(tail_selected)
    elif len(tail_set) < tail_target:
        for tid in tail_candidates:
            if len(tail_set) >= tail_target:
                break
            if tid in tail_set:
                continue
            tail_selected.append(tid)
            tail_set.add(tid)

    final_ranked = list(head_selected)
    final_seen: Set[str] = set(final_ranked)
    for tid in tail_selected:
        if tid in final_seen:
            continue
        if len(final_ranked) >= target_count:
            break
        final_ranked.append(tid)
        final_seen.add(tid)

    if len(final_ranked) < target_count:
        for tid in tail_candidates:
            if len(final_ranked) >= target_count:
                break
            if tid in final_seen:
                continue
            final_ranked.append(tid)
            final_seen.add(tid)

    selected_set = set(final_ranked[:target_count])
    final_ranked = [tid for tid in final_ranked if tid in selected_set][:target_count]

    post_guard_normal = sum(1 for tid in tail_selected if tid in by_id and (not by_id[tid].has_error))
    post_guard_error = len(tail_selected) - post_guard_normal
    selected_normal = sum(1 for tid in selected_set if tid in by_id and (not by_id[tid].has_error))
    selected_error = len(selected_set) - selected_normal

    selected_from_consensus_count = sum(1 for tid in selected_set if tid in set(consensus_pool))
    selected_from_remainder_count = sum(1 for tid in selected_set if tid in set(remainder_pool))
    selected_from_outside_count = sum(1 for tid in selected_set if tid in set(outside_pool))

    signed_vals = list(signed_metric_by_id.values())
    delta_vals = list(delta_logit_by_id.values())

    _emit_telemetry(
        debug_trace_logs=debug_trace_logs,
        payload={
            "version": "v394.2-metric-native-signed-contrast-self-contained",
            "target_count": int(target_count),
            "selected_count": int(len(selected_set)),
            "head_k": int(head_k),
            "tail_target_count": int(tail_target),
            "vote_pool_size": int(len(vote_pool)),
            "consensus_pool_size": int(len(consensus_pool)),
            "remainder_pool_size": int(len(remainder_pool)),
            "outside_pool_size": int(len(outside_pool)),
            "selected_from_consensus_count": int(selected_from_consensus_count),
            "selected_from_remainder_count": int(selected_from_remainder_count),
            "selected_from_outside_count": int(selected_from_outside_count),
            "head_from_consensus_count": int(head_from_consensus_count),
            "head_from_remainder_count": int(head_from_remainder_count),
            "head_from_outside_count": int(head_from_outside_count),
            "pre_guard_normal_count": int(pre_guard_normal),
            "pre_guard_error_count": int(pre_guard_error),
            "post_guard_normal_count": int(post_guard_normal),
            "post_guard_error_count": int(post_guard_error),
            "post_guard_normal_ratio": float(post_guard_normal) / float(max(1, len(tail_selected))),
            "post_guard_error_ratio": float(post_guard_error) / float(max(1, len(tail_selected))),
            "selected_normal_count": int(selected_normal),
            "selected_error_count": int(selected_error),
            "selected_normal_ratio": float(selected_normal) / float(max(1, len(selected_set))),
            "selected_error_ratio": float(selected_error) / float(max(1, len(selected_set))),
            "metric_tau": float(tau),
            "metric_scale": float(scale),
            "signed_metric_gain_effective": float(signed_gain),
            "signed_metric_mean": _mean(signed_vals),
            "signed_metric_std": _std(signed_vals),
            "delta_logit_mean": _mean(delta_vals),
            "delta_logit_min": min(delta_vals) if delta_vals else 0.0,
            "delta_logit_max": max(delta_vals) if delta_vals else 0.0,
            "delta_logit_positive_count": int(sum(1 for x in delta_vals if x > 0.0)),
            "delta_logit_negative_count": int(sum(1 for x in delta_vals if x < 0.0)),
            "metric_evidence_mean": _mean(list(metric_evidence_by_id.values())),
            "metric_intensity_mean": _mean(list(metric_intensity_by_id.values())),
            "metric_coverage_mean": _mean(list(metric_coverage_by_id.values())),
            "mapping_reliability_mean": _mean(list(mapping_reliability_by_id.values())),
            "mapping_reliability_std": _std(list(mapping_reliability_by_id.values())),
            "metric_mismatch_count": int(mismatch_count),
            "member_size_mean": _mean([float(x) for x in member_sizes]),
            "member_size_std": _std([float(x) for x in member_sizes]),
            "min_consensus": int(min_consensus_eff),
            "enable_inner_real_metrics": bool(enable_inner_real_metrics),
        },
    )

    return selected_set


def run_v3942_sampler(
    points: List[TracePoint],
    budget_pct: float,
    incident_services: Optional[Set[str]] = None,
    metrics_stream_by_sec: Optional[Dict[int, Dict[str, Dict[str, float]]]] = None,
    seed: int = 42,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
    precomputed_metric_context: Optional[V3942MetricContext] = None,
) -> Set[str]:
    """Mandatory public entrypoint for full v394.2 pipeline."""
    return _run_v3942_pipeline(
        points=points,
        budget_pct=budget_pct,
        incident_services=incident_services,
        metrics_stream_by_sec=metrics_stream_by_sec,
        seed=seed,
        debug_trace_logs=debug_trace_logs,
        scenario_windows=[],
        min_incident_traces_per_scenario=1,
        online_soft_cap=False,
        lookback_sec=60,
        alpha=1.2,
        gamma=0.8,
        rca_weight=0.40,
        high_error_service_threshold=0.15,
        metric_lookback_sec=120,
        min_consensus=2,
        enable_swap_refine=False,
        enable_inner_real_metrics=False,
        min_normal_ratio_override=None,
        min_error_ratio_override=None,
        signed_metric_gain=None,
        metric_tau_mode="budget-percentile",
        metric_scale_mode="robust",
        precomputed_metric_context=precomputed_metric_context,
    )


def _composite_v3942_metric_native_signed_contrast(
    points: List[TracePoint],
    budget_pct: float,
    preference_vector: PreferenceVector,
    scenario_windows: List[Tuple[str, int, int]],
    incident_anchor_sec: Optional[int],
    seed: int,
    metric_stats: Optional[Dict[str, Tuple[float, float]]] = None,
    incident_services: Optional[Set[str]] = None,
    min_incident_traces_per_scenario: int = 1,
    online_soft_cap: bool = False,
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
    anomaly_weight: float = 0.7,
    rca_weight: float = 0.40,
    high_error_service_threshold: float = 0.15,
    metrics_stream_by_sec: Optional[MetricsStreamBySec] = None,
    metric_lookback_sec: int = 120,
    min_consensus: int = 2,
    enable_swap_refine: bool = False,
    enable_inner_real_metrics: bool = False,
    min_normal_ratio: Optional[float] = None,
    min_error_ratio: Optional[float] = None,
    signed_metric_gain: Optional[float] = None,
    metric_tau_mode: str = "budget-percentile",
    metric_scale_mode: str = "robust",
    head_k_override: Optional[int] = None,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
    precomputed_metric_context: Optional[V3942MetricContext] = None,
) -> Set[str]:
    # preference_vector, metric_stats, incident_anchor_sec, anomaly_weight are kept for contract compatibility.
    _ = preference_vector
    _ = metric_stats
    _ = incident_anchor_sec
    _ = anomaly_weight

    return _run_v3942_pipeline(
        points=points,
        budget_pct=budget_pct,
        incident_services=incident_services,
        metrics_stream_by_sec=metrics_stream_by_sec,
        seed=seed,
        debug_trace_logs=debug_trace_logs,
        scenario_windows=scenario_windows,
        min_incident_traces_per_scenario=min_incident_traces_per_scenario,
        online_soft_cap=online_soft_cap,
        lookback_sec=lookback_sec,
        alpha=alpha,
        gamma=gamma,
        rca_weight=rca_weight,
        high_error_service_threshold=high_error_service_threshold,
        metric_lookback_sec=metric_lookback_sec,
        min_consensus=min_consensus,
        enable_swap_refine=enable_swap_refine,
        enable_inner_real_metrics=enable_inner_real_metrics,
        min_normal_ratio_override=min_normal_ratio,
        min_error_ratio_override=min_error_ratio,
        signed_metric_gain=signed_metric_gain,
        metric_tau_mode=metric_tau_mode,
        metric_scale_mode=metric_scale_mode,
        head_k_override=head_k_override,
        precomputed_metric_context=precomputed_metric_context,
    )


def _composite_v3942_metric_native_signed_contrast_sampler(*args: Any, **kwargs: Any) -> Set[str]:
    return _composite_v3942_metric_native_signed_contrast(*args, **kwargs)
