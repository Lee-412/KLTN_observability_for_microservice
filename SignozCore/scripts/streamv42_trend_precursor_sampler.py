
import math
import os
import random
import csv
import heapq
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Set, List, Tuple, Dict, FrozenSet, Optional

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

_DISABLE_OVERRIDE = os.getenv("STREAMV3_DISABLE_OVERRIDE", "0") == "1"


# ---------------------------------------------------------------------------
# Service name normalization
# ---------------------------------------------------------------------------

def _strip_pod_to_service(pod_name: str) -> str:
    parts = str(pod_name).strip().split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return str(pod_name).strip()


# ---------------------------------------------------------------------------
# Real metrics parser (same as v4.1)
# ---------------------------------------------------------------------------

def build_real_metrics_stream(metric_dir: str) -> Dict[int, MetricsSnapshot]:
    """Parse real paper metrics to second-indexed service snapshots.

    Returned fields per service are normalized into [0, 1].
    """
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
                workload_ops = max(0.0, float(row.get("PodWorkload(Ops)", 0.0)))
                success_rate = _clamp(float(row.get("PodSuccessRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                rows_by_service[service].append((sec, cpu, mem, latency, workload_ops, success_rate))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        lat_hist: deque = deque(maxlen=120)
        ops_hist: deque = deque(maxlen=120)

        sec_acc: Dict[int, Dict[str, List[float]]] = defaultdict(lambda: {
            "cpu": [],
            "memory": [],
            "latency": [],
            "workload": [],
            "success_rate": [],
        })

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

            acc = sec_acc[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(norm_latency)
            acc["workload"].append(norm_workload)
            acc["success_rate"].append(success)

        for sec, acc in sec_acc.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "workload": _mean(acc["workload"]),
                "success_rate": _mean(acc["success_rate"]),
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


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


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
    deviations = [abs(value - med) for value in values]
    return _median(deviations)


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    z = math.exp(x)
    return z / (1.0 + z)


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


def _stable_positive_z(value: float, mu: float, sigma: float, epsilon: float) -> float:
    z_score = (value - mu) / (sigma + epsilon)
    return _clamp(z_score, 0.0, 10.0)


# ---------------------------------------------------------------------------
# Option 1: Temporal trend slope scorer (NEW for v4.2)
# ---------------------------------------------------------------------------

def _compute_slope_z(hist: "deque[float]", slope_hist: "deque[float]", n_back: int = 3) -> float:
    """Compute normalized rising slope from rolling history.

    Returns a non-negative z-score of the slope relative to historical slopes.
    Cold-start (< n_back+1 samples): return amplified raw positive slope.
    """
    vals = list(hist)
    if len(vals) < n_back + 1:
        slope_hist.append(0.0)
        return 0.0
    # Average slope over last n_back steps
    slope = (vals[-1] - vals[-1 - n_back]) / float(n_back)
    slope_vals = list(slope_hist)
    if len(slope_vals) >= 5:
        mu_s = _mean(slope_vals)
        sg_s = _std(slope_vals) + 0.01
        slope_z = max(0.0, (slope - mu_s) / sg_s)
    else:
        # Cold start: amplify positive raw slope (scale by 5 assuming [0,1] metric space)
        slope_z = max(0.0, slope * 5.0)
    slope_hist.append(slope)
    return _clamp(slope_z, 0.0, 3.0)


# ---------------------------------------------------------------------------
# v4.2 anomaly + trend scoring (NEW for v4.2)
# ---------------------------------------------------------------------------

def _service_metric_anomaly_and_trend(
    service: str,
    trace_sec: int,
    metrics_stream_by_sec: Dict[int, MetricsSnapshot],
    service_metric_hist: Dict[str, Dict[str, "deque[float]"]],
    metric_lookback_sec: int = 20,
) -> Tuple[float, float, float]:
    """Compute (distress, trend_z, raw_success_rate) for one service.

    distress     : point-in-time anomaly score [0,1] (same as v4.1)
    trend_z      : normalized rising-slope score [0,3] (NEW Option 1)
    raw_success_rate : aligned success_rate [0,1] for Option 3 failure detection
    """
    aligned = get_aligned_metrics(trace_sec, service, metrics_stream_by_sec, lookback_sec=metric_lookback_sec)
    if aligned is None:
        return 0.0, 0.0, 1.0

    hist = service_metric_hist[service]
    x_cpu = _clamp(float(aligned.get("cpu", 0.0)), 0.0, 1.0)
    x_mem = _clamp(float(aligned.get("memory", 0.0)), 0.0, 1.0)
    x_lat = _clamp(float(aligned.get("latency", 0.0)), 0.0, 1.0)
    x_work = _clamp(float(aligned.get("workload", 0.0)), 0.0, 1.0)
    success = _clamp(float(aligned.get("success_rate", 1.0)), 0.0, 1.0)

    # --- Distress (point-in-time z-scores, same as v4.1) ---
    z_cpu = _metric_positive_z(x_cpu, hist["cpu"])
    z_mem = _metric_positive_z(x_mem, hist["memory"])
    z_lat = _metric_positive_z(x_lat, hist["latency"])
    z_work = _metric_positive_z(x_work, hist["workload"])
    error_penalty = 0.5 if success < 0.99 else 0.0
    raw = 0.35 * z_cpu + 0.20 * z_mem + 0.25 * z_lat + 0.20 * z_work + 0.20 * error_penalty
    distress = _clamp(_sigmoid(raw), 0.0, 1.0)

    # --- Trend (rising slope, Option 1) ---
    # CPU trend and latency trend — latency is weighted higher for RCA
    cpu_slope_z = _compute_slope_z(hist["cpu"], hist["cpu_slope"])
    lat_slope_z = _compute_slope_z(hist["latency"], hist["lat_slope"])
    trend_z = _clamp(0.40 * cpu_slope_z + 0.60 * lat_slope_z, 0.0, 3.0)

    # Update rolling histories AFTER computing slopes (streaming-safe)
    hist["cpu"].append(x_cpu)
    hist["memory"].append(x_mem)
    hist["latency"].append(x_lat)
    hist["workload"].append(x_work)

    return distress, trend_z, success


def _metric_positive_z(x: float, history: "deque[float]") -> float:
    if len(history) < 5:
        return 0.0
    mu = _mean(list(history))
    sigma = _std(list(history))
    return max(0.0, (x - mu) / (sigma + 0.001))


# ---------------------------------------------------------------------------
# Trace-level helpers
# ---------------------------------------------------------------------------

def _trace_has_incident_service(p: TracePoint, incident_services: Set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)


def _trace_relative_normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    sigma = _std(values)
    if sigma <= 1e-6:
        return [0.0] * len(values)
    mu = _mean(values)
    return [(value - mu) / (sigma + 1e-6) for value in values]


def _rank_project(values: List[float]) -> float:
    if not values:
        return 0.0
    ranked = sorted(values, reverse=True)
    weights = [0.5, 0.3, 0.2]
    projected = 0.0
    for weight, value in zip(weights, ranked):
        projected += weight * value
    return projected


def _compute_metric_signal_details(
    trace: TracePoint,
    preference_vector: PreferenceVector,
    metric_stats: MetricStats,
) -> Tuple[float, float, float]:
    if not trace.services:
        return 0.0, 0.0, 0.0

    effective_services = [
        service
        for service in trace.services
        if service in preference_vector and int(metric_stats.get(service, {}).get("history_count", 0)) > 10
    ]
    base_coverage = len(effective_services) / float(len(trace.services)) if trace.services else 0.0
    if not effective_services:
        return 0.0, 0.0, 0.0

    service_scores: List[float] = []
    history_counts: List[float] = []
    for service in effective_services:
        pvec = preference_vector.get(service, {})
        z_lat = _clamp(float(pvec.get("latency", 0.0)), 0.0, 10.0)
        z_err = _clamp(float(pvec.get("error", 0.0)), 0.0, 10.0)
        z_thr = _clamp(float(pvec.get("throughput", 0.0)), 0.0, 10.0)
        z_bur = _clamp(float(pvec.get("burst", 0.0)), 0.0, 10.0)
        svc_score = 0.35 * z_lat + 0.30 * z_err + 0.20 * z_thr + 0.15 * z_bur
        service_scores.append(svc_score)
        hc = float(metric_stats.get(service, {}).get("history_count", 0))
        history_counts.append(hc)

    metric_score = _rank_project(service_scores)
    signal_entropy = _std(service_scores)

    total_history = sum(history_counts)
    hub_service_ratio = (max(history_counts) / total_history) if total_history > 0.0 else 0.0
    coverage_ratio = base_coverage * math.exp(-1.0 * hub_service_ratio)
    return metric_score, coverage_ratio, signal_entropy


def compute_metric_score(
    trace: TracePoint,
    preference_vector: PreferenceVector,
    metric_stats: MetricStats,
) -> Tuple[float, float]:
    metric_score, coverage_ratio, _signal_entropy = _compute_metric_signal_details(trace, preference_vector, metric_stats)
    return metric_score, coverage_ratio


def compute_sampling_probability(trace: TracePoint, context: Dict[str, Any]) -> float:
    incident_service = float(context.get("incident_service", 0.0))
    lat_norm = float(context.get("lat_norm", 0.0))
    err_flag = float(context.get("err_flag", 0.0))
    time_prox = float(context.get("time_prox", 0.5))
    alpha_eff = float(context.get("alpha_eff", 1.0))
    z_mix = float(context.get("z_mix", 0.0))
    p_min = float(context.get("p_min", 0.02))
    p_max = float(context.get("p_max", 0.98))

    base_sys = 0.45 * incident_service + 0.25 * lat_norm + 0.15 * err_flag + 0.15 * time_prox
    shock = math.tanh(alpha_eff * z_mix)
    p_s_v3 = _clamp(0.55 * base_sys + 0.45 * shock, p_min, p_max)

    preference_vector = context.get("preference_vector") or {}
    metric_stats = context.get("metric_stats") or {}
    trace_metric_score, coverage_ratio, signal_entropy = _compute_metric_signal_details(trace, preference_vector, metric_stats)

    lambda_gate = _clamp(signal_entropy / 3.0, 0.2, 1.0)
    lambda_eff = 0.2 * coverage_ratio * _sigmoid(2.0 * (trace_metric_score - 1.5)) * lambda_gate

    metric_boost = min(0.4, lambda_eff * trace_metric_score)
    p_s_final = p_s_v3 + metric_boost
    return _clamp(p_s_final, p_min, p_max)


def build_metric_inputs(
    points: List[TracePoint],
) -> Tuple[PreferenceVector, MetricStats]:
    if not points:
        return {}, {}
    return {}, {}


def _trace_preference_score(p: TracePoint, preference_vector: PreferenceVector) -> float:
    if not p.services or not preference_vector:
        return 0.0
    best = 0.0
    for svc in p.services:
        pvec = preference_vector.get(svc, {})
        if not pvec:
            continue
        s = sum(float(v) for v in pvec.values())
        if s > best:
            best = s
    return best


# ---------------------------------------------------------------------------
# v4.2 core sampler
# ---------------------------------------------------------------------------

def _composite_v42_trend_precursor_core(
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
    metrics_stream_by_sec: Optional[Dict[int, MetricsSnapshot]] = None,
    metric_lookback_sec: int = 20,
    precedence_threshold: float = 0.65,
) -> Set[str]:
    """v4.2 deterministic rank-select.

    Composite score:
        w1 * incident_service +
        w2 * lat_norm +
        w3 * distress +
        w4 * trend_z_norm +
        w5 * time_prox

    Selection is deterministic: global sort by composite score, then enforce
    per-scenario floor by score-aware add/drop.
    """
    total = len(points)
    if total == 0:
        return set()
    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    target_count = max(1, min(total, int(round(total * beta))))
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    composite_score_by_id: Dict[str, float] = {}
    trend_norm_by_id: Dict[str, float] = {}
    distress_by_id: Dict[str, float] = {}
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}

    # Streaming histories for distress and trend (Option 1).
    service_metric_hist: Dict[str, Dict[str, deque]] = defaultdict(
        lambda: {
            "cpu": deque(maxlen=10),
            "memory": deque(maxlen=10),
            "latency": deque(maxlen=10),
            "workload": deque(maxlen=10),
            "cpu_slope": deque(maxlen=10),   # NEW: rolling slope history for CPU
            "lat_slope": deque(maxlen=10),   # NEW: rolling slope history for latency
        }
    )

    # Tuned deterministic weights for rank-select.
    w_incident = 0.28
    w_lat = 0.18
    w_distress = 0.30
    w_trend = 0.16
    w_time = 0.08

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        # Option 1 signals from real metrics.
        trace_sec_abs = int(p.start_ns // 1_000_000_000)
        distress = 0.0
        trend_z_max = 0.0
        for svc in p.services:
            svc_distress, svc_trend_z, svc_sr = _service_metric_anomaly_and_trend(
                service=svc,
                trace_sec=trace_sec_abs,
                metrics_stream_by_sec=metrics_stream_by_sec,
                service_metric_hist=service_metric_hist,
                metric_lookback_sec=metric_lookback_sec,
            )
            distress = max(distress, svc_distress)
            trend_z_max = max(trend_z_max, svc_trend_z)

        trend_z_norm = _clamp(trend_z_max / 3.0, 0.0, 1.0)
        composite_score = (
            w_incident * incident_service
            + w_lat * lat_pressure
            + w_distress * distress
            + w_trend * trend_z_norm
            + w_time * time_prox
        )
        composite_score_by_id[p.trace_id] = composite_score
        trend_norm_by_id[p.trace_id] = trend_z_norm
        distress_by_id[p.trace_id] = distress

    def _rank_key(tid: str) -> Tuple[float, float, float, float, str]:
        trace = by_id[tid]
        return (
            composite_score_by_id.get(tid, 0.0),
            1.0 if trace.has_error else 0.0,
            float(trace.duration_ms),
            float(trace.span_count),
            tid,
        )

    ranked_ids_desc = sorted(by_id.keys(), key=_rank_key, reverse=True)
    base_selected: Set[str] = set(ranked_ids_desc[:target_count])

    # Strict cap per scenario (same as v4.1)
    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        return base_selected

    by_id_2: Dict[str, TracePoint] = {p.trace_id: p for p in points}
    start_s_by_id: Dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}
    scenario_ids_by_idx: List[Set[str]] = []
    scenario_membership: Dict[str, Set[int]] = defaultdict(set)
    non_empty_scenarios = 0
    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, sec_v in start_s_by_id.items() if st <= sec_v <= ed}
        scenario_ids_by_idx.append(ids)
        if ids:
            non_empty_scenarios += 1
            for tid in ids:
                scenario_membership[tid].add(idx)

    if non_empty_scenarios == 0:
        return base_selected

    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    def _priority(tid: str) -> Tuple[float, float, float, float, int, str]:
        pt = by_id_2.get(tid)
        if pt is None:
            return (0.0, 0.0, 0.0, 0.0, 0, tid)
        return (
            composite_score_by_id.get(tid, 0.0),
            1.0 if pt.has_error else 0.0,
            float(pt.duration_ms),
            float(pt.span_count),
            len(scenario_membership.get(tid, set())),
            tid,
        )

    final_ids: Set[str] = set(base_selected)
    scenario_floor_fail_count = 0
    if floor_each > 0:
        for idx, ids in enumerate(scenario_ids_by_idx):
            if not ids:
                scenario_floor_fail_count += 1
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(key=_priority, reverse=True)
            if not candidates:
                scenario_floor_fail_count += 1
                continue
            for tid in candidates[:need]:
                final_ids.add(tid)
            if len([tid for tid in final_ids if tid in ids]) < floor_each:
                scenario_floor_fail_count += 1

    if len(final_ids) <= target_count:
        return final_ids

    scenario_counts = [0] * len(scenario_ids_by_idx)
    for tid in final_ids:
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] += 1

    def _can_remove(tid: str) -> bool:
        if floor_each <= 0:
            return True
        for idx in scenario_membership.get(tid, set()):
            if scenario_counts[idx] <= floor_each:
                return False
        return True

    removable = sorted(final_ids, key=_priority)
    for tid in removable:
        if len(final_ids) <= target_count:
            break
        if not _can_remove(tid):
            continue
        final_ids.remove(tid)
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] -= 1

    if len(final_ids) > target_count:
        removable_ids = sorted(
            final_ids,
            key=_priority,
            reverse=True,
        )
        final_ids = set(removable_ids[:target_count])

    if len(final_ids) < target_count:
        for tid in ranked_ids_desc:
            if tid in final_ids:
                continue
            final_ids.add(tid)
            if len(final_ids) >= target_count:
                break

    return final_ids


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def _composite_v42_trend_precursor_sampler(
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
    metrics_stream_by_sec: Optional[Dict[int, MetricsSnapshot]] = None,
    metric_lookback_sec: int = 20,
    precedence_threshold: float = 0.65,
) -> Set[str]:
    """v4.2 Trend + Precursor sampler.

    Option 1: Temporal trend scoring via rolling slope of CPU/latency metrics.
    Option 3: Incident precursor window — retroactive [T-10,T] force-keep
              and prospective [T, T+20] decay boost on failure detection.
    """
    return _composite_v42_trend_precursor_core(
        points=points,
        budget_pct=budget_pct,
        preference_vector=preference_vector,
        scenario_windows=scenario_windows,
        incident_anchor_sec=incident_anchor_sec,
        seed=seed,
        metric_stats=metric_stats,
        incident_services=incident_services,
        min_incident_traces_per_scenario=min_incident_traces_per_scenario,
        online_soft_cap=online_soft_cap,
        lookback_sec=lookback_sec,
        alpha=alpha,
        gamma=gamma,
        anomaly_weight=anomaly_weight,
        metrics_stream_by_sec=metrics_stream_by_sec,
        metric_lookback_sec=metric_lookback_sec,
        precedence_threshold=precedence_threshold,
    )
