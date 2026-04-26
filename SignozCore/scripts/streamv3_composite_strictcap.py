

import math
import os
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Set, List, Tuple, Dict, FrozenSet, Optional

from streamv9_metric_aware_sampling_from_v35 import (
    _build_pod_metric_index,
    _build_service_pod_mapping,
    _lookup_aligned_pod_snapshot,
)

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

_DISABLE_OVERRIDE = os.getenv("STREAMV3_DISABLE_OVERRIDE", "0") == "1"

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
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
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


def _trace_has_incident_service(p: TracePoint, incident_services: Set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)


def _trace_relative_normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    sigma = _std(values)
    if sigma <= 1e-6:
        return [0.0 for _ in values]
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
        return 0.0, base_coverage, 0.0

    service_scores: List[float] = []
    history_counts: List[float] = []
    for service in effective_services:
        metric_vector = preference_vector.get(service, {})
        raw_z = [
            float(metric_vector.get("latency", 0.0)),
            float(metric_vector.get("error", 0.0)),
            float(metric_vector.get("throughput", 0.0)),
            float(metric_vector.get("burst", 0.0)),
        ]
        normalized_z = _trace_relative_normalize(raw_z)
        service_scores.append(5.0 * _rank_project(normalized_z))
        history_counts.append(float(metric_stats.get(service, {}).get("history_count", 0.0)))

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
    # Exact Stream V3 baseline core.
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
    # Build bounded-memory service metric states for compatibility with external callers.
    if not points:
        return {}, {}

    # The online sampler maintains the streaming states; here we return empty structures
    # so callers can still bind to the interface without leaking future information.
    return {}, {}

def _trace_preference_score(p: TracePoint, preference_vector: PreferenceVector) -> float:
    # Legacy helper kept for compatibility with existing analysis scripts.
    if not p.services or not preference_vector:
        return 0.0
    best = 0.0
    for svc in p.services:
        metric_vector = preference_vector.get(svc, {})
        service_score = (
            0.4 * float(metric_vector.get("latency", 0.0))
            + 0.3 * float(metric_vector.get("error", 0.0))
            + 0.2 * float(metric_vector.get("throughput", 0.0))
            + 0.1 * float(metric_vector.get("burst", 0.0))
        )
        best = max(best, service_score)
    return best


def _minmax_normalize_scores(score_by_id: Dict[str, float]) -> Dict[str, float]:
    if not score_by_id:
        return {}
    lo = min(score_by_id.values())
    hi = max(score_by_id.values())
    if hi <= lo + 1e-12:
        return {tid: 0.5 for tid in score_by_id}
    scale = hi - lo
    return {tid: (float(score) - lo) / scale for tid, score in score_by_id.items()}


def _cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a <= 1e-12 or norm_b <= 1e-12:
        return 0.0
    return _clamp(dot / (norm_a * norm_b), 0.0, 1.0)


def _composite_v12_lite_bias_align(
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
    metrics_stream_by_sec: Optional[Dict[int, Dict[str, Dict[str, float]]]] = None,
    metric_lookback_sec: int = 120,
    w_v35: float = 0.60,
    w_bias: float = 0.25,
    w_div: float = 0.15,
    bias_window_sec: int = 90,
) -> Set[str]:
    # V12lite: keep v3.5 signal core, then apply lightweight bias-aligned rerank.
    del preference_vector, metric_stats, online_soft_cap, anomaly_weight, seed

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
    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}
    metric_lookback_sec = max(1, int(metric_lookback_sec))

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    def _dur_bucket(d_ms: float) -> str:
        if d_ms < 100:
            return "lt100"
        if d_ms < 500:
            return "100_500"
        if d_ms < 2000:
            return "500_2000"
        return "ge2000"

    def _span_bucket(sc: int) -> str:
        if sc <= 3:
            return "s1"
        if sc <= 8:
            return "s2"
        if sc <= 20:
            return "s3"
        return "s4"

    def _trace_tokens(p: TracePoint) -> FrozenSet[str]:
        toks = {f"svc:{s}" for s in p.services}
        toks.add(f"dur:{_dur_bucket(p.duration_ms)}")
        toks.add(f"span:{_span_bucket(p.span_count)}")
        return frozenset(toks)

    def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
        if not a and not b:
            return 1.0
        union_size = len(a | b)
        if union_size <= 0:
            return 0.0
        return len(a & b) / float(union_size)

    def _robust_sigma(xs: List[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((x - mu) * (x - mu) for x in xs) / max(1, len(xs) - 1)
        std = math.sqrt(max(0.0, var))
        sxs = sorted(xs)
        med = sxs[len(sxs) // 2]
        abs_dev = sorted(abs(x - med) for x in xs)
        mad = abs_dev[len(abs_dev) // 2]
        robust = 1.4826 * mad
        return max(std, robust)

    all_services = {svc for p in traces for svc in p.services}
    all_pods = {pod for sec_snap in metrics_stream_by_sec.values() for pod in sec_snap.keys()}
    service_to_pods, _covered_services, _mismatch_services = _build_service_pod_mapping(all_services, all_pods)
    sec_index, snap_index = _build_pod_metric_index(metrics_stream_by_sec)

    def _trace_state_vector(trace: TracePoint) -> List[float]:
        trace_sec_abs = int(trace.start_ns // 1_000_000_000)
        cpu_vals: List[float] = []
        mem_vals: List[float] = []
        latency_vals: List[float] = []
        restart_vals: List[float] = []
        throughput_vals: List[float] = []

        for service in trace.services:
            candidates = service_to_pods.get(service, [])
            best_snap: Optional[Dict[str, float]] = None
            best_conf = 0.0
            best_quality = -1.0

            for pod, conf, _kind in candidates:
                snap = _lookup_aligned_pod_snapshot(
                    pod=pod,
                    trace_sec=trace_sec_abs,
                    sec_index=sec_index,
                    snap_index=snap_index,
                    lookback_sec=metric_lookback_sec,
                )
                if snap is None:
                    continue
                quality = conf * (
                    0.30 * float(snap.get("latency_p99_z", 0.0))
                    + 0.25 * float(snap.get("cpu_z", 0.0))
                    + 0.20 * float(snap.get("mem_z", 0.0))
                    + 0.15 * float(snap.get("workload_z", 0.0))
                    + 0.10 * float(snap.get("node_pressure_z", 0.0))
                )
                if quality > best_quality:
                    best_quality = quality
                    best_snap = snap
                    best_conf = conf

            if best_snap is None:
                continue

            cpu_vals.append(best_conf * float(best_snap.get("cpu_z", 0.0)))
            mem_vals.append(best_conf * float(best_snap.get("mem_z", 0.0)))
            latency_vals.append(best_conf * float(best_snap.get("latency_p99_z", 0.0)))
            restart_vals.append(best_conf * float(best_snap.get("node_pressure_z", 0.0)))
            throughput_vals.append(best_conf * float(best_snap.get("workload_z", 0.0)))

        error_signal = 1.0 if trace.has_error else 0.0
        return [
            max(0.0, _mean(cpu_vals)),
            max(0.0, _mean(mem_vals)),
            max(0.0, _mean(latency_vals)),
            error_signal,
            max(0.0, _mean(restart_vals)),
            max(0.0, _mean(throughput_vals)),
        ]

    trace_abs_secs = [int(p.start_ns // 1_000_000_000) for p in traces]
    if incident_anchor_sec is None:
        incident_abs_sec = int(_median([float(sec) for sec in trace_abs_secs])) if trace_abs_secs else 0
    else:
        incident_abs_sec = int(t0_sec + int(incident_anchor_sec))
    bias_window = max(15, int(bias_window_sec))
    win_lo = incident_abs_sec - bias_window
    win_hi = incident_abs_sec + bias_window

    window_snaps: List[Dict[str, float]] = []
    for sec, pod_snaps in metrics_stream_by_sec.items():
        sec_i = int(sec)
        if win_lo <= sec_i <= win_hi:
            window_snaps.extend(pod_snaps.values())

    if window_snaps:
        bias_cpu = max(0.0, _mean([float(s.get("cpu_z", 0.0)) for s in window_snaps]))
        bias_mem = max(0.0, _mean([float(s.get("mem_z", 0.0)) for s in window_snaps]))
        bias_lat = max(0.0, _mean([float(s.get("latency_p99_z", 0.0)) for s in window_snaps]))
        bias_restart = max(0.0, _mean([float(s.get("node_pressure_z", 0.0)) for s in window_snaps]))
        bias_throughput = max(0.0, _mean([float(s.get("workload_z", 0.0)) for s in window_snaps]))
    else:
        bias_cpu = 0.0
        bias_mem = 0.0
        bias_lat = 0.0
        bias_restart = 0.0
        bias_throughput = 0.0

    win_errors = [
        1.0 if p.has_error else 0.0
        for p in traces
        if win_lo <= int(p.start_ns // 1_000_000_000) <= win_hi
    ]
    if win_errors:
        bias_error = _mean(win_errors)
    else:
        bias_error = _mean([1.0 if p.has_error else 0.0 for p in traces])

    bias_vector = [
        bias_cpu,
        bias_mem,
        bias_lat,
        max(0.0, bias_error),
        bias_restart,
        bias_throughput,
    ]
    bias_mass = sum(bias_vector)
    if bias_mass <= 1e-12:
        bias_confidence = 0.0
    else:
        probs = [v / bias_mass for v in bias_vector if v > 1e-12]
        entropy = -sum(p * math.log(p) for p in probs)
        max_entropy = math.log(float(len(bias_vector))) if len(bias_vector) > 1 else 1.0
        concentration = 1.0 - (entropy / max_entropy if max_entropy > 0.0 else 0.0)
        strength = _clamp(math.log1p(bias_mass) / math.log(6.0), 0.0, 1.0)
        bias_confidence = _clamp(0.10 + 0.60 * concentration + 0.30 * strength, 0.0, 1.0)

    seen_win: deque[Tuple[int, float, float, float]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[Tuple[int, Tuple[str, ...]]] = deque()
    eps0 = 1e-6

    history_size = max(8, lookback_sec)
    metric_state: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "latency": deque(maxlen=history_size),
            "error": deque(maxlen=history_size),
            "throughput": deque(maxlen=history_size),
            "burst": deque(maxlen=history_size),
            "ema": 0.0,
            "sec": None,
            "count": 0.0,
        }
    )

    sv35_by_id: Dict[str, float] = {}
    div_by_id: Dict[str, float] = {}
    bias_by_id: Dict[str, float] = {}

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        historical_preference_vector: PreferenceVector = {}
        historical_metric_stats: MetricStats = {}
        for service in sorted(p.services):
            state = metric_state[service]
            current_count = float(state["count"]) if state["sec"] == sec else 0.0

            throughput = math.log1p(current_count + 1.0)
            ema = 0.2 * throughput + 0.8 * float(state["ema"])
            burst = throughput - ema

            latency_hist = list(state["latency"])
            error_hist = list(state["error"])
            throughput_hist = list(state["throughput"])
            burst_hist = list(state["burst"])

            latency_med = _median(latency_hist)
            latency_mad = _mad(latency_hist)
            error_med = _median(error_hist)
            error_mad = _mad(error_hist)
            throughput_med = _median(throughput_hist)
            throughput_mad = _mad(throughput_hist)
            burst_med = _median(burst_hist)
            burst_mad = _mad(burst_hist)

            historical_preference_vector[service] = {
                "latency": _stable_positive_z(lat_pressure, latency_med, latency_mad, 1.0),
                "error": _stable_positive_z(err_flag, error_med, error_mad, 0.01),
                "throughput": _stable_positive_z(throughput, throughput_med, throughput_mad, 0.5),
                "burst": _stable_positive_z(burst, burst_med, burst_mad, 0.01),
            }
            historical_metric_stats[service] = {
                "latency": (latency_med, latency_mad),
                "error": (error_med, error_mad),
                "throughput": (throughput_med, throughput_mad),
                "burst": (burst_med, burst_mad),
                "history_count": len(latency_hist),
            }

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        mu_err = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        mu_lat = (sum(hist_lat) / len(hist_lat)) if hist_lat else 0.0
        mu_inc = (sum(hist_inc) / len(hist_inc)) if hist_inc else 0.0
        sg_err = _robust_sigma(hist_err)
        sg_lat = _robust_sigma(hist_lat)
        sg_inc = _robust_sigma(hist_inc)
        z_err = abs(err_flag - mu_err) / (sg_err + eps0)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + eps0)
        z_inc = abs(incident_service - mu_inc) / (sg_inc + eps0)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc
        alpha_eff = alpha * (0.90 if beta >= 0.01 else 1.00)

        p_s = compute_sampling_probability(
            p,
            {
                "incident_service": incident_service,
                "lat_norm": lat_pressure,
                "err_flag": err_flag,
                "time_prox": time_prox,
                "alpha_eff": alpha_eff,
                "z_mix": z_mix,
                "p_min": 0.02,
                "p_max": 0.98,
                "preference_vector": historical_preference_vector,
                "metric_stats": historical_metric_stats,
            },
        )

        incident_floor = 0.0
        if incident_service > 0.0:
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
        for key, key_tokens in cluster_templates.items():
            sim = _jaccard(toks, key_tokens)
            if sim > best_sim:
                best_sim = sim
                best_key = key

        if best_key is not None and best_sim >= 0.5:
            assigned_key = best_key
            mass = cluster_counts.get(assigned_key, 0)
            effective_mass = mass * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            effective_mass = 0.0

        if incident_service > 0.0:
            effective_mass *= 0.35
            gamma_eff = max(0.35, gamma * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = gamma
            pd_floor = 0.05
        p_d = _clamp((1.0 + effective_mass) ** (-gamma_eff), pd_floor, 1.0)

        trace_state = _trace_state_vector(p)
        bias_score = _cosine_similarity(trace_state, bias_vector)

        sv35_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        div_by_id[p.trace_id] = p_d
        bias_by_id[p.trace_id] = bias_score

        for service in sorted(p.services):
            state = metric_state[service]
            if state["sec"] != sec:
                state["sec"] = sec
                state["count"] = 0.0
            state["count"] += 1.0
            throughput = math.log1p(float(state["count"]))
            ema = 0.2 * throughput + 0.8 * float(state["ema"])
            burst = throughput - ema
            state["ema"] = ema
            state["latency"].append(lat_pressure)
            state["error"].append(err_flag)
            state["throughput"].append(throughput)
            state["burst"].append(burst)

        seen_win.append((sec, err_flag, lat_pressure, incident_service))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

    sv35_norm = _minmax_normalize_scores(sv35_by_id)
    div_norm = _minmax_normalize_scores(div_by_id)
    bias_norm = _minmax_normalize_scores(bias_by_id)

    w_v35_eff = max(0.0, float(w_v35))
    w_div_eff = max(0.0, float(w_div))
    w_bias_eff = max(0.0, float(w_bias)) * bias_confidence
    weight_sum = max(1e-9, w_v35_eff + w_div_eff + w_bias_eff)

    stage_a_score_by_id: Dict[str, float] = {}
    for tid in sv35_by_id.keys():
        stage_a_score_by_id[tid] = (
            w_v35_eff * sv35_norm.get(tid, 0.0)
            + w_div_eff * div_norm.get(tid, 0.0)
            + w_bias_eff * bias_norm.get(tid, 0.0)
        ) / weight_sum

    stage_a_ranked_ids = sorted(stage_a_score_by_id.keys(), key=lambda tid: (-stage_a_score_by_id.get(tid, 0.0), tid))

    # Stage-B rescue rerank: pull top-1 from top-3-style candidates.
    if beta <= 0.0015:
        rescue_pool_mult = 3.0
    elif beta <= 0.01:
        rescue_pool_mult = 2.0
    else:
        rescue_pool_mult = 1.6
    rescue_pool_count = max(target_count, min(total, int(round(target_count * rescue_pool_mult))))
    rescue_candidates = set(stage_a_ranked_ids[:rescue_pool_count])

    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}

    # Local error density in a fixed time window around each trace.
    rescue_window = 60
    abs_secs = [int(p.start_ns // 1_000_000_000) for p in traces]
    err_flags = [1 if p.has_error else 0 for p in traces]
    local_error_density_by_id: Dict[str, float] = {}
    left = 0
    right = -1
    win_err = 0
    win_total = 0
    for i, p in enumerate(traces):
        cur_sec = abs_secs[i]
        lo = cur_sec - rescue_window
        hi = cur_sec + rescue_window

        while (right + 1) < total and abs_secs[right + 1] <= hi:
            right += 1
            win_total += 1
            win_err += err_flags[right]

        while left <= right and abs_secs[left] < lo:
            win_total -= 1
            win_err -= err_flags[left]
            left += 1

        if win_total > 0:
            local_error_density_by_id[p.trace_id] = win_err / float(win_total)
        else:
            local_error_density_by_id[p.trace_id] = 1.0 if p.has_error else 0.0

    microrank_score_norm = _minmax_normalize_scores(stage_a_score_by_id)
    rescue_score_by_id: Dict[str, float] = {}

    for tid in rescue_candidates:
        trace = by_id.get(tid)
        if trace is None:
            continue

        latency_shock = _norm(float(trace.duration_ms), float(dur_lo), float(dur_hi))
        if trace.services and incident_services:
            overlap = len(set(trace.services) & incident_services) / float(len(trace.services))
        else:
            overlap = 0.0
        temporal_closeness = math.exp(-abs(int(trace.start_ns // 1_000_000_000) - incident_abs_sec) / 60.0)

        root_cause_prior = (
            0.40 * local_error_density_by_id.get(tid, 0.0)
            + 0.30 * latency_shock
            + 0.20 * overlap
            + 0.10 * temporal_closeness
        )

        rescue_score_by_id[tid] = (
            0.55 * microrank_score_norm.get(tid, 0.0)
            + 0.25 * bias_norm.get(tid, 0.0)
            + 0.20 * root_cause_prior
        )

    score_by_id: Dict[str, float] = dict(stage_a_score_by_id)
    score_by_id.update(rescue_score_by_id)

    selected_ranked = sorted(
        rescue_candidates,
        key=lambda tid: (
            -score_by_id.get(tid, 0.0),
            -stage_a_score_by_id.get(tid, 0.0),
            tid,
        ),
    )
    selected_ids: Set[str] = set(selected_ranked[:target_count])

    ranked_ids = sorted(score_by_id.keys(), key=lambda tid: (-score_by_id.get(tid, 0.0), tid))

    if selected_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in selected_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [tid for tid, p in by_id.items() if (tid not in selected_ids) and p.has_error]
            cand_add.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
            add_ids = cand_add[:deficit]
            if add_ids:
                cand_drop = [tid for tid in selected_ids if by_id.get(tid) and (not by_id[tid].has_error)]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    selected_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    selected_ids.add(tid)

    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        return set(sorted(selected_ids, key=lambda tid: (-score_by_id.get(tid, 0.0), tid))[:target_count])

    start_s_by_id: Dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}
    scenario_ids_by_idx: List[Set[str]] = []
    scenario_membership: Dict[str, Set[int]] = defaultdict(set)
    non_empty_scenarios = 0
    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, sec in start_s_by_id.items() if st <= sec <= ed}
        scenario_ids_by_idx.append(ids)
        if ids:
            non_empty_scenarios += 1
            for tid in ids:
                scenario_membership[tid].add(idx)

    if non_empty_scenarios == 0:
        return set(sorted(selected_ids, key=lambda tid: (-score_by_id.get(tid, 0.0), tid))[:target_count])

    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    def _priority(tid: str) -> Tuple[float, float, float, float, int, str]:
        p = by_id.get(tid)
        if p is None:
            return (0.0, 0.0, 0.0, 0.0, 0, tid)
        return (
            1.0 if p.has_error else 0.0,
            float(score_by_id.get(tid, 0.0)),
            float(p.duration_ms),
            float(p.span_count),
            len(scenario_membership.get(tid, set())),
            tid,
        )

    final_ids: Set[str] = set(selected_ids)
    if floor_each > 0:
        for idx, ids in enumerate(scenario_ids_by_idx):
            if not ids:
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(key=_priority, reverse=True)
            for tid in candidates[:need]:
                final_ids.add(tid)

    if len(final_ids) <= target_count:
        if len(final_ids) < target_count:
            for tid in ranked_ids:
                if tid in final_ids:
                    continue
                final_ids.add(tid)
                if len(final_ids) >= target_count:
                    break
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
        forced = sorted(final_ids, key=_priority)
        for tid in forced:
            if len(final_ids) <= target_count:
                break
            final_ids.remove(tid)

    return final_ids

def _composite_v3_metrics_strictcap(
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
) -> Set[str]:
    # --- V3+metrics sampling ---
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
    def _dur_bucket(d_ms: float) -> str:
        if d_ms < 100:
            return "lt100"
        if d_ms < 500:
            return "100_500"
        if d_ms < 2000:
            return "500_2000"
        return "ge2000"
    def _span_bucket(sc: int) -> str:
        if sc <= 3:
            return "s1"
        if sc <= 8:
            return "s2"
        if sc <= 20:
            return "s3"
        return "s4"
    def _trace_tokens(p: TracePoint) -> FrozenSet[str]:
        toks = {f"svc:{s}" for s in p.services}
        toks.add(f"dur:{_dur_bucket(p.duration_ms)}")
        toks.add(f"span:{_span_bucket(p.span_count)}")
        return frozenset(toks)
    def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
        if not a and not b:
            return 1.0
        u = len(a | b)
        if u <= 0:
            return 0.0
        return len(a & b) / u
    def _robust_sigma(xs: List[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((x - mu) * (x - mu) for x in xs) / max(1, len(xs) - 1)
        std = math.sqrt(max(0.0, var))
        sxs = sorted(xs)
        med = sxs[len(sxs) // 2]
        abs_dev = sorted(abs(x - med) for x in xs)
        mad = abs_dev[len(abs_dev) // 2]
        robust = 1.4826 * mad
        return max(std, robust)
    rng = random.Random(seed)
    kept_ids: Set[str] = set()
    seen_win: deque[Tuple[int, float, float, float]] = deque()
    kept_win: deque[Tuple[int, bool]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[Tuple[int, Tuple[str, ...]]] = deque()
    seen = 0
    eps0 = 1e-6
    cluster_merge_threshold = 0.5
    normal_ratio_target = 0.30
    score_by_id: Dict[str, float] = {}
    p_s_final_by_id: Dict[str, float] = {}
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
    incident_services = incident_services or set()

    # Streaming-safe, bounded metric state per service.
    history_size = max(8, lookback_sec)
    metric_state: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "latency": deque(maxlen=history_size),
            "error": deque(maxlen=history_size),
            "throughput": deque(maxlen=history_size),
            "burst": deque(maxlen=history_size),
            "ema": 0.0,
            "sec": None,
            "count": 0.0,
        }
    )
    current_preference_vector: PreferenceVector = {}
    current_metric_stats: MetricStats = {}

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()
        while kept_win and kept_win[0][0] < sec - lookback_sec + 1:
            kept_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        historical_preference_vector: PreferenceVector = {}
        historical_metric_stats: MetricStats = {}
        for service in sorted(p.services):
            state = metric_state[service]
            if state["sec"] != sec:
                current_count = 0.0
            else:
                current_count = float(state["count"])

            throughput = math.log1p(current_count + 1.0)
            ema = 0.2 * throughput + 0.8 * float(state["ema"])
            burst = throughput - ema

            latency_hist = list(state["latency"])
            error_hist = list(state["error"])
            throughput_hist = list(state["throughput"])
            burst_hist = list(state["burst"])

            latency_med = _median(latency_hist)
            latency_mad = _mad(latency_hist)
            error_med = _median(error_hist)
            error_mad = _mad(error_hist)
            throughput_med = _median(throughput_hist)
            throughput_mad = _mad(throughput_hist)
            burst_med = _median(burst_hist)
            burst_mad = _mad(burst_hist)

            historical_preference_vector[service] = {
                "latency": _stable_positive_z(lat_pressure, latency_med, latency_mad, 1.0),
                "error": _stable_positive_z(err_flag, error_med, error_mad, 0.01),
                "throughput": _stable_positive_z(throughput, throughput_med, throughput_mad, 0.5),
                "burst": _stable_positive_z(burst, burst_med, burst_mad, 0.01),
            }
            historical_metric_stats[service] = {
                "latency": (latency_med, latency_mad),
                "error": (error_med, error_mad),
                "throughput": (throughput_med, throughput_mad),
                "burst": (burst_med, burst_mad),
                "history_count": len(latency_hist),
            }

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        mu_err = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        mu_lat = (sum(hist_lat) / len(hist_lat)) if hist_lat else 0.0
        mu_inc = (sum(hist_inc) / len(hist_inc)) if hist_inc else 0.0
        sg_err = _robust_sigma(hist_err)
        sg_lat = _robust_sigma(hist_lat)
        sg_inc = _robust_sigma(hist_inc)
        z_err = abs(err_flag - mu_err) / (sg_err + eps0)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + eps0)
        z_inc = abs(incident_service - mu_inc) / (sg_inc + eps0)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc
        alpha_eff = alpha * (0.90 if beta >= 0.01 else 1.00)

        p_s = compute_sampling_probability(
            p,
            {
                "incident_service": incident_service,
                "lat_norm": lat_pressure,
                "err_flag": err_flag,
                "time_prox": time_prox,
                "alpha_eff": alpha_eff,
                "z_mix": z_mix,
                "p_min": 0.02,
                "p_max": 0.98,
                "preference_vector": historical_preference_vector,
                "metric_stats": historical_metric_stats,
            },
        )

        incident_floor = 0.0
        if incident_service > 0.0:
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
        for k, ktoks in cluster_templates.items():
            sim = _jaccard(toks, ktoks)
            if sim > best_sim:
                best_sim = sim
                best_key = k
        if best_key is not None and best_sim >= cluster_merge_threshold:
            assigned_key = best_key
            mass = cluster_counts.get(assigned_key, 0)
            effective_mass = mass * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            effective_mass = 0.0
        if incident_service > 0.0:
            effective_mass *= 0.35
            gamma_eff = max(0.35, gamma * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = gamma
            pd_floor = 0.05
        p_d = _clamp((1.0 + effective_mass) ** (-gamma_eff), pd_floor, 1.0)
        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        if beta <= 0.0015:
            ps_or_floor = 0.12
        elif beta <= 0.01:
            ps_or_floor = 0.20
        else:
            ps_or_floor = 0.30
        if incident_service > 0.0 and time_prox >= 0.55:
            ps_or_floor = max(0.08, ps_or_floor - 0.06)
        kept_hist = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio = (sum(kept_hist) / len(kept_hist)) if kept_hist else 0.0
        kept_norm_ratio = 1.0 - kept_err_ratio
        trace_metric_score, _, _signal_entropy = _compute_metric_signal_details(p, historical_preference_vector, historical_metric_stats)
        is_high_confidence_anomaly = trace_metric_score >= 3.0
        u = rng.random()
        if theta > beta:
            if (not _DISABLE_OVERRIDE) and is_high_confidence_anomaly:
                keep = (u < p_s)
            else:
                keep = (u < p_s) and (u < p_d)
        else:
            rare_noise = (p_d >= 0.72) and (p_s < ps_or_floor)
            if rare_noise:
                keep = False
            else:
                keep = (u < p_s)

            is_clean_normal = (not p.has_error) and (incident_service <= 0.0) and (p_s < ps_or_floor) and (p_d <= 0.45)
            if (not keep) and is_clean_normal and (kept_norm_ratio < normal_ratio_target):
                if rng.random() < beta:
                    keep = True

            if (not keep) and online_soft_cap and is_clean_normal:
                gap = max(0.0, beta - theta)
                p_gap = _clamp(gap, 0.0, 0.20)
                if rng.random() < p_gap:
                    keep = True

        for service in sorted(p.services):
            state = metric_state[service]
            if state["sec"] != sec:
                state["sec"] = sec
                state["count"] = 0.0
            state["count"] += 1.0
            throughput = math.log1p(float(state["count"]))
            ema = 0.2 * throughput + 0.8 * float(state["ema"])
            burst = throughput - ema
            state["ema"] = ema
            state["latency"].append(lat_pressure)
            state["error"].append(err_flag)
            state["throughput"].append(throughput)
            state["burst"].append(burst)
        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))
        p_s_final_by_id[p.trace_id] = p_s
        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        seen_win.append((sec, err_flag, lat_pressure, incident_service))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))
    # Keep minimum error density protection from baseline Stream V3.
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [tid for tid, trace_point in by_id.items() if (tid not in kept_ids) and trace_point.has_error]
            cand_add.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
            add_ids = cand_add[:deficit]
            if add_ids:
                cand_drop = [tid for tid in kept_ids if by_id.get(tid) and (not by_id[tid].has_error)]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    kept_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    kept_ids.add(tid)

    # --- Strict cap per scenario ---
    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        ranked_ids = sorted(
            kept_ids,
            key=lambda tid: (
                -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
                -p_s_final_by_id.get(tid, 0.0),
                tid,
            ),
        )
        return set(ranked_ids[:target_count])
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in points}
    start_s_by_id: Dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}
    scenario_ids_by_idx: List[Set[str]] = []
    scenario_membership: Dict[str, Set[int]] = defaultdict(set)
    non_empty_scenarios = 0
    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, sec in start_s_by_id.items() if st <= sec <= ed}
        scenario_ids_by_idx.append(ids)
        if ids:
            non_empty_scenarios += 1
            for tid in ids:
                scenario_membership[tid].add(idx)
    if non_empty_scenarios == 0:
        ranked_ids = sorted(
            kept_ids,
            key=lambda tid: (
                -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
                -p_s_final_by_id.get(tid, 0.0),
                tid,
            ),
        )
        return set(ranked_ids[:target_count])
    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0
    def _priority(tid: str) -> Tuple[float, float, float, int, str]:
        p = by_id.get(tid)
        if p is None:
            return (0.0, 0.0, 0.0, 0, tid)
        return (
            1.0 if p.has_error else 0.0,
            float(p.duration_ms),
            float(p.span_count),
            len(scenario_membership.get(tid, set())),
            tid,
        )
    final_ids: Set[str] = set(kept_ids)
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
            key=lambda tid: (
                -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
                -p_s_final_by_id.get(tid, 0.0),
                tid,
            ),
        )
        final_ids = set(removable_ids[:target_count])
    return final_ids
