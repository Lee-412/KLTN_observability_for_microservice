import math
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import streamv36 as sv36

TracePoint = sv36.TracePoint
PreferenceVector = sv36.PreferenceVector
MetricStats = sv36.MetricStats


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    return sv36.build_metric_inputs(points)


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
    tokens = {f"svc:{service}" for service in p.services}
    tokens.add(f"dur:{_dur_bucket(p.duration_ms)}")
    tokens.add(f"span:{_span_bucket(p.span_count)}")
    return frozenset(tokens)


def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
    if not a and not b:
        return 1.0
    union_size = len(a | b)
    if union_size <= 0:
        return 0.0
    return len(a & b) / float(union_size)


def _local_error_ratio(trace: TracePoint) -> float:
    # The dataset provides trace-level has_error instead of per-span error labels.
    # We map local error ratio to a bounded proxy in [0, 1] using available signal.
    if trace.span_count <= 0:
        return 1.0 if trace.has_error else 0.0
    error_spans = trace.span_count if trace.has_error else 0
    return _clamp(error_spans / float(trace.span_count), 0.0, 1.0)


def _compute_global_error_ratio(
    traces: List[TracePoint],
    incident_anchor_sec: Optional[int],
    incident_window_sec: int,
) -> float:
    if not traces:
        return 0.0

    if incident_anchor_sec is None:
        error_count = sum(1 for trace in traces if trace.has_error)
        return error_count / float(len(traces))

    window_members: List[TracePoint] = []
    for trace in traces:
        sec = int(trace.start_ns // 1_000_000_000)
        if abs(sec - incident_anchor_sec) <= incident_window_sec:
            window_members.append(trace)

    if not window_members:
        error_count = sum(1 for trace in traces if trace.has_error)
        return error_count / float(len(traces))

    error_count = sum(1 for trace in window_members if trace.has_error)
    return error_count / float(len(window_members))


def _system_score(
    trace: TracePoint,
    incident_services: Set[str],
    incident_anchor_sec: Optional[int],
    dur_lo: float,
    dur_hi: float,
    span_lo: int,
    span_hi: int,
) -> float:
    lat_norm = 0.7 * _norm(trace.duration_ms, dur_lo, dur_hi) + 0.3 * _norm(float(trace.span_count), float(span_lo), float(span_hi))
    incident_hit = 1.0 if sv36._trace_has_incident_service(trace, incident_services) else 0.0
    err_flag = 1.0 if trace.has_error else 0.0

    if incident_anchor_sec is None:
        time_prox = 0.5
    else:
        sec = int(trace.start_ns // 1_000_000_000)
        time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    return _clamp(0.45 * incident_hit + 0.25 * lat_norm + 0.15 * err_flag + 0.15 * time_prox, 0.0, 1.0)


def _contrast_score(trace: TracePoint, global_error_ratio: float) -> float:
    local_ratio = _local_error_ratio(trace)
    return _clamp(abs(local_ratio - global_error_ratio), 0.0, 1.0)


def _diversity_score(tokens: FrozenSet[str], reference_tokens: List[FrozenSet[str]]) -> float:
    if not reference_tokens:
        return 1.0
    max_similarity = max(_jaccard(tokens, ref_tokens) for ref_tokens in reference_tokens)
    return _clamp(1.0 - max_similarity, 0.0, 1.0)


def _min_error_count(traces: List[TracePoint], target_count: int) -> int:
    if not traces or target_count <= 0:
        return 0
    base_error_ratio = sum(1 for trace in traces if trace.has_error) / float(len(traces))
    min_err_ratio = _clamp(max(0.08, base_error_ratio * 1.2), 0.08, 0.40)
    return int(round(target_count * min_err_ratio))


def stream_sampler_v13(
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
    swap_ratio: float = 0.10,
    max_swap_ratio: float = 0.15,
    normal_ratio_min: float = 0.30,
    incident_window_sec: int = 60,
    debug_stats: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    incident_services = incident_services or set()

    # Step 1: run the full v36 stable core unchanged.
    kept_ids = sv36._composite_v3_metrics_strictcap(
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
        enable_service_quota=False,
    )

    traces = sorted(points, key=lambda trace: (trace.start_ns, trace.trace_id))
    total = len(traces)
    target_count = len(kept_ids)
    if total == 0 or target_count == 0:
        if debug_stats is not None:
            debug_stats.clear()
            debug_stats.update(
                {
                    "target_count": target_count,
                    "swap_quota": 0,
                    "swap_count": 0,
                    "min_error_count": 0,
                    "min_normal_count": 0,
                    "global_error_ratio": 0.0,
                    "contrast_min": 0.0,
                    "contrast_max": 0.0,
                }
            )
        return kept_ids

    by_id: Dict[str, TracePoint] = {trace.trace_id: trace for trace in traces}
    all_ids = {trace.trace_id for trace in traces}

    dur_lo = min(trace.duration_ms for trace in traces)
    dur_hi = max(trace.duration_ms for trace in traces)
    span_lo = min(trace.span_count for trace in traces)
    span_hi = max(trace.span_count for trace in traces)

    global_error_ratio = _compute_global_error_ratio(traces, incident_anchor_sec, max(1, int(incident_window_sec)))

    features: Dict[str, Dict[str, Any]] = {}
    for trace in traces:
        system = _system_score(trace, incident_services, incident_anchor_sec, dur_lo, dur_hi, span_lo, span_hi)
        contrast = _contrast_score(trace, global_error_ratio)
        features[trace.trace_id] = {
            "system_score": system,
            "contrast_score": contrast,
            "tokens": _trace_tokens(trace),
        }

    min_err_count = _min_error_count(traces, target_count)
    requested_min_normal_count = int(math.ceil(target_count * _clamp(normal_ratio_min, 0.0, 1.0)))

    swap_ratio_eff = _clamp(float(swap_ratio), 0.0, float(max_swap_ratio))
    max_swap_count = max(1, int(target_count * _clamp(float(max_swap_ratio), 0.0, 1.0)))
    swap_quota = max(1, int(target_count * swap_ratio_eff))
    swap_quota = min(swap_quota, max_swap_count)

    initial_err_count = sum(1 for trace_id in kept_ids if by_id[trace_id].has_error)
    initial_normal_count = target_count - initial_err_count
    feasible_normal_from_error_floor = max(0, target_count - min_err_count)
    reachable_normal_from_quota = initial_normal_count + swap_quota
    # With bounded swaps, enforce the highest reachable normal floor without violating error floor.
    min_normal_count = min(
        requested_min_normal_count,
        feasible_normal_from_error_floor,
        reachable_normal_from_quota,
    )

    swap_count = 0

    while swap_count < swap_quota:
        kept_list = sorted(kept_ids)
        kept_tokens = [features[trace_id]["tokens"] for trace_id in kept_list]

        err_count = sum(1 for trace_id in kept_ids if by_id[trace_id].has_error)
        normal_count = len(kept_ids) - err_count
        normal_deficit = max(0, min_normal_count - normal_count)
        err_deficit = max(0, min_err_count - err_count)
        force_add_normal = normal_deficit > 0
        force_add_error = (not force_add_normal) and (err_deficit > 0)

        weak_candidates: List[Tuple[int, float, str]] = []
        for trace_id in kept_list:
            trace = by_id[trace_id]
            trace_tokens = features[trace_id]["tokens"]

            if force_add_normal and (not trace.has_error):
                continue
            if force_add_error and trace.has_error:
                continue

            if trace.has_error and err_count <= min_err_count:
                continue
            if (not trace.has_error) and normal_count <= min_normal_count:
                continue

            redundancy_refs = [
                features[other_id]["tokens"]
                for other_id in kept_list
                if other_id != trace_id
            ]
            redundancy_penalty = 1.0 - _diversity_score(trace_tokens, redundancy_refs)

            system_score = float(features[trace_id]["system_score"])
            contrast_score = float(features[trace_id]["contrast_score"])

            weak_score = (
                0.50 * (1.0 - system_score)
                + 0.30 * redundancy_penalty
                + 0.20 * (1.0 - contrast_score)
            )

            if force_add_normal:
                normal_priority = 0 if trace.has_error else 1
            elif force_add_error:
                normal_priority = 0 if (not trace.has_error) else 1
            else:
                normal_priority = 0 if ((not trace.has_error) and (normal_count > min_normal_count)) else 1
            weak_candidates.append((normal_priority, -weak_score, trace_id))

        if not weak_candidates:
            break

        weak_candidates.sort()
        remove_id = weak_candidates[0][2]

        rejected_ids = sorted(all_ids - kept_ids)
        if not rejected_ids:
            break

        strong_candidates: List[Tuple[float, str]] = []
        for trace_id in rejected_ids:
            if force_add_normal and by_id[trace_id].has_error:
                continue
            if force_add_error and (not by_id[trace_id].has_error):
                continue

            trace_tokens = features[trace_id]["tokens"]
            replacement_score = (
                0.45 * float(features[trace_id]["system_score"])
                + 0.30 * _diversity_score(trace_tokens, kept_tokens)
                + 0.25 * float(features[trace_id]["contrast_score"])
            )
            strong_candidates.append((-replacement_score, trace_id))

        if not strong_candidates:
            break

        strong_candidates.sort()
        add_id = strong_candidates[0][1]

        if add_id == remove_id:
            break

        kept_ids.remove(remove_id)
        kept_ids.add(add_id)
        swap_count += 1

    if debug_stats is not None:
        contrast_values = [float(features[trace_id]["contrast_score"]) for trace_id in features]
        final_err = sum(1 for trace_id in kept_ids if by_id[trace_id].has_error)
        final_normal = len(kept_ids) - final_err
        debug_stats.clear()
        debug_stats.update(
            {
                "target_count": target_count,
                "swap_quota": swap_quota,
                "swap_count": swap_count,
                "min_error_count": min_err_count,
                "min_normal_count": min_normal_count,
                "requested_min_normal_count": requested_min_normal_count,
                "initial_error_count": initial_err_count,
                "initial_normal_count": initial_normal_count,
                "final_error_count": final_err,
                "final_normal_count": final_normal,
                "global_error_ratio": global_error_ratio,
                "contrast_min": min(contrast_values) if contrast_values else 0.0,
                "contrast_max": max(contrast_values) if contrast_values else 0.0,
            }
        )

    return kept_ids
