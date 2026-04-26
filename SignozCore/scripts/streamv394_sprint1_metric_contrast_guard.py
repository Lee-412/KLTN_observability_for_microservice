"""
Stream v3.94 Sprint 1 -- real-metric injected consensus + contrast guard.

Backbone: v3.94 weighted consensus ensemble on v3.9.
Sprint 1 scope (low-risk, high-gain):
1) Pass real metric signal into each inner v3.9 ensemble member.
2) Apply contrast guard minimum floor (normal/error) before final truncation.
3) Emit telemetry for metric signal quality, contrast balance, and ensemble disagreement.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import streamv39_tuned_rca as sv39comp
from streamv39_tuned_rca import TracePoint, PreferenceVector, MetricStats, _clamp, _rca_quadrant_value


MIN_NORMAL_RATIO = 0.35
MIN_ERROR_RATIO = 0.25


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _std(values: List[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mu = _mean(values)
    var = sum((v - mu) * (v - mu) for v in values) / float(len(values))
    return var ** 0.5


def build_metric_inputs(
    points: List[TracePoint],
) -> Tuple[PreferenceVector, MetricStats]:
    """Build non-stub metric inputs from trace-level service proxies.

    This preserves v3.9 interfaces while avoiding empty {} inputs.
    """
    if not points:
        return {}, {}

    per_service: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "trace_count": 0,
            "error_count": 0,
            "duration_sum": 0.0,
            "per_sec_count": defaultdict(int),
        }
    )

    traces_sorted = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    for p in traces_sorted:
        sec = int(p.start_ns // 1_000_000_000)
        for svc in p.services:
            stat = per_service[svc]
            stat["trace_count"] += 1
            stat["duration_sum"] += float(p.duration_ms)
            stat["per_sec_count"][sec] += 1
            if p.has_error:
                stat["error_count"] += 1

    if not per_service:
        return {}, {}

    svc_names = sorted(per_service.keys())
    svc_mean_duration: Dict[str, float] = {}
    svc_error_rate: Dict[str, float] = {}
    svc_qps_mean: Dict[str, float] = {}
    svc_burst: Dict[str, float] = {}

    dur_vals: List[float] = []
    err_vals: List[float] = []
    qps_vals: List[float] = []
    burst_vals: List[float] = []

    for svc in svc_names:
        stat = per_service[svc]
        trace_count = max(1, int(stat["trace_count"]))
        mean_dur = float(stat["duration_sum"]) / float(trace_count)
        err_rate = float(stat["error_count"]) / float(trace_count)

        sec_counts = list(stat["per_sec_count"].values())
        if sec_counts:
            qps_mean = _mean([float(v) for v in sec_counts])
            sorted_counts = sorted(float(v) for v in sec_counts)
            p95_idx = int(0.95 * (len(sorted_counts) - 1))
            qps_p95 = sorted_counts[p95_idx]
            burst = max(0.0, qps_p95 - qps_mean)
        else:
            qps_mean = 0.0
            burst = 0.0

        svc_mean_duration[svc] = mean_dur
        svc_error_rate[svc] = err_rate
        svc_qps_mean[svc] = qps_mean
        svc_burst[svc] = burst

        dur_vals.append(mean_dur)
        err_vals.append(err_rate)
        qps_vals.append(qps_mean)
        burst_vals.append(burst)

    dur_mu, dur_std = _mean(dur_vals), _std(dur_vals)
    err_mu, err_std = _mean(err_vals), _std(err_vals)
    qps_mu, qps_std = _mean(qps_vals), _std(qps_vals)
    burst_mu, burst_std = _mean(burst_vals), _std(burst_vals)

    def _pos_z(v: float, mu: float, sigma: float, eps: float = 1e-3) -> float:
        return _clamp((v - mu) / (sigma + eps), 0.0, 10.0)

    preference_vector: PreferenceVector = {}
    metric_stats: MetricStats = {}

    for svc in svc_names:
        preference_vector[svc] = {
            "latency": _pos_z(svc_mean_duration[svc], dur_mu, dur_std),
            "error": _pos_z(svc_error_rate[svc], err_mu, err_std),
            "throughput": _pos_z(svc_qps_mean[svc], qps_mu, qps_std),
            "burst": _pos_z(svc_burst[svc], burst_mu, burst_std),
        }
        metric_stats[svc] = {
            "history_count": int(per_service[svc]["trace_count"]),
            "mean_duration_ms": svc_mean_duration[svc],
            "error_rate": svc_error_rate[svc],
            "qps_mean": svc_qps_mean[svc],
            "burst": svc_burst[svc],
        }

    return preference_vector, metric_stats


def _metric_boost_distribution(
    points: List[TracePoint],
    preference_vector: PreferenceVector,
    metric_stats: MetricStats,
) -> Tuple[float, float, float]:
    if not points or not preference_vector:
        return 0.0, 0.0, 0.0

    boosts: List[float] = []
    non_zero = 0
    for p in points:
        trace_metric_score, coverage_ratio, signal_entropy = sv39comp._compute_metric_signal_details(
            p,
            preference_vector,
            metric_stats,
        )
        lambda_gate = _clamp(signal_entropy / 3.0, 0.2, 1.0)
        lambda_eff = 0.2 * coverage_ratio * sv39comp._sigmoid(2.0 * (trace_metric_score - 1.5)) * lambda_gate
        metric_boost = min(0.4, lambda_eff * trace_metric_score)
        boosts.append(metric_boost)
        if metric_boost > 1e-9:
            non_zero += 1

    avg_boost = _mean(boosts)
    max_boost = max(boosts) if boosts else 0.0
    pct_non_zero = (non_zero / float(len(boosts))) * 100.0 if boosts else 0.0
    return avg_boost, max_boost, pct_non_zero


def _apply_contrast_guard(
    global_ranked: List[str],
    by_id: Dict[str, TracePoint],
    target_count: int,
    min_normal_ratio: float,
    min_error_ratio: float,
) -> Tuple[List[str], int, int]:
    if target_count <= 0 or not global_ranked:
        return [], 0, 0

    min_normal = max(0, min(target_count, int(target_count * min_normal_ratio)))
    min_error = max(0, min(target_count, int(target_count * min_error_ratio)))

    normal_ranked = [tid for tid in global_ranked if by_id.get(tid) and (not by_id[tid].has_error)]
    error_ranked = [tid for tid in global_ranked if by_id.get(tid) and by_id[tid].has_error]

    guaranteed_ids: Set[str] = set(normal_ranked[:min_normal])
    guaranteed_ids.update(error_ranked[:min_error])

    result: List[str] = []
    seen: Set[str] = set()

    for tid in global_ranked:
        if tid in guaranteed_ids and tid not in seen:
            result.append(tid)
            seen.add(tid)

    for tid in global_ranked:
        if len(result) >= target_count:
            break
        if tid in seen:
            continue
        result.append(tid)
        seen.add(tid)

    return result[:target_count], min_normal, min_error


def _composite_v394_sprint1_metric_contrast_guard(
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
    n_seeds: int = 5,
    min_consensus: int = 2,
    metrics_stream_by_sec: Optional[Dict[int, Any]] = None,
    metric_lookback_sec: int = 20,
    min_normal_ratio: float = MIN_NORMAL_RATIO,
    min_error_ratio: float = MIN_ERROR_RATIO,
    emit_debug: bool = True,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    total = len(points)
    if total == 0:
        return set()
    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    target_count = max(1, min(total, int(round(total * beta))))

    by_id = {p.trace_id: p for p in points}
    metrics_stream_by_sec = metrics_stream_by_sec or {}
    metric_stats = metric_stats or {}

    pref_service_count = len(preference_vector)
    pref_dim_total = sum(len(vec) for vec in preference_vector.values())
    pref_non_zero_dims = sum(1 for vec in preference_vector.values() for v in vec.values() if abs(float(v)) > 1e-9)
    avg_metric_boost, max_metric_boost, pct_metric_boost_non_zero = _metric_boost_distribution(
        points,
        preference_vector,
        metric_stats,  # type: ignore[arg-type]
    )

    # ---------------------------------------------------------------
    # Phase 1: Run base sampler with multiple seeds, collect votes
    # ---------------------------------------------------------------
    all_kept: Dict[str, int] = {}  # trace_id -> vote count
    member_kept_sizes: List[float] = []
    for i in range(n_seeds):
        run_seed = seed + i
        kept = sv39comp._composite_v39_tuned_rca(
            points=points,
            budget_pct=budget_pct,
            preference_vector=preference_vector,
            scenario_windows=scenario_windows,
            incident_anchor_sec=incident_anchor_sec,
            seed=run_seed,
            metric_stats=metric_stats,
            incident_services=incident_services,
            min_incident_traces_per_scenario=min_incident_traces_per_scenario,
            online_soft_cap=online_soft_cap,
            lookback_sec=lookback_sec,
            alpha=alpha,
            gamma=gamma,
            anomaly_weight=anomaly_weight,
            rca_weight=rca_weight,
            high_error_service_threshold=high_error_service_threshold,
            metrics_stream_by_sec=metrics_stream_by_sec,
            metric_lookback_sec=metric_lookback_sec,
        )
        member_kept_sizes.append(float(len(kept)))
        for tid in kept:
            all_kept[tid] = all_kept.get(tid, 0) + 1

    # ---------------------------------------------------------------
    # Phase 2: Compute RCA quadrant values for all voted traces
    # (deterministic pass, independent of seed)
    # ---------------------------------------------------------------
    traces_sorted = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    incident_services = incident_services or set()

    # Adaptive suspicious threshold (same as v3.9)
    min_suspicious_history = 4 if beta <= 0.0015 else 8

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_trace_count: Dict[str, int] = defaultdict(int)
    rca_value_by_id: Dict[str, float] = {}

    for p in traces_sorted:
        # Build dynamic suspicious set (same logic as base sampler)
        dynamic_suspicious = set(incident_services)
        for svc in p.services:
            if svc_trace_count[svc] > min_suspicious_history:
                err_rate = svc_error_trace_count[svc] / svc_trace_count[svc]
                if err_rate > high_error_service_threshold:
                    dynamic_suspicious.add(svc)

        suspicious_hit = len(set(p.services) & dynamic_suspicious)
        suspicious_ratio = suspicious_hit / max(1, len(p.services))
        rca_value = _rca_quadrant_value(p.has_error, suspicious_hit, suspicious_ratio)
        rca_value_by_id[p.trace_id] = rca_value

        # Update counters (must happen after read, same as base)
        for svc in p.services:
            svc_trace_count[svc] += 1
            if p.has_error:
                svc_error_trace_count[svc] += 1

    # ---------------------------------------------------------------
    # Phase 3: Consensus-weighted ranking and selection
    # ---------------------------------------------------------------
    # Composite score: vote confidence * (rca_value + error bonus)
    def _consensus_score(tid: str) -> float:
        vote = all_kept.get(tid, 0)
        vote_frac = vote / n_seeds  # 0.0 to 1.0
        rca_val = rca_value_by_id.get(tid, 0.40)
        err_bonus = 0.15 if (by_id.get(tid) and by_id[tid].has_error) else 0.0
        return vote_frac * (rca_val + err_bonus)

    # Separate into consensus pool (>= min_consensus) and remainder
    consensus_pool = [tid for tid in all_kept if all_kept[tid] >= min_consensus]
    remainder_pool = [tid for tid in all_kept if all_kept[tid] < min_consensus]

    # Sort each pool by consensus score descending
    consensus_pool.sort(key=lambda tid: (-_consensus_score(tid), tid))
    remainder_pool.sort(key=lambda tid: (-_consensus_score(tid), tid))
    global_ranked = consensus_pool + remainder_pool

    # Pre-guard (original v3.94 behavior, for telemetry only)
    pre_guard: List[str] = []
    for tid in consensus_pool:
        if len(pre_guard) >= target_count:
            break
        pre_guard.append(tid)
    if len(pre_guard) < target_count:
        for tid in remainder_pool:
            if len(pre_guard) >= target_count:
                break
            pre_guard.append(tid)

    before_normal = sum(1 for tid in pre_guard if by_id.get(tid) and (not by_id[tid].has_error))
    before_error = sum(1 for tid in pre_guard if by_id.get(tid) and by_id[tid].has_error)

    # Sprint 1: contrast guard floor before final truncation.
    guarded_ranked, min_normal, min_error = _apply_contrast_guard(
        global_ranked=global_ranked,
        by_id=by_id,
        target_count=target_count,
        min_normal_ratio=min_normal_ratio,
        min_error_ratio=min_error_ratio,
    )
    result = set(guarded_ranked)

    after_normal = sum(1 for tid in guarded_ranked if by_id.get(tid) and (not by_id[tid].has_error))
    after_error = sum(1 for tid in guarded_ranked if by_id.get(tid) and by_id[tid].has_error)
    kept_total = max(1, len(guarded_ranked))
    after_normal_ratio = after_normal / float(kept_total)
    after_error_ratio = after_error / float(kept_total)

    vote_fracs = [all_kept[tid] / float(n_seeds) for tid in all_kept]
    vote_confidence = _mean(vote_fracs)
    vote_variance = _std(vote_fracs) ** 2 if vote_fracs else 0.0
    member_variance = _std(member_kept_sizes) ** 2 if member_kept_sizes else 0.0

    if emit_debug:
        print(
            "[v394-s1][metric] "
            f"pref_services={pref_service_count} "
            f"pref_dims={pref_dim_total} "
            f"pref_non_zero_dims={pref_non_zero_dims} "
            f"boost_avg={avg_metric_boost:.6f} "
            f"boost_max={max_metric_boost:.6f} "
            f"boost_non_zero_pct={pct_metric_boost_non_zero:.2f}"
        )
        print(
            "[v394-s1][ensemble] "
            f"n_seeds={n_seeds} "
            f"member_variance={member_variance:.6f} "
            f"vote_variance={vote_variance:.6f} "
            f"consensus_confidence={vote_confidence:.6f}"
        )
        print(
            "[v394-s1][contrast][before] "
            f"selected={len(pre_guard)} normal={before_normal} error={before_error}"
        )
        print(
            "[v394-s1][contrast][after] "
            f"selected={len(guarded_ranked)} "
            f"min_normal={min_normal} min_error={min_error} "
            f"normal_ratio={after_normal_ratio:.4f} error_ratio={after_error_ratio:.4f}"
        )

    if debug_trace_logs is not None:
        debug_trace_logs.append(
            {
                "pref_services": pref_service_count,
                "pref_dims": pref_dim_total,
                "pref_non_zero_dims": pref_non_zero_dims,
                "metric_boost_avg": avg_metric_boost,
                "metric_boost_max": max_metric_boost,
                "metric_boost_non_zero_pct": pct_metric_boost_non_zero,
                "member_variance": member_variance,
                "vote_variance": vote_variance,
                "consensus_confidence": vote_confidence,
                "pre_guard_normal": before_normal,
                "pre_guard_error": before_error,
                "post_guard_normal_ratio": after_normal_ratio,
                "post_guard_error_ratio": after_error_ratio,
                "target_count": target_count,
                "kept_count": len(guarded_ranked),
            }
        )

    return result


def _composite_v394_consensus_ensemble(
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
    n_seeds: int = 5,
    min_consensus: int = 2,
    metrics_stream_by_sec: Optional[Dict[int, Any]] = None,
    metric_lookback_sec: int = 20,
    min_normal_ratio: float = MIN_NORMAL_RATIO,
    min_error_ratio: float = MIN_ERROR_RATIO,
    emit_debug: bool = True,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    return _composite_v394_sprint1_metric_contrast_guard(
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
        rca_weight=rca_weight,
        high_error_service_threshold=high_error_service_threshold,
        n_seeds=n_seeds,
        min_consensus=min_consensus,
        metrics_stream_by_sec=metrics_stream_by_sec,
        metric_lookback_sec=metric_lookback_sec,
        min_normal_ratio=min_normal_ratio,
        min_error_ratio=min_error_ratio,
        emit_debug=emit_debug,
        debug_trace_logs=debug_trace_logs,
    )
