"""
Stream v3.94 -- weighted consensus ensemble on v3.9 baseline.

Key improvement over v3.93 (naive union + vote ranking):
1. Minimum consensus filter: only keep traces with vote_count >= min_consensus
   (default 2 out of 5 seeds). Filter out "lucky" single-seed anomalies.
2. Weighted ranking: combine vote_count with RCA quadrant value and error flag
   for tie-breaking, instead of just vote_count + has_error.
3. If consensus pool is too small, relax to vote >= 1 for remaining slots.

This should squeeze 0.5-1.0 pp over v3.93 by removing noise traces that
got through on random luck of a single seed.
"""

from typing import Any, Set, List, Tuple, Dict, Optional
from collections import defaultdict

import streamv39_tuned_rca as sv39comp
from streamv39_tuned_rca import (
    TracePoint, PreferenceVector, MetricStats, build_metric_inputs,
    _clamp, _rca_quadrant_value,
)


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
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    total = len(points)
    if total == 0:
        return set()
    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    target_count = max(1, min(total, int(round(total * beta))))

    # ---------------------------------------------------------------
    # Phase 1: Run base sampler with multiple seeds, collect votes
    # ---------------------------------------------------------------
    all_kept: Dict[str, int] = {}  # trace_id -> vote count
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
        )
        for tid in kept:
            all_kept[tid] = all_kept.get(tid, 0) + 1

    # ---------------------------------------------------------------
    # Phase 2: Compute RCA quadrant values for all voted traces
    # (deterministic pass, independent of seed)
    # ---------------------------------------------------------------
    by_id = {p.trace_id: p for p in points}
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

    # Fill from consensus first, then remainder if needed
    result = []
    for tid in consensus_pool:
        if len(result) >= target_count:
            break
        result.append(tid)

    if len(result) < target_count:
        for tid in remainder_pool:
            if len(result) >= target_count:
                break
            result.append(tid)

    return set(result)
