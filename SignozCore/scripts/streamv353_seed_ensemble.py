"""
Stream v3.53 -- seed-robust ensemble on v3.5 baseline.

Runs the v3.5 sampler with 5 different seeds, unions all kept sets,
then trims to target_count by score ranking. This reduces random seed
variance which is dominant at 0.1% budget (~10 traces).
"""

from typing import Any, Set, List, Tuple, Dict, Optional

import streamv36 as sv36comp
from streamv36 import TracePoint, PreferenceVector, MetricStats, build_metric_inputs, _clamp


def _composite_v353_seed_ensemble(
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
    n_seeds: int = 5,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    total = len(points)
    if total == 0:
        return set()
    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    target_count = max(1, min(total, int(round(total * beta))))

    # Run base sampler with multiple seeds and collect all kept IDs
    all_kept: Dict[str, int] = {}  # trace_id -> count of seeds that kept it
    for i in range(n_seeds):
        run_seed = seed + i
        kept = sv36comp._composite_v3_metrics_strictcap(
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
            enable_service_quota=False,
        )
        for tid in kept:
            all_kept[tid] = all_kept.get(tid, 0) + 1

    # Rank by (vote count desc, has_error desc, trace_id)
    by_id = {p.trace_id: p for p in points}
    ranked = sorted(
        all_kept.keys(),
        key=lambda tid: (
            -all_kept[tid],
            -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
            tid,
        ),
    )
    return set(ranked[:target_count])
