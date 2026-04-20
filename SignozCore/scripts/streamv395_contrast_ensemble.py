"""
Stream v3.95 — Contrast-Aware Diverse Ensemble.

Key improvement over v3.94 (consensus ensemble with identical configs):
  v3.94 runs the SAME v3.9 sampler with 5 different seeds — "5 biased copies"
  converge to the same systematic bias.  v3.95 uses 3 structurally DIVERSE
  member configs, each biased toward a different aspect of the trace
  population, then merges with contrast-aware stratified selection.

Design:
  Member A (Error-biased):  rca_weight=0.55, normal_ratio=0.22, err_mult=1.6
    → Collects more gold (error + suspicious) traces.
  Member B (Normal-biased): rca_weight=0.25, normal_ratio=0.42, gamma=1.0
    → Collects more clean-normal (silver) traces for MicroRank contrast.
  Member C (Balanced):      rca_weight=0.40, normal_ratio=0.30 (standard v3.9)
    → Baseline diversity.

Each member runs with 2 seeds → 6 total runs.

Merge strategy:
  1. Collect vote counts per trace across all 6 runs.
  2. Separate into error pool and normal pool.
  3. Compute target error ratio from population, clamped to [12%, 35%].
  4. Fill error quota from error pool (ranked by contrast score).
  5. Fill normal quota from normal pool (ranked by contrast score).
  6. Post-hoc: same scenario floor enforcement as v3.9.

Rationale: MicroRank needs error-vs-success contrast — too many error
traces is as harmful as too few.  By assembling members with different
biases and merging with explicit ratio control, we provide MicroRank
with a balanced spectrum regardless of which member "wins" overall.
"""

import math
from typing import Any, Set, List, Tuple, Dict, Optional
from collections import defaultdict

import streamv39_tuned_rca as sv39comp
from streamv39_tuned_rca import (
    TracePoint, PreferenceVector, MetricStats, build_metric_inputs,
    _clamp, _rca_quadrant_value,
)


# ---------------------------------------------------------------------------
# Member configurations
# ---------------------------------------------------------------------------
_MEMBER_CONFIGS = [
    {   # Member A: Error-biased — more gold traces
        "name": "error_biased",
        "rca_weight": 0.55,
        "gamma": 0.8,
        "high_error_service_threshold": 0.12,
        "n_seeds": 2,
    },
    {   # Member B: Normal-biased — more clean contrast traces
        "name": "normal_biased",
        "rca_weight": 0.25,
        "gamma": 1.0,
        "high_error_service_threshold": 0.18,
        "n_seeds": 2,
    },
    {   # Member C: Balanced — standard v3.9
        "name": "balanced",
        "rca_weight": 0.40,
        "gamma": 0.8,
        "high_error_service_threshold": 0.15,
        "n_seeds": 2,
    },
]


def _composite_v395_contrast_ensemble(
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
    anomaly_weight: float = 0.7,
    rca_weight: float = 0.40,          # ignored — each member uses its own
    high_error_service_threshold: float = 0.15,  # ignored — per-member
    target_error_lo: float = 0.12,
    target_error_hi: float = 0.35,
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
    # Phase 1: Run diverse members, each with multiple seeds
    # ---------------------------------------------------------------
    all_kept: Dict[str, int] = {}          # trace_id -> total vote count
    member_kept: Dict[str, Set[str]] = {}  # member_name -> union of kept across seeds
    total_runs = 0

    seed_offset = 0
    for cfg in _MEMBER_CONFIGS:
        member_union: Set[str] = set()
        for i in range(cfg["n_seeds"]):
            run_seed = seed + seed_offset
            seed_offset += 1
            total_runs += 1

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
                gamma=cfg["gamma"],
                anomaly_weight=anomaly_weight,
                rca_weight=cfg["rca_weight"],
                high_error_service_threshold=cfg["high_error_service_threshold"],
            )
            for tid in kept:
                all_kept[tid] = all_kept.get(tid, 0) + 1
            member_union |= kept
        member_kept[cfg["name"]] = member_union

    # ---------------------------------------------------------------
    # Phase 2: Compute RCA quadrant values (deterministic)
    # ---------------------------------------------------------------
    by_id = {p.trace_id: p for p in points}
    traces_sorted = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    incident_services = incident_services or set()

    min_suspicious_history = 4 if beta <= 0.0015 else 8

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_trace_count: Dict[str, int] = defaultdict(int)
    rca_value_by_id: Dict[str, float] = {}

    for p in traces_sorted:
        dynamic_suspicious = set(incident_services)
        for svc in p.services:
            if svc_trace_count[svc] > min_suspicious_history:
                err_rate = svc_error_trace_count[svc] / svc_trace_count[svc]
                if err_rate > 0.15:
                    dynamic_suspicious.add(svc)
        suspicious_hit = len(set(p.services) & dynamic_suspicious)
        suspicious_ratio = suspicious_hit / max(1, len(p.services))
        rca_value = _rca_quadrant_value(p.has_error, suspicious_hit, suspicious_ratio)
        rca_value_by_id[p.trace_id] = rca_value
        for svc in p.services:
            svc_trace_count[svc] += 1
            if p.has_error:
                svc_error_trace_count[svc] += 1

    # ---------------------------------------------------------------
    # Phase 3: Contrast-aware stratified merge
    # ---------------------------------------------------------------

    # Diversity bonus: trace selected by multiple DIFFERENT members
    def _member_diversity(tid: str) -> int:
        return sum(1 for m in member_kept.values() if tid in m)

    def _contrast_score(tid: str) -> float:
        vote = all_kept.get(tid, 0)
        vote_frac = vote / total_runs
        rca_val = rca_value_by_id.get(tid, 0.40)
        err_bonus = 0.15 if (by_id.get(tid) and by_id[tid].has_error) else 0.0
        div_bonus = 0.10 * (_member_diversity(tid) - 1)  # +0.1 per extra member
        return vote_frac * (rca_val + err_bonus) + div_bonus

    # Separate voted traces into error and normal pools
    error_pool = [tid for tid in all_kept if by_id.get(tid) and by_id[tid].has_error]
    normal_pool = [tid for tid in all_kept if by_id.get(tid) and not by_id[tid].has_error]

    error_pool.sort(key=lambda tid: (-_contrast_score(tid), tid))
    normal_pool.sort(key=lambda tid: (-_contrast_score(tid), tid))

    # Compute target error ratio from population, clamped to sweet spot
    pop_err_ratio = sum(1 for p in points if p.has_error) / total if total > 0 else 0.0
    target_err_ratio = _clamp(pop_err_ratio, target_error_lo, target_error_hi)

    # Budget allocation
    err_quota = max(1, int(round(target_count * target_err_ratio)))
    norm_quota = target_count - err_quota

    # Fill from each pool
    result: List[str] = []

    # Error traces
    for tid in error_pool:
        if len(result) >= err_quota:
            break
        result.append(tid)
    err_filled = len(result)

    # Normal traces
    norm_added = 0
    for tid in normal_pool:
        if norm_added >= norm_quota:
            break
        result.append(tid)
        norm_added += 1

    # If either pool was exhausted, fill remainder from the other
    if len(result) < target_count:
        remaining_err = [tid for tid in error_pool if tid not in set(result)]
        remaining_norm = [tid for tid in normal_pool if tid not in set(result)]
        combined = remaining_err + remaining_norm
        combined.sort(key=lambda tid: (-_contrast_score(tid), tid))
        for tid in combined:
            if len(result) >= target_count:
                break
            result.append(tid)

    # If still short (very unlikely), add un-voted traces by score heuristic
    if len(result) < target_count:
        in_result = set(result)
        unvoted = [p.trace_id for p in traces_sorted if p.trace_id not in in_result and p.trace_id not in all_kept]
        for tid in unvoted:
            if len(result) >= target_count:
                break
            result.append(tid)

    final_ids = set(result[:target_count])

    # ---------------------------------------------------------------
    # Phase 4: Scenario floor enforcement (same as v3.9)
    # ---------------------------------------------------------------
    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        return final_ids

    start_s_by_id = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}
    scenario_ids_by_idx: List[Set[str]] = []
    scenario_membership: Dict[str, Set[int]] = defaultdict(set)
    non_empty_scenarios = 0

    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, s in start_s_by_id.items() if st <= s <= ed}
        scenario_ids_by_idx.append(ids)
        if ids:
            non_empty_scenarios += 1
            for tid in ids:
                scenario_membership[tid].add(idx)

    if non_empty_scenarios == 0:
        return final_ids

    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    if floor_each > 0:
        for idx, ids in enumerate(scenario_ids_by_idx):
            if not ids:
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(key=lambda tid: (-_contrast_score(tid), tid))
            for tid in candidates[:need]:
                final_ids.add(tid)

    if len(final_ids) <= target_count:
        return final_ids

    # Trim back to target while respecting scenario floors
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

    removable = sorted(final_ids, key=lambda tid: (_contrast_score(tid), tid))
    for tid in removable:
        if len(final_ids) <= target_count:
            break
        if not _can_remove(tid):
            continue
        final_ids.remove(tid)
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] -= 1

    return final_ids
