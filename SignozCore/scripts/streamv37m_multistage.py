"""
Stream v3.7m — Multi-stage sampler (Direction C).

Stage 1: Run v3.5 at ``coarse_multiplier × budget`` to capture a broad
         high-quality pool.
Stage 2: Re-score every trace in that pool by RCA contrast value and
         trim to the actual target budget, preferring traces that
         optimise MicroRank's spectrum analysis (ef/ep contrast).

Scenario-floor constraints from v3.5 are honoured in both stages.
"""

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ── RCA quadrant scoring (same as v3.7) ──────────────────────────────────

def _rca_quadrant_value(
    has_error: bool,
    suspicious_hit: int,
    suspicious_ratio: float,
) -> float:
    if has_error and suspicious_hit > 0:
        return 0.85 + 0.15 * suspicious_ratio           # Gold
    elif (not has_error) and suspicious_hit == 0:
        return 0.55                                       # Silver
    elif has_error and suspicious_hit == 0:
        return 0.10                                       # Noise
    else:
        return 0.35                                       # Mixed


# ── Main entry ────────────────────────────────────────────────────────────

def _composite_v37m_multistage(
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
    coarse_multiplier: float = 3.0,
    rca_time_decay: float = 60.0,
    high_error_service_threshold: float = 0.15,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    """Two-stage: broad capture → RCA-contrast refinement."""
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    target_count = max(1, min(total, int(round(total * beta))))
    incident_services = incident_services or set()

    # ── Stage 1: coarse capture via frozen v3.5 ──────────────────────────
    coarse_budget = min(budget_pct * coarse_multiplier, 100.0)
    coarse_kept = sv35comp._composite_v3_metrics_strictcap(
        points=points,
        budget_pct=coarse_budget,
        preference_vector=preference_vector,
        metric_stats=metric_stats,
        scenario_windows=scenario_windows,
        incident_anchor_sec=incident_anchor_sec,
        seed=seed,
        incident_services=incident_services,
        min_incident_traces_per_scenario=min_incident_traces_per_scenario,
        online_soft_cap=online_soft_cap,
    )

    if len(coarse_kept) <= target_count:
        return coarse_kept

    # ── Build dynamic suspicious set ─────────────────────────────────────
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in points}
    t0_sec = min(p.start_ns for p in points) // 1_000_000_000

    svc_total: Dict[str, int] = defaultdict(int)
    svc_error: Dict[str, int] = defaultdict(int)
    for p in points:
        for svc in p.services:
            svc_total[svc] += 1
            if p.has_error:
                svc_error[svc] += 1

    suspicious: Set[str] = set(incident_services)
    for svc, cnt in svc_total.items():
        if cnt > 10 and svc_error.get(svc, 0) / cnt > high_error_service_threshold:
            suspicious.add(svc)

    # ── Stage 2: score each coarse trace by RCA contrast ─────────────────
    rca_scores: Dict[str, float] = {}
    for tid in coarse_kept:
        p = by_id.get(tid)
        if p is None:
            rca_scores[tid] = 0.0
            continue

        suspicious_hit = len(set(p.services) & suspicious)
        suspicious_ratio = suspicious_hit / max(1, len(p.services))
        contrast = _rca_quadrant_value(p.has_error, suspicious_hit, suspicious_ratio)

        if incident_anchor_sec is not None:
            sec = int(p.start_ns // 1_000_000_000) - t0_sec
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / rca_time_decay)
        else:
            time_prox = 0.5

        rca_scores[tid] = (
            0.65 * contrast
            + 0.25 * time_prox
            + 0.10 * (1.0 if p.has_error else 0.0)
        )

    # ── Scenario-floor-aware trimming ────────────────────────────────────
    start_s_by_id: Dict[str, int] = {
        p.trace_id: int(p.start_ns // 1_000_000_000)
        for p in points
    }
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

    floor_each = min_incident_traces_per_scenario
    if non_empty_scenarios > 0 and target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    # Start with coarse_kept, ensure scenario floors
    final_ids: Set[str] = set(coarse_kept)
    if floor_each > 0:
        for idx, ids in enumerate(scenario_ids_by_idx):
            if not ids:
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(key=lambda tid: rca_scores.get(tid, 0.0), reverse=True)
            for tid in candidates[:need]:
                final_ids.add(tid)

    # Trim to target_count by removing lowest-RCA traces
    if len(final_ids) > target_count:
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

        removable = sorted(final_ids, key=lambda tid: rca_scores.get(tid, 0.0))
        for tid in removable:
            if len(final_ids) <= target_count:
                break
            if not _can_remove(tid):
                continue
            final_ids.remove(tid)
            for idx in scenario_membership.get(tid, set()):
                scenario_counts[idx] -= 1

    # Hard cap
    if len(final_ids) > target_count:
        ranked = sorted(final_ids, key=lambda tid: rca_scores.get(tid, 0.0), reverse=True)
        final_ids = set(ranked[:target_count])

    return final_ids
