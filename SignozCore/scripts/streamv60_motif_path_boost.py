import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import streamv39_tuned_rca as sv39tuned


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


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    if not points:
        return {}, {}
    return {}, {}


def _safe_service_path(p: Any, max_depth: int = 5) -> Tuple[str, ...]:
    path = getattr(p, "service_path", None)
    if isinstance(path, tuple) and path:
        return tuple(path[:max_depth])
    if isinstance(path, list) and path:
        return tuple(path[:max_depth])
    # Fallback when service_path is missing: use deterministic service signature.
    svcs = sorted(list(getattr(p, "services", set()) or set()))
    if not svcs:
        return ("_empty",)
    return tuple(svcs[:max_depth])


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    q = _clamp(q, 0.0, 1.0)
    if len(xs) == 1:
        return float(xs[0])
    pos = q * (len(xs) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(xs[lo])
    frac = pos - lo
    return float(xs[lo] + (xs[hi] - xs[lo]) * frac)


def _composite_v60_motif_path_boost(
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
    rca_weight: float = 0.40,
    motif_weight: float = 0.35,
    root_weight: float = 0.20,
    base_weight: float = 0.45,
    motif_swap_ratio: float = 0.30,
    motif_admission_percentile: float = 0.70,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    total = len(traces)
    if total == 0:
        return set()

    incident_services = incident_services or set()

    # Stage A: keep v3.9 tuned RCA membership as the stability baseline.
    base_kept = sv39tuned._composite_v39_tuned_rca(
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
        rca_weight=rca_weight,
    )
    if not base_kept:
        return set()

    target_count = len(base_kept)
    swap_quota = max(1, int(round(target_count * _clamp(motif_swap_ratio, 0.0, 0.5))))

    dur_lo = min(float(p.duration_ms) for p in traces)
    dur_hi = max(float(p.duration_ms) for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    motif_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    root_counts: Dict[str, int] = defaultdict(int)
    path_by_id: Dict[str, Tuple[str, ...]] = {}

    for p in traces:
        path_key = _safe_service_path(p, max_depth=5)
        path_by_id[p.trace_id] = path_key
        motif_counts[path_key] += 1
        root_svc = str(getattr(p, "root_service", "") or "")
        if root_svc:
            root_counts[root_svc] += 1

    def _trace_score(p: Any) -> Tuple[float, float, float, float]:
        path_key = path_by_id.get(p.trace_id, ("_empty",))
        motif_freq = motif_counts.get(path_key, 0)
        motif_boost = 1.0 / math.sqrt(float(motif_freq) + 1.0)

        root_svc = str(getattr(p, "root_service", "") or "")
        root_freq = root_counts.get(root_svc, 0)
        root_service_boost = 1.0 / math.sqrt(float(root_freq) + 1.0) if root_svc else 0.0

        has_incident_hit = 1.0 if (set(getattr(p, "services", set()) or set()) & incident_services) else 0.0
        base_v39_proxy = _clamp(
            0.45 * _norm(float(getattr(p, "duration_ms", 0.0)), dur_lo, dur_hi)
            + 0.35 * (1.0 if bool(getattr(p, "has_error", False)) else 0.0)
            + 0.20 * has_incident_hit,
            0.0,
            1.0,
        )

        final_score = (
            _clamp(base_weight, 0.0, 1.0) * base_v39_proxy
            + _clamp(motif_weight, 0.0, 1.0) * motif_boost
            + _clamp(root_weight, 0.0, 1.0) * root_service_boost
        )
        return final_score, motif_boost, root_service_boost, base_v39_proxy

    scored_all: Dict[str, Tuple[float, float, float, float]] = {}
    for p in traces:
        scored_all[p.trace_id] = _trace_score(p)

    candidate_scores = [scored_all[tid][1] for tid in scored_all]
    motif_gate = _percentile(candidate_scores, motif_admission_percentile)

    current_ids = set(base_kept)
    outside_candidates = [p for p in traces if p.trace_id not in current_ids]
    inside_candidates = [p for p in traces if p.trace_id in current_ids]

    outside_candidates.sort(
        key=lambda p: (
            scored_all[p.trace_id][0],
            scored_all[p.trace_id][1],
            scored_all[p.trace_id][2],
        ),
        reverse=True,
    )
    inside_candidates.sort(
        key=lambda p: (
            scored_all[p.trace_id][0],
            scored_all[p.trace_id][1],
            scored_all[p.trace_id][2],
        )
    )

    start_s_by_id: Dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in traces}
    scenario_membership: Dict[str, Set[int]] = defaultdict(set)
    scenario_counts: List[int] = [0] * len(scenario_windows)
    non_empty: Set[int] = set()

    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        for tid, sec in start_s_by_id.items():
            if st <= sec <= ed:
                scenario_membership[tid].add(idx)
                non_empty.add(idx)

    for tid in current_ids:
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] += 1

    floor_each = min_incident_traces_per_scenario
    if floor_each > 0 and target_count < len(non_empty) * floor_each:
        floor_each = 0

    def _can_remove(tid: str) -> bool:
        if floor_each <= 0:
            return True
        for idx in scenario_membership.get(tid, set()):
            if idx in non_empty and scenario_counts[idx] <= floor_each:
                return False
        return True

    swaps_done = 0
    used_inside: Set[str] = set()
    for p_out in outside_candidates:
        if swaps_done >= swap_quota:
            break
        tid_out = p_out.trace_id
        out_score, out_motif_boost, _out_root_boost, _ = scored_all[tid_out]
        if out_motif_boost < motif_gate:
            continue

        victim = None
        for p_in in inside_candidates:
            tid_in = p_in.trace_id
            if tid_in in used_inside:
                continue
            if not _can_remove(tid_in):
                continue
            in_score = scored_all[tid_in][0]
            if out_score <= in_score:
                continue
            victim = tid_in
            break

        if victim is None:
            continue

        current_ids.remove(victim)
        current_ids.add(tid_out)
        used_inside.add(victim)
        swaps_done += 1
        for idx in scenario_membership.get(victim, set()):
            scenario_counts[idx] -= 1
        for idx in scenario_membership.get(tid_out, set()):
            scenario_counts[idx] += 1

    if len(current_ids) > target_count:
        ranked = sorted(
            current_ids,
            key=lambda tid: (scored_all[tid][0], scored_all[tid][1], scored_all[tid][2]),
            reverse=True,
        )
        current_ids = set(ranked[:target_count])

    if debug_trace_logs is not None:
        for p in traces:
            s_final, s_motif, s_root, s_base = scored_all[p.trace_id]
            debug_trace_logs.append(
                {
                    "trace_id": p.trace_id,
                    "kept": p.trace_id in current_ids,
                    "path_key": "->".join(path_by_id.get(p.trace_id, ("_empty",))),
                    "motif_boost": round(s_motif, 6),
                    "root_service_boost": round(s_root, 6),
                    "base_v39_proxy": round(s_base, 6),
                    "final_score": round(s_final, 6),
                    "swap_quota": swap_quota,
                    "swaps_done": swaps_done,
                    "motif_gate": round(motif_gate, 6),
                }
            )

    return current_ids
