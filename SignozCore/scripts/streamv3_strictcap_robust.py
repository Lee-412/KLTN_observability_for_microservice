"""
StreamV3-strictcap-robust Standalone Sampler
--------------------------------------------
A minimal, readable base for experimentation with the StreamV3-strictcap-robust sampling method.

- Extracted from run_paper_sampled_rca.py (SignozCore/scripts)
- Only StreamV3 logic and strict-cap enforcement, with minimal dependencies
- For research, prototyping, and future method development
"""

import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Set, List, Tuple, Dict, FrozenSet

@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: float
    span_count: int
    has_error: bool
    services: FrozenSet[str]

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def _trace_has_incident_service(p: TracePoint, incident_services: Set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)

def _stream_v3_select_ids(
    points: List[TracePoint],
    budget_pct: float,
    incident_services: Set[str],
    incident_anchor_sec: int | None,
    seed: int,
    online_soft_cap: bool = False,
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
) -> Set[str]:
    """Stream-Hybrid V3: V2 core + OR soft quota 70/30 + no hard strict-cap."""
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
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}

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

        has_incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

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
        z_inc = abs(has_incident_service - mu_inc) / (sg_inc + eps0)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc

        alpha_eff = alpha * (0.90 if beta >= 0.01 else 1.00)
        shock = math.tanh(alpha_eff * z_mix)
        base_sys = 0.45 * has_incident_service + 0.25 * lat_pressure + 0.15 * err_flag + 0.15 * time_prox
        p_s = _clamp(0.55 * base_sys + 0.45 * shock, 0.02, 0.98)

        incident_floor = 0.0
        if has_incident_service > 0.0:
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

        if has_incident_service > 0.0:
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
        if has_incident_service > 0.0 and time_prox >= 0.55:
            ps_or_floor = max(0.08, ps_or_floor - 0.06)

        kept_hist = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio = (sum(kept_hist) / len(kept_hist)) if kept_hist else 0.0
        kept_norm_ratio = 1.0 - kept_err_ratio

        u = rng.random()
        if theta > beta:
            keep = (u < p_s) and (u < p_d)
        else:
            rare_noise = (p_d >= 0.72) and (p_s < ps_or_floor)
            if rare_noise:
                keep = False
            else:
                keep = (u < p_s)

            is_clean_normal = (not p.has_error) and (has_incident_service <= 0.0) and (p_s < ps_or_floor) and (p_d <= 0.45)
            if (not keep) and is_clean_normal and (kept_norm_ratio < normal_ratio_target):
                if rng.random() < beta:
                    keep = True

            if (not keep) and online_soft_cap and is_clean_normal:
                gap = max(0.0, beta - theta)
                p_gap = _clamp(gap, 0.0, 0.20)
                if rng.random() < p_gap:
                    keep = True

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))

        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        seen_win.append((sec, err_flag, lat_pressure, has_incident_service))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

    # Keep minimum error density protection (same spirit as v2).
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [tid for tid, p in by_id.items() if (tid not in kept_ids) and p.has_error]
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

    return kept_ids

def _enforce_budget_cap_per_scenario(
    points: List[TracePoint],
    kept_ids: Set[str],
    budget_pct: float,
    scenario_windows: List[Tuple[str, int, int]],
    min_incident_traces_per_scenario: int = 1,
) -> Tuple[Set[str], int, int, int]:
    """Strict-cap with per-scenario floor to avoid global top-up distortion."""
    total = len(points)
    if total == 0:
        return set(), 0, 0, 0

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))
    pre_cap_count = len(kept_ids)

    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        return set(list(kept_ids)[:target_count]), target_count, pre_cap_count, 0

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
        return set(list(kept_ids)[:target_count]), target_count, pre_cap_count, len(scenario_windows)

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
        return final_ids, target_count, pre_cap_count, scenario_floor_fail_count

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
        force = sorted(final_ids, key=_priority)
        for tid in force:
            if len(final_ids) <= target_count:
                break
            final_ids.remove(tid)

    return final_ids, target_count, pre_cap_count, scenario_floor_fail_count
