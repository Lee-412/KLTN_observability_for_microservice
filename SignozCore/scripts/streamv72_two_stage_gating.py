"""
Stream v7.2 — Two-Stage Gating.

Addresses the core budget-scaling failure of v3.9/v3.95:
  v3.95 A@1 DECREASES with budget (24.41→23.69→22.90)
  TraStrainer A@1 INCREASES (42.59→45.16→50.00)

Root cause: v3.9 uses probabilistic online admission (one pass). At large
budget (2.5%), there is a long "free" window (theta << beta) where only the
p_s gate applies, admitting many noisy traces without a diversity check. The
final target_count cut cannot recover because all admitted traces have similar
composite scores.

Fix: Replace the stochastic 1-pass gate with deterministic 2-phase selection.

Phase 1 — System Scoring (full population scan):
  Process ALL traces in time order using the same formulas as v3.9.
  For every trace compute:
    p_s    = system/anomaly probability (EWMA rolling z-score)
    rca_value = RCA quadrant (Gold/Silver/Noise/Mixed)
    system_score = alpha_budget * p_s + (1 - alpha_budget) * rca_value

  alpha_budget is budget-adaptive (precision-first at tight budget):
    0.1%  → alpha = 0.80  (trust system signal heavily)
    1.0%  → alpha = 0.65  (balanced)
    2.5%  → alpha = 0.50  (more RCA weight)

  No admission gate — all traces are scored. Metric state is updated for
  every trace (same order as v3.9), preserving rolling EWMA fidelity.

Phase 2 — System Filter (top-K₁ pool):
  Sort all traces by system_score descending.
  K₁ = target_count × K1_mult, where:
    0.1%  → K1_mult=8   (pool = 0.8% of traces)
    1.0%  → K1_mult=5   (pool = 5% of traces)
    2.5%  → K1_mult=3   (pool = 7.5% of traces)
  The pool contains only "system-important" traces.

Phase 3 — Diversity Filter (within K₁):
  Iterate K₁ pool in system_score order (best first).
  Maintain kept-cluster state (Jaccard-based, same threshold 0.5 as v3.9).
  Admit trace if:
    (a) Novel — Jaccard similarity < 0.5 vs all kept clusters, OR
    (b) Cluster not full — kept_cluster_size < max_cluster_size, OR
    (c) Gold trace (rca_value >= 0.80) — always admit (MicroRank needs error contrast)
    (d) Incident-service trace — 2× cluster cap
  max_cluster_size is budget-adaptive: 1 at 0.1%, 3 at 1.0%, 5 at 2.5%.

  Fallback: if the diversity filter doesn't fill target_count after exhausting K₁,
  fill remaining slots from K₁ in system_score order (no diversity check).

Phase 4 — Scenario Floor + Error Density (same as v3.9):
  Same error density protection (1.4× base rate, prefer Gold errors).
  Same scenario floor enforcement with RCA-priority selection.

Why this fixes budget scaling:
  At 0.1%, K₁=8×target: pool is tiny and high-quality; diversity filter fills
  quickly. At 2.5%, K₁=3×target: pool is 3× target, diversity filter picks the
  most diverse traces within the high-scoring pool. Scores improve WITH budget
  because marginal traces admitted at 2.5% come from a pre-filtered high-quality
  pool, not from raw probabilistic admission.
"""

import math
import os
import random
from collections import defaultdict, deque
from typing import Any, Set, List, Tuple, Dict, FrozenSet, Optional

from streamv39_tuned_rca import (
    TracePoint, PreferenceVector, MetricStats,
    _DISABLE_OVERRIDE, _clamp, _median, _mad, _mean, _std,
    _stable_positive_z, _trace_has_incident_service,
    _compute_metric_signal_details, compute_sampling_probability,
    build_metric_inputs, _rca_quadrant_value,
)


def _composite_v72_two_stage_gating(
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
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    total = len(points)
    if total == 0:
        return set()
    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()
    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    target_count = max(1, min(total, int(round(total * beta))))

    # Budget-adaptive parameters
    if beta <= 0.0015:
        alpha_sys = 0.80
        K1_mult = 8
        max_cluster_size = 1    # 0.1%: strict — at most 1 trace per cluster
    elif beta <= 0.0125:
        alpha_sys = 0.65
        K1_mult = 5
        max_cluster_size = 3    # 1.0%: allow up to 3 per cluster
    else:
        alpha_sys = 0.50
        K1_mult = 3
        max_cluster_size = 5    # 2.5%: allow up to 5 per cluster

    # K1: system-pool size. At least target+50 to avoid starvation at tiny budgets.
    K1 = min(total, max(target_count + min(50, total // 20), int(target_count * K1_mult)))

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
        return (len(a & b) / u) if u > 0 else 0.0

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
        return max(std, 1.4826 * mad)

    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
    incident_services = incident_services or set()

    # ---------------------------------------------------------------------------
    # Phase 1: Score all traces (v3.9 computation, no admission gate)
    # ---------------------------------------------------------------------------
    seen_win: deque[Tuple[int, float, float, float]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[Tuple[int, Tuple[str, ...]]] = deque()
    seen = 0
    eps0 = 1e-6
    cluster_merge_threshold = 0.5

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_trace_count: Dict[str, int] = defaultdict(int)
    min_suspicious_history = 4 if beta <= 0.0015 else 8

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

    # Per-trace outputs from Phase 1
    system_score_all: Dict[str, float] = {}
    p_s_all: Dict[str, float] = {}
    rca_value_all: Dict[str, float] = {}
    trace_toks_all: Dict[str, FrozenSet[str]] = {}
    incident_service_all: Dict[str, float] = {}

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        # Evict stale from rolling windows
        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        lat_pressure = (
            0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) +
            0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        )
        incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        # Dynamic suspicious set (same as v3.9)
        dynamic_suspicious = set(incident_services)
        for svc in p.services:
            if svc_trace_count[svc] > min_suspicious_history:
                err_rate = svc_error_trace_count[svc] / svc_trace_count[svc]
                if err_rate > high_error_service_threshold:
                    dynamic_suspicious.add(svc)

        suspicious_hit = len(set(p.services) & dynamic_suspicious)
        suspicious_ratio = suspicious_hit / max(1, len(p.services))
        rca_value = _rca_quadrant_value(p.has_error, suspicious_hit, suspicious_ratio)

        # Metric state (same as v3.9)
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

        # Incident floor (same as v3.9)
        incident_floor = 0.0
        if incident_service > 0.0:
            if (err_flag > 0.0) and (time_prox >= 0.55):
                incident_floor = 0.62
            elif (time_prox >= 0.78) and (lat_pressure >= 0.58):
                incident_floor = 0.56
            elif (time_prox >= 0.88) and (lat_pressure >= 0.50):
                incident_floor = 0.50
        p_s = max(p_s, incident_floor)

        # RCA boost (same as v3.9)
        rca_boost = _clamp(rca_weight * (rca_value - 0.45), -0.12, 0.20)
        p_s = _clamp(p_s + rca_boost, 0.02, 0.98)

        # Update metric state (same as v3.9, unconditional)
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

        for svc in p.services:
            svc_trace_count[svc] += 1
            if p.has_error:
                svc_error_trace_count[svc] += 1

        # Track cluster tokens (population-level, for Phase 3 init)
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
        else:
            assigned_key = tuple(sorted(toks))
        cluster_templates[assigned_key] = (
            toks if best_key is None else cluster_templates.get(assigned_key, toks)
        )
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))
        seen_win.append((sec, err_flag, lat_pressure, incident_service))

        # Record per-trace scores
        system_score_all[p.trace_id] = alpha_sys * p_s + (1.0 - alpha_sys) * rca_value
        p_s_all[p.trace_id] = p_s
        rca_value_all[p.trace_id] = rca_value
        trace_toks_all[p.trace_id] = toks
        incident_service_all[p.trace_id] = incident_service

    # ---------------------------------------------------------------------------
    # Phase 2: System filter — top K1 by system_score
    # ---------------------------------------------------------------------------
    sorted_by_sys: List[TracePoint] = sorted(
        traces, key=lambda p: -system_score_all.get(p.trace_id, 0.0)
    )
    system_pool: Set[str] = set(p.trace_id for p in sorted_by_sys[:K1])

    # ---------------------------------------------------------------------------
    # Phase 3: Diversity filter within system pool
    # ---------------------------------------------------------------------------
    kept_ids: Set[str] = set()
    kept_cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    kept_cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)

    for p in sorted_by_sys:
        if len(kept_ids) >= target_count:
            break
        if p.trace_id not in system_pool:
            continue

        toks = trace_toks_all[p.trace_id]
        rca_val = rca_value_all[p.trace_id]
        inc_svc = incident_service_all[p.trace_id]

        # Find best matching cluster in KEPT set (not population)
        best_sim = 0.0
        best_key = None
        for k, ktoks in kept_cluster_templates.items():
            sim = _jaccard(toks, ktoks)
            if sim > best_sim:
                best_sim = sim
                best_key = k

        if best_key is not None and best_sim >= cluster_merge_threshold:
            cluster_mass = kept_cluster_counts.get(best_key, 0)
            # Gold traces always override diversity gate (MicroRank needs contrast).
            if rca_val >= 0.80:
                admit = True
            # Incident-service traces get a 2× relaxed cap.
            elif inc_svc > 0.0:
                admit = cluster_mass < max_cluster_size * 2
            else:
                admit = cluster_mass < max_cluster_size
        else:
            admit = True  # Novel cluster — always admit

        if admit:
            kept_ids.add(p.trace_id)
            # Update kept-cluster state
            if best_key is not None and best_sim >= cluster_merge_threshold:
                assigned_key = best_key
            else:
                assigned_key = tuple(sorted(toks))
            if assigned_key not in kept_cluster_templates:
                kept_cluster_templates[assigned_key] = toks
            kept_cluster_counts[assigned_key] = kept_cluster_counts.get(assigned_key, 0) + 1

    # Fallback: diversity filter may not fill target_count if pool is too clustered.
    # Fill remaining slots from K1 pool in system_score order (no diversity check).
    if len(kept_ids) < target_count:
        for p in sorted_by_sys:
            if len(kept_ids) >= target_count:
                break
            if p.trace_id not in kept_ids:
                kept_ids.add(p.trace_id)

    # score_by_id for post-hoc operations (error swap, scenario floor pruning)
    score_by_id: Dict[str, float] = {
        tid: system_score_all.get(tid, 0.0)
        for tid in by_id
    }

    # ---------------------------------------------------------------------------
    # Phase 4: Error density protection (same logic as v3.9)
    # ---------------------------------------------------------------------------
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.4), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [tid for tid, tp in by_id.items() if (tid not in kept_ids) and tp.has_error]
            cand_add.sort(
                key=lambda tid: (rca_value_all.get(tid, 0.0), score_by_id.get(tid, 0.0)),
                reverse=True,
            )
            add_ids = cand_add[:deficit]
            if add_ids:
                cand_drop = [
                    tid for tid in kept_ids
                    if by_id.get(tid) and (not by_id[tid].has_error)
                ]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    kept_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    kept_ids.add(tid)

    def _rca_rank_key(tid: str) -> Tuple[float, float, float, str]:
        return (
            -rca_value_all.get(tid, 0.0),
            -(1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0),
            -p_s_all.get(tid, 0.0),
            tid,
        )

    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        ranked_ids = sorted(kept_ids, key=_rca_rank_key)
        return set(ranked_ids[:target_count])

    # Scenario floor enforcement (same as v3.9)
    by_id_full: Dict[str, TracePoint] = {p.trace_id: p for p in points}
    start_s_by_id: Dict[str, int] = {
        p.trace_id: int(p.start_ns // 1_000_000_000) for p in points
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

    if non_empty_scenarios == 0:
        ranked_ids = sorted(kept_ids, key=_rca_rank_key)
        return set(ranked_ids[:target_count])

    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    def _priority(tid: str) -> Tuple[float, float, float, float, int, str]:
        tp = by_id_full.get(tid)
        if tp is None:
            return (0.0, 0.0, 0.0, 0.0, 0, tid)
        return (
            rca_value_all.get(tid, 0.0),
            1.0 if tp.has_error else 0.0,
            float(tp.duration_ms),
            float(tp.span_count),
            len(scenario_membership.get(tid, set())),
            tid,
        )

    final_ids: Set[str] = set(kept_ids)
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
        removable_ids = sorted(final_ids, key=_rca_rank_key)
        final_ids = set(removable_ids[:target_count])

    return final_ids
