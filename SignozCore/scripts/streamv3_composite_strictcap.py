

import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Set, List, Tuple, Dict, FrozenSet, Optional

@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: float
    span_count: int
    has_error: bool
    services: FrozenSet[str]

PreferenceVector = Dict[str, float]  

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _trace_has_incident_service(p: TracePoint, incident_services: Set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)


def compute_metric_score(
    trace: TracePoint,
    preference_vector: PreferenceVector,
    metric_stats: Dict[str, Tuple[float, float]],
) -> Tuple[float, float]:
    # Covered services are defined strictly by presence in the preference vector.
    covered_services = [service for service in trace.services if service in preference_vector]
    if not trace.services:
        return 0.0, 0.0

    coverage_ratio = len(covered_services) / float(len(trace.services)) if trace.services else 0.0
    if not covered_services:
        return 0.0, coverage_ratio

    z_scores: List[float] = []
    for service in covered_services:
        if service not in metric_stats:
            continue
        raw_score = float(preference_vector.get(service, 0.0))
        mu_s, sigma_s = metric_stats[service]
        z_score = max(0.0, (raw_score - float(mu_s)) / (float(sigma_s) + 1e-9))
        z_scores.append(z_score)

    if not z_scores:
        return 0.0, coverage_ratio

    return max(z_scores), coverage_ratio


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
    trace_metric_score, coverage_ratio = compute_metric_score(trace, preference_vector, metric_stats)

    confidence_threshold = 1.5
    lambda_base = 0.2
    if trace_metric_score >= confidence_threshold:
        lambda_eff = lambda_base * coverage_ratio
    else:
        lambda_eff = 0.0

    p_s_final = p_s_v3 + (lambda_eff * trace_metric_score)
    return _clamp(p_s_final, p_min, p_max)


def build_metric_inputs(
    points: List[TracePoint],
) -> Tuple[PreferenceVector, Dict[str, Tuple[float, float]]]:
    # Derive per-service metric signals from real trace latency and error signals.
    if not points:
        return {}, {}

    dur_lo = min(point.duration_ms for point in points)
    dur_hi = max(point.duration_ms for point in points)
    span_lo = min(point.span_count for point in points)
    span_hi = max(point.span_count for point in points)
    t0_sec = min(point.start_ns for point in points) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    # Aggregate service-local latency/error signals by second.
    service_windows: Dict[str, Dict[int, List[float]]] = defaultdict(dict)
    for point in points:
        sec = int(point.start_ns // 1_000_000_000 - t0_sec)
        lat_norm = 0.7 * _norm(float(point.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(point.span_count), float(span_lo), float(span_hi))
        err_flag = 1.0 if point.has_error else 0.0
        for service in point.services:
            if sec not in service_windows[service]:
                service_windows[service][sec] = [0.0, 0.0, 0.0]
            service_windows[service][sec][0] += 1.0
            service_windows[service][sec][1] += err_flag
            service_windows[service][sec][2] += lat_norm

    preference_vector: PreferenceVector = {}
    metric_stats: Dict[str, Tuple[float, float]] = {}

    for service, by_sec in service_windows.items():
        raw_scores: List[float] = []
        for _sec, values in by_sec.items():
            count, err_sum, lat_sum = values
            if count <= 0:
                continue
            avg_lat = lat_sum / count
            err_rate = err_sum / count
            raw_scores.append(0.7 * avg_lat + 0.3 * err_rate)

        if not raw_scores:
            continue

        mu = sum(raw_scores) / len(raw_scores)
        if len(raw_scores) <= 1:
            sigma = 0.0
        else:
            variance = sum((value - mu) * (value - mu) for value in raw_scores) / (len(raw_scores) - 1)
            sigma = math.sqrt(max(0.0, variance))

        metric_stats[service] = (mu, sigma)
        # Use the strongest observed service-local anomaly evidence as raw preference signal.
        preference_vector[service] = max(raw_scores)

    return preference_vector, metric_stats

def _trace_preference_score(p: TracePoint, preference_vector: PreferenceVector) -> float:
    # Legacy helper kept for compatibility with existing analysis scripts.
    if not p.services or not preference_vector:
        return 0.0
    best = 0.0
    for svc in p.services:
        best = max(best, float(preference_vector.get(svc, 0.0)))
    return best

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
    metric_stats = metric_stats or {}
    incident_services = incident_services or set()
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
                "preference_vector": preference_vector,
                "metric_stats": metric_stats,
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
        u = rng.random()
        if theta > beta:
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
            key=lambda tid: (1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0, p_s_final_by_id.get(tid, 0.0)),
            reverse=True,
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
            key=lambda tid: (1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0, p_s_final_by_id.get(tid, 0.0)),
            reverse=True,
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
            key=lambda tid: (1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0, p_s_final_by_id.get(tid, 0.0)),
            reverse=True,
        )
        final_ids = set(removable_ids[:target_count])
    return final_ids
