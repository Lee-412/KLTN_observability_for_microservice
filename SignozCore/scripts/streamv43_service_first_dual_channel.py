import csv
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple


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
MetricsSnapshot = Dict[str, Dict[str, float]]


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _std(values: List[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mu = _mean(values)
    var = sum((value - mu) * (value - mu) for value in values) / float(len(values))
    return math.sqrt(max(0.0, var))


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _strip_pod_to_service(pod_name: str) -> str:
    parts = str(pod_name).strip().split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return str(pod_name).strip()


def build_real_metrics_stream(metric_dir: str) -> Dict[int, MetricsSnapshot]:
    """Parse real metric CSV files into second->service snapshots."""
    if not metric_dir:
        return {}
    path = os.path.abspath(metric_dir)
    if not os.path.isdir(path):
        return {}

    rows_by_service: Dict[str, List[Tuple[int, float, float, float, float, float]]] = defaultdict(list)

    for name in sorted(os.listdir(path)):
        if not name.endswith(".csv"):
            continue
        if name in {"dependency.csv", "front_service.csv", "source_50.csv", "destination_50.csv"}:
            continue
        file_path = os.path.join(path, name)
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    sec = int(float(str(row.get("TimeStamp", "0")).strip()))
                except Exception:
                    continue
                if sec <= 0:
                    continue

                pod = str(row.get("PodName", "")).strip()
                if not pod:
                    continue
                service = _strip_pod_to_service(pod)

                cpu = _clamp(float(row.get("CpuUsageRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                mem = _clamp(float(row.get("MemoryUsageRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                client_p95 = float(row.get("PodClientLatencyP95(s)", 0.0))
                server_p95 = float(row.get("PodServerLatencyP95(s)", 0.0))
                latency = max(client_p95, server_p95)
                workload_ops = max(0.0, float(row.get("PodWorkload(Ops)", 0.0)))
                success_rate = _clamp(float(row.get("PodSuccessRate(%)", 0.0)) / 100.0, 0.0, 1.0)

                rows_by_service[service].append((sec, cpu, mem, latency, success_rate, workload_ops))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        lat_hist: Deque[float] = deque(maxlen=120)
        work_hist: Deque[float] = deque(maxlen=120)
        per_sec_acc: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {
                "cpu": [],
                "memory": [],
                "latency": [],
                "success_rate": [],
                "workload": [],
            }
        )

        for sec, cpu, mem, latency, success_rate, workload_ops in rows:
            lat_hist.append(latency)
            work_hist.append(workload_ops)
            sorted_lat = sorted(lat_hist)
            sorted_work = sorted(work_hist)
            lat_p95 = sorted_lat[int(0.95 * (len(sorted_lat) - 1))] if sorted_lat else 1.0
            work_p95 = sorted_work[int(0.95 * (len(sorted_work) - 1))] if sorted_work else 1.0
            lat_p95 = max(1e-6, float(lat_p95))
            work_p95 = max(1e-6, float(work_p95))

            norm_latency = _clamp(latency / lat_p95, 0.0, 1.0)
            norm_work = _clamp(workload_ops / work_p95, 0.0, 1.0)

            acc = per_sec_acc[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(norm_latency)
            acc["success_rate"].append(success_rate)
            acc["workload"].append(norm_work)

        for sec, acc in per_sec_acc.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "success_rate": _mean(acc["success_rate"]),
                "workload": _mean(acc["workload"]),
            }

    return dict(out)


def get_aligned_metrics(
    trace_sec: int,
    service: str,
    metrics_stream_by_sec: Dict[int, MetricsSnapshot],
    lookback_sec: int = 20,
) -> Optional[Dict[str, float]]:
    for sec in range(trace_sec, trace_sec - max(1, lookback_sec) - 1, -1):
        snap = metrics_stream_by_sec.get(sec)
        if not snap:
            continue
        metric = snap.get(service)
        if metric is not None:
            return metric
    return None


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    """Compatibility hook; online sampler owns streaming states."""
    if not points:
        return {}, {}
    return {}, {}


def _jaccard(a: FrozenSet[str], b: FrozenSet[str]) -> float:
    if not a and not b:
        return 1.0
    u = len(a | b)
    if u <= 0:
        return 0.0
    return len(a & b) / u


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


def _priority(trace_point: Optional[TracePoint], membership_count: int, score_sys: float, score_div: float) -> Tuple[float, float, float, float, int, str]:
    if trace_point is None:
        return (0.0, 0.0, 0.0, 0.0, 0, "")
    return (
        1.0 if trace_point.has_error else 0.0,
        float(score_sys),
        float(score_div),
        float(trace_point.duration_ms),
        membership_count,
        trace_point.trace_id,
    )


def _safe_positive_z(x: float, hist: Deque[float]) -> float:
    if len(hist) < 5:
        return 0.0
    vals = list(hist)
    mu = _mean(vals)
    sigma = _std(vals)
    return max(0.0, (x - mu) / (sigma + 0.001))


def _composite_v43_service_first_dual_channel(
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
    metrics_stream_by_sec: Optional[Dict[int, MetricsSnapshot]] = None,
    metric_lookback_sec: int = 20,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    """v4.3 paper-grade service-first dual-channel sampler.

    - Channel A (system-biased): max service risk in trace
    - Channel B (diversity-biased): v3.5 Jaccard/mass logic
    - Final decision: adaptive OR/AND voting with fixed slot split (60/40)
    """
    total = len(points)
    if total == 0:
        return set()

    beta_budget = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta_budget <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    target_count = max(1, min(total, int(round(total * beta_budget))))
    sys_slots = max(1, int(math.ceil(target_count * 0.60)))
    div_slots = max(1, int(math.ceil(target_count * 0.40)))
    if sys_slots + div_slots > target_count:
        div_slots = max(1, target_count - sys_slots)

    regime_switch_beta = 0.70
    gamma = 0.8
    cluster_merge_threshold = 0.5
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000

    metrics_stream_by_sec = metrics_stream_by_sec or {}
    incident_services = incident_services or set()

    # Service risk map state: O(V + K)
    service_hist: Dict[str, Dict[str, Deque[float]]] = defaultdict(
        lambda: {
            "cpu": deque(maxlen=10),
            "memory": deque(maxlen=10),
            "latency": deque(maxlen=10),
            "success_rate": deque(maxlen=10),
            "workload": deque(maxlen=10),
            "trend": deque(maxlen=10),
        }
    )
    service_risk: Dict[str, float] = defaultdict(float)

    # Diversity channel state (same shape as v3.5)
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: Deque[Tuple[int, Tuple[str, ...]]] = deque()

    # Temporal precursor buffer
    recent_trace_buffer: Deque[Tuple[str, FrozenSet[str]]] = deque(maxlen=50)
    precursor_boost: Dict[str, float] = defaultdict(float)

    # Scoring and voting bookkeeping
    score_sys_by_id: Dict[str, float] = {}
    score_div_by_id: Dict[str, float] = {}
    voted_keep_ids: Set[str] = set()
    sys_eligible_ids: Set[str] = set()
    div_eligible_ids: Set[str] = set()
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}

    seen_sys_scores: List[float] = []
    seen_div_scores: List[float] = []

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        # Sliding window maintenance for diversity clusters.
        while cluster_win and cluster_win[0][0] < sec - max(1, metric_lookback_sec) + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        trace_service_risk: Dict[str, float] = {}
        high_risk_services: Set[str] = set()

        # Channel A: build/refresh service risk map using real metrics.
        for svc in sorted(p.services):
            aligned = get_aligned_metrics(sec + t0_sec, svc, metrics_stream_by_sec, lookback_sec=metric_lookback_sec)
            if aligned is None:
                trace_service_risk[svc] = float(service_risk.get(svc, 0.0))
                continue

            h = service_hist[svc]
            cpu = _clamp(float(aligned.get("cpu", 0.0)), 0.0, 1.0)
            mem = _clamp(float(aligned.get("memory", 0.0)), 0.0, 1.0)
            lat = _clamp(float(aligned.get("latency", 0.0)), 0.0, 1.0)
            succ = _clamp(float(aligned.get("success_rate", 1.0)), 0.0, 1.0)
            work = _clamp(float(aligned.get("workload", 0.0)), 0.0, 1.0)

            z_cpu = _safe_positive_z(cpu, h["cpu"])
            z_mem = _safe_positive_z(mem, h["memory"])
            z_latency = _safe_positive_z(lat, h["latency"])

            error_penalty = 0.25 if succ < 0.99 else 0.0
            base_risk = 0.35 * z_cpu + 0.20 * z_mem + 0.30 * z_latency + 0.15 * error_penalty

            if len(h["latency"]) >= 3:
                latency_3ago = list(h["latency"])[-3]
                trend = lat - latency_3ago
            else:
                trend = 0.0
            trend_vals = list(h["trend"])
            if len(trend_vals) < 5:
                trend_z = 0.0
            else:
                trend_z = (trend - _mean(trend_vals)) / (_std(trend_vals) + 0.001)
            trend_boost = 0.20 * max(0.0, trend_z)

            risk_score = _clamp(_sigmoid(base_risk + trend_boost), 0.0, 1.0)
            service_risk[svc] = risk_score
            trace_service_risk[svc] = risk_score

            h["cpu"].append(cpu)
            h["memory"].append(mem)
            h["latency"].append(lat)
            h["success_rate"].append(succ)
            h["workload"].append(work)
            h["trend"].append(trend)

            if risk_score > 0.85:
                high_risk_services.add(svc)

        score_sys = max(trace_service_risk.values()) if trace_service_risk else 0.0

        # Precursor: boost recent traces touching currently risky services.
        if high_risk_services:
            for old_tid, old_services in recent_trace_buffer:
                if old_services & high_risk_services:
                    precursor_boost[old_tid] = max(precursor_boost.get(old_tid, 0.0), 0.10)

        score_sys = _clamp(score_sys + precursor_boost.get(p.trace_id, 0.0), 0.0, 1.0)

        # Channel B: diversity score from v3.5 Jaccard + mass.
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

        incident_hit = bool(set(p.services) & incident_services)
        if incident_hit:
            effective_mass *= 0.35
            gamma_eff = max(0.35, gamma * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = gamma
            pd_floor = 0.05
        score_div = _clamp((1.0 + effective_mass) ** (-gamma_eff), pd_floor, 1.0)

        # Rank thresholds (paper-style dual-channel voting).
        seen_sys_scores.append(score_sys)
        seen_div_scores.append(score_div)
        sys_rank = 1 + sum(1 for x in seen_sys_scores if x > score_sys)
        div_rank = 1 + sum(1 for x in seen_div_scores if x > score_div)

        current_kept = len(voted_keep_ids)
        theta = (current_kept / float(target_count)) if target_count > 0 else 1.0
        regime = "OR" if theta <= regime_switch_beta else "AND"

        if sys_rank <= sys_slots:
            sys_eligible_ids.add(p.trace_id)
        if div_rank <= div_slots:
            div_eligible_ids.add(p.trace_id)

        if regime == "OR":
            keep_vote = (sys_rank <= sys_slots) or (div_rank <= div_slots)
        else:
            keep_vote = (sys_rank <= sys_slots) and (div_rank <= div_slots)

        if keep_vote:
            voted_keep_ids.add(p.trace_id)

        score_sys_by_id[p.trace_id] = score_sys
        score_div_by_id[p.trace_id] = score_div

        # Maintain rolling cluster state for future traces.
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

        # Maintain precursor candidate pool.
        recent_trace_buffer.append((p.trace_id, p.services))

        if debug_trace_logs is not None:
            top_service = None
            top_value = -1.0
            for svc, rv in trace_service_risk.items():
                if rv > top_value:
                    top_service = svc
                    top_value = rv
            top_map = {}
            if top_service is not None:
                top_map[top_service] = _clamp(top_value, 0.0, 1.0)
            debug_trace_logs.append(
                {
                    "trace_id": p.trace_id,
                    "score_sys": round(score_sys, 6),
                    "score_div": round(score_div, 6),
                    "service_risk_top": top_map,
                    "regime": regime,
                    "kept": bool(keep_vote),
                }
            )

    # Mandatory hard slot allocation (60/40), plus voted pool fallback.
    sys_ranked = sorted(score_sys_by_id.keys(), key=lambda tid: (score_sys_by_id.get(tid, 0.0), tid), reverse=True)
    div_ranked = sorted(score_div_by_id.keys(), key=lambda tid: (score_div_by_id.get(tid, 0.0), tid), reverse=True)

    final_ids: Set[str] = set(sys_ranked[:sys_slots])
    final_ids.update(div_ranked[:div_slots])

    if len(final_ids) < target_count:
        voted_ranked = sorted(
            voted_keep_ids,
            key=lambda tid: (
                score_sys_by_id.get(tid, 0.0),
                score_div_by_id.get(tid, 0.0),
                tid,
            ),
            reverse=True,
        )
        for tid in voted_ranked:
            if len(final_ids) >= target_count:
                break
            final_ids.add(tid)

    if len(final_ids) < target_count:
        backfill_ranked = sorted(
            score_sys_by_id.keys(),
            key=lambda tid: (
                score_sys_by_id.get(tid, 0.0),
                score_div_by_id.get(tid, 0.0),
                tid,
            ),
            reverse=True,
        )
        for tid in backfill_ranked:
            if len(final_ids) >= target_count:
                break
            final_ids.add(tid)

    # Scenario-aware strict cap (same spirit as v3.5).
    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        if len(final_ids) > target_count:
            trimmed = sorted(
                final_ids,
                key=lambda tid: (
                    score_sys_by_id.get(tid, 0.0),
                    score_div_by_id.get(tid, 0.0),
                    tid,
                ),
                reverse=True,
            )
            return set(trimmed[:target_count])
        return final_ids

    start_s_by_id: Dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in traces}
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
        trimmed = sorted(
            final_ids,
            key=lambda tid: (
                score_sys_by_id.get(tid, 0.0),
                score_div_by_id.get(tid, 0.0),
                tid,
            ),
            reverse=True,
        )
        return set(trimmed[:target_count])

    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    if floor_each > 0:
        for ids in scenario_ids_by_idx:
            if not ids:
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(
                key=lambda tid: _priority(
                    by_id.get(tid),
                    len(scenario_membership.get(tid, set())),
                    score_sys_by_id.get(tid, 0.0),
                    score_div_by_id.get(tid, 0.0),
                ),
                reverse=True,
            )
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

    removable = sorted(
        final_ids,
        key=lambda tid: _priority(
            by_id.get(tid),
            len(scenario_membership.get(tid, set())),
            score_sys_by_id.get(tid, 0.0),
            score_div_by_id.get(tid, 0.0),
        ),
    )
    for tid in removable:
        if len(final_ids) <= target_count:
            break
        if not _can_remove(tid):
            continue
        final_ids.remove(tid)
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] -= 1

    if len(final_ids) > target_count:
        force = sorted(
            final_ids,
            key=lambda tid: (
                score_sys_by_id.get(tid, 0.0),
                score_div_by_id.get(tid, 0.0),
                tid,
            ),
            reverse=True,
        )
        final_ids = set(force[:target_count])

    return final_ids
