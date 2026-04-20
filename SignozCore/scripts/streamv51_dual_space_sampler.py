import csv
import math
import os
import random
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
                success_rate = _clamp(float(row.get("PodSuccessRate(%)", 0.0)) / 100.0, 0.0, 1.0)
                workload = max(0.0, float(row.get("PodWorkload(Ops)", 0.0)))
                rows_by_service[service].append((sec, cpu, mem, latency, success_rate, workload))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        acc_by_sec: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {
                "cpu": [],
                "memory": [],
                "latency": [],
                "success": [],
                "workload": [],
            }
        )

        for sec, cpu, mem, latency, success_rate, workload in rows:
            acc = acc_by_sec[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(latency)
            acc["success"].append(success_rate)
            acc["workload"].append(workload)

        for sec, acc in acc_by_sec.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "success": _mean(acc["success"]),
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
    if not points:
        return {}, {}
    return {}, {}


def _composite_v51_dual_space_sampler(
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
    lookback_sec: int = 60,
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
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)
    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}

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

    seen = 0
    rng = random.Random(seed)
    kept_ids: Set[str] = set()
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}

    # A-space: streaming trace latent state.
    seen_win: Deque[Tuple[int, float, float, float]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: Deque[Tuple[int, Tuple[str, ...]]] = deque()

    # P-space: independent system-state latent, updated by second from real metrics.
    p_service_state: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "cpu": deque(maxlen=30),
            "memory": deque(maxlen=30),
            "latency": deque(maxlen=30),
            "workload": deque(maxlen=30),
            "failure": deque(maxlen=30),
            "ema_distress": 0.0,
        }
    )
    p_processed_secs: Set[int] = set()
    p_pressure = 0.0
    p_instability = 0.0
    p_failure = 0.0

    kept_win: Deque[Tuple[int, bool]] = deque()
    score_by_id: Dict[str, float] = {}
    p_s_by_id: Dict[str, float] = {}

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        # Keep windows bounded to preserve O(1) memory per stream horizon.
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

        # ----- P-space update (independent system state) -----
        if sec not in p_processed_secs:
            p_processed_secs.add(sec)
            abs_sec = t0_sec + sec
            snap: Optional[MetricsSnapshot] = None
            for s in range(abs_sec, abs_sec - max(1, metric_lookback_sec) - 1, -1):
                cand = metrics_stream_by_sec.get(s)
                if cand:
                    snap = cand
                    break

            distress_values: List[float] = []
            failure_values: List[float] = []
            if snap:
                for svc, m in snap.items():
                    state = p_service_state[svc]
                    x_cpu = _clamp(float(m.get("cpu", 0.0)), 0.0, 1.0)
                    x_mem = _clamp(float(m.get("memory", 0.0)), 0.0, 1.0)
                    x_lat = max(0.0, float(m.get("latency", 0.0)))
                    x_work = max(0.0, float(m.get("workload", 0.0)))
                    x_fail = 1.0 - _clamp(float(m.get("success", 1.0)), 0.0, 1.0)

                    h_cpu = list(state["cpu"])
                    h_mem = list(state["memory"])
                    h_lat = list(state["latency"])
                    h_work = list(state["workload"])

                    z_cpu = (x_cpu - _mean(h_cpu)) / (_std(h_cpu) + 1e-3) if len(h_cpu) >= 5 else 0.0
                    z_mem = (x_mem - _mean(h_mem)) / (_std(h_mem) + 1e-3) if len(h_mem) >= 5 else 0.0
                    z_lat = (x_lat - _mean(h_lat)) / (_std(h_lat) + 1e-3) if len(h_lat) >= 5 else 0.0
                    z_work = (x_work - _mean(h_work)) / (_std(h_work) + 1e-3) if len(h_work) >= 5 else 0.0

                    distress = _clamp(
                        0.35 * max(0.0, z_lat)
                        + 0.25 * max(0.0, z_cpu)
                        + 0.15 * max(0.0, z_mem)
                        + 0.15 * max(0.0, z_work)
                        + 0.10 * x_fail,
                        0.0,
                        10.0,
                    )

                    state["ema_distress"] = 0.25 * distress + 0.75 * float(state["ema_distress"])
                    distress_values.append(float(state["ema_distress"]))
                    failure_values.append(x_fail)

                    state["cpu"].append(x_cpu)
                    state["memory"].append(x_mem)
                    state["latency"].append(x_lat)
                    state["workload"].append(x_work)
                    state["failure"].append(x_fail)

            if distress_values:
                ranked = sorted(distress_values, reverse=True)
                top_k = max(1, int(round(len(ranked) * 0.2)))
                p_pressure = _clamp(_mean(ranked[:top_k]) / 3.0, 0.0, 1.0)
                p_instability = _clamp(_std(distress_values) / 3.0, 0.0, 1.0)
            else:
                p_pressure = 0.0
                p_instability = 0.0

            p_failure = _clamp(_mean(failure_values), 0.0, 1.0) if failure_values else 0.0

        # ----- A-space (trace latent representation) -----
        lat_norm = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        span_norm = _norm(float(p.span_count), float(span_lo), float(span_hi))
        svc_norm = _clamp(len(p.services) / 10.0, 0.0, 1.0)
        err_flag = 1.0 if p.has_error else 0.0
        incident_hit = 1.0 if (set(p.services) & incident_services) else 0.0

        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        # novelty from online token-mass approximation (A-space local diversity feature)
        toks = _trace_tokens(p)
        best_key = None
        best_sim = 0.0
        for key, ktoks in cluster_templates.items():
            sim = _jaccard(toks, ktoks)
            if sim > best_sim:
                best_sim = sim
                best_key = key

        if best_key is not None and best_sim >= 0.5:
            assigned_key = best_key
            mass = cluster_counts.get(assigned_key, 0)
            effective_mass = mass * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            effective_mass = 0.0

        a_novelty = _clamp((1.0 + effective_mass) ** -0.8, 0.0, 1.0)
        a_struct = _clamp(0.45 * lat_norm + 0.35 * span_norm + 0.20 * svc_norm, 0.0, 1.0)
        a_risk = _clamp(0.45 * err_flag + 0.35 * incident_hit + 0.20 * time_prox, 0.0, 1.0)

        # ----- Interaction Layer: A . P -----
        interaction_raw = (
            a_struct * p_pressure
            + a_risk * p_failure
            + a_novelty * p_instability
        ) / 3.0
        interaction = _clamp(_sigmoid(5.0 * (interaction_raw - 0.20)), 0.0, 1.0)

        # System-biased probability from true interaction + residual A-space risk.
        p_s = _clamp(0.75 * interaction + 0.20 * a_risk + 0.05 * lat_norm, 0.02, 0.98)

        # Diversity-biased probability from A-space novelty.
        p_d_floor = 0.20 if incident_hit > 0.0 else 0.05
        p_d = _clamp(a_novelty, p_d_floor, 1.0)

        # True dynamic voting gate (TraStrainer-inspired).
        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        u = rng.random()
        system_vote = u < p_s
        diversity_vote = u < p_d
        if theta > beta:
            keep = system_vote and diversity_vote
        else:
            keep = system_vote or diversity_vote

        # Spectrum-aware breathing hole for normal traces when under budget.
        if not keep and online_soft_cap and theta < beta:
            is_clean_normal = (err_flag <= 0.0) and (incident_hit <= 0.0)
            if is_clean_normal and rng.random() < min(0.20, beta - theta):
                keep = True

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))

        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        p_s_by_id[p.trace_id] = p_s
        seen_win.append((sec, err_flag, lat_norm, incident_hit))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

        if debug_trace_logs is not None:
            debug_trace_logs.append(
                {
                    "trace_id": p.trace_id,
                    "a_struct": round(float(a_struct), 6),
                    "a_risk": round(float(a_risk), 6),
                    "a_novelty": round(float(a_novelty), 6),
                    "p_pressure": round(float(p_pressure), 6),
                    "p_instability": round(float(p_instability), 6),
                    "p_failure": round(float(p_failure), 6),
                    "interaction": round(float(interaction), 6),
                    "p_s": round(float(p_s), 6),
                    "p_d": round(float(p_d), 6),
                    "theta": round(float(theta), 6),
                    "keep": keep,
                    "in_final": False,
                }
            )

    # Minimum error-density guardrail for spectrum-based RCA.
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        need_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < need_err:
            deficit = need_err - len(kept_err)
            cand_add = [tid for tid, point in by_id.items() if (tid not in kept_ids) and point.has_error]
            cand_add.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
            add_ids = cand_add[:deficit]
            cand_drop = [tid for tid in kept_ids if by_id.get(tid) and (not by_id[tid].has_error)]
            cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
            drop_n = min(len(add_ids), len(cand_drop))
            for tid in cand_drop[:drop_n]:
                kept_ids.discard(tid)
            for tid in add_ids[:drop_n]:
                kept_ids.add(tid)

    # Normalize to target cardinality deterministically.
    if len(kept_ids) > target_count:
        ranked_keep = sorted(kept_ids, key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
        kept_ids = set(ranked_keep[:target_count])
    elif len(kept_ids) < target_count:
        fillers = [p.trace_id for p in traces if p.trace_id not in kept_ids]
        fillers.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
        for tid in fillers:
            kept_ids.add(tid)
            if len(kept_ids) >= target_count:
                break

    # Scenario strict-cap floor.
    if scenario_windows and min_incident_traces_per_scenario > 0:
        start_s_by_id: Dict[str, int] = {tp.trace_id: int(tp.start_ns // 1_000_000_000) for tp in points}
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
        if target_count < non_empty_scenarios * floor_each:
            floor_each = 0

        if floor_each > 0:
            for ids in scenario_ids_by_idx:
                present = [tid for tid in kept_ids if tid in ids]
                if len(present) >= floor_each:
                    continue
                need = floor_each - len(present)
                cands = [tid for tid in ids if tid not in kept_ids]
                cands.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
                for tid in cands[:need]:
                    kept_ids.add(tid)

        if len(kept_ids) > target_count:
            scenario_counts = [0] * len(scenario_ids_by_idx)
            for tid in kept_ids:
                for idx in scenario_membership.get(tid, set()):
                    scenario_counts[idx] += 1

            def _can_remove(tid: str) -> bool:
                if floor_each <= 0:
                    return True
                for idx in scenario_membership.get(tid, set()):
                    if scenario_counts[idx] <= floor_each:
                        return False
                return True

            removable = sorted(kept_ids, key=lambda tid: score_by_id.get(tid, 0.0))
            for tid in removable:
                if len(kept_ids) <= target_count:
                    break
                if not _can_remove(tid):
                    continue
                kept_ids.remove(tid)
                for idx in scenario_membership.get(tid, set()):
                    scenario_counts[idx] -= 1

            if len(kept_ids) > target_count:
                ranked_keep = sorted(kept_ids, key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
                kept_ids = set(ranked_keep[:target_count])

    if debug_trace_logs is not None:
        final_set = set(kept_ids)
        for row in debug_trace_logs:
            row["in_final"] = row["trace_id"] in final_set

    return kept_ids
