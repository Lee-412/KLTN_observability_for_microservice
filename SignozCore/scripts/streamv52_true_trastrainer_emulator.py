import csv
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

import numpy as np


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


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    if not points:
        return {}, {}
    return {}, {}


def _composite_v52_true_trastrainer_emulator(
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
    tau: float = 0.35,
    tau_low: float = 0.55,
    tau_high: float = 0.72,
    tau_div: float = 0.60,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    del preference_vector, metric_stats, seed, online_soft_cap

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

    # A-space online states.
    seen = 0
    kept_ids: Set[str] = set()
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
    kept_win: Deque[Tuple[int, bool]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: Deque[Tuple[int, Tuple[str, ...]]] = deque()

    # P-space online states (independent from traces).
    service_state: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "obs_latency": deque(maxlen=30),
            "obs_failure": deque(maxlen=30),
            "obs_workload": deque(maxlen=30),
            "ema_latency": 0.0,
            "ema_failure": 0.0,
            "ema_workload": 0.0,
        }
    )
    p_service_vector: Dict[str, np.ndarray] = {}
    p_processed_secs: Set[int] = set()

    score_by_id: Dict[str, float] = {}
    p_s_by_id: Dict[str, float] = {}

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        while kept_win and kept_win[0][0] < sec - lookback_sec + 1:
            kept_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        # Layer P: update predictive system latent vector only from real metrics.
        if sec not in p_processed_secs:
            p_processed_secs.add(sec)
            abs_sec = t0_sec + sec
            snap: Optional[MetricsSnapshot] = None
            for s in range(abs_sec, abs_sec - max(1, metric_lookback_sec) - 1, -1):
                cand = metrics_stream_by_sec.get(s)
                if cand:
                    snap = cand
                    break

            if snap:
                for svc, m in snap.items():
                    st = service_state[svc]
                    latency = max(0.0, float(m.get("latency", 0.0)))
                    failure = 1.0 - _clamp(float(m.get("success", 1.0)), 0.0, 1.0)
                    workload = max(0.0, float(m.get("workload", 0.0)))

                    # Predictive baseline with EMA.
                    if len(st["obs_latency"]) == 0:
                        st["ema_latency"] = latency
                        st["ema_failure"] = failure
                        st["ema_workload"] = workload
                    else:
                        st["ema_latency"] = 0.2 * latency + 0.8 * float(st["ema_latency"])
                        st["ema_failure"] = 0.2 * failure + 0.8 * float(st["ema_failure"])
                        st["ema_workload"] = 0.2 * workload + 0.8 * float(st["ema_workload"])

                    obs_latency = list(st["obs_latency"])
                    obs_failure = list(st["obs_failure"])
                    obs_workload = list(st["obs_workload"])

                    lat_std = _std(obs_latency)
                    fail_std = _std(obs_failure)
                    work_std = _std(obs_workload)

                    z_latency = _clamp((latency - float(st["ema_latency"])) / (lat_std + 1e-3), 0.0, 3.0)
                    z_failure = _clamp((failure - float(st["ema_failure"])) / (fail_std + 1e-3), 0.0, 3.0)
                    z_workload = _clamp((workload - float(st["ema_workload"])) / (work_std + 1e-3), 0.0, 3.0)

                    # Instability from rolling relative variation.
                    lat_mu = _mean(obs_latency)
                    fail_mu = _mean(obs_failure)
                    work_mu = _mean(obs_workload)
                    lat_inst = lat_std / (abs(lat_mu) + 1e-3) if obs_latency else 0.0
                    fail_inst = fail_std / (abs(fail_mu) + 1e-3) if obs_failure else 0.0
                    work_inst = work_std / (abs(work_mu) + 1e-3) if obs_workload else 0.0
                    z_instability = _clamp((lat_inst + fail_inst + work_inst) / 3.0, 0.0, 3.0)

                    # Trend: recent mean - older mean.
                    if len(obs_latency) >= 6:
                        half = len(obs_latency) // 2
                        older = _mean(obs_latency[:half])
                        recent = _mean(obs_latency[half:])
                        trend_raw = recent - older
                    else:
                        trend_raw = latency - _mean(obs_latency) if obs_latency else 0.0
                    z_trend = _clamp(trend_raw / (lat_std + 1e-3), 0.0, 3.0)

                    p_service_vector[svc] = np.array(
                        [z_latency, z_failure, z_workload, z_instability, z_trend],
                        dtype=float,
                    )

                    st["obs_latency"].append(latency)
                    st["obs_failure"].append(failure)
                    st["obs_workload"].append(workload)

        # Layer A: true trace vector (no early scalar collapse).
        lat_norm = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        err_flag = 1.0 if p.has_error else 0.0
        svc_count_norm = _clamp(len(p.services) / 10.0, 0.0, 1.0)
        incident_hit = 1.0 if (set(p.services) & incident_services) else 0.0

        if incident_anchor_sec is None:
            temporal_proximity = 0.5
        else:
            temporal_proximity = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

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

        novelty_score = _clamp((1.0 + effective_mass) ** -0.8, 0.0, 1.0)

        a_vector = np.array(
            [
                lat_norm,
                err_flag,
                svc_count_norm,
                incident_hit,
                novelty_score,
                temporal_proximity,
            ],
            dtype=float,
        )

        # Alignment projection: max-pool service-level P-space over trace path.
        aligned = [p_service_vector.get(svc) for svc in p.services if svc in p_service_vector]
        if aligned:
            p_aligned = np.max(np.stack(aligned, axis=0), axis=0)
        else:
            p_aligned = np.zeros(5, dtype=float)

        p_projected = np.array(
            [
                float(p_aligned[0]),
                float(p_aligned[1]),
                float(p_aligned[2]),
                float(p_aligned[3]),
                float(p_aligned[4]),
                float(np.max(p_aligned)),
            ],
            dtype=float,
        )

        # True interaction.
        interaction = float(np.dot(a_vector, p_projected))
        p_s = _clamp(_sigmoid(6.0 * (interaction - tau)), 0.01, 0.99)

        # Diversity channel.
        p_d_floor = 0.20 if incident_hit > 0.0 else 0.05
        p_d = _clamp(novelty_score, p_d_floor, 1.0)

        # Dynamic voting gate.
        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        rho = theta / beta if beta > 0 else float("inf")
        if rho <= 1.0:
            gate_mode = "or"
            keep = (p_s > tau_low) or (p_d > tau_div)
        else:
            gate_mode = "and"
            keep = (p_s > tau_high) and (p_d > tau_div)

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))

        score_by_id[p.trace_id] = 0.80 * p_s + 0.20 * p_d
        p_s_by_id[p.trace_id] = p_s
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

        if debug_trace_logs is not None:
            debug_trace_logs.append(
                {
                    "trace_id": p.trace_id,
                    "A_vector": [round(float(v), 6) for v in a_vector.tolist()],
                    "P_projected": [round(float(v), 6) for v in p_projected.tolist()],
                    "interaction": round(float(interaction), 6),
                    "p_s": round(float(p_s), 6),
                    "p_d": round(float(p_d), 6),
                    "gate_mode": gate_mode,
                    "keep": keep,
                    "in_final": False,
                }
            )

    # MicroRank-specific spectrum guard (minimum error ratio).
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
