import csv
import json
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

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
OracleMetricSignal = Dict[int, Dict[str, np.ndarray]]


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


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


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    if not points:
        return {}, {}
    return {}, {}


def _centered_moving_average(values: List[float], window_sec: int) -> List[float]:
    if not values:
        return []
    radius = max(1, int(window_sec // 2))
    n = len(values)
    prefix = [0.0] * (n + 1)
    for i, v in enumerate(values):
        prefix[i + 1] = prefix[i] + v

    out = [0.0] * n
    for i in range(n):
        lo = max(0, i - radius)
        hi = min(n - 1, i + radius)
        total = prefix[hi + 1] - prefix[lo]
        out[i] = total / float(hi - lo + 1)
    return out


def _maybe_load_priority_a(metric_dir: Path) -> OracleMetricSignal:
    # Best effort parser for precomputed DLinear/residual anomaly artifacts.
    out: OracleMetricSignal = defaultdict(dict)
    candidate_names = (
        "dlinear",
        "oracle",
        "residual",
        "anomaly",
        "prediction",
    )

    for file_path in metric_dir.rglob("*"):
        if not file_path.is_file():
            continue
        name_l = file_path.name.lower()
        if not any(tok in name_l for tok in candidate_names):
            continue

        if file_path.suffix.lower() == ".csv":
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        continue
                    headers = {h.lower(): h for h in reader.fieldnames}

                    # Minimal schema needed.
                    ts_key = headers.get("timestamp") or headers.get("time") or headers.get("sec")
                    svc_key = headers.get("service") or headers.get("svc") or headers.get("podname")
                    if not ts_key or not svc_key:
                        continue

                    cpu_key = headers.get("cpu_anomaly_score") or headers.get("cpu_score")
                    mem_key = headers.get("memory_anomaly_score") or headers.get("memory_score")
                    lat_key = headers.get("latency_anomaly_score") or headers.get("latency_score")
                    work_key = headers.get("workload_anomaly_score") or headers.get("workload_score")
                    fail_key = headers.get("failure_score") or headers.get("error_score")
                    if not all([cpu_key, mem_key, lat_key, work_key, fail_key]):
                        continue

                    for row in reader:
                        try:
                            sec = int(float(str(row.get(ts_key, "0")).strip()))
                            svc_raw = str(row.get(svc_key, "")).strip()
                            svc = _strip_pod_to_service(svc_raw)
                            vec = np.array(
                                [
                                    float(row.get(cpu_key, 0.0)),
                                    float(row.get(mem_key, 0.0)),
                                    float(row.get(lat_key, 0.0)),
                                    float(row.get(work_key, 0.0)),
                                    float(row.get(fail_key, 0.0)),
                                ],
                                dtype=float,
                            )
                        except Exception:
                            continue
                        out[sec][svc] = vec
            except Exception:
                continue

        if file_path.suffix.lower() == ".json":
            try:
                data = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            if not isinstance(data, list):
                continue
            for row in data:
                if not isinstance(row, dict):
                    continue
                try:
                    sec = int(float(row.get("timestamp", row.get("time", row.get("sec", 0)))))
                    svc = _strip_pod_to_service(str(row.get("service", row.get("svc", ""))))
                    vec = np.array(
                        [
                            float(row.get("cpu_anomaly_score", row.get("cpu_score", 0.0))),
                            float(row.get("memory_anomaly_score", row.get("memory_score", 0.0))),
                            float(row.get("latency_anomaly_score", row.get("latency_score", 0.0))),
                            float(row.get("workload_anomaly_score", row.get("workload_score", 0.0))),
                            float(row.get("failure_score", row.get("error_score", 0.0))),
                        ],
                        dtype=float,
                    )
                except Exception:
                    continue
                out[sec][svc] = vec

    return dict(out)


def _fallback_offline_oracle(metric_dir: Path, window_sec: int = 60) -> OracleMetricSignal:
    rows_by_service: Dict[str, List[Tuple[int, float, float, float, float, float]]] = defaultdict(list)
    for name in sorted(os.listdir(metric_dir)):
        if not name.endswith(".csv"):
            continue
        if name in {"dependency.csv", "front_service.csv", "source_50.csv", "destination_50.csv"}:
            continue

        file_path = metric_dir / name
        with file_path.open("r", encoding="utf-8") as f:
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
                failure = 1.0 - success_rate
                rows_by_service[service].append((sec, cpu, mem, latency, workload, failure))

    out: OracleMetricSignal = defaultdict(dict)
    for svc, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        secs = [r[0] for r in rows]
        cpu_vals = [r[1] for r in rows]
        mem_vals = [r[2] for r in rows]
        lat_vals = [r[3] for r in rows]
        work_vals = [r[4] for r in rows]
        fail_vals = [r[5] for r in rows]

        cpu_cma = _centered_moving_average(cpu_vals, window_sec)
        mem_cma = _centered_moving_average(mem_vals, window_sec)
        lat_cma = _centered_moving_average(lat_vals, window_sec)
        work_cma = _centered_moving_average(work_vals, window_sec)
        fail_cma = _centered_moving_average(fail_vals, window_sec)

        for i, sec in enumerate(secs):
            cpu_anom = abs(cpu_vals[i] - cpu_cma[i]) / (abs(cpu_cma[i]) + 1e-6)
            mem_anom = abs(mem_vals[i] - mem_cma[i]) / (abs(mem_cma[i]) + 1e-6)
            lat_anom = abs(lat_vals[i] - lat_cma[i]) / (abs(lat_cma[i]) + 1e-6)
            work_anom = abs(work_vals[i] - work_cma[i]) / (abs(work_cma[i]) + 1e-6)
            fail_anom = abs(fail_vals[i] - fail_cma[i]) / (abs(fail_cma[i]) + 1e-6)

            vec = np.array(
                [
                    _clamp(cpu_anom, 0.0, 5.0),
                    _clamp(mem_anom, 0.0, 5.0),
                    _clamp(lat_anom, 0.0, 5.0),
                    _clamp(work_anom, 0.0, 5.0),
                    _clamp(fail_anom, 0.0, 5.0),
                ],
                dtype=float,
            )
            out[sec][svc] = vec
    return dict(out)


def load_oracle_metric_signal(metric_dir: str) -> OracleMetricSignal:
    path = Path(metric_dir).resolve()
    if not path.exists() or not path.is_dir():
        return {}

    # Priority A: use any precomputed DLinear/oracle-style artifact if present.
    priority_a = _maybe_load_priority_a(path)
    if priority_a:
        return priority_a

    # Priority B: future-aware centered moving average residual oracle proxy.
    return _fallback_offline_oracle(path, window_sec=60)


def _composite_v52_oracle_metrics_upper_bound(
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
    oracle_signal_by_sec: Optional[OracleMetricSignal] = None,
    metric_lookback_sec: int = 20,
    lookback_sec: int = 60,
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
    oracle_signal_by_sec = oracle_signal_by_sec or {}

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
    kept_ids: Set[str] = set()
    by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
    kept_win: deque[Tuple[int, bool]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[Tuple[int, Tuple[str, ...]]] = deque()

    score_by_id: Dict[str, float] = {}

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

        aligned = []
        abs_sec = t0_sec + sec
        for svc in p.services:
            oracle_vec = None
            for s in range(abs_sec, abs_sec - max(1, metric_lookback_sec) - 1, -1):
                snap = oracle_signal_by_sec.get(s)
                if snap is None:
                    continue
                cand = snap.get(svc)
                if cand is not None:
                    oracle_vec = cand
                    break
            if oracle_vec is not None:
                aligned.append(oracle_vec)

        if aligned:
            p5 = np.max(np.stack(aligned, axis=0), axis=0)
        else:
            p5 = np.zeros(5, dtype=float)

        # Project to 6D to align with A-vector dimensionality.
        p_aligned = np.array(
            [
                float(p5[0]),
                float(p5[1]),
                float(p5[2]),
                float(p5[3]),
                float(p5[4]),
                float(np.max(p5)),
            ],
            dtype=float,
        )

        alignment_score = float(np.dot(a_vector, p_aligned))
        p_s = _clamp(_sigmoid(8.0 * (alignment_score - 0.35)), 0.01, 0.99)

        p_d_floor = 0.20 if incident_hit > 0.0 else 0.05
        p_d = _clamp(novelty_score, p_d_floor, 1.0)

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
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

        if debug_trace_logs is not None:
            debug_trace_logs.append(
                {
                    "trace_id": p.trace_id,
                    "A_vector": [round(float(v), 6) for v in a_vector.tolist()],
                    "P_aligned": [round(float(v), 6) for v in p_aligned.tolist()],
                    "alignment_score": round(float(alignment_score), 6),
                    "p_s": round(float(p_s), 6),
                    "p_d": round(float(p_d), 6),
                    "gate_mode": gate_mode,
                    "keep": keep,
                    "in_final": False,
                }
            )

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