import csv
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

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

    rows_by_service: Dict[str, List[Tuple[int, float, float, float, float]]] = defaultdict(list)

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
                rows_by_service[service].append((sec, cpu, mem, latency, success_rate))

    out: Dict[int, MetricsSnapshot] = defaultdict(dict)

    for service, rows in rows_by_service.items():
        rows.sort(key=lambda x: x[0])
        sec_acc: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {
                "cpu": [],
                "memory": [],
                "latency": [],
                "success_rate": [],
            }
        )

        for sec, cpu, mem, latency, success_rate in rows:
            acc = sec_acc[sec]
            acc["cpu"].append(cpu)
            acc["memory"].append(mem)
            acc["latency"].append(latency)
            acc["success_rate"].append(success_rate)

        for sec, acc in sec_acc.items():
            out[sec][service] = {
                "cpu": _mean(acc["cpu"]),
                "memory": _mean(acc["memory"]),
                "latency": _mean(acc["latency"]),
                "success_rate": _mean(acc["success_rate"]),
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


def _service_risk_and_top(
    traces: List[TracePoint],
    metrics_stream_by_sec: Dict[int, MetricsSnapshot],
    metric_lookback_sec: int,
) -> Tuple[Dict[str, float], Dict[str, str], Dict[str, float]]:
    service_hist: Dict[str, Dict[str, Deque[float]]] = defaultdict(
        lambda: {
            "cpu": deque(maxlen=10),
            "memory": deque(maxlen=10),
            "latency": deque(maxlen=10),
            "success_rate": deque(maxlen=10),
        }
    )

    service_risk: Dict[str, float] = defaultdict(float)
    trace_risk: Dict[str, float] = {}
    trace_top_service: Dict[str, str] = {}
    trace_top_risk: Dict[str, float] = {}

    # For precursor boost over previous 10s.
    recent_by_service: Dict[str, Deque[Tuple[int, str]]] = defaultdict(deque)
    boost_by_trace: Dict[str, float] = defaultdict(float)

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000)
        high_risk_services: Set[str] = set()

        top_svc = ""
        top_rv = 0.0

        for svc in sorted(p.services):
            aligned = get_aligned_metrics(sec, svc, metrics_stream_by_sec, lookback_sec=metric_lookback_sec)
            if aligned is None:
                rv = float(service_risk.get(svc, 0.0))
            else:
                h = service_hist[svc]

                x_cpu = _clamp(float(aligned.get("cpu", 0.0)), 0.0, 1.0)
                x_mem = _clamp(float(aligned.get("memory", 0.0)), 0.0, 1.0)
                x_lat = max(0.0, float(aligned.get("latency", 0.0)))
                x_succ = _clamp(float(aligned.get("success_rate", 1.0)), 0.0, 1.0)

                z_cpu = max(0.0, (x_cpu - _mean(list(h["cpu"]))) / (_std(list(h["cpu"])) + 0.001)) if len(h["cpu"]) >= 5 else 0.0
                z_mem = max(0.0, (x_mem - _mean(list(h["memory"]))) / (_std(list(h["memory"])) + 0.001)) if len(h["memory"]) >= 5 else 0.0
                z_lat = max(0.0, (x_lat - _mean(list(h["latency"]))) / (_std(list(h["latency"])) + 0.001)) if len(h["latency"]) >= 5 else 0.0
                error = 1.0 if x_succ < 0.99 else 0.0

                risk_raw = 0.4 * z_lat + 0.3 * z_cpu + 0.2 * z_mem + 0.1 * error
                rv = _clamp(_sigmoid(risk_raw), 0.0, 1.0)
                service_risk[svc] = rv

                h["cpu"].append(x_cpu)
                h["memory"].append(x_mem)
                h["latency"].append(x_lat)
                h["success_rate"].append(x_succ)

            if rv > top_rv:
                top_rv = rv
                top_svc = svc
            if rv > 0.80:
                high_risk_services.add(svc)

        if high_risk_services:
            for svc in high_risk_services:
                q = recent_by_service[svc]
                while q and q[0][0] < sec - 10:
                    q.popleft()
                for old_sec, old_tid in q:
                    dt = sec - old_sec
                    if dt < 0 or dt > 10:
                        continue
                    boost = math.exp(-dt / 10.0)
                    if boost > boost_by_trace.get(old_tid, 0.0):
                        boost_by_trace[old_tid] = boost

        for svc in p.services:
            q = recent_by_service[svc]
            q.append((sec, p.trace_id))
            while q and q[0][0] < sec - 10:
                q.popleft()

        trace_risk[p.trace_id] = top_rv
        trace_top_service[p.trace_id] = top_svc
        trace_top_risk[p.trace_id] = top_rv

    reserve_score: Dict[str, float] = {}
    for tid, risk in trace_risk.items():
        boost = boost_by_trace.get(tid, 0.0)
        reserve_score[tid] = _clamp(0.7 * risk + 0.3 * boost, 0.0, 1.0)

    return reserve_score, trace_top_service, trace_top_risk


def _main_priority_scores(
    points: List[TracePoint],
    incident_services: Set[str],
    incident_anchor_sec: Optional[int],
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
) -> Dict[str, float]:
    if not points:
        return {}

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    metric_state: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "latency": deque(maxlen=max(8, lookback_sec)),
            "error": deque(maxlen=max(8, lookback_sec)),
            "throughput": deque(maxlen=max(8, lookback_sec)),
            "burst": deque(maxlen=max(8, lookback_sec)),
            "ema": 0.0,
            "sec": None,
            "count": 0.0,
        }
    )

    seen_win: Deque[Tuple[int, float, float, float]] = deque()
    cluster_templates: Dict[Tuple[str, ...], FrozenSet[str]] = {}
    cluster_counts: Dict[Tuple[str, ...], int] = defaultdict(int)
    cluster_win: Deque[Tuple[int, Tuple[str, ...]]] = deque()

    cluster_merge_threshold = 0.5
    score_by_id: Dict[str, float] = {}

    for p in traces:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        incident_service = 1.0 if (set(p.services) & incident_services) else 0.0
        err_flag = 1.0 if p.has_error else 0.0

        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        historical_preference_vector: PreferenceVector = {}
        historical_metric_stats: MetricStats = {}

        for service in sorted(p.services):
            state = metric_state[service]
            if state["sec"] != sec:
                current_count = 0.0
            else:
                current_count = float(state["count"])

            throughput = math.log1p(current_count + 1.0)
            ema = 0.2 * throughput + 0.8 * float(state["ema"])
            burst = throughput - ema

            latency_hist = list(state["latency"])
            error_hist = list(state["error"])
            throughput_hist = list(state["throughput"])
            burst_hist = list(state["burst"])

            latency_mu = _mean(latency_hist)
            latency_sg = _std(latency_hist)
            error_mu = _mean(error_hist)
            error_sg = _std(error_hist)
            throughput_mu = _mean(throughput_hist)
            throughput_sg = _std(throughput_hist)
            burst_mu = _mean(burst_hist)
            burst_sg = _std(burst_hist)

            historical_preference_vector[service] = {
                "latency": _clamp((lat_pressure - latency_mu) / (latency_sg + 1.0), 0.0, 10.0),
                "error": _clamp((err_flag - error_mu) / (error_sg + 0.01), 0.0, 10.0),
                "throughput": _clamp((throughput - throughput_mu) / (throughput_sg + 0.5), 0.0, 10.0),
                "burst": _clamp((burst - burst_mu) / (burst_sg + 0.01), 0.0, 10.0),
            }
            historical_metric_stats[service] = {
                "history_count": len(latency_hist),
            }

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        mu_err = _mean(hist_err)
        mu_lat = _mean(hist_lat)
        mu_inc = _mean(hist_inc)
        sg_err = _std(hist_err)
        sg_lat = _std(hist_lat)
        sg_inc = _std(hist_inc)

        z_err = abs(err_flag - mu_err) / (sg_err + 1e-6)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + 1e-6)
        z_inc = abs(incident_service - mu_inc) / (sg_inc + 1e-6)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc
        alpha_eff = alpha

        p_s = sv35comp.compute_sampling_probability(
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

        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d

        seen_win.append((sec, err_flag, lat_pressure, incident_service))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

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

    return score_by_id


def _composite_v44_reserve_channel_sampler(
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
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    by_id = {p.trace_id: p for p in traces}
    incident_services = incident_services or set()
    metrics_stream_by_sec = metrics_stream_by_sec or {}

    k_total = max(1, min(total, int(round(total * beta))))
    reserve_k = int(math.ceil(0.2 * k_total))
    main_k = k_total - reserve_k

    # Main channel (80%): frozen v3.5 backbone.
    if main_k > 0:
        main_budget_pct = (main_k / float(total)) * 100.0
        main_selected = sv35comp._composite_v3_metrics_strictcap(
            points=traces,
            budget_pct=main_budget_pct,
            preference_vector=preference_vector,
            metric_stats=metric_stats,
            scenario_windows=scenario_windows,
            incident_anchor_sec=incident_anchor_sec,
            seed=seed,
            incident_services=incident_services,
            min_incident_traces_per_scenario=min_incident_traces_per_scenario,
            online_soft_cap=online_soft_cap,
        )
    else:
        main_selected = set()

    # Reserve channel (20%): deterministic metric ranking.
    reserve_score, trace_top_service, trace_top_risk = _service_risk_and_top(
        traces=traces,
        metrics_stream_by_sec=metrics_stream_by_sec,
        metric_lookback_sec=metric_lookback_sec,
    )

    reserve_ranked = sorted(
        reserve_score.keys(),
        key=lambda tid: (
            reserve_score.get(tid, 0.0),
            1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0,
            by_id[tid].duration_ms if tid in by_id else 0.0,
            tid,
        ),
        reverse=True,
    )
    reserve_selected = set(reserve_ranked[: max(0, reserve_k)])

    # Merge without evicting main traces.
    final_selected = set(main_selected)
    final_selected.update(reserve_selected)

    # Priority-3 score source for final strict cap.
    main_score = _main_priority_scores(
        points=traces,
        incident_services=incident_services,
        incident_anchor_sec=incident_anchor_sec,
    )

    if len(final_selected) > k_total:
        ranked_final = sorted(
            final_selected,
            key=lambda tid: (
                1.0 if (tid in main_selected and tid in reserve_selected) else 0.0,
                reserve_score.get(tid, 0.0),
                main_score.get(tid, 0.0),
                1.0 if by_id.get(tid) and by_id[tid].has_error else 0.0,
                tid,
            ),
            reverse=True,
        )
        final_selected = set(ranked_final[:k_total])

    if debug_trace_logs is not None:
        for tid in sorted(final_selected):
            in_main = tid in main_selected
            in_reserve = tid in reserve_selected
            if in_main and in_reserve:
                channel = "both"
            elif in_main:
                channel = "main"
            else:
                channel = "reserve"

            debug_trace_logs.append(
                {
                    "trace_id": tid,
                    "channel": channel,
                    "reserve_score": round(float(reserve_score.get(tid, 0.0)), 6),
                    "service_top": str(trace_top_service.get(tid, "")),
                    "risk": round(float(trace_top_risk.get(tid, 0.0)), 6),
                }
            )

    return final_selected
