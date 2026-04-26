"""
Stream v3.94.1 -- metric-native consensus ensemble.

Mandatory design order:
1) v3.9 tuned RCA core as inner member sampler.
2) v9-style metric evidence extraction + logit-space modulation.
3) v3.94 consensus over fixed seed ensemble.
4) optional v13-style bounded swap refine (disabled for v3.94.1 default).

This module intentionally avoids:
- direct probability uplift (uses logit modulation only)
- weak prefix-only service->pod matching
- raw max-only metric aggregation (uses top-k projection)
"""

from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import streamv13_v36_swap_contrast as sv13
import streamv39_tuned_rca as sv39
from streamv39_tuned_rca import (
    MetricStats,
    PreferenceVector,
    TracePoint,
    _clamp,
    _rca_quadrant_value,
)

MetricsSnapshotByPod = Dict[str, Dict[str, float]]
MetricsStreamBySec = Dict[int, MetricsSnapshotByPod]
ServicePodRegistry = Dict[str, List[Dict[str, Any]]]

FIXED_ENSEMBLE_SEEDS = [11, 22, 33, 44, 55]


def build_metric_inputs(points: List[TracePoint]) -> Tuple[PreferenceVector, MetricStats]:
    """Compatibility hook for the runner interface."""
    return sv39.build_metric_inputs(points)


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


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    mid = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[mid])
    return 0.5 * (float(xs[mid - 1]) + float(xs[mid]))


def _mad(values: List[float]) -> float:
    if not values:
        return 0.0
    med = _median(values)
    dev = [abs(value - med) for value in values]
    return _median(dev)


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _logit(p: float, eps: float = 1e-6) -> float:
    pp = _clamp(float(p), eps, 1.0 - eps)
    return math.log(pp / (1.0 - pp))


def _topk_projection(values: List[float], weights: Tuple[float, float, float] = (0.6, 0.3, 0.1)) -> float:
    if not values:
        return 0.0
    ranked = sorted(values, reverse=True)
    used_values = ranked[: len(weights)]
    used_weights = list(weights[: len(used_values)])
    weight_sum = sum(used_weights)
    if weight_sum <= 0.0:
        return 0.0
    return sum(v * w for v, w in zip(used_values, used_weights)) / weight_sum


def _normalize_name(name: str) -> str:
    return str(name).strip().lower().replace("_", "-")


def _strip_k8s_hash_suffix(name: str) -> str:
    parts = _normalize_name(name).split("-")
    if len(parts) < 3:
        return _normalize_name(name)
    tail1 = parts[-1]
    tail2 = parts[-2]
    is_hash1 = bool(tail1) and tail1.isalnum() and 5 <= len(tail1) <= 20
    is_hash2 = bool(tail2) and tail2.isalnum() and 5 <= len(tail2) <= 20
    if is_hash1 and is_hash2:
        return "-".join(parts[:-2])
    return _normalize_name(name)


def _tokenize_name(name: str) -> Set[str]:
    base = _normalize_name(name)
    tokens = {tok for tok in base.split("-") if tok}
    stripped = _strip_k8s_hash_suffix(base)
    tokens.update(tok for tok in stripped.split("-") if tok)
    return tokens


def _name_similarity(service: str, pod: str) -> float:
    svc = _normalize_name(service)
    pod_norm = _normalize_name(pod)
    pod_stripped = _strip_k8s_hash_suffix(pod_norm)
    seq_raw = SequenceMatcher(None, svc, pod_norm).ratio()
    seq_stripped = SequenceMatcher(None, svc, pod_stripped).ratio()
    svc_tokens = _tokenize_name(svc)
    pod_tokens = _tokenize_name(pod_norm)
    union = len(svc_tokens | pod_tokens)
    token_jaccard = (len(svc_tokens & pod_tokens) / float(union)) if union > 0 else 0.0
    return _clamp(max(seq_raw, seq_stripped, token_jaccard), 0.0, 1.0)


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    pos = _clamp(pct, 0.0, 100.0) / 100.0 * float(len(xs) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(xs[lo])
    w = pos - lo
    return float(xs[lo] * (1.0 - w) + xs[hi] * w)


def _budget_suspicious_percentile(budget_pct: float) -> float:
    if budget_pct <= 0.1:
        return 85.0
    if budget_pct <= 1.0:
        return 75.0
    return 65.0


def normalize_metric_stream(raw_metrics: Optional[MetricsStreamBySec]) -> MetricsStreamBySec:
    """Normalize raw metric snapshots to mandatory v394.1 schema.

    Output schema:
    metric_stream[second][pod_id] = {
      cpu, memory, latency_p99, workload,
      node_pressure, success_proxy, error_proxy,
      cpu_z, memory_z, latency_p99_z, workload_z, node_pressure_z
    }
    """
    if not raw_metrics:
        return {}

    by_pod_by_sec: Dict[str, Dict[int, Dict[str, float]]] = defaultdict(dict)
    for sec, sec_snap in raw_metrics.items():
        sec_int = int(sec)
        for pod_id, row in sec_snap.items():
            cpu = _clamp(float(row.get("cpu", row.get("cpu_rate", 0.0))), 0.0, 1.0)
            memory = _clamp(float(row.get("memory", row.get("mem_rate", 0.0))), 0.0, 1.0)
            latency_p99 = max(0.0, float(row.get("latency_p99", row.get("latency", 0.0))))
            workload = max(0.0, float(row.get("workload", 0.0)))
            node_pressure = float(row.get("node_pressure", row.get("node_pressure_z", max(cpu, memory))))
            node_pressure = _clamp(node_pressure, 0.0, 10.0)
            success_proxy = _clamp(float(row.get("success_proxy", row.get("success_rate", 1.0))), 0.0, 1.0)
            error_proxy = _clamp(float(row.get("error_proxy", 1.0 - success_proxy)), 0.0, 1.0)

            by_pod_by_sec[pod_id][sec_int] = {
                "cpu": cpu,
                "memory": memory,
                "latency_p99": latency_p99,
                "workload": workload,
                "node_pressure": node_pressure,
                "success_proxy": success_proxy,
                "error_proxy": error_proxy,
            }

    out: MetricsStreamBySec = defaultdict(dict)
    for pod_id, sec_map in by_pod_by_sec.items():
        hist_cpu: List[float] = []
        hist_mem: List[float] = []
        hist_lat: List[float] = []
        hist_work: List[float] = []
        hist_node: List[float] = []

        for sec in sorted(sec_map.keys()):
            snap = sec_map[sec]

            def _robust_positive_z(value: float, hist: List[float]) -> float:
                if len(hist) < 5:
                    return 0.0
                med = _median(hist)
                mad = _mad(hist)
                sigma = max(1e-6, 1.4826 * mad)
                return _clamp((value - med) / sigma, 0.0, 10.0)

            cpu_z = _robust_positive_z(snap["cpu"], hist_cpu)
            mem_z = _robust_positive_z(snap["memory"], hist_mem)
            lat_z = _robust_positive_z(snap["latency_p99"], hist_lat)
            work_z = _robust_positive_z(snap["workload"], hist_work)
            node_z = _robust_positive_z(snap["node_pressure"], hist_node)

            out[sec][pod_id] = {
                **snap,
                "cpu_z": cpu_z,
                "memory_z": mem_z,
                "latency_p99_z": lat_z,
                "workload_z": work_z,
                "node_pressure_z": node_z,
            }

            hist_cpu.append(snap["cpu"])
            hist_mem.append(snap["memory"])
            hist_lat.append(snap["latency_p99"])
            hist_work.append(snap["workload"])
            hist_node.append(snap["node_pressure"])

    return dict(out)


def _build_pod_metric_index(metrics_stream: MetricsStreamBySec) -> Tuple[Dict[str, List[int]], Dict[str, List[Dict[str, float]]]]:
    sec_index: Dict[str, List[int]] = defaultdict(list)
    snap_index: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for sec in sorted(metrics_stream.keys()):
        for pod_id, snap in metrics_stream[sec].items():
            sec_index[pod_id].append(int(sec))
            snap_index[pod_id].append(snap)
    return dict(sec_index), dict(snap_index)


def _lookup_nearest_snapshot(
    pod_id: str,
    trace_sec: int,
    sec_index: Dict[str, List[int]],
    snap_index: Dict[str, List[Dict[str, float]]],
    lookback_sec: int,
) -> Tuple[Optional[Dict[str, float]], float]:
    """Lookup nearest timestamp snapshot within lookback and return temporal alignment score."""
    secs = sec_index.get(pod_id)
    snaps = snap_index.get(pod_id)
    if not secs or not snaps:
        return None, 0.0

    idx = bisect_right(secs, int(trace_sec)) - 1
    if idx < 0:
        return None, 0.0

    sec = secs[idx]
    gap = int(trace_sec) - int(sec)
    if gap < 0 or gap > max(1, int(lookback_sec)):
        return None, 0.0

    temporal_alignment = math.exp(-float(gap) / float(max(1, lookback_sec)))
    return snaps[idx], _clamp(temporal_alignment, 0.0, 1.0)


def build_service_pod_registry(
    points: List[TracePoint],
    metric_stream: MetricsStreamBySec,
    deployment_registry: Optional[Dict[str, Set[str]]] = None,
) -> ServicePodRegistry:
    """Build confidence-weighted service->pod registry.

    Confidence factors:
    1) naming similarity
    2) deployment/service alignment
    3) temporal co-occurrence
    4) trace pod occurrence frequency proxy
    """
    if not points or not metric_stream:
        return {}

    deployment_registry = deployment_registry or {}

    service_trace_count: Dict[str, int] = defaultdict(int)
    service_secs: Dict[str, Set[int]] = defaultdict(set)
    for p in points:
        sec = int(p.start_ns // 1_000_000_000)
        for svc in p.services:
            service_trace_count[svc] += 1
            service_secs[svc].add(sec)

    pod_secs: Dict[str, Set[int]] = defaultdict(set)
    for sec, sec_snap in metric_stream.items():
        for pod_id in sec_snap.keys():
            pod_secs[pod_id].add(int(sec))

    max_service_trace = max(service_trace_count.values()) if service_trace_count else 1
    registry: ServicePodRegistry = {}

    w_name = 0.35
    w_registry = 0.25
    w_temporal = 0.25
    w_trace = 0.15

    for svc in sorted(service_secs.keys()):
        svc_secs = service_secs.get(svc, set())
        if not svc_secs:
            registry[svc] = []
            continue

        candidates: List[Dict[str, Any]] = []
        for pod_id, pod_active_secs in pod_secs.items():
            if not pod_active_secs:
                continue

            similarity = _name_similarity(svc, pod_id)

            # Deployment alignment from explicit registry (if provided) + canonical k8s stripping.
            canonical_pod = _strip_k8s_hash_suffix(pod_id)
            expected_pods = deployment_registry.get(svc, set())
            registry_alignment = 1.0 if pod_id in expected_pods else 0.0
            if canonical_pod == _normalize_name(svc):
                registry_alignment = max(registry_alignment, 0.85)
            elif _name_similarity(svc, canonical_pod) >= 0.75:
                registry_alignment = max(registry_alignment, 0.55)

            inter = len(svc_secs & pod_active_secs)
            union = len(svc_secs | pod_active_secs)
            temporal_overlap = (inter / float(union)) if union > 0 else 0.0

            trace_occurrence_frequency = (inter / float(max(1, len(svc_secs))))
            trace_occurrence_frequency *= (service_trace_count[svc] / float(max(1, max_service_trace)))
            trace_occurrence_frequency = _clamp(trace_occurrence_frequency, 0.0, 1.0)

            confidence = (
                w_name * similarity
                + w_registry * registry_alignment
                + w_temporal * temporal_overlap
                + w_trace * trace_occurrence_frequency
            )
            confidence = _clamp(confidence, 0.0, 1.0)

            if confidence < 0.08:
                continue

            candidates.append(
                {
                    "pod_id": pod_id,
                    "confidence_score": confidence,
                    "mapping_source": "name+registry+temporal+trace",
                    "temporal_validity": temporal_overlap,
                    "name_similarity": similarity,
                    "registry_alignment": registry_alignment,
                    "trace_occurrence_frequency": trace_occurrence_frequency,
                }
            )

        candidates.sort(
            key=lambda item: (
                -float(item["confidence_score"]),
                -float(item["temporal_validity"]),
                str(item["pod_id"]),
            )
        )
        registry[svc] = candidates[:6]

    return registry


def _dur_bucket(duration_ms: float) -> str:
    if duration_ms < 100.0:
        return "lt100"
    if duration_ms < 500.0:
        return "100_500"
    if duration_ms < 2000.0:
        return "500_2000"
    return "ge2000"


def _span_bucket(span_count: int) -> str:
    if span_count <= 3:
        return "s1"
    if span_count <= 8:
        return "s2"
    if span_count <= 20:
        return "s3"
    return "s4"


def _trace_tokens(trace: TracePoint) -> FrozenSet[str]:
    tokens = {f"svc:{svc}" for svc in trace.services}
    tokens.add(f"dur:{_dur_bucket(trace.duration_ms)}")
    tokens.add(f"span:{_span_bucket(trace.span_count)}")
    return frozenset(tokens)


def _compute_rca_state(
    points: List[TracePoint],
    incident_services: Set[str],
    beta: float,
    high_error_service_threshold: float,
) -> Tuple[Dict[str, float], Dict[str, int], Dict[str, float], Dict[str, float], Dict[str, float]]:
    """Compute RCA values + suspicious features for all traces in stream order."""
    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    min_suspicious_history = 4 if beta <= 0.0015 else 8

    svc_trace_count: Dict[str, int] = defaultdict(int)
    svc_error_trace_count: Dict[str, int] = defaultdict(int)
    cluster_count: Dict[Tuple[str, ...], int] = defaultdict(int)

    rca_value_by_id: Dict[str, float] = {}
    suspicious_hit_by_id: Dict[str, int] = {}
    suspicious_score_by_id: Dict[str, float] = {}
    structural_rarity_by_id: Dict[str, float] = {}
    contrast_term_by_id: Dict[str, float] = {}

    for trace in traces:
        dynamic_suspicious = set(incident_services)
        for svc in trace.services:
            if svc_trace_count[svc] > min_suspicious_history:
                err_rate = svc_error_trace_count[svc] / float(max(1, svc_trace_count[svc]))
                if err_rate > high_error_service_threshold:
                    dynamic_suspicious.add(svc)

        suspicious_hit = len(set(trace.services) & dynamic_suspicious)
        suspicious_ratio = suspicious_hit / float(max(1, len(trace.services)))
        rca_value = _rca_quadrant_value(trace.has_error, suspicious_hit, suspicious_ratio)

        key = tuple(sorted(trace.services)) + (_dur_bucket(trace.duration_ms), _span_bucket(trace.span_count))
        rarity = 1.0 / math.sqrt(1.0 + float(cluster_count[key]))

        err_rates: List[float] = []
        anomaly_flags: List[float] = []
        for svc in trace.services:
            seen = svc_trace_count[svc]
            if seen <= 0:
                continue
            err_rate = svc_error_trace_count[svc] / float(max(1, seen))
            err_rates.append(err_rate)
            anomaly_flags.append(1.0 if err_rate > high_error_service_threshold else 0.0)

        error_density = _mean(err_rates) if err_rates else (1.0 if trace.has_error else 0.0)
        anomaly_frequency = _mean(anomaly_flags) if anomaly_flags else 0.0
        suspicious_score = error_density + anomaly_frequency + rca_value + rarity

        if trace.has_error and suspicious_hit > 0:
            contrast_term = 0.80
        elif (not trace.has_error) and suspicious_hit == 0:
            contrast_term = 1.00
        elif trace.has_error and suspicious_hit == 0:
            contrast_term = 0.30
        else:
            contrast_term = 0.55

        rca_value_by_id[trace.trace_id] = rca_value
        suspicious_hit_by_id[trace.trace_id] = suspicious_hit
        suspicious_score_by_id[trace.trace_id] = suspicious_score
        structural_rarity_by_id[trace.trace_id] = rarity
        contrast_term_by_id[trace.trace_id] = contrast_term

        for svc in trace.services:
            svc_trace_count[svc] += 1
            if trace.has_error:
                svc_error_trace_count[svc] += 1
        cluster_count[key] += 1

    return (
        rca_value_by_id,
        suspicious_hit_by_id,
        suspicious_score_by_id,
        structural_rarity_by_id,
        contrast_term_by_id,
    )


def _trace_metric_evidence(
    trace: TracePoint,
    trace_sec: int,
    service_pod_registry: ServicePodRegistry,
    sec_index: Dict[str, List[int]],
    snap_index: Dict[str, List[Dict[str, float]]],
    lookback_sec: int,
) -> Tuple[float, float, float, float, float, int]:
    """Compute v9-style metric evidence: intensity * coverage * confidence."""
    if not trace.services:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0

    mapped_services = 0
    mismatches = 0
    per_service_intensity: List[float] = []
    per_service_confidence: List[float] = []
    per_service_temporal: List[float] = []

    for service in trace.services:
        candidates = service_pod_registry.get(service, [])
        if not candidates:
            mismatches += 1
            continue

        pod_scores: List[float] = []
        pod_conf_temporal: List[float] = []
        for cand in candidates:
            pod_id = str(cand.get("pod_id", ""))
            conf = _clamp(float(cand.get("confidence_score", 0.0)), 0.0, 1.0)
            snap, temporal_alignment = _lookup_nearest_snapshot(
                pod_id=pod_id,
                trace_sec=trace_sec,
                sec_index=sec_index,
                snap_index=snap_index,
                lookback_sec=lookback_sec,
            )
            if snap is None:
                continue

            intensity = (
                0.24 * float(snap.get("cpu_z", 0.0))
                + 0.20 * float(snap.get("memory_z", 0.0))
                + 0.24 * float(snap.get("latency_p99_z", 0.0))
                + 0.12 * float(snap.get("workload_z", 0.0))
                + 0.12 * float(snap.get("node_pressure_z", 0.0))
                + 0.08 * float(snap.get("error_proxy", 0.0))
            )
            intensity = max(0.0, intensity)

            score = intensity * conf * temporal_alignment
            if score > 0.0:
                pod_scores.append(score)
                pod_conf_temporal.append(conf * temporal_alignment)

        if not pod_scores:
            mismatches += 1
            continue

        mapped_services += 1
        service_intensity = _topk_projection(pod_scores)
        service_conf = _topk_projection(pod_conf_temporal)
        per_service_intensity.append(service_intensity)
        per_service_confidence.append(service_conf)
        per_service_temporal.append(max(pod_conf_temporal))

    if mapped_services <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, mismatches

    intensity = _topk_projection(per_service_intensity)
    coverage = mapped_services / float(max(1, len(trace.services)))
    confidence = _mean(per_service_confidence)
    temporal_alignment = _mean(per_service_temporal)

    selectivity = _clamp(
        0.15 + 0.40 * coverage + 0.30 * confidence + 0.15 * temporal_alignment,
        0.0,
        1.0,
    )

    metric_evidence = intensity * coverage * confidence
    metric_evidence = max(0.0, metric_evidence)
    return metric_evidence, selectivity, intensity, coverage, confidence, mismatches


def _consensus_weights_for_budget(
    budget_pct: float,
) -> Tuple[float, float, float, float, float]:
    if budget_pct <= 0.1:
        return 0.45, 0.30, 0.20, 0.05, 0.00
    if budget_pct <= 1.0:
        return 0.35, 0.25, 0.30, 0.10, 0.00
    # Remaining budget weight reserved for diversity/contrast.
    return 0.25, 0.20, 0.30, 0.10, 0.15


def _bounded_swap_refine_v13_style(
    selected_ids: Set[str],
    points: List[TracePoint],
    target_count: int,
    metric_evidence_by_id: Dict[str, float],
    swap_ratio: float = 0.10,
    max_swap_ratio: float = 0.15,
) -> Set[str]:
    """v13-style bounded swap refine for future v3.94.2 line."""
    if not selected_ids or not points or target_count <= 0:
        return set(selected_ids)

    by_id = {p.trace_id: p for p in points}
    all_ids = {p.trace_id for p in points}
    global_error_ratio = (
        sum(1 for p in points if p.has_error) / float(len(points)) if points else 0.0
    )

    quota = max(1, int(target_count * _clamp(float(swap_ratio), 0.0, float(max_swap_ratio))))
    max_quota = max(1, int(target_count * _clamp(float(max_swap_ratio), 0.0, 1.0)))
    quota = min(quota, max_quota)

    kept = set(selected_ids)
    swap_count = 0

    while swap_count < quota:
        kept_list = sorted(kept)
        kept_tokens = [_trace_tokens(by_id[tid]) for tid in kept_list if tid in by_id]
        if not kept_tokens:
            break

        weak_candidates: List[Tuple[float, str]] = []
        for tid in kept_list:
            trace = by_id.get(tid)
            if trace is None:
                continue
            trace_tokens = _trace_tokens(trace)
            diversity_gain = sv13._diversity_score(trace_tokens, [t for t in kept_tokens if t != trace_tokens])
            contrast_gain = abs((1.0 if trace.has_error else 0.0) - global_error_ratio)
            metric_comp = metric_evidence_by_id.get(tid, 0.0)
            redundancy_penalty = 1.0 - diversity_gain
            refine_score = contrast_gain + diversity_gain + metric_comp - redundancy_penalty
            weak_candidates.append((refine_score, tid))

        if not weak_candidates:
            break
        weak_candidates.sort(key=lambda item: (item[0], item[1]))
        remove_id = weak_candidates[0][1]

        outside_ids = sorted(all_ids - kept)
        if not outside_ids:
            break

        strong_candidates: List[Tuple[float, str]] = []
        for tid in outside_ids:
            trace = by_id.get(tid)
            if trace is None:
                continue
            trace_tokens = _trace_tokens(trace)
            diversity_gain = sv13._diversity_score(trace_tokens, kept_tokens)
            contrast_gain = abs((1.0 if trace.has_error else 0.0) - global_error_ratio)
            metric_comp = metric_evidence_by_id.get(tid, 0.0)
            redundancy_penalty = 1.0 - diversity_gain
            refine_score = contrast_gain + diversity_gain + metric_comp - redundancy_penalty
            strong_candidates.append((refine_score, tid))

        if not strong_candidates:
            break
        strong_candidates.sort(key=lambda item: (-item[0], item[1]))
        add_id = strong_candidates[0][1]

        if add_id == remove_id:
            break

        kept.remove(remove_id)
        kept.add(add_id)
        swap_count += 1

    if len(kept) > target_count:
        ranked = sorted(kept, key=lambda tid: (-metric_evidence_by_id.get(tid, 0.0), tid))
        kept = set(ranked[:target_count])
    elif len(kept) < target_count:
        ranked_outside = sorted(all_ids - kept, key=lambda tid: (-metric_evidence_by_id.get(tid, 0.0), tid))
        for tid in ranked_outside:
            if len(kept) >= target_count:
                break
            kept.add(tid)

    return kept


def _composite_v3941_metric_native_consensus(
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
    metrics_stream_by_sec: Optional[MetricsStreamBySec] = None,
    metric_lookback_sec: int = 120,
    min_consensus: int = 2,
    enable_swap_refine: bool = False,
    debug_trace_logs: Optional[List[Dict[str, Any]]] = None,
) -> Set[str]:
    total = len(points)
    if total <= 0:
        return set()

    beta = _clamp(float(budget_pct) / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    target_count = max(1, min(total, int(round(total * beta))))
    incident_services = incident_services or set()
    metric_stats = metric_stats or {}

    normalized_metrics = normalize_metric_stream(metrics_stream_by_sec or {})
    sec_index, snap_index = _build_pod_metric_index(normalized_metrics)
    service_pod_registry = build_service_pod_registry(points, normalized_metrics)

    (
        rca_value_by_id,
        suspicious_hit_by_id,
        suspicious_score_by_id,
        structural_rarity_by_id,
        contrast_term_by_id,
    ) = _compute_rca_state(
        points=points,
        incident_services=incident_services,
        beta=beta,
        high_error_service_threshold=high_error_service_threshold,
    )

    score_values = list(suspicious_score_by_id.values())
    suspicious_threshold = _percentile(score_values, _budget_suspicious_percentile(budget_pct))

    metric_evidence_by_id: Dict[str, float] = {}
    metric_selectivity_by_id: Dict[str, float] = {}
    metric_coverage_by_id: Dict[str, float] = {}
    metric_confidence_by_id: Dict[str, float] = {}
    metric_mismatch_count = 0

    for trace in points:
        trace_sec = int(trace.start_ns // 1_000_000_000)
        evidence, selectivity, _intensity, coverage, confidence, mismatches = _trace_metric_evidence(
            trace=trace,
            trace_sec=trace_sec,
            service_pod_registry=service_pod_registry,
            sec_index=sec_index,
            snap_index=snap_index,
            lookback_sec=metric_lookback_sec,
        )
        metric_evidence_by_id[trace.trace_id] = evidence
        metric_selectivity_by_id[trace.trace_id] = selectivity
        metric_coverage_by_id[trace.trace_id] = coverage
        metric_confidence_by_id[trace.trace_id] = confidence
        metric_mismatch_count += mismatches

    all_kept_votes: Dict[str, int] = defaultdict(int)
    member_sizes: List[int] = []

    original_compute_sampling_probability = sv39.compute_sampling_probability

    def _metric_native_sampling_probability(trace: TracePoint, context: Dict[str, Any]) -> float:
        p_trace = original_compute_sampling_probability(trace, context)
        trace_id = trace.trace_id
        evidence = metric_evidence_by_id.get(trace_id, 0.0)
        selectivity = metric_selectivity_by_id.get(trace_id, 0.0)

        # Mandatory logit-space modulation only.
        delta_logit = 0.90 * evidence * selectivity

        # Keep RCA objective dominant by suppressing modulation on low suspicious traces.
        suspicious_score = suspicious_score_by_id.get(trace_id, 0.0)
        if suspicious_score < suspicious_threshold:
            delta_logit *= 0.35

        delta_logit = _clamp(delta_logit, -0.30, 0.90)
        return _clamp(_sigmoid(_logit(p_trace) + delta_logit), 0.02, 0.98)

    try:
        sv39.compute_sampling_probability = _metric_native_sampling_probability
        for run_seed in FIXED_ENSEMBLE_SEEDS:
            kept = sv39._composite_v39_tuned_rca(
                points=points,
                budget_pct=budget_pct,
                preference_vector=preference_vector,
                scenario_windows=scenario_windows,
                incident_anchor_sec=incident_anchor_sec,
                seed=run_seed,
                metric_stats=metric_stats,
                incident_services=incident_services,
                min_incident_traces_per_scenario=min_incident_traces_per_scenario,
                online_soft_cap=online_soft_cap,
                lookback_sec=lookback_sec,
                alpha=alpha,
                gamma=gamma,
                anomaly_weight=anomaly_weight,
                rca_weight=rca_weight,
                high_error_service_threshold=high_error_service_threshold,
                metrics_stream_by_sec=metrics_stream_by_sec or {},
                metric_lookback_sec=20,
                debug_trace_logs=None,
            )
            member_sizes.append(len(kept))
            for trace_id in kept:
                all_kept_votes[trace_id] += 1
    finally:
        sv39.compute_sampling_probability = original_compute_sampling_probability

    vote_pool = list(all_kept_votes.keys())
    if not vote_pool:
        return set()

    w_vote, w_rca, w_metric, w_err, w_div = _consensus_weights_for_budget(budget_pct)

    by_id = {p.trace_id: p for p in points}

    def _final_consensus_score(trace_id: str) -> float:
        vote_frac = all_kept_votes.get(trace_id, 0) / float(max(1, len(FIXED_ENSEMBLE_SEEDS)))
        rca_val = rca_value_by_id.get(trace_id, 0.40)
        metric_evidence = metric_evidence_by_id.get(trace_id, 0.0)
        err_bonus = 0.15 if (by_id.get(trace_id) and by_id[trace_id].has_error) else 0.0

        diversity_contrast = 0.6 * structural_rarity_by_id.get(trace_id, 0.0) + 0.4 * contrast_term_by_id.get(trace_id, 0.0)

        return (
            w_vote * vote_frac
            + w_rca * rca_val
            + w_metric * metric_evidence
            + w_err * err_bonus
            + w_div * diversity_contrast
        )

    consensus_pool = [tid for tid in vote_pool if all_kept_votes[tid] >= int(min_consensus)]
    remainder_pool = [tid for tid in vote_pool if all_kept_votes[tid] < int(min_consensus)]

    consensus_pool.sort(key=lambda tid: (-_final_consensus_score(tid), tid))
    remainder_pool.sort(key=lambda tid: (-_final_consensus_score(tid), tid))

    ranked: List[str] = []
    for tid in consensus_pool:
        if len(ranked) >= target_count:
            break
        ranked.append(tid)
    if len(ranked) < target_count:
        for tid in remainder_pool:
            if len(ranked) >= target_count:
                break
            ranked.append(tid)

    # Exact budget fallback: fill from all traces by final score if vote pool is not enough.
    if len(ranked) < target_count:
        outside = [p.trace_id for p in points if p.trace_id not in set(ranked)]
        outside.sort(key=lambda tid: (-_final_consensus_score(tid), tid))
        for tid in outside:
            if len(ranked) >= target_count:
                break
            ranked.append(tid)

    selected = set(ranked[:target_count])

    if enable_swap_refine:
        selected = _bounded_swap_refine_v13_style(
            selected_ids=selected,
            points=points,
            target_count=target_count,
            metric_evidence_by_id=metric_evidence_by_id,
            swap_ratio=0.10,
            max_swap_ratio=0.15,
        )

    if len(selected) > target_count:
        trimmed = sorted(selected, key=lambda tid: (-_final_consensus_score(tid), tid))
        selected = set(trimmed[:target_count])
    elif len(selected) < target_count:
        leftovers = sorted((p.trace_id for p in points if p.trace_id not in selected), key=lambda tid: (-_final_consensus_score(tid), tid))
        for tid in leftovers:
            if len(selected) >= target_count:
                break
            selected.add(tid)

    if debug_trace_logs is not None:
        vote_fracs = [all_kept_votes[tid] / float(len(FIXED_ENSEMBLE_SEEDS)) for tid in vote_pool]
        selected_normals = sum(1 for tid in selected if by_id.get(tid) and (not by_id[tid].has_error))
        selected_errors = len(selected) - selected_normals
        debug_trace_logs.append(
            {
                "version": "v394.1-metric-native-consensus",
                "target_count": target_count,
                "selected_count": len(selected),
                "member_size_mean": _mean([float(v) for v in member_sizes]),
                "member_size_std": _std([float(v) for v in member_sizes]),
                "vote_confidence_mean": _mean(vote_fracs),
                "suspicious_threshold": suspicious_threshold,
                "registry_services": len(service_pod_registry),
                "metric_mismatch_count": metric_mismatch_count,
                "metric_evidence_mean": _mean(list(metric_evidence_by_id.values())),
                "metric_coverage_mean": _mean(list(metric_coverage_by_id.values())),
                "metric_confidence_mean": _mean(list(metric_confidence_by_id.values())),
                "selected_normal_count": selected_normals,
                "selected_error_count": selected_errors,
                "weights": {
                    "vote": w_vote,
                    "rca": w_rca,
                    "metric": w_metric,
                    "error": w_err,
                    "diversity": w_div,
                },
            }
        )

    return selected


# Backward-compatible alias for internal callers.
def _composite_v3941_metric_native_consensus_sampler(*args: Any, **kwargs: Any) -> Set[str]:
    return _composite_v3941_metric_native_consensus(*args, **kwargs)
