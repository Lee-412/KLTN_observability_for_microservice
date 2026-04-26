#!/usr/bin/env python3
"""V10 guard-aware sampler.

This module keeps compatibility with the existing sampler interface while adding:
1) set-aware guard marginal scoring,
2) one-pass greedy marginal selection,
3) bounded residual hard-constraint repair (<5% swaps),
4) coherence telemetry between pre/post repair selections.
"""

from __future__ import annotations

import heapq
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from streamv9_metric_aware_sampling_from_v35 import (
    TracePoint,
    _build_pod_metric_index,
    _build_service_pod_mapping,
    _trace_metric_score,
    build_metric_inputs,
    build_real_metrics_stream,
)

MetricsSnapshotByPod = Dict[str, Dict[str, float]]
MetricsStreamBySec = Dict[int, MetricsSnapshotByPod]


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _safe_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _jaccard(a: Set[str], b: Set[str]) -> float:
    union_size = len(a | b)
    if union_size == 0:
        return 1.0
    return len(a & b) / float(union_size)


def _kendall_tau(order_a: Sequence[str], order_b: Sequence[str]) -> float:
    if not order_a or not order_b:
        return 1.0
    rank_a = {tid: idx for idx, tid in enumerate(order_a)}
    rank_b = {tid: idx for idx, tid in enumerate(order_b)}
    ids = list(rank_a.keys())
    if len(ids) <= 1:
        return 1.0

    concordant = 0
    discordant = 0
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a_i = rank_a[ids[i]]
            a_j = rank_a[ids[j]]
            b_i = rank_b[ids[i]]
            b_j = rank_b[ids[j]]
            sign_a = 1 if a_i < a_j else -1
            sign_b = 1 if b_i < b_j else -1
            if sign_a == sign_b:
                concordant += 1
            else:
                discordant += 1

    total = concordant + discordant
    if total == 0:
        return 1.0
    return (concordant - discordant) / float(total)


@dataclass
class SelectionDiagnostics:
    target_count: int
    selected_count: int
    min_normal_target: int
    min_error_target: int
    normal_ratio: float
    error_ratio: float
    swap_count: int
    pre_post_jaccard: float
    kendall_tau: float
    displacement_rate: float
    average_swap_score_loss: float


class V10GuardAwareSampler:
    """Set-aware sampler with guard-aware marginal gains."""

    def __init__(
        self,
        lambda_base: float = 0.45,
        mu_evidence: float = 0.35,
        eta_guard: float = 0.20,
        tau: float = 0.50,
        min_normal_ratio: float = 0.30,
        min_error_ratio: float = 0.25,
        redundancy_weight: float = 0.15,
    ) -> None:
        total = max(1e-9, lambda_base + mu_evidence + eta_guard)
        self.lambda_base = lambda_base / total
        self.mu_evidence = mu_evidence / total
        self.eta_guard = eta_guard / total
        self.tau = tau
        self.min_normal_ratio = _clamp(min_normal_ratio, 0.0, 1.0)
        self.min_error_ratio = _clamp(min_error_ratio, 0.0, 1.0)
        self.redundancy_weight = max(0.0, redundancy_weight)

    def compute_base_score(
        self,
        trace: TracePoint,
        incident_services: Set[str],
        incident_anchor_sec: Optional[int],
        duration_low: float,
        duration_high: float,
        span_low: int,
        span_high: int,
    ) -> float:
        """Compute baseline anomaly score before metric evidence."""
        if duration_high <= duration_low:
            lat_norm = 0.0
        else:
            lat_norm = _clamp((trace.duration_ms - duration_low) / (duration_high - duration_low), 0.0, 1.0)

        if span_high <= span_low:
            span_norm = 0.0
        else:
            span_norm = _clamp((trace.span_count - span_low) / float(span_high - span_low), 0.0, 1.0)

        has_incident = bool(set(trace.services) & incident_services)
        incident_flag = 1.0 if has_incident else 0.0
        err_flag = 1.0 if trace.has_error else 0.0

        if incident_anchor_sec is None:
            time_prox = 0.50
        else:
            sec = int(trace.start_ns // 1_000_000_000)
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        base_sys = (
            0.36 * incident_flag
            + 0.24 * lat_norm
            + 0.14 * span_norm
            + 0.16 * err_flag
            + 0.10 * time_prox
        )
        anomaly_mix = 0.45 * err_flag + 0.35 * lat_norm + 0.20 * incident_flag
        shock = math.tanh(1.20 * anomaly_mix)
        return _clamp(0.55 * base_sys + 0.45 * shock, 0.01, 0.99)

    def compute_evidence_score(
        self,
        trace: TracePoint,
        trace_sec: int,
        service_to_pods: Dict[str, List[Tuple[str, float, str]]],
        sec_index: Dict[str, List[int]],
        snap_index: Dict[str, List[Dict[str, float]]],
        metric_lookback_sec: int,
    ) -> Tuple[float, float, float]:
        """Compute metric evidence score from aligned pod snapshots."""
        metric_score, coverage, confidence, _mismatches = _trace_metric_score(
            trace=trace,
            trace_sec=trace_sec,
            service_to_pods=service_to_pods,
            sec_index=sec_index,
            snap_index=snap_index,
            lookback_sec=metric_lookback_sec,
        )
        metric_signal = _sigmoid(metric_score - self.tau)
        selectivity = _clamp(0.15 + 0.55 * coverage + 0.30 * confidence, 0.0, 1.0)
        evidence = _clamp(metric_signal * selectivity, 0.0, 1.0)
        return evidence, coverage, confidence

    def compute_guard_marginal(
        self,
        trace_id: str,
        trace: TracePoint,
        selected_size: int,
        selected_error_count: int,
        selected_normal_count: int,
        target_count: int,
        min_normal_target: int,
        min_error_target: int,
        selected_service_counter: Counter[str],
        scenario_membership: Dict[str, Set[int]],
        scenario_counts: List[int],
        scenario_floor_each: int,
    ) -> float:
        """Compute guard marginal utility for a candidate trace."""
        if target_count <= 0:
            return 0.0

        error_deficit = max(0, min_error_target - selected_error_count)
        normal_deficit = max(0, min_normal_target - selected_normal_count)

        error_component = 0.0
        normal_component = 0.0
        if trace.has_error and error_deficit > 0:
            error_component = error_deficit / float(target_count)
        if (not trace.has_error) and normal_deficit > 0:
            normal_component = normal_deficit / float(target_count)

        scenario_component = 0.0
        if scenario_floor_each > 0 and scenario_counts:
            covered = scenario_membership.get(trace_id, set())
            if covered:
                gains = 0
                for idx in covered:
                    if idx < len(scenario_counts) and scenario_counts[idx] < scenario_floor_each:
                        gains += 1
                scenario_component = gains / float(max(1, len(covered)))

        if trace.services:
            novelty = sum(1 for svc in trace.services if selected_service_counter.get(svc, 0) == 0) / float(len(trace.services))
            overlap = sum(1 for svc in trace.services if selected_service_counter.get(svc, 0) > 0) / float(len(trace.services))
        else:
            novelty = 0.0
            overlap = 0.0

        if selected_size <= 0:
            balance_bonus = 0.0
        else:
            err_ratio = selected_error_count / float(selected_size)
            if trace.has_error:
                balance_bonus = max(0.0, 0.5 - err_ratio)
            else:
                balance_bonus = max(0.0, err_ratio - 0.5)

        guard_score = (
            0.45 * error_component
            + 0.30 * normal_component
            + 0.35 * scenario_component
            + 0.20 * novelty
            + 0.10 * balance_bonus
            - 0.15 * overlap
        )
        return guard_score

    def select_traces(
        self,
        points: Sequence[TracePoint],
        budget_pct: float,
        scenario_windows: Sequence[Tuple[str, int, int]],
        incident_anchor_sec: Optional[int],
        incident_services: Optional[Set[str]] = None,
        metrics_stream_by_sec: Optional[MetricsStreamBySec] = None,
        metric_lookback_sec: int = 120,
        min_incident_traces_per_scenario: int = 1,
        enable_residual_fix: bool = True,
    ) -> Tuple[Set[str], Dict[str, float], SelectionDiagnostics]:
        if not points:
            empty_diag = SelectionDiagnostics(
                target_count=0,
                selected_count=0,
                min_normal_target=0,
                min_error_target=0,
                normal_ratio=0.0,
                error_ratio=0.0,
                swap_count=0,
                pre_post_jaccard=1.0,
                kendall_tau=1.0,
                displacement_rate=0.0,
                average_swap_score_loss=0.0,
            )
            return set(), {}, empty_diag

        traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
        by_id: Dict[str, TracePoint] = {p.trace_id: p for p in traces}
        incident_services = incident_services or set()
        metrics_stream_by_sec = metrics_stream_by_sec or {}

        budget = _clamp(budget_pct / 100.0, 0.0, 1.0)
        target_count = max(1, min(len(traces), int(round(len(traces) * budget))))

        min_normal_target = int(round(target_count * self.min_normal_ratio))
        min_error_target = int(round(target_count * self.min_error_ratio))
        if min_normal_target + min_error_target > target_count:
            overflow = (min_normal_target + min_error_target) - target_count
            reduce_error = min(overflow, min_error_target)
            min_error_target -= reduce_error
            overflow -= reduce_error
            if overflow > 0:
                min_normal_target = max(0, min_normal_target - overflow)
        available_normal = sum(1 for p in traces if not p.has_error)
        available_error = len(traces) - available_normal
        min_normal_target = min(min_normal_target, available_normal, target_count)
        min_error_target = min(min_error_target, available_error, max(0, target_count - min_normal_target))

        # Prepare scenario membership map once.
        start_sec_by_id = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in traces}
        scenario_membership: Dict[str, Set[int]] = defaultdict(set)
        non_empty_scenarios = 0
        for idx, (_sid, st, ed) in enumerate(scenario_windows):
            ids = [tid for tid, sec in start_sec_by_id.items() if st <= sec <= ed]
            if ids:
                non_empty_scenarios += 1
                for tid in ids:
                    scenario_membership[tid].add(idx)
        scenario_floor_each = min_incident_traces_per_scenario
        if non_empty_scenarios > 0 and target_count < non_empty_scenarios * scenario_floor_each:
            scenario_floor_each = 0

        duration_low = min(p.duration_ms for p in traces)
        duration_high = max(p.duration_ms for p in traces)
        span_low = min(p.span_count for p in traces)
        span_high = max(p.span_count for p in traces)

        all_services = {svc for p in traces for svc in p.services}
        all_pods = {pod for sec_snap in metrics_stream_by_sec.values() for pod in sec_snap.keys()}
        service_to_pods, _covered_services, _mismatch_services = _build_service_pod_mapping(all_services, all_pods)
        sec_index, snap_index = _build_pod_metric_index(metrics_stream_by_sec)

        base_by_id: Dict[str, float] = {}
        evidence_by_id: Dict[str, float] = {}
        score_by_id: Dict[str, float] = {}

        for trace in traces:
            base_score = self.compute_base_score(
                trace=trace,
                incident_services=incident_services,
                incident_anchor_sec=incident_anchor_sec,
                duration_low=duration_low,
                duration_high=duration_high,
                span_low=span_low,
                span_high=span_high,
            )
            trace_sec = int(trace.start_ns // 1_000_000_000)
            evidence_score, _coverage, _confidence = self.compute_evidence_score(
                trace=trace,
                trace_sec=trace_sec,
                service_to_pods=service_to_pods,
                sec_index=sec_index,
                snap_index=snap_index,
                metric_lookback_sec=metric_lookback_sec,
            )
            base_by_id[trace.trace_id] = base_score
            evidence_by_id[trace.trace_id] = evidence_score
            score_by_id[trace.trace_id] = (
                self.lambda_base * base_score
                + self.mu_evidence * evidence_score
            )

        ranked_ids = sorted(score_by_id.keys(), key=lambda tid: (-score_by_id[tid], tid))

        selected_ids: Set[str] = set()
        selected_heap: List[Tuple[float, str]] = []
        selected_error_count = 0
        selected_normal_count = 0
        selected_service_counter: Counter[str] = Counter()
        scenario_counts = [0] * len(scenario_windows)

        def _add_trace(trace_id: str, marginal: float) -> None:
            nonlocal selected_error_count, selected_normal_count
            trace = by_id[trace_id]
            selected_ids.add(trace_id)
            heapq.heappush(selected_heap, (marginal, trace_id))
            if trace.has_error:
                selected_error_count += 1
            else:
                selected_normal_count += 1
            for svc in trace.services:
                selected_service_counter[svc] += 1
            for idx in scenario_membership.get(trace_id, set()):
                if idx < len(scenario_counts):
                    scenario_counts[idx] += 1

        def _remove_trace(trace_id: str) -> None:
            nonlocal selected_error_count, selected_normal_count
            if trace_id not in selected_ids:
                return
            trace = by_id[trace_id]
            selected_ids.remove(trace_id)
            if trace.has_error:
                selected_error_count = max(0, selected_error_count - 1)
            else:
                selected_normal_count = max(0, selected_normal_count - 1)
            for svc in trace.services:
                selected_service_counter[svc] -= 1
                if selected_service_counter[svc] <= 0:
                    selected_service_counter.pop(svc, None)
            for idx in scenario_membership.get(trace_id, set()):
                if idx < len(scenario_counts):
                    scenario_counts[idx] = max(0, scenario_counts[idx] - 1)

        def _can_remove(trace_id: str) -> bool:
            trace = by_id[trace_id]
            if trace.has_error and (selected_error_count - 1) < min_error_target:
                return False
            if (not trace.has_error) and (selected_normal_count - 1) < min_normal_target:
                return False
            if scenario_floor_each > 0:
                for idx in scenario_membership.get(trace_id, set()):
                    if idx < len(scenario_counts) and (scenario_counts[idx] - 1) < scenario_floor_each:
                        return False
            return True

        def _pop_worst_removable() -> Optional[Tuple[float, str]]:
            parked: List[Tuple[float, str]] = []
            chosen: Optional[Tuple[float, str]] = None
            while selected_heap:
                marginal, tid = heapq.heappop(selected_heap)
                if tid not in selected_ids:
                    continue
                if _can_remove(tid):
                    chosen = (marginal, tid)
                    break
                parked.append((marginal, tid))
            for item in parked:
                heapq.heappush(selected_heap, item)
            return chosen

        # Seed class quotas up front to avoid contrast collapse.
        error_ranked = [tid for tid in ranked_ids if by_id[tid].has_error]
        normal_ranked = [tid for tid in ranked_ids if not by_id[tid].has_error]
        for trace_id in error_ranked[:min_error_target]:
            if len(selected_ids) >= target_count:
                break
            if trace_id in selected_ids:
                continue
            _add_trace(trace_id, score_by_id[trace_id])
        for trace_id in normal_ranked[:min_normal_target]:
            if len(selected_ids) >= target_count:
                break
            if trace_id in selected_ids:
                continue
            _add_trace(trace_id, score_by_id[trace_id])

        for trace_id in ranked_ids:
            trace = by_id[trace_id]
            guard_score = self.compute_guard_marginal(
                trace_id=trace_id,
                trace=trace,
                selected_size=len(selected_ids),
                selected_error_count=selected_error_count,
                selected_normal_count=selected_normal_count,
                target_count=target_count,
                min_normal_target=min_normal_target,
                min_error_target=min_error_target,
                selected_service_counter=selected_service_counter,
                scenario_membership=scenario_membership,
                scenario_counts=scenario_counts,
                scenario_floor_each=scenario_floor_each,
            )
            if trace.services:
                overlap = sum(1 for svc in trace.services if selected_service_counter.get(svc, 0) > 0) / float(len(trace.services))
            else:
                overlap = 0.0
            marginal = score_by_id[trace_id] + self.eta_guard * guard_score - self.redundancy_weight * overlap

            if len(selected_ids) < target_count:
                _add_trace(trace_id, marginal)
                continue

            if trace_id in selected_ids:
                continue

            worst_candidate = _pop_worst_removable()
            if worst_candidate is None:
                continue
            worst_marginal, worst_id = worst_candidate
            if marginal > worst_marginal:
                _remove_trace(worst_id)
                _add_trace(trace_id, marginal)
            else:
                heapq.heappush(selected_heap, (worst_marginal, worst_id))

        # Fill if heap replacement removed too much due duplicate pops.
        if len(selected_ids) < target_count:
            for trace_id in ranked_ids:
                if trace_id in selected_ids:
                    continue
                _add_trace(trace_id, score_by_id[trace_id])
                if len(selected_ids) >= target_count:
                    break

        pre_guard_ids = set(selected_ids)

        swap_count = 0
        swap_losses: List[float] = []
        if enable_residual_fix and target_count > 0:
            selected_ids, swap_count, swap_losses = self._apply_residual_fix(
                selected_ids=selected_ids,
                by_id=by_id,
                score_by_id=score_by_id,
                target_count=target_count,
                min_normal_target=min_normal_target,
                min_error_target=min_error_target,
                scenario_membership=scenario_membership,
                scenario_floor_each=scenario_floor_each,
                scenario_count=len(scenario_windows),
            )

        selected_error = sum(1 for tid in selected_ids if by_id[tid].has_error)
        selected_normal = len(selected_ids) - selected_error
        denom = float(max(1, len(selected_ids)))
        normal_ratio = selected_normal / denom
        error_ratio = selected_error / denom

        pre_order = sorted(pre_guard_ids, key=lambda tid: (-score_by_id.get(tid, 0.0), tid))
        post_order = sorted(selected_ids, key=lambda tid: (-score_by_id.get(tid, 0.0), tid))
        union_ids = set(pre_order) | set(post_order)
        pre_tail = sorted(union_ids - set(pre_order), key=lambda tid: (-score_by_id.get(tid, 0.0), tid))
        post_tail = sorted(union_ids - set(post_order), key=lambda tid: (-score_by_id.get(tid, 0.0), tid))
        full_pre_order = pre_order + pre_tail
        full_post_order = post_order + post_tail

        pre_post_jaccard = _jaccard(pre_guard_ids, selected_ids)
        displacement_rate = (len(pre_guard_ids - selected_ids) / float(max(1, target_count)))
        kendall_tau = _kendall_tau(full_pre_order, full_post_order)
        avg_swap_loss = _safe_mean(swap_losses)

        diagnostics = SelectionDiagnostics(
            target_count=target_count,
            selected_count=len(selected_ids),
            min_normal_target=min_normal_target,
            min_error_target=min_error_target,
            normal_ratio=normal_ratio,
            error_ratio=error_ratio,
            swap_count=swap_count,
            pre_post_jaccard=pre_post_jaccard,
            kendall_tau=kendall_tau,
            displacement_rate=displacement_rate,
            average_swap_score_loss=avg_swap_loss,
        )
        return selected_ids, score_by_id, diagnostics

    def _apply_residual_fix(
        self,
        selected_ids: Set[str],
        by_id: Dict[str, TracePoint],
        score_by_id: Dict[str, float],
        target_count: int,
        min_normal_target: int,
        min_error_target: int,
        scenario_membership: Dict[str, Set[int]],
        scenario_floor_each: int,
        scenario_count: int,
    ) -> Tuple[Set[str], int, List[float]]:
        if target_count <= 0:
            return set(), 0, []

        # Strictly less than 5% replacements.
        max_swaps = int(math.floor(target_count * 0.049))
        if max_swaps <= 0:
            return set(selected_ids), 0, []

        selected = set(selected_ids)
        all_ids = set(by_id.keys())
        non_selected = all_ids - selected

        scenario_counts = [0] * scenario_count
        for tid in selected:
            for idx in scenario_membership.get(tid, set()):
                if idx < scenario_count:
                    scenario_counts[idx] += 1

        def counts(cur: Set[str]) -> Tuple[int, int]:
            err = sum(1 for tid in cur if by_id[tid].has_error)
            normal = len(cur) - err
            return normal, err

        def unmet_scenarios() -> Set[int]:
            if scenario_floor_each <= 0:
                return set()
            return {idx for idx in range(scenario_count) if scenario_counts[idx] < scenario_floor_each}

        def is_valid_after_removal(tid: str, normal_count: int, error_count: int) -> bool:
            trace = by_id[tid]
            if trace.has_error and (error_count - 1) < min_error_target:
                return False
            if (not trace.has_error) and (normal_count - 1) < min_normal_target:
                return False
            if scenario_floor_each > 0:
                for idx in scenario_membership.get(tid, set()):
                    if idx < scenario_count and (scenario_counts[idx] - 1) < scenario_floor_each:
                        return False
            return True

        swap_losses: List[float] = []
        swaps = 0

        while swaps < max_swaps:
            normal_count, error_count = counts(selected)
            need_normal = max(0, min_normal_target - normal_count)
            need_error = max(0, min_error_target - error_count)
            missing_scenarios = unmet_scenarios()

            if need_normal <= 0 and need_error <= 0 and not missing_scenarios:
                break

            best_add = None
            best_gain = -1.0
            for tid in non_selected:
                trace = by_id[tid]
                gain = 0.0
                if need_normal > 0 and (not trace.has_error):
                    gain += 2.0
                if need_error > 0 and trace.has_error:
                    gain += 2.0
                covered_missing = len(missing_scenarios & scenario_membership.get(tid, set()))
                gain += 1.5 * covered_missing
                gain += 0.1 * score_by_id.get(tid, 0.0)
                if gain > best_gain:
                    best_gain = gain
                    best_add = tid

            if best_add is None or best_gain <= 0.0:
                break

            best_drop = None
            best_drop_score = math.inf
            for tid in selected:
                if not is_valid_after_removal(tid, normal_count, error_count):
                    continue
                score = score_by_id.get(tid, 0.0)
                if score < best_drop_score:
                    best_drop_score = score
                    best_drop = tid

            if best_drop is None:
                break

            add_score = score_by_id.get(best_add, 0.0)
            selected.remove(best_drop)
            selected.add(best_add)
            non_selected.remove(best_add)
            non_selected.add(best_drop)

            for idx in scenario_membership.get(best_drop, set()):
                if idx < scenario_count:
                    scenario_counts[idx] = max(0, scenario_counts[idx] - 1)
            for idx in scenario_membership.get(best_add, set()):
                if idx < scenario_count:
                    scenario_counts[idx] += 1

            swap_losses.append(best_drop_score - add_score)
            swaps += 1

        return selected, swaps, swap_losses


def _composite_v10_guard_aware(
    points: List[TracePoint],
    budget_pct: float,
    preference_vector: Dict[str, Dict[str, float]],
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
    metrics_stream_by_sec: Optional[MetricsStreamBySec] = None,
    metric_lookback_sec: int = 120,
    lambda_base: float = 0.45,
    mu_evidence: float = 0.35,
    eta_guard: float = 0.20,
    tau: float = 0.50,
    min_normal_ratio: float = 0.30,
    min_error_ratio: float = 0.25,
    telemetry_output_path: Optional[str] = None,
    dataset_name: str = "unknown",
    budget_label: str = "unknown",
    ablation_variant: str = "V10-D",
) -> Set[str]:
    """Compatibility wrapper for existing runner dispatch."""
    del preference_vector, metric_stats, online_soft_cap, lookback_sec, alpha, gamma, anomaly_weight

    sampler = V10GuardAwareSampler(
        lambda_base=lambda_base,
        mu_evidence=mu_evidence,
        eta_guard=eta_guard,
        tau=tau,
        min_normal_ratio=min_normal_ratio,
        min_error_ratio=min_error_ratio,
    )
    selected_ids, score_by_id, diagnostics = sampler.select_traces(
        points=points,
        budget_pct=budget_pct,
        scenario_windows=scenario_windows,
        incident_anchor_sec=incident_anchor_sec,
        incident_services=incident_services,
        metrics_stream_by_sec=metrics_stream_by_sec,
        metric_lookback_sec=metric_lookback_sec,
        min_incident_traces_per_scenario=min_incident_traces_per_scenario,
        enable_residual_fix=True,
    )

    if telemetry_output_path:
        out = {
            "dataset": dataset_name,
            "budget": budget_label,
            "variant": ablation_variant,
            "target_count": diagnostics.target_count,
            "selected_count": diagnostics.selected_count,
            "normal_ratio": diagnostics.normal_ratio,
            "error_ratio": diagnostics.error_ratio,
            "min_normal_target": diagnostics.min_normal_target,
            "min_error_target": diagnostics.min_error_target,
            "coherence": {
                "pre_post_jaccard": diagnostics.pre_post_jaccard,
                "kendall_tau": diagnostics.kendall_tau,
                "displacement_rate": diagnostics.displacement_rate,
                "average_swap_score_loss": diagnostics.average_swap_score_loss,
                "swap_count": diagnostics.swap_count,
            },
            "score_stats": {
                "score_mean": _safe_mean(list(score_by_id.values())),
                "score_max": max(score_by_id.values()) if score_by_id else 0.0,
                "score_min": min(score_by_id.values()) if score_by_id else 0.0,
            },
            "rca_top1_confidence": None,
            "rca_top1_top2_gap": None,
        }
        output_path = Path(telemetry_output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(out, indent=2, ensure_ascii=True), encoding="utf-8")

    return selected_ids
