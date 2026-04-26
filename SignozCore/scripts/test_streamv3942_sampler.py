#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Dict, List, Set

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import streamv39_tuned_rca as sv39
import streamv3942_metric_native_signed_contrast as sv3942

TracePoint = sv39.TracePoint


def _tp(
    trace_id: str,
    start_sec: int,
    duration_ms: float,
    span_count: int,
    has_error: bool,
    services: Set[str],
) -> TracePoint:
    return TracePoint(
        trace_id=trace_id,
        start_ns=start_sec * 1_000_000_000,
        duration_ms=duration_ms,
        span_count=span_count,
        has_error=has_error,
        services=frozenset(services),
    )


class TestStreamV3942Sampler(unittest.TestCase):
    def _build_points(self, total: int = 120) -> List[TracePoint]:
        points: List[TracePoint] = []
        for idx in range(total):
            has_error = (idx % 2 == 0)
            base_service = f"svc-{idx % 10}"
            services = {base_service}
            if idx % 5 == 0:
                services.add("svc-incident")
            duration = 900.0 - (idx % 30) * 9.0 if has_error else 380.0 + (idx % 20) * 3.0
            span_count = 7 + (idx % 11)
            points.append(
                _tp(
                    trace_id=f"t-{idx}",
                    start_sec=10_000 + idx,
                    duration_ms=duration,
                    span_count=span_count,
                    has_error=has_error,
                    services=services,
                )
            )
        return points

    def _build_metrics_stream(self, points: List[TracePoint]) -> Dict[int, Dict[str, Dict[str, float]]]:
        stream: Dict[int, Dict[str, Dict[str, float]]] = {}
        for point in points:
            sec = int(point.start_ns // 1_000_000_000)
            snap = stream.setdefault(sec, {})
            for svc in point.services:
                snap[svc] = {
                    "cpu": 0.85 if point.has_error else 0.45,
                    "memory": 0.80 if point.has_error else 0.40,
                    "latency_p99": 2.0 if point.has_error else 0.6,
                    "workload": 150.0 + (sec % 17),
                    "node_pressure": 0.9 if point.has_error else 0.3,
                    "success_proxy": 0.80 if point.has_error else 0.99,
                    "error_proxy": 0.20 if point.has_error else 0.01,
                }
        return stream

    def test_selected_count_matches_target(self) -> None:
        points = self._build_points(total=120)
        metrics_stream = self._build_metrics_stream(points)

        selected = sv3942._composite_v3942_metric_native_signed_contrast(
            points=points,
            budget_pct=10.0,
            preference_vector={},
            metric_stats={},
            scenario_windows=[("s1", 10_000, 10_300)],
            incident_anchor_sec=10_050,
            seed=42,
            incident_services={"svc-incident", "svc-1"},
            min_incident_traces_per_scenario=1,
            online_soft_cap=False,
            metrics_stream_by_sec=metrics_stream,
            min_consensus=2,
            enable_inner_real_metrics=False,
        )

        target = int(round(len(points) * 0.10))
        self.assertEqual(len(selected), target)

    def test_dual_contrast_floors_enforced_when_pool_sufficient(self) -> None:
        points = self._build_points(total=120)
        metrics_stream = self._build_metrics_stream(points)

        selected = sv3942._composite_v3942_metric_native_signed_contrast(
            points=points,
            budget_pct=20.0,
            preference_vector={},
            metric_stats={},
            scenario_windows=[("s1", 10_000, 10_300)],
            incident_anchor_sec=10_050,
            seed=7,
            incident_services={"svc-incident", "svc-2"},
            min_incident_traces_per_scenario=1,
            online_soft_cap=False,
            metrics_stream_by_sec=metrics_stream,
            min_normal_ratio=0.40,
            min_error_ratio=0.25,
            enable_inner_real_metrics=False,
        )

        by_id = {p.trace_id: p for p in points}
        normals = sum(1 for tid in selected if not by_id[tid].has_error)
        errors = len(selected) - normals
        target = int(round(len(points) * 0.20))
        head_k = sv3942._default_head_k_for_budget(20.0, target)
        tail_target = max(0, target - head_k)

        self.assertEqual(len(selected), target)
        self.assertGreaterEqual(normals, int(round(tail_target * 0.40)))
        self.assertGreaterEqual(errors, int(round(tail_target * 0.25)))

    def test_signed_delta_logit_has_both_signs(self) -> None:
        values = [0.05, 0.45, 0.95]
        tau = sv3942._resolve_tau(values, budget_pct=1.0, mode="median")
        scale = sv3942._resolve_scale(values, mode="robust")

        deltas = [
            0.90 * sv3942._signed_metric_value(v, tau, scale) * 1.0 * 1.0
            for v in values
        ]
        self.assertLess(min(deltas), 0.0)
        self.assertGreater(max(deltas), 0.0)

    def test_disable_inner_real_metrics_is_invariant_to_metric_stream(self) -> None:
        points = self._build_points(total=80)
        noisy_metrics: Dict[int, Dict[str, Dict[str, float]]] = {}
        for p in points:
            sec = int(p.start_ns // 1_000_000_000)
            noisy_metrics.setdefault(sec, {})[f"svc-{sec % 10}"] = {
                "cpu": 0.99,
                "memory": 0.99,
                "latency": 0.99,
                "workload": 0.99,
                "success_rate": 0.10,
            }

        kwargs = {
            "points": points,
            "budget_pct": 15.0,
            "preference_vector": {},
            "metric_stats": {},
            "scenario_windows": [("s1", 10_000, 10_200)],
            "incident_anchor_sec": 10_040,
            "seed": 123,
            "incident_services": {"svc-incident", "svc-1"},
            "min_incident_traces_per_scenario": 1,
            "online_soft_cap": False,
            "enable_inner_real_metrics": False,
        }

        selected_with_empty = sv39._composite_v39_tuned_rca(
            metrics_stream_by_sec={},
            metric_lookback_sec=20,
            **kwargs,
        )
        selected_with_noisy = sv39._composite_v39_tuned_rca(
            metrics_stream_by_sec=noisy_metrics,
            metric_lookback_sec=20,
            **kwargs,
        )

        self.assertEqual(selected_with_empty, selected_with_noisy)


if __name__ == "__main__":
    unittest.main()
