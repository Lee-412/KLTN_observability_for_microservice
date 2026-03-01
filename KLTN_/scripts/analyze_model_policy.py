#!/usr/bin/env python3
"""Analyze model tail-sampling selectivity on line-delimited OTLP trace JSON.

This script reads collector fileexporter outputs (format: json, line-delimited),
aggregates spans into traces, computes model features, then evaluates a linear
model score:

  score = intercept + Σ (weight[feature] * feature_value)

Kept condition:
  score >= threshold

Features implemented (match the Go sampler):
- duration_ms: (max_end - min_start) / 1e6
- span_count:  number of spans in the trace
- has_error:   1 if any span has status ERROR or http.status_code >= 500 else 0

Usage:
  python3 scripts/analyze_model_policy.py \
    --input signoz/deploy/docker/otel-export/baseline2/<file>.json \
    --threshold 3.0 \
    --intercept 0 \
    --weight duration_ms=0.01 \
    --weight span_count=0.05 \
    --weight has_error=2.0

Tip:
  Use the BASE (unsampled) export to simulate new thresholds/weights offline.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SUPPORTED_FEATURES = {"duration_ms", "span_count", "has_error"}


def _iter_scope_spans(resource_span: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    if "scopeSpans" in resource_span:
        yield from (resource_span.get("scopeSpans") or [])
        return
    if "instrumentationLibrarySpans" in resource_span:
        yield from (resource_span.get("instrumentationLibrarySpans") or [])
        return


def _span_attr(span: Dict[str, Any], key: str) -> Optional[Any]:
    attrs = span.get("attributes") or []
    for a in attrs:
        if a.get("key") != key:
            continue
        v = (a.get("value") or {})
        for k in (
            "stringValue",
            "boolValue",
            "intValue",
            "doubleValue",
            "bytesValue",
        ):
            if k in v:
                return v.get(k)
    return None


def _span_is_error(span: Dict[str, Any]) -> bool:
    status = span.get("status") or {}
    if status.get("code") in ("STATUS_CODE_ERROR", 2):
        return True

    http_code = _span_attr(span, "http.status_code")
    if isinstance(http_code, (int, float)):
        return http_code >= 500
    if isinstance(http_code, str):
        try:
            return int(http_code) >= 500
        except ValueError:
            return False
    return False


def _parse_unix_nano(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        v = v.strip()
        if not v:
            return None
        try:
            return int(v)
        except ValueError:
            return None
    return None


@dataclass
class TraceAgg:
    trace_id: str
    span_count: int = 0
    min_start_ns: Optional[int] = None
    max_end_ns: Optional[int] = None
    has_error: bool = False

    def add_span(self, span: Dict[str, Any]) -> None:
        self.span_count += 1
        self.has_error = self.has_error or _span_is_error(span)

        start_ns = _parse_unix_nano(span.get("startTimeUnixNano"))
        end_ns = _parse_unix_nano(span.get("endTimeUnixNano"))

        if start_ns is not None:
            self.min_start_ns = start_ns if self.min_start_ns is None else min(self.min_start_ns, start_ns)
        if end_ns is not None:
            self.max_end_ns = end_ns if self.max_end_ns is None else max(self.max_end_ns, end_ns)

    def duration_ms(self) -> float:
        if self.min_start_ns is None or self.max_end_ns is None:
            return 0.0
        d_ns = max(0, self.max_end_ns - self.min_start_ns)
        return d_ns / 1_000_000.0


def _finite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def _pct(n: int, d: int) -> str:
    if d <= 0:
        return "0%"
    return f"{(100.0 * n / d):.2f}%"


def _percentile(sorted_vals: List[float], p: float) -> float:
    """Nearest-rank percentile, p in [0,1]."""
    if not sorted_vals:
        return 0.0
    p = min(1.0, max(0.0, p))
    idx = int(math.ceil(p * len(sorted_vals))) - 1
    idx = min(len(sorted_vals) - 1, max(0, idx))
    return float(sorted_vals[idx])


def _parse_weight_kv(s: str) -> Tuple[str, float]:
    if "=" not in s:
        raise ValueError("--weight must be KEY=VALUE")
    k, v = s.split("=", 1)
    k = k.strip()
    if k not in SUPPORTED_FEATURES:
        raise ValueError(f"Unsupported feature '{k}'. Supported: {', '.join(sorted(SUPPORTED_FEATURES))}")
    try:
        f = float(v)
    except ValueError as e:
        raise ValueError(f"Invalid weight for {k}: {v}") from e
    if not _finite(f):
        raise ValueError(f"Weight for {k} must be finite")
    return k, f


def load_traces(path: Path) -> Dict[str, TraceAgg]:
    traces: Dict[str, TraceAgg] = {}

    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        for rs in obj.get("resourceSpans") or []:
            for ss in _iter_scope_spans(rs):
                for sp in (ss.get("spans") or []):
                    tid = sp.get("traceId")
                    if not tid:
                        continue
                    agg = traces.get(tid)
                    if agg is None:
                        agg = TraceAgg(trace_id=tid)
                        traces[tid] = agg
                    agg.add_span(sp)

    return traces


def score_trace(agg: TraceAgg, *, weights: Dict[str, float], intercept: float) -> Tuple[Dict[str, float], float]:
    feats: Dict[str, float] = {
        "duration_ms": float(agg.duration_ms()),
        "span_count": float(agg.span_count),
        "has_error": 1.0 if agg.has_error else 0.0,
    }

    s = float(intercept)
    for k, w in weights.items():
        s += w * feats.get(k, 0.0)

    return feats, s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Line-delimited OTLP trace JSON (fileexporter format: json)")
    ap.add_argument("--threshold", type=float, required=True, help="Keep when score >= threshold")
    ap.add_argument("--intercept", type=float, default=0.0, help="Model intercept")
    ap.add_argument(
        "--weight",
        action="append",
        default=[],
        help="Feature weight as KEY=VALUE. Supported: duration_ms, span_count, has_error. Can repeat.",
    )
    ap.add_argument(
        "--suggest-keep",
        action="append",
        type=float,
        default=[0.5, 0.2, 0.1],
        help="Suggest thresholds for target keep rates (0..1). Can repeat.",
    )
    args = ap.parse_args()

    if not _finite(args.threshold) or not _finite(args.intercept):
        raise SystemExit("threshold/intercept must be finite")

    weights: Dict[str, float] = {}
    for w in args.weight:
        k, v = _parse_weight_kv(w)
        weights[k] = v

    if not weights:
        raise SystemExit("At least one --weight KEY=VALUE is required")

    input_path = Path(args.input)
    traces = load_traces(input_path)

    durations: List[float] = []
    spans: List[float] = []
    errors: int = 0
    scores: List[float] = []
    kept: int = 0

    for agg in traces.values():
        feats, s = score_trace(agg, weights=weights, intercept=args.intercept)
        if not _finite(s):
            # Mirror Go sampler behavior: NaN/Inf => not sampled.
            continue
        durations.append(feats["duration_ms"])
        spans.append(feats["span_count"])
        if feats["has_error"] >= 1.0:
            errors += 1
        scores.append(s)
        if s >= args.threshold:
            kept += 1

    n = len(scores)
    if n == 0:
        print("No traces scored (input empty or all invalid).")
        return 0

    durations.sort()
    spans.sort()
    scores.sort()

    print("== INPUT ==")
    print(f"file: {input_path}")
    print(f"unique traceIds: {len(traces)}")
    print(f"scored traces: {n}")
    print(f"error traces: {errors} ({_pct(errors, n)})")
    print()

    print("== MODEL ==")
    print(f"intercept: {args.intercept}")
    print(f"threshold: {args.threshold}")
    print("weights:")
    for k in sorted(weights.keys()):
        print(f"  - {k}: {weights[k]}")
    print()

    print("== FEATURES (per-trace) ==")
    for label, arr in (
        ("duration_ms", durations),
        ("span_count", spans),
    ):
        print(f"{label}:")
        print(f"  p50: {_percentile(arr, 0.50):.2f}")
        print(f"  p90: {_percentile(arr, 0.90):.2f}")
        print(f"  p95: {_percentile(arr, 0.95):.2f}")
        print(f"  p99: {_percentile(arr, 0.99):.2f}")
    print()

    print("== SCORE ==")
    print(f"kept traces: {kept}/{n} ({_pct(kept, n)})")
    print(f"score p50: {_percentile(scores, 0.50):.4f}")
    print(f"score p90: {_percentile(scores, 0.90):.4f}")
    print(f"score p95: {_percentile(scores, 0.95):.4f}")
    print(f"score p99: {_percentile(scores, 0.99):.4f}")
    print()

    print("== THRESHOLD SUGGESTIONS ==")
    # For keep-rate K, threshold should be near the (1-K) quantile (ascending).
    for k in args.suggest_keep:
        if k <= 0 or k >= 1:
            continue
        q = 1.0 - k
        thr = _percentile(scores, q)
        print(f"target_keep={k:.2f} -> threshold≈{thr:.4f} (q={q:.2f})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
