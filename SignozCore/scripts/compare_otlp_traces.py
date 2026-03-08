#!/usr/bin/env python3
"""Compare two line-delimited OTLP trace export JSON files.

These files come from the collector fileexporter with `format: json`.
Each line is a JSON object containing `resourceSpans`.

Outputs:
- total JSON records (lines)
- total spans
- unique traceIds
- spans per service.name (top N)
- error spans count
- optional: top http.route / http.target / rpc.method (best-effort)

Usage:
  python3 scripts/compare_otlp_traces.py --base <baseline.json> --test <model.json>

Tip:
  Use --details to print more breakdowns.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _iter_scope_spans(resource_span: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # OTLP JSON may be `scopeSpans` (new) or `instrumentationLibrarySpans` (old).
    if "scopeSpans" in resource_span:
        yield from (resource_span.get("scopeSpans") or [])
        return
    if "instrumentationLibrarySpans" in resource_span:
        yield from (resource_span.get("instrumentationLibrarySpans") or [])
        return


def _resource_attr(resource_span: Dict[str, Any], key: str) -> Optional[Any]:
    res = (resource_span.get("resource") or {})
    attrs = res.get("attributes") or []
    for a in attrs:
        if a.get("key") != key:
            continue
        v = (a.get("value") or {})
        # OTLP JSON value is one-of.
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
    # Best-effort: status.code==STATUS_CODE_ERROR or http.status_code>=500.
    status = span.get("status") or {}
    if status.get("code") in ("STATUS_CODE_ERROR", 2):
        return True
    http_code = _span_attr(span, "http.status_code")
    if isinstance(http_code, (int, float)) and http_code >= 500:
        return True
    # Sometimes status code is stored as string.
    if isinstance(http_code, str):
        try:
            return int(http_code) >= 500
        except ValueError:
            return False
    return False


@dataclass
class TraceStats:
    path: str
    records: int
    spans: int
    unique_trace_ids: int
    error_spans: int
    spans_by_service: Counter
    top_routes: Counter
    trace_duration_ms: List[float]


def _parse_ns(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    rank = (len(values) - 1) * percentile
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return values[lo]
    return values[lo] + (values[hi] - values[lo]) * (rank - lo)


def _duration_buckets(values: List[float]) -> List[Tuple[str, int]]:
    c_lt_10 = 0
    c_10_100 = 0
    c_100_500 = 0
    c_ge_500 = 0

    for ms in values:
        if ms < 10:
            c_lt_10 += 1
        elif ms < 100:
            c_10_100 += 1
        elif ms < 500:
            c_100_500 += 1
        else:
            c_ge_500 += 1

    return [
        ("<10ms", c_lt_10),
        ("10-100ms", c_10_100),
        ("100-500ms", c_100_500),
        (">=500ms", c_ge_500),
    ]


def summarize(path: str, top_n: int = 10) -> TraceStats:
    p = Path(path)
    records = 0
    spans = 0
    error_spans = 0
    trace_ids = set()
    trace_bounds: Dict[str, Tuple[int, int]] = {}
    spans_by_service: Counter = Counter()
    top_routes: Counter = Counter()

    for line in p.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        records += 1
        obj = json.loads(line)
        for rs in obj.get("resourceSpans") or []:
            service_name = _resource_attr(rs, "service.name") or "(unknown)"
            for ss in _iter_scope_spans(rs):
                for sp in (ss.get("spans") or []):
                    spans += 1
                    spans_by_service[service_name] += 1
                    tid = sp.get("traceId")
                    if tid:
                        trace_ids.add(tid)

                        start_ns = _parse_ns(sp.get("startTimeUnixNano"))
                        end_ns = _parse_ns(sp.get("endTimeUnixNano"))
                        if start_ns is not None and end_ns is not None and end_ns >= start_ns:
                            prev = trace_bounds.get(tid)
                            if prev is None:
                                trace_bounds[tid] = (start_ns, end_ns)
                            else:
                                trace_bounds[tid] = (min(prev[0], start_ns), max(prev[1], end_ns))

                    if _span_is_error(sp):
                        error_spans += 1

                    route = (
                        _span_attr(sp, "http.route")
                        or _span_attr(sp, "http.target")
                        or _span_attr(sp, "rpc.method")
                    )
                    if isinstance(route, str) and route:
                        top_routes[route] += 1

    trace_duration_ms: List[float] = []
    for start_ns, end_ns in trace_bounds.values():
        trace_duration_ms.append((end_ns - start_ns) / 1_000_000.0)
    trace_duration_ms.sort()

    return TraceStats(
        path=path,
        records=records,
        spans=spans,
        unique_trace_ids=len(trace_ids),
        error_spans=error_spans,
        spans_by_service=spans_by_service,
        top_routes=Counter(dict(top_routes.most_common(top_n))),
        trace_duration_ms=trace_duration_ms,
    )


def _pct(n: int, d: int) -> str:
    if d <= 0:
        return "0%"
    return f"{(100.0 * n / d):.2f}%"


def print_stats(label: str, s: TraceStats, details: bool) -> None:
    print(f"== {label} ==")
    print(f"file: {s.path}")
    print(f"records(lines): {s.records}")
    print(f"spans: {s.spans}")
    print(f"unique traceIds: {s.unique_trace_ids}")
    print(f"error spans: {s.error_spans} ({_pct(s.error_spans, s.spans)})")

    if s.trace_duration_ms:
        p50 = _percentile(s.trace_duration_ms, 0.50)
        p95 = _percentile(s.trace_duration_ms, 0.95)
        p99 = _percentile(s.trace_duration_ms, 0.99)
        print(
            "trace duration(ms): "
            f"p50={p50:.2f}, p95={p95:.2f}, p99={p99:.2f}, "
            f"min={s.trace_duration_ms[0]:.2f}, max={s.trace_duration_ms[-1]:.2f}"
        )
        print("-- trace duration buckets --")
        for name, count in _duration_buckets(s.trace_duration_ms):
            print(f"{name:10s}: {count} ({_pct(count, len(s.trace_duration_ms))})")

    if details:
        print("\n-- spans by service.name (top 10) --")
        for name, count in s.spans_by_service.most_common(10):
            print(f"{count:8d}  {name}")

        if s.top_routes:
            print("\n-- top routes/methods (best-effort, top 10) --")
            for name, count in s.top_routes.most_common(10):
                print(f"{count:8d}  {name}")

    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="Path to baseline line-delimited OTLP trace JSON")
    ap.add_argument("--test", required=True, help="Path to test/model line-delimited OTLP trace JSON")
    ap.add_argument("--details", action="store_true", help="Print breakdowns")
    args = ap.parse_args()

    base = summarize(args.base)
    test = summarize(args.test)

    print_stats("BASE", base, args.details)
    print_stats("TEST", test, args.details)

    print("== DELTA (TEST vs BASE) ==")
    for name, b, t in (
        ("records(lines)", base.records, test.records),
        ("spans", base.spans, test.spans),
        ("unique traceIds", base.unique_trace_ids, test.unique_trace_ids),
        ("error spans", base.error_spans, test.error_spans),
    ):
        diff = t - b
        sign = "+" if diff >= 0 else ""
        ratio = "n/a" if b == 0 else f"{(t / b):.3f}x"
        print(f"{name:16s}: {b} -> {t} ({sign}{diff}), ratio={ratio}")

    base_buckets = dict(_duration_buckets(base.trace_duration_ms))
    test_buckets = dict(_duration_buckets(test.trace_duration_ms))
    if base_buckets or test_buckets:
        print("\ntrace duration buckets delta (TEST vs BASE):")
        for key in ("<10ms", "10-100ms", "100-500ms", ">=500ms"):
            b = base_buckets.get(key, 0)
            t = test_buckets.get(key, 0)
            diff = t - b
            sign = "+" if diff >= 0 else ""
            ratio = "n/a" if b == 0 else f"{(t / b):.3f}x"
            print(f"{key:10s}: {b} -> {t} ({sign}{diff}), ratio={ratio}")

    print()
    if base.spans > 0:
        kept = test.spans
        print(f"Approx kept spans rate: {kept}/{base.spans} = {_pct(kept, base.spans)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
