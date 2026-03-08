#!/usr/bin/env python3
"""Evaluate A/B tail-sampling results from two fileexporter OTLP JSON outputs.

Assumes each line is an OTLP JSON record (exporter 'file' format=json).

Outputs:
- baseline traces count
- sampled traces count
- kept/dropped trace rate (by trace_id)
- error trace retention
- breakdown by `test.case` attribute when present

Usage:
  /home/leeduc/miniconda3/bin/python scripts/evaluate_ab_sampling.py \
    --baseline signoz/deploy/docker/otel-export/latencytest_baseline/traces.baseline.json \
    --sampled  signoz/deploy/docker/otel-export/latencytest_sampled/traces.sampled.json \
    --run-id 20260225-040156
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


@dataclass(frozen=True)
class SpanRow:
    trace_id: str
    has_error: bool
    attrs: Dict[str, Any]


def _iter_records(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _hex_trace_id(any_val: Any) -> Optional[str]:
    if any_val is None:
        return None
    if isinstance(any_val, str):
        # already hex
        return any_val.lower()
    if isinstance(any_val, list):
        # array of ints
        try:
            return bytes(int(x) & 0xFF for x in any_val).hex()
        except Exception:
            return None
    return None


def _attrs_from(span: dict) -> Dict[str, Any]:
    """Extract span attributes into a simple dict.

    Supports multiple OTLP JSON shapes seen in fileexporter outputs:
    - attributes: [{"key":"k","value":{...}}, ...]
    - attributes: {"keyValues": [{"key":"k","value":{...}}, ...]}
    """

    out: Dict[str, Any] = {}

    attrs = span.get("attributes")

    kvs: Optional[list]
    if isinstance(attrs, list):
        kvs = attrs
    elif isinstance(attrs, dict):
        maybe = attrs.get("keyValues")
        kvs = maybe if isinstance(maybe, list) else None
    else:
        kvs = None

    if not kvs:
        return out

    for kv in kvs:
        if not isinstance(kv, dict):
            continue
        k = kv.get("key")
        v = kv.get("value")
        if not isinstance(k, str) or not isinstance(v, dict):
            continue

        # The value is oneof: stringValue/intValue/doubleValue/boolValue/bytesValue
        for field in (
            "stringValue",
            "intValue",
            "doubleValue",
            "boolValue",
            "bytesValue",
        ):
            if field in v:
                out[k] = v[field]
                break

    return out


def _span_has_error(span: dict, attrs: Dict[str, Any]) -> bool:
    status = span.get("status")
    if isinstance(status, dict):
        code = status.get("code")
        # In OTLP JSON, code may be 2 for ERROR.
        if code == 2 or code == "STATUS_CODE_ERROR":
            return True

    http_code = attrs.get("http.status_code")
    try:
        if http_code is not None and int(http_code) >= 500:
            return True
    except Exception:
        pass

    return False


def _extract_spans(path: Path) -> List[SpanRow]:
    rows: List[SpanRow] = []
    for rec in _iter_records(path):
        rs = rec.get("resourceSpans")
        if not isinstance(rs, list):
            continue
        for r in rs:
            scopes = r.get("scopeSpans")
            if not isinstance(scopes, list):
                continue
            for s in scopes:
                spans = s.get("spans")
                if not isinstance(spans, list):
                    continue
                for sp in spans:
                    tid = _hex_trace_id(sp.get("traceId"))
                    if not tid:
                        continue
                    attrs = _attrs_from(sp)
                    rows.append(SpanRow(trace_id=tid, has_error=_span_has_error(sp, attrs), attrs=attrs))
    return rows


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _quantile(values: List[int], q: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return float(xs[0])
    pos = (len(xs) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo
    return float(xs[lo] + (xs[hi] - xs[lo]) * frac)


def _duration_bucket_counts(values: List[int]) -> Dict[str, int]:
    counts = {
        "<10ms": 0,
        "10-100ms": 0,
        "100-500ms": 0,
        ">=500ms": 0,
    }
    for ms in values:
        if ms < 10:
            counts["<10ms"] += 1
        elif ms < 100:
            counts["10-100ms"] += 1
        elif ms < 500:
            counts["100-500ms"] += 1
        else:
            counts[">=500ms"] += 1
    return counts


def _print_duration_buckets(label: str, values: List[int]) -> None:
    print(f"{label} duration buckets:")
    if not values:
        print("  n/a")
        return

    counts = _duration_bucket_counts(values)
    total = len(values)
    for bucket in ("<10ms", "10-100ms", "100-500ms", ">=500ms"):
        c = counts[bucket]
        pct = (c / total) * 100 if total > 0 else 0
        print(f"  {bucket:10s}: {c}/{total} ({pct:.2f}%)")


def _safe_quantile_str(values: List[int], q: float) -> str:
    v = _quantile(values, q)
    return "n/a" if v is None else f"{v:.1f}"


def _trace_level(rows: List[SpanRow], run_id: Optional[str]) -> Dict[str, dict]:
    # trace_id -> summary
    out: Dict[str, dict] = {}
    for r in rows:
        if run_id is not None and r.attrs.get("test.run_id") != run_id:
            continue
        cur = out.get(r.trace_id)
        if cur is None:
            cur = {
                "trace_id": r.trace_id,
                "has_error": False,
                "case": r.attrs.get("test.case"),
                "duration_ms": None,
                "declared_span_count": None,
                "observed_span_rows": 0,
            }
            out[r.trace_id] = cur
        if r.has_error:
            cur["has_error"] = True
        cur["observed_span_rows"] += 1
        # keep first non-null case
        if cur.get("case") is None and r.attrs.get("test.case") is not None:
            cur["case"] = r.attrs.get("test.case")
        if cur.get("duration_ms") is None:
            cur["duration_ms"] = _to_int(r.attrs.get("test.duration_ms"))
        if cur.get("declared_span_count") is None:
            cur["declared_span_count"] = _to_int(r.attrs.get("test.span_count"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--sampled", required=True)
    ap.add_argument("--run-id", default=None, help="Optional filter by test.run_id")
    args = ap.parse_args()

    baseline_path = Path(args.baseline)
    sampled_path = Path(args.sampled)

    baseline_rows = _extract_spans(baseline_path)
    sampled_rows = _extract_spans(sampled_path)

    if args.run_id is not None:
        baseline_rows_filtered = [r for r in baseline_rows if r.attrs.get("test.run_id") == args.run_id]
        sampled_rows_filtered = [r for r in sampled_rows if r.attrs.get("test.run_id") == args.run_id]
    else:
        baseline_rows_filtered = baseline_rows
        sampled_rows_filtered = sampled_rows

    baseline_traces = _trace_level(baseline_rows, args.run_id)
    sampled_traces = _trace_level(sampled_rows, args.run_id)

    base_ids: Set[str] = set(baseline_traces.keys())
    samp_ids: Set[str] = set(sampled_traces.keys())

    kept_ids = base_ids & samp_ids
    dropped_ids = base_ids - samp_ids

    base_span_rows = len(baseline_rows_filtered)
    sampled_span_rows = len(sampled_rows_filtered)

    base_err_ids = {tid for tid, t in baseline_traces.items() if t["has_error"]}
    base_non_err_ids = base_ids - base_err_ids
    kept_err_ids = kept_ids & base_err_ids
    kept_non_err_ids = kept_ids & base_non_err_ids

    print("== INPUT ==")
    print(f"baseline: {args.baseline}")
    print(f"sampled : {args.sampled}")
    print(f"run_id  : {args.run_id or '(none)'}")
    print()

    print("== COUNTS ==")
    print(f"baseline traces: {len(base_ids)}")
    print(f"sampled  traces: {len(samp_ids)}")
    print(f"baseline spans : {base_span_rows}")
    print(f"sampled  spans : {sampled_span_rows}")
    if len(base_ids) > 0:
        print(f"kept traces    : {len(kept_ids)}/{len(base_ids)} ({len(kept_ids)/len(base_ids)*100:.2f}%)")
        print(f"dropped traces : {len(dropped_ids)}/{len(base_ids)} ({len(dropped_ids)/len(base_ids)*100:.2f}%)")
    if base_span_rows > 0:
        print(f"kept spans rate: {sampled_span_rows}/{base_span_rows} ({sampled_span_rows/base_span_rows*100:.2f}%)")
    print()

    print("== ERROR RETENTION ==")
    base_err = [t for t in baseline_traces.values() if t["has_error"]]
    samp_err = [t for t in sampled_traces.values() if t["has_error"]]
    kept_err = [t for t in base_err if t["trace_id"] in kept_ids]
    print(f"baseline error traces: {len(base_err)}/{len(base_ids)} ({(len(base_err)/len(base_ids)*100 if base_ids else 0):.2f}%)")
    print(f"sampled  error traces: {len(samp_err)}/{len(samp_ids)} ({(len(samp_err)/len(samp_ids)*100 if samp_ids else 0):.2f}%)")
    if len(base_err) > 0:
        print(f"kept error traces    : {len(kept_err)}/{len(base_err)} ({len(kept_err)/len(base_err)*100:.2f}%)")
    if len(base_non_err_ids) > 0:
        print(
            f"kept non-error traces: {len(kept_non_err_ids)}/{len(base_non_err_ids)} "
            f"({len(kept_non_err_ids)/len(base_non_err_ids)*100:.2f}%)"
        )
    if len(base_ids) > 0:
        print(f"error lift (kept_err/kept_all): {len(kept_err_ids)}/{len(kept_ids)} ({(len(kept_err_ids)/len(kept_ids)*100) if kept_ids else 0:.2f}%)")
    print()

    kept_duration = [
        int(t["duration_ms"])
        for tid, t in baseline_traces.items()
        if tid in kept_ids and t.get("duration_ms") is not None
    ]
    dropped_duration = [
        int(t["duration_ms"])
        for tid, t in baseline_traces.items()
        if tid in dropped_ids and t.get("duration_ms") is not None
    ]
    kept_non_err_duration = [
        int(baseline_traces[tid]["duration_ms"])
        for tid in kept_non_err_ids
        if baseline_traces[tid].get("duration_ms") is not None
    ]
    dropped_non_err_duration = [
        int(baseline_traces[tid]["duration_ms"])
        for tid in (base_non_err_ids - kept_non_err_ids)
        if baseline_traces[tid].get("duration_ms") is not None
    ]

    print("== KEEP DURATION / THRESHOLD (EMPIRICAL) ==")
    if kept_duration:
        print(
            "kept duration_ms p50/p95/p99: "
            f"{_safe_quantile_str(kept_duration, 0.50)}/"
            f"{_safe_quantile_str(kept_duration, 0.95)}/"
            f"{_safe_quantile_str(kept_duration, 0.99)}"
        )
        print(f"kept duration_ms range      : {min(kept_duration)}..{max(kept_duration)}")
    else:
        print("kept duration_ms p50/p95/p99: n/a")
        print("kept duration_ms range      : n/a")

    if dropped_duration:
        print(
            "dropped duration_ms p50/p95/p99: "
            f"{_safe_quantile_str(dropped_duration, 0.50)}/"
            f"{_safe_quantile_str(dropped_duration, 0.95)}/"
            f"{_safe_quantile_str(dropped_duration, 0.99)}"
        )
        print(f"dropped duration_ms range      : {min(dropped_duration)}..{max(dropped_duration)}")
    else:
        print("dropped duration_ms p50/p95/p99: n/a")
        print("dropped duration_ms range      : n/a")

    if kept_non_err_duration:
        est_keep_threshold = min(kept_non_err_duration)
        print(f"estimated keep threshold duration_ms (non-error, min kept): {est_keep_threshold}")
    else:
        print("estimated keep threshold duration_ms (non-error, min kept): n/a")

    if kept_non_err_duration and dropped_non_err_duration:
        max_drop = max(dropped_non_err_duration)
        min_keep = min(kept_non_err_duration)
        if min_keep > max_drop:
            print(f"non-error separation: clean (max dropped={max_drop}, min kept={min_keep})")
        else:
            print(f"non-error separation: overlap (max dropped={max_drop}, min kept={min_keep})")
    print()

    print("== TRACE SHAPE (BASELINE) ==")
    base_duration = [int(t["duration_ms"]) for t in baseline_traces.values() if t.get("duration_ms") is not None]
    sampled_duration = [int(t["duration_ms"]) for t in sampled_traces.values() if t.get("duration_ms") is not None]
    base_declared_spans = [int(t["declared_span_count"]) for t in baseline_traces.values() if t.get("declared_span_count") is not None]
    if base_duration:
        p50 = _quantile(base_duration, 0.50)
        p95 = _quantile(base_duration, 0.95)
        p99 = _quantile(base_duration, 0.99)
        print(f"duration_ms p50/p95/p99: {p50:.1f}/{p95:.1f}/{p99:.1f}")
    else:
        print("duration_ms p50/p95/p99: n/a")
    if base_declared_spans:
        p50s = _quantile(base_declared_spans, 0.50)
        p90s = _quantile(base_declared_spans, 0.90)
        p99s = _quantile(base_declared_spans, 0.99)
        print(f"declared span_count p50/p90/p99: {p50s:.1f}/{p90s:.1f}/{p99s:.1f}")
    else:
        print("declared span_count p50/p90/p99: n/a")
    _print_duration_buckets("baseline", base_duration)
    _print_duration_buckets("sampled ", sampled_duration)
    _print_duration_buckets("kept    ", kept_duration)
    print()

    print("== BY test.case ==")
    base_by_case: Dict[str, List[str]] = defaultdict(list)
    samp_by_case: Dict[str, Set[str]] = defaultdict(set)
    for tid, t in baseline_traces.items():
        case = t.get("case") or "(none)"
        base_by_case[str(case)].append(tid)
    for tid, t in sampled_traces.items():
        case = t.get("case") or "(none)"
        samp_by_case[str(case)].add(tid)

    for case, tids in sorted(base_by_case.items()):
        tids_set = set(tids)
        kept = tids_set & kept_ids
        errs = [tid for tid in tids if baseline_traces[tid]["has_error"]]
        kept_errs = [tid for tid in errs if tid in kept_ids]
        print(f"case={case}")
        print(f"  baseline traces: {len(tids)}")
        print(f"  sampled traces : {len(samp_by_case.get(case, set()))}")
        if len(tids) > 0:
            print(f"  kept traces    : {len(kept)}/{len(tids)} ({len(kept)/len(tids)*100:.2f}%)")
            print(f"  dropped traces : {len(tids)-len(kept)}/{len(tids)} ({(len(tids)-len(kept))/len(tids)*100:.2f}%)")
        if len(errs) > 0:
            print(f"  error traces   : {len(errs)}/{len(tids)} ({len(errs)/len(tids)*100:.2f}%)")
            print(f"  kept error     : {len(kept_errs)}/{len(errs)} ({len(kept_errs)/len(errs)*100:.2f}%)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
