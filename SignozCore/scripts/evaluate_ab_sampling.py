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
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


@dataclass(frozen=True)
class SpanRow:
    trace_id: str
    has_error: bool
    attrs: Dict[str, Any]
    start_time_unix_nano: Optional[int]


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
                    start_ns: Optional[int] = None
                    raw_start = sp.get("startTimeUnixNano")
                    try:
                        if raw_start is not None:
                            start_ns = int(raw_start)
                    except Exception:
                        start_ns = None
                    rows.append(
                        SpanRow(
                            trace_id=tid,
                            has_error=_span_has_error(sp, attrs),
                            attrs=attrs,
                            start_time_unix_nano=start_ns,
                        )
                    )
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
                "endpoint": r.attrs.get("http.route") or r.attrs.get("http.target"),
                "endpoint_tier": r.attrs.get("test.endpoint_tier"),
                "duration_ms": None,
                "declared_span_count": None,
                "start_time_unix_nano": None,
                "observed_span_rows": 0,
            }
            out[r.trace_id] = cur
        if r.has_error:
            cur["has_error"] = True
        cur["observed_span_rows"] += 1
        # keep first non-null case
        if cur.get("case") is None and r.attrs.get("test.case") is not None:
            cur["case"] = r.attrs.get("test.case")
        if cur.get("endpoint") is None:
            maybe_endpoint = r.attrs.get("http.route") or r.attrs.get("http.target")
            if maybe_endpoint is not None:
                cur["endpoint"] = maybe_endpoint
        if cur.get("endpoint_tier") is None and r.attrs.get("test.endpoint_tier") is not None:
            cur["endpoint_tier"] = r.attrs.get("test.endpoint_tier")
        if cur.get("duration_ms") is None:
            cur["duration_ms"] = _to_int(r.attrs.get("test.duration_ms"))
        if cur.get("declared_span_count") is None:
            cur["declared_span_count"] = _to_int(r.attrs.get("test.span_count"))
        if r.start_time_unix_nano is not None:
            current_start = cur.get("start_time_unix_nano")
            if current_start is None or int(r.start_time_unix_nano) < int(current_start):
                cur["start_time_unix_nano"] = int(r.start_time_unix_nano)
    return out


def _stddev(values: List[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    mean_v = sum(values) / len(values)
    var = sum((v - mean_v) ** 2 for v in values) / len(values)
    return math.sqrt(var)


def _window_threshold_quantile(
    threshold_by_sec: Dict[int, float],
    sec: int,
    window_seconds: int,
    q: float = 0.5,
) -> Optional[float]:
    if window_seconds <= 0:
        return None
    lo = sec - window_seconds + 1
    vals = [v for s, v in threshold_by_sec.items() if lo <= s <= sec]
    if not vals:
        return None
    v = _quantile([int(round(x)) for x in vals], q)
    if v is None:
        return None
    return float(v)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--sampled", required=True)
    ap.add_argument("--run-id", default=None, help="Optional filter by test.run_id")
    ap.add_argument("--error-rate-threshold", type=float, default=0.10)
    ap.add_argument("--latency-p95-threshold-ms", type=float, default=2000.0)
    ap.add_argument("--early-window-seconds", type=int, default=10)
    ap.add_argument("--delta-keep-min", type=float, default=0.10)
    ap.add_argument("--hold-seconds", type=int, default=3)
    ap.add_argument("--short-window-seconds", type=int, default=5)
    ap.add_argument("--long-window-seconds", type=int, default=30)
    ap.add_argument("--dual-alpha-normal", type=float, default=0.35)
    ap.add_argument("--dual-alpha-incident", type=float, default=0.75)
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

    print("== KEPT TRACE ROUTES (TOP, BASELINE vs SAMPLED) ==")
    base_by_endpoint: Dict[str, Set[str]] = defaultdict(set)
    sampled_by_endpoint: Dict[str, Set[str]] = defaultdict(set)
    for tid, t in baseline_traces.items():
        endpoint = str(t.get("endpoint") or "(none)")
        base_by_endpoint[endpoint].add(tid)
    for tid, t in sampled_traces.items():
        endpoint = str(t.get("endpoint") or "(none)")
        sampled_by_endpoint[endpoint].add(tid)

    top_endpoints = sorted(base_by_endpoint.keys(), key=lambda ep: len(base_by_endpoint[ep]), reverse=True)[:10]
    print("endpoint                                         kept       rate     sampled   baseline")
    for endpoint in top_endpoints:
        base_total = len(base_by_endpoint[endpoint])
        sampled_total = len(sampled_by_endpoint.get(endpoint, set()))
        kept_total = len(base_by_endpoint[endpoint] & kept_ids)
        rate = (kept_total / base_total * 100.0) if base_total > 0 else 0.0
        print(f"{endpoint:40s} {kept_total:5d}/{base_total:<5d} {rate:8.2f}% {sampled_total:9d} {base_total:10d}")
    print()

    print("== METRICS V2 ==")
    print("incident params:")
    print(f"  E_thr: {args.error_rate_threshold}")
    print(f"  L_thr: {args.latency_p95_threshold_ms}")
    print(f"  N_seconds: {args.early_window_seconds}")
    print(f"  delta_keep_min: {args.delta_keep_min}")
    print(f"  hold_seconds: {args.hold_seconds}")
    print(f"  short_window_seconds: {args.short_window_seconds}")
    print(f"  long_window_seconds: {args.long_window_seconds}")
    print(f"  dual_alpha_normal: {args.dual_alpha_normal}")
    print(f"  dual_alpha_incident: {args.dual_alpha_incident}")

    base_with_start = {
        tid: t
        for tid, t in baseline_traces.items()
        if t.get("start_time_unix_nano") is not None
    }

    incident_capture_latency_ms: Optional[float] = None
    early_incident_retention_pct: Optional[float] = None
    threshold_volatility_pct: Optional[float] = None
    max_step_change_pct: Optional[float] = None

    if base_with_start:
        t0_ns = min(int(t["start_time_unix_nano"]) for t in base_with_start.values())
        bins: Dict[int, Dict[str, Any]] = defaultdict(
            lambda: {
                "base_total": 0,
                "base_error": 0,
                "base_durations": [],
                "sampled_total": 0,
                "kept_non_err_durations": [],
            }
        )

        for tid, t in base_with_start.items():
            sec_idx = int((int(t["start_time_unix_nano"]) - t0_ns) // 1_000_000_000)
            bucket = bins[sec_idx]
            bucket["base_total"] += 1
            if t["has_error"]:
                bucket["base_error"] += 1
            d_ms = t.get("duration_ms")
            if d_ms is not None:
                bucket["base_durations"].append(int(d_ms))
            if tid in samp_ids:
                bucket["sampled_total"] += 1
            if tid in kept_non_err_ids and d_ms is not None:
                bucket["kept_non_err_durations"].append(int(d_ms))

        sec_keys = sorted(bins.keys())
        keep_rate_by_sec: Dict[int, float] = {}
        error_rate_by_sec: Dict[int, float] = {}
        p95_by_sec: Dict[int, float] = {}
        threshold_by_sec: Dict[int, float] = {}

        for sec in sec_keys:
            b = bins[sec]
            base_total = int(b["base_total"])
            if base_total <= 0:
                continue
            keep_rate_by_sec[sec] = float(b["sampled_total"]) / base_total
            error_rate_by_sec[sec] = float(b["base_error"]) / base_total
            durations_sec = b["base_durations"]
            if durations_sec:
                p95 = _quantile(durations_sec, 0.95)
                if p95 is not None:
                    p95_by_sec[sec] = float(p95)
            kept_non_err_durations_sec = b["kept_non_err_durations"]
            if kept_non_err_durations_sec:
                threshold_by_sec[sec] = float(min(kept_non_err_durations_sec))

        incident_sec: Optional[int] = None
        for sec in sec_keys:
            er = error_rate_by_sec.get(sec)
            p95 = p95_by_sec.get(sec)
            if er is None:
                continue
            if er > args.error_rate_threshold or (p95 is not None and p95 > args.latency_p95_threshold_ms):
                incident_sec = sec
                break

        if incident_sec is not None:
            pre_secs = [s for s in sec_keys if s < incident_sec and s in keep_rate_by_sec]
            if pre_secs:
                baseline_keep = sum(keep_rate_by_sec[s] for s in pre_secs) / len(pre_secs)
            else:
                baseline_keep = 0.0

            hold = max(1, int(args.hold_seconds))
            rise_sec: Optional[int] = None
            for sec in sec_keys:
                if sec < incident_sec:
                    continue
                ok = True
                for k in range(hold):
                    candidate = sec + k
                    kr = keep_rate_by_sec.get(candidate)
                    if kr is None or kr < baseline_keep + args.delta_keep_min:
                        ok = False
                        break
                if ok:
                    rise_sec = sec
                    break

            if rise_sec is not None:
                incident_capture_latency_ms = float((rise_sec - incident_sec) * 1000)

            early_start = incident_sec
            early_end = incident_sec + max(1, int(args.early_window_seconds)) - 1
            total_early = 0
            kept_early = 0
            for sec in sec_keys:
                if sec < early_start or sec > early_end:
                    continue
                total_early += int(bins[sec]["base_total"])
                kept_early += int(bins[sec]["sampled_total"])
            if total_early > 0:
                early_incident_retention_pct = kept_early / total_early * 100.0

        dual_threshold_by_sec: Dict[int, float] = {}
        for sec in sorted(threshold_by_sec.keys()):
            short_thr = _window_threshold_quantile(
                threshold_by_sec,
                sec,
                max(1, int(args.short_window_seconds)),
                q=0.5,
            )
            long_thr = _window_threshold_quantile(
                threshold_by_sec,
                sec,
                max(1, int(args.long_window_seconds)),
                q=0.5,
            )

            base_thr = threshold_by_sec[sec]
            if short_thr is None:
                short_thr = base_thr
            if long_thr is None:
                long_thr = base_thr

            if incident_sec is not None and sec >= incident_sec:
                alpha = float(args.dual_alpha_incident)
            else:
                alpha = float(args.dual_alpha_normal)

            if alpha < 0.0:
                alpha = 0.0
            elif alpha > 1.0:
                alpha = 1.0

            dual_threshold_by_sec[sec] = alpha * short_thr + (1.0 - alpha) * long_thr

        threshold_values = [dual_threshold_by_sec[s] for s in sorted(dual_threshold_by_sec.keys())]
        if threshold_values:
            mean_threshold = sum(threshold_values) / len(threshold_values)
            std_threshold = _stddev(threshold_values)
            if std_threshold is not None and mean_threshold != 0:
                threshold_volatility_pct = std_threshold / mean_threshold * 100.0

            step_changes: List[float] = []
            ordered = [dual_threshold_by_sec[s] for s in sorted(dual_threshold_by_sec.keys())]
            for idx in range(1, len(ordered)):
                prev = ordered[idx - 1]
                curr = ordered[idx]
                if prev == 0:
                    continue
                step_changes.append(abs(curr - prev) / abs(prev) * 100.0)
            if step_changes:
                max_step_change_pct = max(step_changes)

    if incident_capture_latency_ms is None:
        print("incident_capture_latency_ms: NA")
    else:
        print(f"incident_capture_latency_ms: {incident_capture_latency_ms:.1f}")

    if early_incident_retention_pct is None:
        print("early_incident_retention_pct: NA")
    else:
        print(f"early_incident_retention_pct: {early_incident_retention_pct:.2f}")

    critical_base_ids = {
        tid for tid, t in baseline_traces.items() if str(t.get("endpoint_tier") or "").lower() == "critical"
    }
    if critical_base_ids:
        critical_kept = len(critical_base_ids & kept_ids)
        critical_cov = critical_kept / len(critical_base_ids) * 100.0
        print(f"critical_endpoint_coverage_pct: {critical_cov:.2f} ({critical_kept}/{len(critical_base_ids)})")
    else:
        print("critical_endpoint_coverage_pct: NA")

    if threshold_volatility_pct is None:
        print("threshold_volatility_pct: NA")
    else:
        print(f"threshold_volatility_pct: {threshold_volatility_pct:.2f}")

    if max_step_change_pct is None:
        print("max_step_change_pct: NA")
    else:
        print(f"max_step_change_pct: {max_step_change_pct:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
