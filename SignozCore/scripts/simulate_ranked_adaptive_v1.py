#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: int
    span_count: int
    has_error: bool
    endpoint: Optional[str]
    endpoint_tier: Optional[str]


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
        return any_val.lower()
    if isinstance(any_val, list):
        try:
            return bytes(int(x) & 0xFF for x in any_val).hex()
        except Exception:
            return None
    return None


def _attrs_from(span: dict) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    attrs = span.get("attributes")
    if isinstance(attrs, list):
        kvs = attrs
    elif isinstance(attrs, dict) and isinstance(attrs.get("keyValues"), list):
        kvs = attrs.get("keyValues")
    else:
        kvs = None

    if not kvs:
        return out

    for kv in kvs:
        if not isinstance(kv, dict):
            continue
        key = kv.get("key")
        val = kv.get("value")
        if not isinstance(key, str) or not isinstance(val, dict):
            continue
        for field in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
            if field in val:
                out[key] = val[field]
                break
    return out


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _span_has_error(span: dict, attrs: Dict[str, Any]) -> bool:
    status = span.get("status")
    if isinstance(status, dict):
        code = status.get("code")
        if code == 2 or code == "STATUS_CODE_ERROR":
            return True
    http_code = attrs.get("http.status_code")
    try:
        if http_code is not None and int(http_code) >= 500:
            return True
    except Exception:
        pass
    return False


def load_trace_points(path: Path, run_id: Optional[str]) -> List[TracePoint]:
    grouped: Dict[str, Dict[str, Any]] = {}

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
                    trace_id = _hex_trace_id(sp.get("traceId"))
                    if not trace_id:
                        continue
                    attrs = _attrs_from(sp)
                    if run_id is not None and attrs.get("test.run_id") != run_id:
                        continue

                    cur = grouped.get(trace_id)
                    if cur is None:
                        cur = {
                            "trace_id": trace_id,
                            "start_ns": None,
                            "duration_ms": None,
                            "span_count": None,
                            "has_error": False,
                            "endpoint": None,
                            "endpoint_tier": None,
                        }
                        grouped[trace_id] = cur

                    if _span_has_error(sp, attrs):
                        cur["has_error"] = True

                    start_raw = sp.get("startTimeUnixNano")
                    try:
                        if start_raw is not None:
                            start_ns = int(start_raw)
                            if cur["start_ns"] is None or start_ns < cur["start_ns"]:
                                cur["start_ns"] = start_ns
                    except Exception:
                        pass

                    if cur["duration_ms"] is None and attrs.get("test.duration_ms") is not None:
                        cur["duration_ms"] = _to_int(attrs.get("test.duration_ms"), 0)
                    if cur["span_count"] is None and attrs.get("test.span_count") is not None:
                        cur["span_count"] = _to_int(attrs.get("test.span_count"), 0)

                    if cur["endpoint"] is None:
                        ep = attrs.get("http.route") or attrs.get("http.target")
                        if ep is not None:
                            cur["endpoint"] = str(ep)

                    if cur["endpoint_tier"] is None and attrs.get("test.endpoint_tier") is not None:
                        cur["endpoint_tier"] = str(attrs.get("test.endpoint_tier"))

    points: List[TracePoint] = []
    for t in grouped.values():
        if t["start_ns"] is None:
            continue
        points.append(
            TracePoint(
                trace_id=t["trace_id"],
                start_ns=int(t["start_ns"]),
                duration_ms=int(t["duration_ms"] or 0),
                span_count=int(t["span_count"] or 0),
                has_error=bool(t["has_error"]),
                endpoint=t["endpoint"],
                endpoint_tier=t["endpoint_tier"],
            )
        )

    points.sort(key=lambda x: x.start_ns)
    return points


def quantile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return float(xs[0])
    q = min(max(q, 0.0), 1.0)
    pos = q * (len(xs) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(xs[lo])
    frac = pos - lo
    return float(xs[lo] + (xs[hi] - xs[lo]) * frac)


def _norm(v: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    x = (v - lo) / (hi - lo)
    return max(0.0, min(1.0, x))


def detect_incident_start_sec(
    points: List[TracePoint],
    short_window_seconds: int,
    long_window_seconds: int,
    error_rate_threshold: float,
    latency_p95_threshold_ms: float,
) -> Optional[int]:
    if not points:
        return None

    t0 = points[0].start_ns
    by_sec: Dict[int, List[TracePoint]] = defaultdict(list)
    for t in points:
        sec = int((t.start_ns - t0) // 1_000_000_000)
        by_sec[sec].append(t)

    sec_keys = sorted(by_sec.keys())
    short_hist: deque[tuple[int, bool, int]] = deque()
    long_hist: deque[tuple[int, bool, int]] = deque()

    for sec in range(sec_keys[0], sec_keys[-1] + 1):
        for tr in by_sec.get(sec, []):
            short_hist.append((sec, tr.has_error, tr.duration_ms))
            long_hist.append((sec, tr.has_error, tr.duration_ms))

        while short_hist and short_hist[0][0] < sec - short_window_seconds + 1:
            short_hist.popleft()
        while long_hist and long_hist[0][0] < sec - long_window_seconds + 1:
            long_hist.popleft()

        short_err_rate = (sum(1 for x in short_hist if x[1]) / len(short_hist)) if short_hist else 0.0
        short_p95 = quantile([float(x[2]) for x in short_hist], 0.95) if short_hist else None

        incident_raw = short_err_rate > error_rate_threshold or (
            short_p95 is not None and short_p95 > latency_p95_threshold_ms
        )
        if incident_raw:
            return sec

    return None


def run_budget_selection(
    points: List[TracePoint],
    budget_pct: float,
    error_budget_ratio: float,
    incident_start_sec: Optional[int],
    time_decay_sec: float,
    context_window_sec: int,
    early_window_seconds: int,
) -> Dict[str, Any]:
    total = len(points)
    if total == 0:
        return {
            "budget_pct": budget_pct,
            "kept_traces": 0,
            "total_traces": 0,
            "kept_traces_pct": 0.0,
            "kept_error_traces": 0,
            "error_traces": 0,
            "kept_error_traces_pct": None,
            "early_incident_retention_pct": None,
            "critical_endpoint_coverage_pct": None,
            "critical_endpoint_coverage_kept": 0,
            "critical_endpoint_coverage_total": 0,
            "incident_capture_latency_ms": None,
            "selected_error_count": 0,
            "selected_context_count": 0,
        }

    t0 = min(p.start_ns for p in points)

    target_keep = int(round(total * (budget_pct / 100.0)))
    target_keep = max(1, min(total, target_keep))

    error_quota = int(round(target_keep * error_budget_ratio))
    error_quota = max(0, min(target_keep, error_quota))
    context_quota = target_keep - error_quota

    dur_lo = min(p.duration_ms for p in points)
    dur_hi = max(p.duration_ms for p in points)
    span_lo = min(p.span_count for p in points)
    span_hi = max(p.span_count for p in points)

    def time_score(p: TracePoint) -> float:
        if incident_start_sec is None:
            return 0.5
        sec = int((p.start_ns - t0) // 1_000_000_000)
        return math.exp(-abs(sec - incident_start_sec) / max(1.0, time_decay_sec))

    def severity_score(p: TracePoint) -> float:
        d = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        s = _norm(float(p.span_count), float(span_lo), float(span_hi))
        return 0.5 * d + 0.5 * s

    def service_score(p: TracePoint) -> float:
        tier = str(p.endpoint_tier or "").lower()
        if tier == "critical":
            return 1.0
        if p.endpoint:
            return 0.7
        return 0.5

    def context_score(p: TracePoint) -> float:
        tier = str(p.endpoint_tier or "").lower()
        c = _norm(float(p.span_count), float(span_lo), float(span_hi))
        if tier == "critical":
            return min(1.0, 0.7 + 0.3 * c)
        return c

    error_candidates = [p for p in points if p.has_error]
    non_error_candidates = [p for p in points if not p.has_error]

    error_ranked: List[tuple[float, TracePoint]] = []
    for p in error_candidates:
        sc = (
            0.35 * time_score(p)
            + 0.30 * severity_score(p)
            + 0.20 * service_score(p)
            + 0.15 * context_score(p)
        )
        error_ranked.append((sc, p))
    error_ranked.sort(key=lambda x: (x[0], x[1].duration_ms, x[1].span_count, x[1].trace_id), reverse=True)

    selected_error = [p for _sc, p in error_ranked[: min(error_quota, len(error_ranked))]]

    selected_error_endpoints = {p.endpoint for p in selected_error if p.endpoint}

    def context_related_score(p: TracePoint) -> float:
        if incident_start_sec is None:
            t_rel = 0.5
        else:
            sec = int((p.start_ns - t0) // 1_000_000_000)
            if abs(sec - incident_start_sec) <= context_window_sec:
                t_rel = 1.0
            else:
                t_rel = math.exp(-abs(sec - incident_start_sec) / max(1.0, time_decay_sec))

        ep_rel = 1.0 if (p.endpoint and p.endpoint in selected_error_endpoints) else 0.0
        sev = severity_score(p)
        return 0.50 * t_rel + 0.30 * ep_rel + 0.20 * sev

    context_ranked: List[tuple[float, TracePoint]] = []
    for p in non_error_candidates:
        context_ranked.append((context_related_score(p), p))
    context_ranked.sort(key=lambda x: (x[0], x[1].duration_ms, x[1].span_count, x[1].trace_id), reverse=True)

    selected_context = [p for _sc, p in context_ranked[: min(context_quota, len(context_ranked))]]

    kept_ids = {p.trace_id for p in selected_error}
    kept_ids.update(p.trace_id for p in selected_context)

    if len(kept_ids) < target_keep:
        fillers = [p for _sc, p in error_ranked[min(error_quota, len(error_ranked)) :]]
        fillers += [p for _sc, p in context_ranked[min(context_quota, len(context_ranked)) :]]
        for p in fillers:
            if p.trace_id in kept_ids:
                continue
            kept_ids.add(p.trace_id)
            if len(kept_ids) >= target_keep:
                break

    err_ids = {p.trace_id for p in points if p.has_error}
    kept_err = len(err_ids & kept_ids)

    critical_ids = {p.trace_id for p in points if str(p.endpoint_tier or "").lower() == "critical"}
    kept_critical = len(critical_ids & kept_ids)

    incident_capture_latency_ms: Optional[float] = None
    early_incident_retention_pct: Optional[float] = None
    if incident_start_sec is not None:
        kept_secs = sorted(
            int((p.start_ns - t0) // 1_000_000_000)
            for p in points
            if p.trace_id in kept_ids
        )
        first_after = next((s for s in kept_secs if s >= incident_start_sec), None)
        if first_after is not None:
            incident_capture_latency_ms = float((first_after - incident_start_sec) * 1000)

        early_end = incident_start_sec + max(1, early_window_seconds) - 1
        early_total = 0
        early_kept = 0
        for p in points:
            sec = int((p.start_ns - t0) // 1_000_000_000)
            if incident_start_sec <= sec <= early_end:
                early_total += 1
                if p.trace_id in kept_ids:
                    early_kept += 1
        if early_total > 0:
            early_incident_retention_pct = early_kept / early_total * 100.0

    return {
        "budget_pct": budget_pct,
        "kept_traces": len(kept_ids),
        "total_traces": total,
        "kept_traces_pct": len(kept_ids) / total * 100.0,
        "kept_error_traces": kept_err,
        "error_traces": len(err_ids),
        "kept_error_traces_pct": (kept_err / len(err_ids) * 100.0) if err_ids else None,
        "early_incident_retention_pct": early_incident_retention_pct,
        "critical_endpoint_coverage_pct": (kept_critical / len(critical_ids) * 100.0) if critical_ids else None,
        "critical_endpoint_coverage_kept": kept_critical,
        "critical_endpoint_coverage_total": len(critical_ids),
        "incident_capture_latency_ms": incident_capture_latency_ms,
        "selected_error_count": len(selected_error),
        "selected_context_count": len(selected_context),
    }


def write_outputs(out_md: Path, out_csv: Path, rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "budget_pct",
        "kept_traces",
        "total_traces",
        "kept_traces_pct",
        "selected_error_count",
        "selected_context_count",
        "kept_error_traces",
        "error_traces",
        "kept_error_traces_pct",
        "early_incident_retention_pct",
        "critical_endpoint_coverage_pct",
        "critical_endpoint_coverage_kept",
        "critical_endpoint_coverage_total",
        "incident_capture_latency_ms",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    md_lines: List[str] = []
    md_lines.append("# Ranked Adaptive Simulation (One-click Data)")
    md_lines.append("")
    md_lines.append(f"input: {meta['input']}")
    md_lines.append(f"run_id: {meta['run_id']}")
    md_lines.append(f"total_traces: {meta['total_traces']}")
    md_lines.append(f"incident_start_sec: {meta['incident_start_sec']}")
    md_lines.append(f"error_budget_ratio: {meta['error_budget_ratio']}")
    md_lines.append("")
    md_lines.append("| budget | kept traces | selected error/context | kept error traces | early incident retention | critical endpoint coverage | incident_capture_latency_ms |")
    md_lines.append("|---:|---:|---:|---:|---:|---:|---:|")

    for r in rows:
        kept_err_txt = (
            f"{int(r['kept_error_traces'])}/{int(r['error_traces'])} ({float(r['kept_error_traces_pct']):.2f}%)"
            if r["kept_error_traces_pct"] is not None
            else "NA"
        )
        cov_txt = (
            f"{float(r['critical_endpoint_coverage_pct']):.2f} ({int(r['critical_endpoint_coverage_kept'])}/{int(r['critical_endpoint_coverage_total'])})"
            if r["critical_endpoint_coverage_pct"] is not None
            else "NA"
        )
        md_lines.append(
            "| {b:.0f}% | {k}/{t} ({kp:.2f}%) | {se}/{sc} | {ke} | {early} | {cov} | {lat} |".format(
                b=float(r["budget_pct"]),
                k=int(r["kept_traces"]),
                t=int(r["total_traces"]),
                kp=float(r["kept_traces_pct"]),
                se=int(r["selected_error_count"]),
                sc=int(r["selected_context_count"]),
                ke=kept_err_txt,
                early=("NA" if r["early_incident_retention_pct"] is None else f"{float(r['early_incident_retention_pct']):.2f}"),
                cov=cov_txt,
                lat=("NA" if r["incident_capture_latency_ms"] is None else f"{float(r['incident_capture_latency_ms']):.1f}"),
            )
        )

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Ranked adaptive simulation v1 on OTLP one-click data")
    ap.add_argument("--input", required=True, help="Path to line-delimited OTLP json file")
    ap.add_argument("--run-id", default=None, help="Optional run_id filter")
    ap.add_argument("--budgets", default="10,20,30,40,50,60,70")
    ap.add_argument("--error-budget-ratio", type=float, default=0.40)

    ap.add_argument("--short-window-seconds", type=int, default=5)
    ap.add_argument("--long-window-seconds", type=int, default=30)
    ap.add_argument("--error-rate-threshold", type=float, default=0.10)
    ap.add_argument("--latency-p95-threshold-ms", type=float, default=2200.0)

    ap.add_argument("--time-decay-sec", type=float, default=60.0)
    ap.add_argument("--context-window-sec", type=int, default=30)
    ap.add_argument("--early-window-seconds", type=int, default=10)

    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    budgets = [float(x.strip()) for x in args.budgets.split(",") if x.strip()]

    points = load_trace_points(Path(args.input), args.run_id)
    if not points:
        print("No traces loaded for simulation")
        return 1

    incident_start_sec = detect_incident_start_sec(
        points,
        short_window_seconds=args.short_window_seconds,
        long_window_seconds=args.long_window_seconds,
        error_rate_threshold=args.error_rate_threshold,
        latency_p95_threshold_ms=args.latency_p95_threshold_ms,
    )

    rows: List[Dict[str, Any]] = []
    for b in budgets:
        rows.append(
            run_budget_selection(
                points=points,
                budget_pct=b,
                error_budget_ratio=args.error_budget_ratio,
                incident_start_sec=incident_start_sec,
                time_decay_sec=args.time_decay_sec,
                context_window_sec=args.context_window_sec,
                early_window_seconds=args.early_window_seconds,
            )
        )

    write_outputs(
        out_md=Path(args.out_md),
        out_csv=Path(args.out_csv),
        rows=rows,
        meta={
            "input": args.input,
            "run_id": args.run_id or "(none)",
            "total_traces": len(points),
            "incident_start_sec": incident_start_sec,
            "error_budget_ratio": args.error_budget_ratio,
        },
    )

    print(f"out_md={args.out_md}")
    print(f"out_csv={args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
