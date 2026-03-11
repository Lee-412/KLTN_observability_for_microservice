#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


PRESETS: Dict[str, Dict[str, Any]] = {
    "v2": {
        "target_traces_per_sec": 50.0,
        "incident_keep_ratio": 0.80,
        "short_window_seconds": 5,
        "long_window_seconds": 30,
        "alpha_normal": 0.35,
        "alpha_incident": 0.75,
        "exit_incident_seconds": 20,
        "error_rate_threshold": 0.10,
        "latency_p95_threshold_ms": 2000.0,
    },
    "v2-balanced": {
        "target_traces_per_sec": 43.0,
        "incident_keep_ratio": 0.64,
        "short_window_seconds": 5,
        "long_window_seconds": 30,
        "alpha_normal": 0.30,
        "alpha_incident": 0.60,
        "exit_incident_seconds": 20,
        "error_rate_threshold": 0.10,
        "latency_p95_threshold_ms": 2200.0,
    },
    "v2-balanced-lite": {
        "target_traces_per_sec": 40.0,
        "incident_keep_ratio": 0.62,
        "short_window_seconds": 5,
        "long_window_seconds": 30,
        "alpha_normal": 0.30,
        "alpha_incident": 0.58,
        "exit_incident_seconds": 20,
        "error_rate_threshold": 0.10,
        "latency_p95_threshold_ms": 2200.0,
    },
}


ARG_KEY_TO_FLAG: Dict[str, str] = {
    "target_traces_per_sec": "--target-traces-per-sec",
    "min_keep_ratio": "--min-keep-ratio",
    "max_keep_ratio": "--max-keep-ratio",
    "incident_keep_ratio": "--incident-keep-ratio",
    "short_window_seconds": "--short-window-seconds",
    "long_window_seconds": "--long-window-seconds",
    "recompute_seconds": "--recompute-seconds",
    "alpha_normal": "--alpha-normal",
    "alpha_incident": "--alpha-incident",
    "exit_incident_seconds": "--exit-incident-seconds",
    "error_rate_threshold": "--error-rate-threshold",
    "latency_p95_threshold_ms": "--latency-p95-threshold-ms",
    "w_duration": "--w-duration",
    "w_span_count": "--w-span-count",
    "w_error": "--w-error",
    "intercept": "--intercept",
    "early_window_seconds": "--early-window-seconds",
    "delta_keep_min": "--delta-keep-min",
    "hold_seconds": "--hold-seconds",
}


def _arg_provided(flag: str) -> bool:
    for token in sys.argv[1:]:
        if token == flag or token.startswith(flag + "="):
            return True
    return False


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


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def tie_keep(trace_id: str, keep_ratio: float) -> bool:
    if keep_ratio <= 0:
        return False
    if keep_ratio >= 1:
        return True
    try:
        x = int(trace_id[-16:], 16) / float(0xFFFFFFFFFFFFFFFF)
        return x <= keep_ratio
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Stable adaptive simulator (dual-window + incident boost + hysteresis)")
    ap.add_argument("--input", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--preset", choices=sorted(PRESETS.keys()), default=None)

    ap.add_argument("--target-traces-per-sec", type=float, default=50.0)
    ap.add_argument("--min-keep-ratio", type=float, default=0.05)
    ap.add_argument("--max-keep-ratio", type=float, default=1.0)
    ap.add_argument("--incident-keep-ratio", type=float, default=0.80)
    ap.add_argument("--always-keep-errors", action="store_true", default=True)

    ap.add_argument("--short-window-seconds", type=int, default=5)
    ap.add_argument("--long-window-seconds", type=int, default=30)
    ap.add_argument("--recompute-seconds", type=int, default=1)
    ap.add_argument("--alpha-normal", type=float, default=0.35)
    ap.add_argument("--alpha-incident", type=float, default=0.75)
    ap.add_argument("--exit-incident-seconds", type=int, default=20)

    ap.add_argument("--error-rate-threshold", type=float, default=0.10)
    ap.add_argument("--latency-p95-threshold-ms", type=float, default=2000.0)

    ap.add_argument("--w-duration", type=float, default=0.01)
    ap.add_argument("--w-span-count", type=float, default=0.5)
    ap.add_argument("--w-error", type=float, default=10.0)
    ap.add_argument("--intercept", type=float, default=0.0)

    ap.add_argument("--early-window-seconds", type=int, default=10)
    ap.add_argument("--delta-keep-min", type=float, default=0.10)
    ap.add_argument("--hold-seconds", type=int, default=3)

    args = ap.parse_args()

    if args.preset:
        for key, value in PRESETS[args.preset].items():
            flag = ARG_KEY_TO_FLAG.get(key)
            if flag and _arg_provided(flag):
                continue
            setattr(args, key, value)

    traces = load_trace_points(Path(args.input), args.run_id)
    if not traces:
        print("No traces loaded")
        return 1

    t0 = traces[0].start_ns
    by_sec: Dict[int, List[TracePoint]] = defaultdict(list)
    for t in traces:
        sec = int((t.start_ns - t0) // 1_000_000_000)
        by_sec[sec].append(t)

    sec_keys = sorted(by_sec.keys())
    short_hist: deque[tuple[int, float, bool, int]] = deque()  # (sec, score, has_error, duration)
    long_hist: deque[tuple[int, float, bool, int]] = deque()

    kept_ids: set[str] = set()
    threshold_by_sec: Dict[int, float] = {}
    keep_rate_by_sec: Dict[int, float] = {}
    error_rate_short_by_sec: Dict[int, float] = {}
    p95_short_by_sec: Dict[int, float] = {}
    incident_raw_by_sec: Dict[int, bool] = {}

    incident_mode = False
    last_incident_sec: Optional[int] = None

    current_threshold = 5.0
    current_keep_ratio = 1.0

    for sec in range(sec_keys[0], sec_keys[-1] + 1):
        traces_sec = by_sec.get(sec, [])

        scored_sec: List[tuple[TracePoint, float]] = []
        for tr in traces_sec:
            score = (
                float(args.intercept)
                + float(args.w_duration) * float(tr.duration_ms)
                + float(args.w_span_count) * float(tr.span_count)
                + (float(args.w_error) if tr.has_error else 0.0)
            )
            scored_sec.append((tr, score))
            short_hist.append((sec, score, tr.has_error, tr.duration_ms))
            long_hist.append((sec, score, tr.has_error, tr.duration_ms))

        while short_hist and short_hist[0][0] < sec - args.short_window_seconds + 1:
            short_hist.popleft()
        while long_hist and long_hist[0][0] < sec - args.long_window_seconds + 1:
            long_hist.popleft()

        short_scores = [x[1] for x in short_hist]
        long_scores = [x[1] for x in long_hist]
        short_err_rate = (sum(1 for x in short_hist if x[2]) / len(short_hist)) if short_hist else 0.0
        short_p95 = quantile([float(x[3]) for x in short_hist], 0.95) if short_hist else None

        error_rate_short_by_sec[sec] = short_err_rate
        p95_short_by_sec[sec] = short_p95 if short_p95 is not None else 0.0

        incident_raw = short_err_rate > args.error_rate_threshold or (
            short_p95 is not None and short_p95 > args.latency_p95_threshold_ms
        )
        incident_raw_by_sec[sec] = incident_raw

        if incident_raw:
            incident_mode = True
            last_incident_sec = sec
        elif incident_mode and last_incident_sec is not None and (sec - last_incident_sec) >= args.exit_incident_seconds:
            incident_mode = False

        if sec % max(1, args.recompute_seconds) == 0:
            incoming_rate = (len(long_hist) / max(1, args.long_window_seconds)) if args.long_window_seconds > 0 else len(long_hist)
            if incoming_rate <= 0:
                target_keep = 1.0
            else:
                target_keep = args.target_traces_per_sec / incoming_rate

            target_keep = clamp(target_keep, args.min_keep_ratio, args.max_keep_ratio)
            if incident_mode:
                target_keep = max(target_keep, args.incident_keep_ratio)
                alpha = clamp(args.alpha_incident, 0.0, 1.0)
            else:
                alpha = clamp(args.alpha_normal, 0.0, 1.0)

            q = 1.0 - target_keep
            thr_short = quantile(short_scores, q)
            thr_long = quantile(long_scores, q)
            if thr_short is None:
                thr_short = current_threshold
            if thr_long is None:
                thr_long = current_threshold

            current_threshold = alpha * thr_short + (1.0 - alpha) * thr_long
            current_keep_ratio = target_keep

        threshold_by_sec[sec] = current_threshold

        if traces_sec:
            kept_count = 0
            for tr, score in scored_sec:
                keep = False
                if args.always_keep_errors and tr.has_error:
                    keep = True
                elif score > current_threshold:
                    keep = True
                elif score == current_threshold:
                    keep = tie_keep(tr.trace_id, current_keep_ratio)

                if keep:
                    kept_ids.add(tr.trace_id)
                    kept_count += 1

            keep_rate_by_sec[sec] = kept_count / len(traces_sec)
        else:
            keep_rate_by_sec[sec] = keep_rate_by_sec.get(sec - 1, 0.0)

    total = len(traces)
    kept_total = len(kept_ids)

    err_ids = {t.trace_id for t in traces if t.has_error}
    kept_err = len(err_ids & kept_ids)

    critical_ids = {t.trace_id for t in traces if str(t.endpoint_tier or "").lower() == "critical"}

    incident_start = None
    for sec in sorted(incident_raw_by_sec.keys()):
        if incident_raw_by_sec[sec]:
            incident_start = sec
            break

    incident_capture_latency_ms = None
    early_incident_retention_pct = None

    if incident_start is not None:
        pre = [s for s in keep_rate_by_sec.keys() if s < incident_start]
        baseline_keep = (sum(keep_rate_by_sec[s] for s in pre) / len(pre)) if pre else 0.0

        rise_sec = None
        hold = max(1, args.hold_seconds)
        for sec in sorted(keep_rate_by_sec.keys()):
            if sec < incident_start:
                continue
            ok = True
            for i in range(hold):
                if keep_rate_by_sec.get(sec + i, 0.0) < baseline_keep + args.delta_keep_min:
                    ok = False
                    break
            if ok:
                rise_sec = sec
                break
        if rise_sec is not None:
            incident_capture_latency_ms = float((rise_sec - incident_start) * 1000)

        early_sec_end = incident_start + max(1, args.early_window_seconds) - 1
        early_total = 0
        early_kept = 0
        for sec in range(incident_start, early_sec_end + 1):
            items = by_sec.get(sec, [])
            if not items:
                continue
            early_total += len(items)
            early_kept += sum(1 for t in items if t.trace_id in kept_ids)
        if early_total > 0:
            early_incident_retention_pct = early_kept / early_total * 100.0

    thr_vals = [threshold_by_sec[s] for s in sorted(threshold_by_sec.keys())]
    if thr_vals:
        mean_thr = sum(thr_vals) / len(thr_vals)
        std_thr = math.sqrt(sum((x - mean_thr) ** 2 for x in thr_vals) / len(thr_vals)) if len(thr_vals) > 1 else 0.0
        thr_vol = (std_thr / mean_thr * 100.0) if mean_thr != 0 else None
        step_changes: List[float] = []
        for i in range(1, len(thr_vals)):
            prev = thr_vals[i - 1]
            cur = thr_vals[i]
            if prev == 0:
                continue
            step_changes.append(abs(cur - prev) / abs(prev) * 100.0)
        max_step = max(step_changes) if step_changes else None
    else:
        thr_vol = None
        max_step = None

    print("== STABLE ADAPTIVE SIMULATION ==")
    print(f"input: {args.input}")
    print(f"run_id: {args.run_id or '(none)'}")
    print(f"preset: {args.preset or '(custom)'}")
    print(
        "config: "
        f"target_tps={args.target_traces_per_sec}, "
        f"incident_keep={args.incident_keep_ratio}, "
        f"short={args.short_window_seconds}s, long={args.long_window_seconds}s, "
        f"alpha=({args.alpha_normal},{args.alpha_incident}), "
        f"exit_incident={args.exit_incident_seconds}s"
    )
    print(f"total traces: {total}")
    print(f"kept traces : {kept_total}/{total} ({(kept_total/total*100 if total else 0):.2f}%)")
    if err_ids:
        print(f"kept error traces: {kept_err}/{len(err_ids)} ({kept_err/len(err_ids)*100:.2f}%)")
    else:
        print("kept error traces: NA")

    if critical_ids:
        kept_critical = len(critical_ids & kept_ids)
        print(f"critical_endpoint_coverage_pct: {kept_critical/len(critical_ids)*100:.2f} ({kept_critical}/{len(critical_ids)})")
    else:
        print("critical_endpoint_coverage_pct: NA")

    print("\n== METRICS V2 ==")
    print(f"incident_capture_latency_ms: {incident_capture_latency_ms:.1f}" if incident_capture_latency_ms is not None else "incident_capture_latency_ms: NA")
    print(f"early_incident_retention_pct: {early_incident_retention_pct:.2f}" if early_incident_retention_pct is not None else "early_incident_retention_pct: NA")
    print(f"threshold_volatility_pct: {thr_vol:.2f}" if thr_vol is not None else "threshold_volatility_pct: NA")
    print(f"max_step_change_pct: {max_step:.2f}" if max_step is not None else "max_step_change_pct: NA")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
