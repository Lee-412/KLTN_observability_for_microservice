#!/usr/bin/env python3
"""Evaluate adaptive sampling quality directly on TraStrainer paper CSV traces.

This script runs an offline sampling simulation on paper-source traces and reports
metrics aligned with the existing one-click sampler proof style:
- kept traces
- kept spans rate
- kept error traces (derived from incident labels)
- early incident retention

Dataset assumptions:
- trace CSV columns include: TraceID, PodName, OperationName, StartTimeUnixNano, Duration
- label.json contains inject_timestamp / inject_pod
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class TracePoint:
    trace_id: str
    start_ns: int
    duration_ms: float
    span_count: int
    has_error: bool
    root_service: str
    services: frozenset[str]
    service_path: tuple[str, ...]


def strip_pod_to_service(pod_name: str) -> str:
    parts = pod_name.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return pod_name


def iter_trace_csv_rows(trace_dir: Path) -> Iterable[dict[str, str]]:
    for csv_file in sorted(trace_dir.glob("*.csv")):
        with csv_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row


def load_labels(label_file: Path) -> list[tuple[int, str]]:
    obj = json.loads(label_file.read_text(encoding="utf-8"))
    out: list[tuple[int, str]] = []
    if not isinstance(obj, dict):
        return out
    for bucket in obj.values():
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if not isinstance(item, dict):
                continue
            ts_raw = item.get("inject_timestamp")
            pod = str(item.get("inject_pod", "")).strip()
            if not ts_raw or not pod:
                continue
            try:
                ts = int(str(ts_raw).strip())
            except Exception:
                continue
            out.append((ts, strip_pod_to_service(pod)))
    return out


def trace_has_service(pods: set[str], service: str) -> bool:
    for p in pods:
        if strip_pod_to_service(p) == service:
            return True
    return False


def build_trace_points(
    trace_dir: Path,
    labels: list[tuple[int, str]],
    before_sec: int,
    after_sec: int,
) -> tuple[list[TracePoint], list[tuple[int, int]]]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in iter_trace_csv_rows(trace_dir):
        trace_id = str(row.get("TraceID", "")).strip().lower()
        if not trace_id:
            continue
        try:
            start_ns = int(str(row.get("StartTimeUnixNano", "0")).strip())
        except Exception:
            continue

        duration_ns = 0
        try:
            duration_ns = int(str(row.get("Duration", "0")).strip())
        except Exception:
            duration_ns = 0

        pod_name = str(row.get("PodName", "")).strip()

        cur = grouped.get(trace_id)
        if cur is None:
            cur = {
                "start_ns": start_ns,
                "end_ns": start_ns + max(duration_ns, 0),
                "span_count": 0,
                "pods": set(),
                "root_service": "",
                "services": set(),
                "span_seq": [],
            }
            grouped[trace_id] = cur

        cur["start_ns"] = min(cur["start_ns"], start_ns)
        cur["end_ns"] = max(cur["end_ns"], start_ns + max(duration_ns, 0))
        cur["span_count"] += 1
        if pod_name:
            cur["pods"].add(pod_name)
            svc = strip_pod_to_service(pod_name)
            cur["services"].add(svc)
            cur["span_seq"].append((start_ns, svc))
            if not cur["root_service"]:
                cur["root_service"] = svc

    incidents: list[tuple[int, int, str]] = []
    for ts_sec, svc in labels:
        incidents.append((ts_sec - before_sec, ts_sec + after_sec, svc))

    points: list[TracePoint] = []
    early_windows: list[tuple[int, int]] = []

    for s_sec, _e_sec, _svc in incidents:
        early_windows.append((s_sec, s_sec + 10 - 1))

    for tid, g in grouped.items():
        start_sec = g["start_ns"] // 1_000_000_000
        end_sec = g["end_ns"] // 1_000_000_000
        has_error = False
        pods = g["pods"]

        for s_sec, e_sec, svc in incidents:
            overlap = not (end_sec < s_sec or start_sec > e_sec)
            if overlap and trace_has_service(pods, svc):
                has_error = True
                break

        dur_ms = max(0.0, (g["end_ns"] - g["start_ns"]) / 1_000_000.0)
        span_seq = sorted(g.get("span_seq", []), key=lambda x: (x[0], x[1]))
        ordered_services: list[str] = []
        for _ns, svc in span_seq:
            if not ordered_services or ordered_services[-1] != svc:
                ordered_services.append(svc)
        points.append(
            TracePoint(
                trace_id=tid,
                start_ns=g["start_ns"],
                duration_ms=dur_ms,
                span_count=int(g["span_count"]),
                has_error=has_error,
                root_service=str(g["root_service"]),
                services=frozenset(g["services"]),
                service_path=tuple(ordered_services[:12]),
            )
        )

    points.sort(key=lambda x: x.start_ns)
    return points, early_windows


def quantile(values: list[float], q: float) -> Optional[float]:
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


def simulate_with_metrics(
    points: list[TracePoint],
    target_tps: float = 40.0,
    incident_anchor_sec: int | None = None,
) -> tuple[set[str], dict[str, float | None]]:
    if not points:
        return set(), {
            "incident_capture_latency_ms": None,
            "threshold_volatility_pct": None,
            "max_step_change_pct": None,
        }

    t0 = points[0].start_ns
    by_sec: dict[int, list[TracePoint]] = defaultdict(list)
    for t in points:
        sec = int((t.start_ns - t0) // 1_000_000_000)
        by_sec[sec].append(t)

    sec_keys = sorted(by_sec.keys())
    short_hist: deque[tuple[int, float, bool, float]] = deque()
    long_hist: deque[tuple[int, float, bool, float]] = deque()

    kept_ids: set[str] = set()
    incident_mode = False
    last_incident_sec: Optional[int] = None

    current_threshold = 5.0
    current_keep_ratio = 1.0
    threshold_history: list[float] = []
    first_incident_detect_sec: int | None = None

    for sec in range(sec_keys[0], sec_keys[-1] + 1):
        traces_sec = by_sec.get(sec, [])

        scored_sec: list[tuple[TracePoint, float]] = []
        for tr in traces_sec:
            score = 0.01 * tr.duration_ms + 0.5 * tr.span_count + (10.0 if tr.has_error else 0.0)
            scored_sec.append((tr, score))
            short_hist.append((sec, score, tr.has_error, tr.duration_ms))
            long_hist.append((sec, score, tr.has_error, tr.duration_ms))

        while short_hist and short_hist[0][0] < sec - 5 + 1:
            short_hist.popleft()
        while long_hist and long_hist[0][0] < sec - 30 + 1:
            long_hist.popleft()

        short_scores = [x[1] for x in short_hist]
        long_scores = [x[1] for x in long_hist]
        short_err_rate = (sum(1 for x in short_hist if x[2]) / len(short_hist)) if short_hist else 0.0
        short_p95 = quantile([x[3] for x in short_hist], 0.95) if short_hist else None

        incident_raw = short_err_rate > 0.10 or (short_p95 is not None and short_p95 > 2200.0)
        if incident_raw:
            if first_incident_detect_sec is None:
                first_incident_detect_sec = sec
            incident_mode = True
            last_incident_sec = sec
        elif incident_mode and last_incident_sec is not None and (sec - last_incident_sec) >= 20:
            incident_mode = False

        incoming_rate = (len(long_hist) / 30.0) if long_hist else 0.0
        target_keep = 1.0 if incoming_rate <= 0 else (target_tps / incoming_rate)
        target_keep = clamp(target_keep, 0.05, 1.0)

        if incident_mode:
            target_keep = max(target_keep, 0.64)
            alpha = 0.60
        else:
            alpha = 0.30

        q = 1.0 - target_keep
        thr_short = quantile(short_scores, q)
        thr_long = quantile(long_scores, q)
        if thr_short is None:
            thr_short = current_threshold
        if thr_long is None:
            thr_long = current_threshold

        current_threshold = alpha * thr_short + (1.0 - alpha) * thr_long
        current_keep_ratio = target_keep
        threshold_history.append(float(current_threshold))

        for tr, score in scored_sec:
            keep = False
            if tr.has_error:
                keep = True
            elif score > current_threshold:
                keep = True
            elif score == current_threshold:
                keep = tie_keep(tr.trace_id, current_keep_ratio)
            if keep:
                kept_ids.add(tr.trace_id)

    threshold_volatility_pct: float | None = None
    max_step_change_pct: float | None = None
    if threshold_history:
        mean_thr = sum(threshold_history) / len(threshold_history)
        if mean_thr > 0:
            var_thr = sum((x - mean_thr) ** 2 for x in threshold_history) / len(threshold_history)
            std_thr = math.sqrt(var_thr)
            threshold_volatility_pct = std_thr / mean_thr * 100.0

        if len(threshold_history) > 1:
            step_changes: list[float] = []
            for prev, cur in zip(threshold_history, threshold_history[1:]):
                if prev <= 0:
                    continue
                step_changes.append(abs(cur - prev) / prev * 100.0)
            if step_changes:
                max_step_change_pct = max(step_changes)

    incident_capture_latency_ms: float | None = None
    if incident_anchor_sec is not None and first_incident_detect_sec is not None:
        incident_capture_latency_ms = float((first_incident_detect_sec - incident_anchor_sec) * 1000)
        if incident_capture_latency_ms < 0:
            incident_capture_latency_ms = 0.0

    metrics = {
        "incident_capture_latency_ms": incident_capture_latency_ms,
        "threshold_volatility_pct": threshold_volatility_pct,
        "max_step_change_pct": max_step_change_pct,
    }
    return kept_ids, metrics


def simulate(points: list[TracePoint], target_tps: float = 40.0) -> set[str]:
    kept_ids, _ = simulate_with_metrics(points, target_tps=target_tps)
    return kept_ids


def evaluate_dataset(
    dataset_name: str,
    trace_dir: Path,
    label_file: Path,
    before_sec: int,
    after_sec: int,
    target_tps: float,
) -> dict[str, Any]:
    labels = load_labels(label_file)
    points, early_windows = build_trace_points(trace_dir, labels, before_sec, after_sec)
    kept_ids = simulate(points, target_tps=target_tps)

    total_traces = len(points)
    kept_traces = sum(1 for p in points if p.trace_id in kept_ids)
    total_spans = sum(p.span_count for p in points)
    kept_spans = sum(p.span_count for p in points if p.trace_id in kept_ids)

    err_ids = {p.trace_id for p in points if p.has_error}
    kept_err = len(err_ids & kept_ids)

    early_total = 0
    early_kept = 0
    for p in points:
        sec = p.start_ns // 1_000_000_000
        in_early = any(s <= sec <= e for s, e in early_windows)
        if in_early:
            early_total += 1
            if p.trace_id in kept_ids:
                early_kept += 1

    return {
        "dataset": dataset_name,
        "total_traces": total_traces,
        "kept_traces": kept_traces,
        "kept_traces_pct": (kept_traces / total_traces * 100.0) if total_traces else 0.0,
        "total_spans": total_spans,
        "kept_spans": kept_spans,
        "kept_spans_rate_pct": (kept_spans / total_spans * 100.0) if total_spans else 0.0,
        "error_traces": len(err_ids),
        "kept_error_traces": kept_err,
        "kept_error_traces_pct": (kept_err / len(err_ids) * 100.0) if err_ids else None,
        "early_incident_retention_pct": (early_kept / early_total * 100.0) if early_total else None,
    }


def write_markdown_report(out_path: Path, rows: list[dict[str, Any]]) -> None:
    lines: list[str] = []
    lines.append("# Paper Sampling Evaluation")
    lines.append("")
    lines.append(f"generated_at: {datetime.now().isoformat()}")
    lines.append("")
    lines.append("| dataset | kept traces | kept spans rate | kept error traces | early incident retention |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in rows:
        kept = f"{r['kept_traces']}/{r['total_traces']} ({r['kept_traces_pct']:.2f}%)"
        spans = f"{r['kept_spans']}/{r['total_spans']} ({r['kept_spans_rate_pct']:.2f}%)"
        if r["kept_error_traces_pct"] is None:
            err = "NA"
        else:
            err = f"{r['kept_error_traces']}/{r['error_traces']} ({r['kept_error_traces_pct']:.2f}%)"
        if r["early_incident_retention_pct"] is None:
            early = "NA"
        else:
            early = f"{r['early_incident_retention_pct']:.2f}%"
        lines.append(f"| {r['dataset']} | {kept} | {spans} | {err} | {early} |")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_csv(out_path: Path, rows: list[dict[str, Any]]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "dataset",
        "total_traces",
        "kept_traces",
        "kept_traces_pct",
        "total_spans",
        "kept_spans",
        "kept_spans_rate_pct",
        "error_traces",
        "kept_error_traces",
        "kept_error_traces_pct",
        "early_incident_retention_pct",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate sampling quality on TraStrainer paper datasets")
    ap.add_argument(
        "--trastrainer-root",
        default="data/paper-source/TraStrainer",
        help="Path to TraStrainer root under SignozCore",
    )
    ap.add_argument("--before-sec", type=int, default=300)
    ap.add_argument("--after-sec", type=int, default=600)
    ap.add_argument("--target-tps", type=float, default=0.2, help="Lower target to force meaningful keep/drop on sparse paper traces")
    ap.add_argument("--tag", default=datetime.now().strftime("%Y%m%d-%H%M%S"))
    args = ap.parse_args()

    root = Path(args.trastrainer_root).resolve()
    datasets = [
        (
            "train-ticket",
            root / "data/dataset/train_ticket/test/trace",
            root / "data/dataset/train_ticket/test/label.json",
        ),
        (
            "hipster-batch1",
            root / "data/dataset/hipster/batches/batch1/trace",
            root / "data/dataset/hipster/batches/batch1/label.json",
        ),
        (
            "hipster-batch2",
            root / "data/dataset/hipster/batches/batch2/trace",
            root / "data/dataset/hipster/batches/batch2/label.json",
        ),
    ]

    rows: list[dict[str, Any]] = []
    for name, tdir, lfile in datasets:
        if not tdir.exists() or not lfile.exists():
            continue
        rows.append(
            evaluate_dataset(
                name,
                tdir,
                lfile,
                args.before_sec,
                args.after_sec,
                args.target_tps,
            )
        )

    reports_dir = Path("reports/compare").resolve()
    csv_out = reports_dir / f"paper-sampling-eval-{args.tag}.csv"
    md_out = reports_dir / f"paper-sampling-eval-{args.tag}.md"

    write_csv(csv_out, rows)
    write_markdown_report(md_out, rows)

    print(f"csv={csv_out}")
    print(f"md={md_out}")
    for r in rows:
        err = "NA" if r["kept_error_traces_pct"] is None else f"{r['kept_error_traces_pct']:.2f}%"
        early = "NA" if r["early_incident_retention_pct"] is None else f"{r['early_incident_retention_pct']:.2f}%"
        print(
            f"[{r['dataset']}] kept_traces={r['kept_traces_pct']:.2f}% "
            f"kept_spans={r['kept_spans_rate_pct']:.2f}% kept_error={err} early={early}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
