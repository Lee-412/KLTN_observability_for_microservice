#!/usr/bin/env python3
"""Two-phase paper benchmark runner: Sampling -> MicroRank RCA (multi-budget).

What this script does on every run:

Phase 1 (per dataset x budget):
1) Run current adaptive sampling simulation on paper traces.
2) Output sampled trace list + sampled CSV trace files.
3) Output a phase1 summary text (combined-style) with keep/error/coverage metrics.

Phase 2 (per dataset x budget):
1) Feed phase1 sampled traces into MicroRank engine.
2) Run adapter + evaluator + paper-table pipeline.
3) Collect RCA metrics into a benchmark CSV (paper-table style, for reporting).
4) Build compare-to-paper markdown (MicroRank-only) for quick presentation.

Default budgets are paper-aligned: 0.1%, 1.0%, 2.5%.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import subprocess
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import evaluate_paper_sampling as eps


@dataclass
class DatasetSpec:
    name: str
    trace_dir: Path
    label_file: Path
    base_scenario_file: Path
    baseline_summary_file: Path


def _run(cmd: list[str], cwd: Path) -> None:
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _safe_slug(text: str) -> str:
    return (
        text.strip()
        .lower()
        .replace("%", "pct")
        .replace(".", "p")
        .replace(" ", "-")
    )


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _materialize_sampled_trace_dir(src_trace_dir: Path, out_trace_dir: Path, kept_ids: set[str]) -> tuple[int, int]:
    out_trace_dir.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    kept_rows = 0

    for src_csv in sorted(src_trace_dir.glob("*.csv")):
        dst_csv = out_trace_dir / src_csv.name
        with src_csv.open("r", encoding="utf-8") as rf, dst_csv.open("w", encoding="utf-8", newline="") as wf:
            reader = csv.DictReader(rf)
            if reader.fieldnames is None:
                continue
            writer = csv.DictWriter(wf, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                total_rows += 1
                tid = str(row.get("TraceID", "")).strip().lower()
                if tid in kept_ids:
                    writer.writerow(row)
                    kept_rows += 1

    return total_rows, kept_rows


def _incident_services_from_labels(label_file: Path) -> set[str]:
    labels = eps.load_labels(label_file)
    return {svc for _ts, svc in labels}


def _trace_has_incident_service(p: eps.TracePoint, incident_services: set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)


def _incident_service_coverage(points: list[eps.TracePoint], kept_ids: set[str], incident_services: set[str]) -> tuple[int, int, float | None]:
    if not incident_services:
        return 0, 0, None

    base_ids = {p.trace_id for p in points if _trace_has_incident_service(p, incident_services)}
    if not base_ids:
        return 0, 0, None

    kept = len(base_ids & kept_ids)
    pct = (kept / len(base_ids)) * 100.0
    return kept, len(base_ids), pct


def _parse_operation_name(raw: str) -> str:
    v = str(raw or "").strip()
    if not v:
        return "unknown"
    if v.startswith("/"):
        return v
    if " " in v:
        parts = [x for x in v.split(" ") if x]
        last = parts[-1]
        if last.startswith("/"):
            return last
    return v


def _endpoint_retention(trace_dir: Path, kept_ids: set[str]) -> list[tuple[str, int, int, float]]:
    # Best-effort endpoint retention table from OperationName.
    total_by_ep: dict[str, int] = {}
    kept_by_ep: dict[str, int] = {}
    seen_trace_ep: set[tuple[str, str]] = set()

    for row in eps.iter_trace_csv_rows(trace_dir):
        tid = str(row.get("TraceID", "")).strip().lower()
        if not tid:
            continue
        op = _parse_operation_name(str(row.get("OperationName", "")))
        key = (tid, op)
        if key in seen_trace_ep:
            continue
        seen_trace_ep.add(key)

        total_by_ep[op] = total_by_ep.get(op, 0) + 1
        if tid in kept_ids:
            kept_by_ep[op] = kept_by_ep.get(op, 0) + 1

    rows: list[tuple[str, int, int, float]] = []
    for ep, total in total_by_ep.items():
        kept = kept_by_ep.get(ep, 0)
        pct = (kept / total * 100.0) if total else 0.0
        rows.append((ep, kept, total, pct))

    rows.sort(key=lambda x: (-x[3], -x[2], x[0]))
    return rows[:12]


def _format_phase1_combined(
    dataset: str,
    budget_pct: float,
    target_tps: float,
    points: list[eps.TracePoint],
    kept_ids: set[str],
    sampled_trace_dir: Path,
    kept_ids_file: Path,
    total_rows: int,
    kept_rows: int,
    err_total: int,
    err_kept: int,
    early_retention_pct: float | None,
    incident_cov_kept: int,
    incident_cov_total: int,
    incident_cov_pct: float | None,
    incident_capture_latency_ms: float | None,
    threshold_volatility_pct: float | None,
    max_step_change_pct: float | None,
    endpoint_rows: list[tuple[str, int, int, float]],
) -> str:
    total_traces = len(points)
    kept_traces = len(kept_ids)
    total_spans = sum(p.span_count for p in points)
    kept_spans = sum(p.span_count for p in points if p.trace_id in kept_ids)

    kept_trace_pct = (kept_traces / total_traces * 100.0) if total_traces else 0.0
    kept_span_pct = (kept_spans / total_spans * 100.0) if total_spans else 0.0
    err_pct = (err_kept / err_total * 100.0) if err_total else None

    lines = []
    lines.append("=== PHASE 1 SAMPLING REPORT ===")
    lines.append(f"dataset: {dataset}")
    lines.append(f"budget_pct: {budget_pct}")
    lines.append(f"calibrated_target_tps: {target_tps:.6f}")
    lines.append(f"sampled_trace_dir: {sampled_trace_dir}")
    lines.append(f"kept_trace_ids_file: {kept_ids_file}")
    lines.append("")

    lines.append("=== QUICK SUMMARY (KEEP + ERROR + COVERAGE) ===")
    lines.append(f"kept traces    : {kept_traces}/{total_traces} ({kept_trace_pct:.2f}%)")
    lines.append(f"dropped traces : {total_traces - kept_traces}/{total_traces} ({100.0 - kept_trace_pct:.2f}%)")
    lines.append(f"kept spans rate: {kept_spans}/{total_spans} ({kept_span_pct:.2f}%)")
    lines.append(f"sampled csv rows: {kept_rows}/{total_rows} ({(kept_rows / total_rows * 100.0) if total_rows else 0.0:.2f}%)")

    if err_pct is None:
        lines.append("kept error traces    : NA")
    else:
        lines.append(f"kept error traces    : {err_kept}/{err_total} ({err_pct:.2f}%)")

    if incident_capture_latency_ms is None:
        lines.append("incident_capture_latency_ms: NA")
    else:
        lines.append(f"incident_capture_latency_ms: {incident_capture_latency_ms:.1f}")

    if early_retention_pct is None:
        lines.append("early_incident_retention_pct: NA")
    else:
        lines.append(f"early_incident_retention_pct: {early_retention_pct:.2f}")

    if incident_cov_pct is None:
        lines.append("critical_endpoint_coverage_pct: NA")
    else:
        lines.append(
            f"critical_endpoint_coverage_pct: {incident_cov_pct:.2f} ({incident_cov_kept}/{incident_cov_total})"
        )

    if threshold_volatility_pct is None:
        lines.append("threshold_volatility_pct: NA")
    else:
        lines.append(f"threshold_volatility_pct: {threshold_volatility_pct:.2f}")

    if max_step_change_pct is None:
        lines.append("max_step_change_pct: NA")
    else:
        lines.append(f"max_step_change_pct: {max_step_change_pct:.2f}")

    lines.append("")
    lines.append("== KEPT TRACE ENDPOINTS (TOP, SAMPLED/BASELINE) ==")
    lines.append("endpoint,kept,baseline,kept_pct")
    if endpoint_rows:
        for ep, kept, total, pct in endpoint_rows:
            lines.append(f"{ep},{kept},{total},{pct:.2f}")
    else:
        lines.append("NA,0,0,0.00")

    return "\n".join(lines) + "\n"


def _avg_trace_rate(points: list[eps.TracePoint]) -> float:
    if not points:
        return 0.0
    t0 = min(p.start_ns for p in points)
    t1 = max(p.start_ns for p in points)
    span_s = max(1.0, (t1 - t0) / 1_000_000_000.0)
    return len(points) / span_s


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _ranked_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    error_budget_ratio: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
) -> set[str]:
    """Select kept trace IDs by ranked error/context split.

    Policy:
    - Keep exactly budget trace count.
    - Reserve error_budget_ratio of kept slots for ranked error traces.
    - Fill the remaining slots with ranked non-error related traces.
    """
    total = len(points)
    if total == 0:
        return set()

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    error_quota = int(round(target_count * max(0.0, min(1.0, error_budget_ratio))))
    error_quota = max(0, min(target_count, error_quota))
    context_quota = target_count - error_quota

    dur_lo = min(p.duration_ms for p in points)
    dur_hi = max(p.duration_ms for p in points)
    span_lo = min(p.span_count for p in points)
    span_hi = max(p.span_count for p in points)
    t0_sec = min(p.start_ns for p in points) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        x = (v - lo) / (hi - lo)
        return max(0.0, min(1.0, x))

    def _time_score(p: eps.TracePoint) -> float:
        if incident_anchor_sec is None:
            return 0.5
        sec = p.start_ns // 1_000_000_000 - t0_sec
        return math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    def _severity_score(p: eps.TracePoint) -> float:
        d = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        s = _norm(float(p.span_count), float(span_lo), float(span_hi))
        return 0.5 * d + 0.5 * s

    def _service_score(p: eps.TracePoint) -> float:
        return 1.0 if _trace_has_incident_service(p, incident_services) else 0.4

    def _context_score(p: eps.TracePoint) -> float:
        return _norm(float(p.span_count), float(span_lo), float(span_hi))

    errors = [p for p in points if p.has_error]
    non_errors = [p for p in points if not p.has_error]

    ranked_errors = sorted(
        errors,
        key=lambda p: (
            0.35 * _time_score(p)
            + 0.30 * _severity_score(p)
            + 0.20 * _service_score(p)
            + 0.15 * _context_score(p),
            p.duration_ms,
            p.span_count,
            p.trace_id,
        ),
        reverse=True,
    )
    selected_errors = ranked_errors[: min(error_quota, len(ranked_errors))]

    ranked_context = sorted(
        non_errors,
        key=lambda p: (
            0.50 * _time_score(p)
            + 0.30 * _service_score(p)
            + 0.20 * _severity_score(p),
            p.duration_ms,
            p.span_count,
            p.trace_id,
        ),
        reverse=True,
    )
    selected_context = ranked_context[: min(context_quota, len(ranked_context))]

    kept_ids = {p.trace_id for p in selected_errors}
    kept_ids.update(p.trace_id for p in selected_context)

    if len(kept_ids) < target_count:
        fillers = ranked_errors[min(error_quota, len(ranked_errors)) :] + ranked_context[min(context_quota, len(ranked_context)) :]
        for p in fillers:
            if p.trace_id in kept_ids:
                continue
            kept_ids.add(p.trace_id)
            if len(kept_ids) >= target_count:
                break

    return kept_ids


def _ranked_v11_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    error_quota_ratio: float | None = None,
) -> set[str]:
    """Select kept trace IDs by V1.1 formula.

    Final = Base * System_Multiplier * Novelty_Penalty * incident_modifier

    where:
    - Base = 0.40*severity + 0.30*time + 0.30*context
    - System_Multiplier = clamp(1 + 2*service_relevance*resource_pressure, 1, 3)
    - Novelty_Penalty = max(0.2, 1/sqrt(count_signature_60s))
    - incident_modifier = 1.0 (normal) or 0.7 (recovery)
    """
    total = len(points)
    if total == 0:
        return set()

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    dur_lo = min(p.duration_ms for p in points)
    dur_hi = max(p.duration_ms for p in points)
    span_lo = min(p.span_count for p in points)
    span_hi = max(p.span_count for p in points)
    t0_sec = min(p.start_ns for p in points) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        x = (v - lo) / (hi - lo)
        return max(0.0, min(1.0, x))

    def _time_score(p: eps.TracePoint) -> float:
        if incident_anchor_sec is None:
            return 0.5
        sec = p.start_ns // 1_000_000_000 - t0_sec
        return math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    def _severity_score(p: eps.TracePoint) -> float:
        d = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        s = _norm(float(p.span_count), float(span_lo), float(span_hi))
        return 0.5 * d + 0.5 * s

    def _context_completeness(p: eps.TracePoint) -> float:
        return _norm(float(p.span_count), float(span_lo), float(span_hi))

    def _service_relevance(p: eps.TracePoint) -> float:
        return 1.0 if _trace_has_incident_service(p, incident_services) else 0.0

    # Approximate resource pressure by recent service-local error intensity and latency pressure.
    by_sec: dict[int, list[eps.TracePoint]] = defaultdict(list)
    service_durations: dict[str, list[float]] = defaultdict(list)
    for p in points:
        sec = p.start_ns // 1_000_000_000 - t0_sec
        by_sec[int(sec)].append(p)
        for svc in p.services:
            service_durations[svc].append(float(p.duration_ms))

    sec_lo = min(by_sec.keys())
    sec_hi = max(by_sec.keys())

    service_p95: dict[str, float] = {}
    for svc, vals in service_durations.items():
        if not vals:
            service_p95[svc] = 0.0
            continue
        xs = sorted(vals)
        idx = int(round((len(xs) - 1) * 0.95))
        idx = max(0, min(len(xs) - 1, idx))
        service_p95[svc] = float(xs[idx])

    pressure_map: dict[tuple[int, str], float] = {}
    window: deque[tuple[int, frozenset[str], bool, float]] = deque()

    for sec in range(sec_lo, sec_hi + 1):
        for p in by_sec.get(sec, []):
            window.append((sec, p.services, p.has_error, float(p.duration_ms)))

        while window and window[0][0] < sec - 60 + 1:
            window.popleft()

        service_stats: dict[str, tuple[int, int, float]] = defaultdict(lambda: (0, 0, 0.0))
        for _s, svcs, has_err, dur in window:
            for svc in svcs:
                n, e, dsum = service_stats[svc]
                service_stats[svc] = (n + 1, e + (1 if has_err else 0), dsum + dur)

        for svc, (n, e, dsum) in service_stats.items():
            if n <= 0:
                pressure_map[(sec, svc)] = 0.0
                continue
            err_rate = e / n
            avg_dur = dsum / n
            p95 = max(1.0, service_p95.get(svc, 1.0))
            latency_pressure = _clamp(avg_dur / p95, 0.0, 1.0)
            pressure_map[(sec, svc)] = _clamp(0.7 * err_rate + 0.3 * latency_pressure, 0.0, 1.0)

    def _resource_pressure(p: eps.TracePoint) -> float:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        if not p.services:
            return 0.0
        return max(pressure_map.get((sec, svc), 0.0) for svc in p.services)

    # Novelty signature count in a 60s recent window.
    by_sec_local: dict[int, list[eps.TracePoint]] = defaultdict(list)
    for p in points:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        by_sec_local[sec].append(p)

    def _dur_bucket(d_ms: float) -> str:
        if d_ms < 100:
            return "lt100"
        if d_ms < 500:
            return "100_500"
        if d_ms < 2000:
            return "500_2000"
        return "ge2000"

    def _span_bucket(sc: int) -> str:
        if sc <= 3:
            return "s1"
        if sc <= 8:
            return "s2"
        if sc <= 20:
            return "s3"
        return "s4"

    sec_hist: deque[tuple[int, str]] = deque()
    sec_counts: dict[str, int] = defaultdict(int)
    novelty_count_by_id: dict[str, int] = {}
    sec_states: dict[int, str] = {}

    in_incident = False
    last_incident_sec: int | None = None
    for sec in range(sec_lo, sec_hi + 1):
        traces_sec = by_sec_local.get(sec, [])
        if traces_sec:
            err_rate = sum(1 for p in traces_sec if p.has_error) / len(traces_sec)
            if err_rate > 0.10:
                in_incident = True
                last_incident_sec = sec
            elif in_incident and last_incident_sec is not None and (sec - last_incident_sec) >= 20:
                in_incident = False

        if in_incident:
            state = "incident"
        elif last_incident_sec is not None and (sec - last_incident_sec) <= 30:
            state = "recovery"
        else:
            state = "normal"
        sec_states[sec] = state

        while sec_hist and sec_hist[0][0] < sec - 60 + 1:
            _old_sec, old_sig = sec_hist.popleft()
            sec_counts[old_sig] -= 1
            if sec_counts[old_sig] <= 0:
                sec_counts.pop(old_sig, None)

        for p in traces_sec:
            status_class = "error" if p.has_error else "ok"
            focus_services = sorted(set(p.services) & incident_services)
            if focus_services:
                focus_service = focus_services[0]
            elif p.root_service:
                focus_service = p.root_service
            elif p.services:
                focus_service = sorted(p.services)[0]
            else:
                focus_service = "unknown"
            sig = f"{focus_service}|{status_class}|{_dur_bucket(p.duration_ms)}|{_span_bucket(p.span_count)}"
            cnt = sec_counts.get(sig, 0) + 1
            novelty_count_by_id[p.trace_id] = cnt
            sec_hist.append((sec, sig))
            sec_counts[sig] = cnt

    ranked: list[tuple[float, eps.TracePoint]] = []
    for p in points:
        sev = _severity_score(p)
        tsc = _time_score(p)
        ctx = _context_completeness(p)
        base = 0.40 * sev + 0.30 * tsc + 0.30 * ctx

        svc_rel = _service_relevance(p)
        press = _resource_pressure(p)
        sys_mul = _clamp(1.0 + 2.0 * svc_rel * press, 1.0, 3.0)

        cnt = max(1, novelty_count_by_id.get(p.trace_id, 1))
        novelty_penalty = max(0.2, 1.0 / math.sqrt(float(cnt)))

        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        state = sec_states.get(sec, "normal")
        incident_modifier = 0.7 if state == "recovery" else 1.0

        final_score = base * sys_mul * novelty_penalty * incident_modifier
        ranked.append((final_score, p))

    ranked.sort(
        key=lambda x: (x[0], x[1].has_error, x[1].duration_ms, x[1].span_count, x[1].trace_id),
        reverse=True,
    )

    if error_quota_ratio is None:
        return {p.trace_id for _sc, p in ranked[:target_count]}

    ratio = _clamp(float(error_quota_ratio), 0.0, 1.0)
    error_quota = int(round(target_count * ratio))
    error_quota = max(0, min(target_count, error_quota))
    normal_quota = target_count - error_quota

    ranked_errors = [p for _sc, p in ranked if p.has_error]
    ranked_normals = [p for _sc, p in ranked if not p.has_error]

    kept_ids: set[str] = set()

    for p in ranked_errors[:error_quota]:
        kept_ids.add(p.trace_id)
    for p in ranked_normals[:normal_quota]:
        kept_ids.add(p.trace_id)

    if len(ranked_errors) < error_quota:
        extra = error_quota - len(ranked_errors)
        for p in ranked_normals[normal_quota: normal_quota + extra]:
            kept_ids.add(p.trace_id)

    if len(ranked_normals) < normal_quota:
        extra = normal_quota - len(ranked_normals)
        for p in ranked_errors[error_quota: error_quota + extra]:
            kept_ids.add(p.trace_id)

    if len(kept_ids) < target_count:
        for _sc, p in ranked:
            if p.trace_id in kept_ids:
                continue
            kept_ids.add(p.trace_id)
            if len(kept_ids) >= target_count:
                break

    return kept_ids


def _ranked_v11_stochastic_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    seed: int,
) -> set[str]:
    """Select kept trace IDs via calibrated stochastic Bernoulli sampling.

    Steps:
    1) Reuse V1.1 final score as base sampling score.
    2) Normalize to [0, 1] by max score.
    3) Calibrate probabilities so expected kept count ~= target budget.
    4) Draw random number per trace and keep if rand < calibrated_prob.
    """
    total = len(points)
    if total == 0:
        return set()

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    dur_lo = min(p.duration_ms for p in points)
    dur_hi = max(p.duration_ms for p in points)
    span_lo = min(p.span_count for p in points)
    span_hi = max(p.span_count for p in points)
    t0_sec = min(p.start_ns for p in points) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        x = (v - lo) / (hi - lo)
        return max(0.0, min(1.0, x))

    def _time_score(p: eps.TracePoint) -> float:
        if incident_anchor_sec is None:
            return 0.5
        sec = p.start_ns // 1_000_000_000 - t0_sec
        return math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    def _severity_score(p: eps.TracePoint) -> float:
        d = _norm(float(p.duration_ms), float(dur_lo), float(dur_hi))
        s = _norm(float(p.span_count), float(span_lo), float(span_hi))
        return 0.5 * d + 0.5 * s

    def _context_completeness(p: eps.TracePoint) -> float:
        return _norm(float(p.span_count), float(span_lo), float(span_hi))

    def _service_relevance(p: eps.TracePoint) -> float:
        return 1.0 if _trace_has_incident_service(p, incident_services) else 0.0

    by_sec: dict[int, list[eps.TracePoint]] = defaultdict(list)
    service_durations: dict[str, list[float]] = defaultdict(list)
    for p in points:
        sec = p.start_ns // 1_000_000_000 - t0_sec
        by_sec[int(sec)].append(p)
        for svc in p.services:
            service_durations[svc].append(float(p.duration_ms))

    sec_lo = min(by_sec.keys())
    sec_hi = max(by_sec.keys())

    service_p95: dict[str, float] = {}
    for svc, vals in service_durations.items():
        if not vals:
            service_p95[svc] = 0.0
            continue
        xs = sorted(vals)
        idx = int(round((len(xs) - 1) * 0.95))
        idx = max(0, min(len(xs) - 1, idx))
        service_p95[svc] = float(xs[idx])

    pressure_map: dict[tuple[int, str], float] = {}
    window: deque[tuple[int, frozenset[str], bool, float]] = deque()

    for sec in range(sec_lo, sec_hi + 1):
        for p in by_sec.get(sec, []):
            window.append((sec, p.services, p.has_error, float(p.duration_ms)))

        while window and window[0][0] < sec - 60 + 1:
            window.popleft()

        service_stats: dict[str, tuple[int, int, float]] = defaultdict(lambda: (0, 0, 0.0))
        for _s, svcs, has_err, dur in window:
            for svc in svcs:
                n, e, dsum = service_stats[svc]
                service_stats[svc] = (n + 1, e + (1 if has_err else 0), dsum + dur)

        for svc, (n, e, dsum) in service_stats.items():
            if n <= 0:
                pressure_map[(sec, svc)] = 0.0
                continue
            err_rate = e / n
            avg_dur = dsum / n
            p95 = max(1.0, service_p95.get(svc, 1.0))
            latency_pressure = _clamp(avg_dur / p95, 0.0, 1.0)
            pressure_map[(sec, svc)] = _clamp(0.7 * err_rate + 0.3 * latency_pressure, 0.0, 1.0)

    def _resource_pressure(p: eps.TracePoint) -> float:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        if not p.services:
            return 0.0
        return max(pressure_map.get((sec, svc), 0.0) for svc in p.services)

    by_sec_local: dict[int, list[eps.TracePoint]] = defaultdict(list)
    for p in points:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        by_sec_local[sec].append(p)

    def _dur_bucket(d_ms: float) -> str:
        if d_ms < 100:
            return "lt100"
        if d_ms < 500:
            return "100_500"
        if d_ms < 2000:
            return "500_2000"
        return "ge2000"

    def _span_bucket(sc: int) -> str:
        if sc <= 3:
            return "s1"
        if sc <= 8:
            return "s2"
        if sc <= 20:
            return "s3"
        return "s4"

    sec_hist: deque[tuple[int, str]] = deque()
    sec_counts: dict[str, int] = defaultdict(int)
    novelty_count_by_id: dict[str, int] = {}
    sec_states: dict[int, str] = {}

    in_incident = False
    last_incident_sec: int | None = None
    for sec in range(sec_lo, sec_hi + 1):
        traces_sec = by_sec_local.get(sec, [])
        if traces_sec:
            err_rate = sum(1 for p in traces_sec if p.has_error) / len(traces_sec)
            if err_rate > 0.10:
                in_incident = True
                last_incident_sec = sec
            elif in_incident and last_incident_sec is not None and (sec - last_incident_sec) >= 20:
                in_incident = False

        if in_incident:
            state = "incident"
        elif last_incident_sec is not None and (sec - last_incident_sec) <= 30:
            state = "recovery"
        else:
            state = "normal"
        sec_states[sec] = state

        while sec_hist and sec_hist[0][0] < sec - 60 + 1:
            _old_sec, old_sig = sec_hist.popleft()
            sec_counts[old_sig] -= 1
            if sec_counts[old_sig] <= 0:
                sec_counts.pop(old_sig, None)

        for p in traces_sec:
            status_class = "error" if p.has_error else "ok"
            focus_services = sorted(set(p.services) & incident_services)
            if focus_services:
                focus_service = focus_services[0]
            elif p.root_service:
                focus_service = p.root_service
            elif p.services:
                focus_service = sorted(p.services)[0]
            else:
                focus_service = "unknown"
            sig = f"{focus_service}|{status_class}|{_dur_bucket(p.duration_ms)}|{_span_bucket(p.span_count)}"
            cnt = sec_counts.get(sig, 0) + 1
            novelty_count_by_id[p.trace_id] = cnt
            sec_hist.append((sec, sig))
            sec_counts[sig] = cnt

    scored: list[tuple[str, float]] = []
    for p in points:
        sev = _severity_score(p)
        tsc = _time_score(p)
        ctx = _context_completeness(p)
        base = 0.40 * sev + 0.30 * tsc + 0.30 * ctx

        svc_rel = _service_relevance(p)
        press = _resource_pressure(p)
        sys_mul = _clamp(1.0 + 2.0 * svc_rel * press, 1.0, 3.0)

        cnt = max(1, novelty_count_by_id.get(p.trace_id, 1))
        novelty_penalty = max(0.2, 1.0 / math.sqrt(float(cnt)))

        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        state = sec_states.get(sec, "normal")
        incident_modifier = 0.7 if state == "recovery" else 1.0

        final_score = base * sys_mul * novelty_penalty * incident_modifier
        scored.append((p.trace_id, final_score))

    max_score = max(sc for _tid, sc in scored) if scored else 0.0
    if max_score <= 0.0:
        return set()

    probs = {tid: (sc / max_score) for tid, sc in scored}
    expected_sum = sum(probs.values())
    if expected_sum <= 0.0:
        return set()

    factor = target_count / expected_sum
    calibrated = {tid: min(1.0, probs[tid] * factor) for tid in probs}

    rng = random.Random(seed)
    kept_ids: set[str] = set()
    for p in points:
        if rng.random() < calibrated.get(p.trace_id, 0.0):
            kept_ids.add(p.trace_id)

    return kept_ids


def _ranked_v11_hybrid_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    core_ratio: float,
    edge_pool_multiplier: float,
    seed: int,
) -> set[str]:
    """Hybrid selector: deterministic core + stochastic edge exploration.

    - Core: top-ranked v11 traces for stable signal.
    - Edge: stochastic sampling from a near-threshold pool for diversity.
    - Always top-up/trim to hit exact target_count.
    """
    total = len(points)
    if total == 0:
        return set()

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    core_ratio = _clamp(float(core_ratio), 0.0, 1.0)
    edge_pool_multiplier = max(1.0, float(edge_pool_multiplier))

    core_count = int(round(target_count * core_ratio))
    core_count = max(0, min(target_count, core_count))

    core_ids: set[str] = set()
    if core_count > 0:
        core_budget_pct = (core_count / total) * 100.0
        core_ids = _ranked_v11_select_ids(
            points=points,
            budget_pct=core_budget_pct,
            incident_services=incident_services,
            incident_anchor_sec=incident_anchor_sec,
            error_quota_ratio=None,
        )

    if len(core_ids) > core_count:
        core_points = [p for p in points if p.trace_id in core_ids]
        trim_budget_pct = (core_count / len(core_points) * 100.0) if core_points else 0.0
        core_ids = _ranked_v11_select_ids(
            points=core_points,
            budget_pct=trim_budget_pct,
            incident_services=incident_services,
            incident_anchor_sec=incident_anchor_sec,
            error_quota_ratio=None,
        )

    kept_ids = set(core_ids)
    remaining_target = target_count - len(kept_ids)
    if remaining_target <= 0:
        return kept_ids

    remaining_points = [p for p in points if p.trace_id not in kept_ids]
    if not remaining_points:
        return kept_ids

    edge_pool_count = int(round(target_count * edge_pool_multiplier))
    edge_pool_count = max(remaining_target, min(len(remaining_points), edge_pool_count))

    edge_pool_budget_pct = (edge_pool_count / len(remaining_points)) * 100.0
    edge_pool_ids = _ranked_v11_select_ids(
        points=remaining_points,
        budget_pct=edge_pool_budget_pct,
        incident_services=incident_services,
        incident_anchor_sec=incident_anchor_sec,
        error_quota_ratio=None,
    )
    edge_pool_points = [p for p in remaining_points if p.trace_id in edge_pool_ids]
    if not edge_pool_points:
        edge_pool_points = remaining_points

    edge_budget_pct = (remaining_target / len(edge_pool_points)) * 100.0
    edge_ids = _ranked_v11_stochastic_select_ids(
        points=edge_pool_points,
        budget_pct=edge_budget_pct,
        incident_services=incident_services,
        incident_anchor_sec=incident_anchor_sec,
        seed=seed,
    )
    kept_ids.update(edge_ids)

    if len(kept_ids) < target_count:
        needed = target_count - len(kept_ids)
        fill_points = [p for p in edge_pool_points if p.trace_id not in kept_ids]
        if fill_points and needed > 0:
            fill_budget_pct = (needed / len(fill_points)) * 100.0
            kept_ids.update(
                _ranked_v11_select_ids(
                    points=fill_points,
                    budget_pct=fill_budget_pct,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    error_quota_ratio=None,
                )
            )

    if len(kept_ids) < target_count:
        needed = target_count - len(kept_ids)
        fill_points = [p for p in points if p.trace_id not in kept_ids]
        if fill_points and needed > 0:
            fill_budget_pct = (needed / len(fill_points)) * 100.0
            kept_ids.update(
                _ranked_v11_select_ids(
                    points=fill_points,
                    budget_pct=fill_budget_pct,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    error_quota_ratio=None,
                )
            )

    if len(kept_ids) > target_count:
        kept_points = [p for p in points if p.trace_id in kept_ids]
        trim_budget_pct = (target_count / len(kept_points)) * 100.0 if kept_points else 0.0
        kept_ids = _ranked_v11_select_ids(
            points=kept_points,
            budget_pct=trim_budget_pct,
            incident_services=incident_services,
            incident_anchor_sec=incident_anchor_sec,
            error_quota_ratio=None,
        )

    return kept_ids


def _stream_v1_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    seed: int,
) -> set[str]:
    """Online-style dynamic voting selector inspired by TraStrainer.

    For each trace in chronological order:
    - Compute p_s (system probability) from service relevance + pressure + time proximity.
    - Compute p_d (diversity probability) from inverse recent signature frequency.
    - Let theta = kept/seen and beta = budget ratio.
      - If theta > beta: keep when (u < p_s) AND (u < p_d)
      - Else: keep when (u < p_s) OR (u < p_d)
    """
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))

    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        x = (v - lo) / (hi - lo)
        return max(0.0, min(1.0, x))

    def _time_score(p: eps.TracePoint) -> float:
        if incident_anchor_sec is None:
            return 0.5
        sec = p.start_ns // 1_000_000_000 - t0_sec
        return math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    def _service_relevance(p: eps.TracePoint) -> float:
        return 1.0 if _trace_has_incident_service(p, incident_services) else 0.0

    # Build service pressure map from recent error intensity + latency pressure.
    by_sec: dict[int, list[eps.TracePoint]] = defaultdict(list)
    service_durations: dict[str, list[float]] = defaultdict(list)
    for p in traces:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        by_sec[sec].append(p)
        for svc in p.services:
            service_durations[svc].append(float(p.duration_ms))

    sec_lo = min(by_sec.keys())
    sec_hi = max(by_sec.keys())

    service_p95: dict[str, float] = {}
    for svc, vals in service_durations.items():
        if not vals:
            service_p95[svc] = 0.0
            continue
        xs = sorted(vals)
        idx = int(round((len(xs) - 1) * 0.95))
        idx = max(0, min(len(xs) - 1, idx))
        service_p95[svc] = float(xs[idx])

    pressure_map: dict[tuple[int, str], float] = {}
    window: deque[tuple[int, frozenset[str], bool, float]] = deque()

    for sec in range(sec_lo, sec_hi + 1):
        for p in by_sec.get(sec, []):
            window.append((sec, p.services, p.has_error, float(p.duration_ms)))

        while window and window[0][0] < sec - 60 + 1:
            window.popleft()

        service_stats: dict[str, tuple[int, int, float]] = defaultdict(lambda: (0, 0, 0.0))
        for _s, svcs, has_err, dur in window:
            for svc in svcs:
                n, e, dsum = service_stats[svc]
                service_stats[svc] = (n + 1, e + (1 if has_err else 0), dsum + dur)

        for svc, (n, e, dsum) in service_stats.items():
            if n <= 0:
                pressure_map[(sec, svc)] = 0.0
                continue
            err_rate = e / n
            avg_dur = dsum / n
            p95 = max(1.0, service_p95.get(svc, 1.0))
            latency_pressure = _clamp(avg_dur / p95, 0.0, 1.0)
            pressure_map[(sec, svc)] = _clamp(0.7 * err_rate + 0.3 * latency_pressure, 0.0, 1.0)

    def _resource_pressure(p: eps.TracePoint) -> float:
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        if not p.services:
            return 0.0
        return max(pressure_map.get((sec, svc), 0.0) for svc in p.services)

    def _dur_bucket(d_ms: float) -> str:
        if d_ms < 100:
            return "lt100"
        if d_ms < 500:
            return "100_500"
        if d_ms < 2000:
            return "500_2000"
        return "ge2000"

    def _span_bucket(sc: int) -> str:
        if sc <= 3:
            return "s1"
        if sc <= 8:
            return "s2"
        if sc <= 20:
            return "s3"
        return "s4"

    rng = random.Random(seed)
    kept_ids: set[str] = set()
    seen = 0

    sig_window: deque[tuple[int, str]] = deque()
    sig_count: dict[str, int] = defaultdict(int)

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        while sig_window and sig_window[0][0] < sec - 60 + 1:
            _old_sec, old_sig = sig_window.popleft()
            sig_count[old_sig] -= 1
            if sig_count[old_sig] <= 0:
                sig_count.pop(old_sig, None)

        # Diversity signature and inverse-frequency probability.
        focus_services = sorted(set(p.services) & incident_services)
        if focus_services:
            focus_service = focus_services[0]
        elif p.root_service:
            focus_service = p.root_service
        elif p.services:
            focus_service = sorted(p.services)[0]
        else:
            focus_service = "unknown"

        status_class = "error" if p.has_error else "ok"
        sig = f"{focus_service}|{status_class}|{_dur_bucket(p.duration_ms)}|{_span_bucket(p.span_count)}"
        cnt = sig_count.get(sig, 0) + 1

        # p_d: higher when rare, lower when repeated.
        p_d = _clamp(1.0 / math.sqrt(float(max(1, cnt))), 0.05, 1.0)

        # p_s: system condition probability.
        svc_rel = _service_relevance(p)
        press = _resource_pressure(p)
        tsc = _time_score(p)
        sev = 0.5 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.5 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        p_s = _clamp(0.45 * svc_rel + 0.35 * press + 0.15 * tsc + 0.05 * sev, 0.0, 1.0)

        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        u = rng.random()
        if theta > beta:
            keep = (u < p_s) and (u < p_d)
        else:
            keep = (u < p_s) or (u < p_d)

        if keep:
            kept_ids.add(p.trace_id)

        sig_window.append((sec, sig))
        sig_count[sig] = cnt

    return kept_ids


def _enforce_budget_cap(points: list[eps.TracePoint], kept_ids: set[str], budget_pct: float) -> tuple[set[str], int, int]:
    """Enforce exact trace-count budget by capping kept set with priority ordering.

    Priority order when trimming:
    1) error traces first
    2) longer duration first
    3) higher span count first
    4) deterministic tie-break by trace_id
    """
    total = len(points)
    if total == 0:
        return set(), 0, 0

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    if len(kept_ids) <= target_count:
        return set(kept_ids), target_count, len(kept_ids)

    by_id = {p.trace_id: p for p in points}
    present = [tid for tid in kept_ids if tid in by_id]
    present.sort(
        key=lambda tid: (
            1 if by_id[tid].has_error else 0,
            by_id[tid].duration_ms,
            by_id[tid].span_count,
            tid,
        ),
        reverse=True,
    )

    capped = set(present[:target_count])
    return capped, target_count, len(kept_ids)


def _calibrate_target_tps(points: list[eps.TracePoint], budget_pct: float) -> tuple[float, set[str], float]:
    """Calibrate target_tps to approximate requested keep budget percentage.

    The sampling simulator controls keep via target_tps, not direct keep ratio.
    We binary-search target_tps to approach budget_pct in trace keep ratio.
    """
    target_ratio = max(0.0001, min(1.0, budget_pct / 100.0))
    if not points:
        return 0.0, set(), 0.0

    avg_rate = _avg_trace_rate(points)
    # Conservative bounds: tiny to above incoming rate.
    lo = 0.0001
    hi = max(1.0, avg_rate * 2.5)

    best_tps = lo
    best_ids: set[str] = set()
    best_diff = math.inf

    for _ in range(18):
        mid = (lo + hi) / 2.0
        kept_ids = eps.simulate(points, target_tps=mid)
        ratio = (len(kept_ids) / len(points)) if points else 0.0
        diff = abs(ratio - target_ratio)

        if diff < best_diff:
            best_diff = diff
            best_tps = mid
            best_ids = kept_ids

        if ratio < target_ratio:
            lo = mid
        else:
            hi = mid

    achieved_pct = (len(best_ids) / len(points) * 100.0) if points else 0.0
    return best_tps, best_ids, achieved_pct


def _dataset_specs(root: Path) -> list[DatasetSpec]:
    return [
        DatasetSpec(
            name="train-ticket",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/train_ticket/test/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/train_ticket/test/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.train-ticket.paper-source.20260401.jsonl",
            baseline_summary_file=root / "reports/compare/rca-benchmark-train-ticket-coverage-first-20260401-microrank-summary.json",
        ),
        DatasetSpec(
            name="hipster-batch1",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.hipster-batch1.paper-source.20260401.jsonl",
            baseline_summary_file=root / "reports/compare/rca-benchmark-hipster-batch1-coverage-first-20260401-microrank-summary.json",
        ),
        DatasetSpec(
            name="hipster-batch2",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.hipster-batch2.paper-source.20260401.jsonl",
            baseline_summary_file=root / "reports/compare/rca-benchmark-hipster-batch2-coverage-first-20260401-microrank-summary.json",
        ),
    ]


def _paper_microrank_reference() -> dict[float, dict[str, float]]:
    # Table-3 MicroRank + TraStrainer row.
    return {
        0.1: {"A@1_pct": 42.59, "A@3_pct": 77.74, "MRR": 0.5509},
        1.0: {"A@1_pct": 45.16, "A@3_pct": 78.52, "MRR": 0.5889},
        2.5: {"A@1_pct": 50.00, "A@3_pct": 82.26, "MRR": 0.6556},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run paper datasets through 2-phase sampling->RCA pipeline for multiple budgets")
    ap.add_argument("--root", default=".", help="Path to SignozCore root")
    ap.add_argument("--tag", default=datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--budgets", default="0.1,1.0,2.5", help="Sampling budgets in percent, comma-separated")
    ap.add_argument("--before-sec", type=int, default=300)
    ap.add_argument("--after-sec", type=int, default=600)
    ap.add_argument(
        "--budget-mode",
        choices=["strict", "approx"],
        default="strict",
        help="strict: enforce exact trace budget after simulation; approx: keep simulator output as-is",
    )
    ap.add_argument(
        "--datasets",
        default="train-ticket,hipster-batch1,hipster-batch2",
        help="Datasets to run, comma-separated",
    )
    ap.add_argument(
        "--selection-mode",
        choices=["adaptive", "ranked", "ranked-v11", "ranked-v11-quota", "ranked-v11-stochastic", "ranked-v11-hybrid", "stream-v1"],
        default="adaptive",
        help="adaptive: existing target_tps simulator; ranked: error/context split ranking; ranked-v11: formula-based global ranking; ranked-v11-quota: formula ranking with hard error/normal partition; ranked-v11-stochastic: calibrated probabilistic sampling; ranked-v11-hybrid: deterministic core + stochastic edge exploration; stream-v1: online dynamic voting with AND/OR gate",
    )
    ap.add_argument(
        "--error-budget-ratio",
        type=float,
        default=0.30,
        help="When selection-mode=ranked, fraction of kept traces reserved for error traces",
    )
    ap.add_argument(
        "--error-quota-ratio",
        type=float,
        default=0.30,
        help="When selection-mode=ranked-v11-quota, fraction of kept traces reserved for error traces",
    )
    ap.add_argument(
        "--stochastic-seed",
        type=int,
        default=42,
        help="When selection-mode=ranked-v11-stochastic, RNG seed for calibrated Bernoulli sampling",
    )
    ap.add_argument(
        "--hybrid-core-ratio",
        type=float,
        default=0.75,
        help="When selection-mode=ranked-v11-hybrid, fraction of budget allocated to deterministic top-ranked core",
    )
    ap.add_argument(
        "--hybrid-edge-pool-multiplier",
        type=float,
        default=3.0,
        help="When selection-mode=ranked-v11-hybrid, edge exploration pool size as multiplier of total budget",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    budgets = [float(x.strip()) for x in args.budgets.split(",") if x.strip()]
    selected = {x.strip() for x in args.datasets.split(",") if x.strip()}
    specs = [s for s in _dataset_specs(root) if s.name in selected]

    if not specs:
        raise SystemExit("No dataset selected. Check --datasets")

    benchmark_rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    sampling_quality_rows: list[dict[str, Any]] = []

    for spec in specs:
        print(f"\\n== DATASET: {spec.name} ==")
        labels = eps.load_labels(spec.label_file)
        points, _ = eps.build_trace_points(spec.trace_dir, labels, args.before_sec, args.after_sec)
        incident_services = _incident_services_from_labels(spec.label_file)
        t0_sec = min(p.start_ns for p in points) // 1_000_000_000 if points else 0
        incident_anchor_sec = (min(ts for ts, _svc in labels) - t0_sec) if labels else None

        for budget in budgets:
            budget_slug = _safe_slug(f"{budget:.3f}pct")
            print(f"  -- budget={budget}%")

            # Phase 1: calibrate simulator to requested budget and materialize sampled traces.
            target_trace_count = None
            pre_cap_trace_count = None

            if args.selection_mode == "ranked":
                target_tps = 0.0
                kept_ids = _ranked_select_ids(
                    points=points,
                    budget_pct=budget,
                    error_budget_ratio=args.error_budget_ratio,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "ranked-v11":
                target_tps = 0.0
                kept_ids = _ranked_v11_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "ranked-v11-quota":
                target_tps = 0.0
                kept_ids = _ranked_v11_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    error_quota_ratio=args.error_quota_ratio,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "ranked-v11-stochastic":
                target_tps = 0.0
                kept_ids = _ranked_v11_stochastic_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "ranked-v11-hybrid":
                target_tps = 0.0
                kept_ids = _ranked_v11_hybrid_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    core_ratio=args.hybrid_core_ratio,
                    edge_pool_multiplier=args.hybrid_edge_pool_multiplier,
                    seed=args.stochastic_seed,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v1":
                target_tps = 0.0
                kept_ids = _stream_v1_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            else:
                target_tps, kept_ids, achieved_keep_pct = _calibrate_target_tps(points, budget)
                pre_cap_keep_pct = achieved_keep_pct

                if args.budget_mode == "strict":
                    kept_ids, target_trace_count, pre_cap_trace_count = _enforce_budget_cap(points, kept_ids, budget)
                    achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0

                _, sim_metrics = eps.simulate_with_metrics(
                    points,
                    target_tps=target_tps,
                    incident_anchor_sec=incident_anchor_sec,
                )

            # For non-adaptive modes, enforce strict budget cap here as well.
            if args.budget_mode == "strict" and target_trace_count is None:
                kept_ids, target_trace_count, pre_cap_trace_count = _enforce_budget_cap(points, kept_ids, budget)
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0

            sampled_trace_dir = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "trace"
            total_rows, kept_rows = _materialize_sampled_trace_dir(spec.trace_dir, sampled_trace_dir, kept_ids)

            kept_ids_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "kept_trace_ids.txt"
            _write_text(kept_ids_file, "\n".join(sorted(kept_ids)) + ("\n" if kept_ids else ""))

            err_total = sum(1 for p in points if p.has_error)
            err_kept = sum(1 for p in points if p.has_error and p.trace_id in kept_ids)

            # Reuse evaluation helper for early-incident retention.
            if args.selection_mode == "ranked":
                # Recompute early retention directly from selected IDs for ranked mode.
                labels_local = eps.load_labels(spec.label_file)
                _pts_local, early_windows = eps.build_trace_points(spec.trace_dir, labels_local, args.before_sec, args.after_sec)
                early_total = 0
                early_kept = 0
                for p in _pts_local:
                    sec = p.start_ns // 1_000_000_000
                    if any(s <= sec <= e for s, e in early_windows):
                        early_total += 1
                        if p.trace_id in kept_ids:
                            early_kept += 1
                phase1_eval = {
                    "early_incident_retention_pct": (early_kept / early_total * 100.0) if early_total else None
                }
            else:
                phase1_eval = eps.evaluate_dataset(
                    spec.name,
                    spec.trace_dir,
                    spec.label_file,
                    args.before_sec,
                    args.after_sec,
                    target_tps,
                )

            cov_kept, cov_total, cov_pct = _incident_service_coverage(points, kept_ids, incident_services)
            endpoint_rows = _endpoint_retention(spec.trace_dir, kept_ids)

            phase1_summary_file = root / "reports/compare" / (
                f"paper-sampling-phase1-{spec.name}-{budget_slug}-{args.tag}.txt"
            )
            phase1_summary_text = _format_phase1_combined(
                dataset=spec.name,
                budget_pct=budget,
                target_tps=target_tps,
                points=points,
                kept_ids=kept_ids,
                sampled_trace_dir=sampled_trace_dir,
                kept_ids_file=kept_ids_file,
                total_rows=total_rows,
                kept_rows=kept_rows,
                err_total=err_total,
                err_kept=err_kept,
                early_retention_pct=phase1_eval.get("early_incident_retention_pct"),
                incident_cov_kept=cov_kept,
                incident_cov_total=cov_total,
                incident_cov_pct=cov_pct,
                incident_capture_latency_ms=sim_metrics.get("incident_capture_latency_ms"),
                threshold_volatility_pct=sim_metrics.get("threshold_volatility_pct"),
                max_step_change_pct=sim_metrics.get("max_step_change_pct"),
                endpoint_rows=endpoint_rows,
            )
            if args.budget_mode == "strict":
                phase1_summary_text += (
                    "\n"
                    "== BUDGET ENFORCEMENT ==\n"
                    f"budget_mode: {args.budget_mode}\n"
                    f"target_trace_count: {target_trace_count}\n"
                    f"pre_cap_kept_trace_count: {pre_cap_trace_count}\n"
                    f"pre_cap_keep_pct: {pre_cap_keep_pct:.4f}\n"
                    f"post_cap_keep_pct: {achieved_keep_pct:.4f}\n"
                )
            else:
                phase1_summary_text += (
                    "\n"
                    "== BUDGET ENFORCEMENT ==\n"
                    f"budget_mode: {args.budget_mode}\n"
                    f"pre_cap_keep_pct: {pre_cap_keep_pct:.4f}\n"
                    f"post_cap_keep_pct: {achieved_keep_pct:.4f}\n"
                )
            _write_text(phase1_summary_file, phase1_summary_text)

            # Build sampled scenarios for Phase 2.
            base_scenarios = _load_jsonl(spec.base_scenario_file)
            sampled_scenarios = []
            for row in base_scenarios:
                row2 = dict(row)
                row2["trace_glob"] = str(sampled_trace_dir / "*.csv")
                sampled_scenarios.append(row2)

            sampled_scenario_file = root / "reports/analysis/rca-benchmark" / (
                f"scenarios.{spec.name}.paper-sampled.{budget_slug}.{args.tag}.jsonl"
            )
            _write_jsonl(sampled_scenario_file, sampled_scenarios)

            # Phase 2: sampled traces -> MicroRank engine -> evaluator artifacts.
            raw_out_dir = root / "reports/compare/rca-engine-raw-microrank-sampled" / spec.name / budget_slug / args.tag
            raw_out_dir.mkdir(parents=True, exist_ok=True)

            _run(
                [
                    "python3",
                    "scripts/generate_microrank_engine_output.py",
                    "--scenario-file",
                    str(sampled_scenario_file),
                    "--out-dir",
                    str(raw_out_dir),
                ],
                cwd=root,
            )

            sampled_dataset_name = f"{spec.name}-sampled-{budget_slug}"
            sampled_eval_tag = f"{args.tag}-microrank-{budget_slug}"

            _run(
                [
                    "bash",
                    "scripts/run_paper_coverage_first_pipeline.sh",
                    str(sampled_scenario_file),
                    str(raw_out_dir),
                    sampled_dataset_name,
                    sampled_eval_tag,
                ],
                cwd=root,
            )

            sampled_summary_file = root / "reports/compare" / (
                f"rca-benchmark-{sampled_dataset_name}-coverage-first-{sampled_eval_tag}-summary.json"
            )
            sampled = json.loads(sampled_summary_file.read_text(encoding="utf-8"))

            benchmark_rows.append(
                {
                    "dataset": spec.name,
                    "budget_pct": budget,
                    "budget_slug": budget_slug,
                    "scenario_count": sampled.get("scenario_count"),
                    "A@1": sampled.get("A@1"),
                    "A@3": sampled.get("A@3"),
                    "MRR": sampled.get("MRR"),
                    "A@1_CI95": sampled.get("A@1_CI95"),
                    "A@3_CI95": sampled.get("A@3_CI95"),
                    "MRR_CI95": sampled.get("MRR_CI95"),
                    "summary_file": str(sampled_summary_file),
                    "phase1_summary_file": str(phase1_summary_file),
                    "sampled_trace_dir": str(sampled_trace_dir),
                    "kept_trace_ids_file": str(kept_ids_file),
                    "calibrated_target_tps": target_tps,
                    "budget_mode": args.budget_mode,
                    "achieved_keep_pct": achieved_keep_pct,
                    "pre_cap_keep_pct": pre_cap_keep_pct,
                }
            )

            compare_rows.append(
                {
                    "dataset": spec.name,
                    "budget_pct": budget,
                    "budget_slug": budget_slug,
                    "paper_A@1_pct": _paper_microrank_reference().get(budget, {}).get("A@1_pct"),
                    "paper_A@3_pct": _paper_microrank_reference().get(budget, {}).get("A@3_pct"),
                    "paper_MRR": _paper_microrank_reference().get(budget, {}).get("MRR"),
                    "ours_A@1_pct": (sampled.get("A@1", 0.0) * 100.0) if sampled.get("A@1") is not None else None,
                    "ours_A@3_pct": (sampled.get("A@3", 0.0) * 100.0) if sampled.get("A@3") is not None else None,
                    "ours_MRR": sampled.get("MRR"),
                    "delta_A@1_pct": ((sampled.get("A@1", 0.0) * 100.0) - _paper_microrank_reference().get(budget, {}).get("A@1_pct", 0.0)),
                    "delta_A@3_pct": ((sampled.get("A@3", 0.0) * 100.0) - _paper_microrank_reference().get(budget, {}).get("A@3_pct", 0.0)),
                    "delta_MRR": (sampled.get("MRR", 0.0) - _paper_microrank_reference().get(budget, {}).get("MRR", 0.0)),
                }
            )

            sampling_quality_rows.append(
                {
                    "dataset": spec.name,
                    "budget_pct": budget,
                    "budget_slug": budget_slug,
                    "incident_capture_latency_ms": sim_metrics.get("incident_capture_latency_ms"),
                    "early_incident_retention_pct": phase1_eval.get("early_incident_retention_pct"),
                    "critical_endpoint_coverage_pct": cov_pct,
                    "critical_endpoint_coverage_kept": cov_kept,
                    "critical_endpoint_coverage_total": cov_total,
                    "threshold_volatility_pct": sim_metrics.get("threshold_volatility_pct"),
                    "max_step_change_pct": sim_metrics.get("max_step_change_pct"),
                    "kept_error_traces": err_kept,
                    "baseline_error_traces": err_total,
                    "kept_error_traces_pct": (err_kept / err_total * 100.0) if err_total else None,
                    "achieved_keep_pct": achieved_keep_pct,
                    "pre_cap_keep_pct": pre_cap_keep_pct,
                    "phase1_summary_file": str(phase1_summary_file),
                }
            )

    # Output 1: benchmark table (teacher-facing, paper-table style with richer columns).
    benchmark_csv = root / "reports/compare" / f"rca-paper-table-sampled-budgets-{args.tag}.csv"
    benchmark_csv.parent.mkdir(parents=True, exist_ok=True)
    b_fields = list(benchmark_rows[0].keys()) if benchmark_rows else []
    with benchmark_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=b_fields)
        w.writeheader()
        for row in benchmark_rows:
            w.writerow(row)

    # Output 2: compare CSV for post-processing.
    compare_csv = root / "reports/compare" / f"rca-paper-vs-ours-microrank-budgets-{args.tag}.csv"
    c_fields = list(compare_rows[0].keys()) if compare_rows else []
    with compare_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=c_fields)
        w.writeheader()
        for row in compare_rows:
            w.writerow(row)

    # Output 3: compare markdown (paper-like summary for advisor presentation).
    compare_md = root / "reports/compare" / f"rca-paper-vs-ours-microrank-budgets-{args.tag}.md"
    lines = []
    lines.append("# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)")
    lines.append("")
    lines.append(f"tag: {args.tag}")
    lines.append(f"budgets_pct: {', '.join(str(b) for b in budgets)}")
    lines.append("")
    lines.append("| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in compare_rows:
        lines.append(
            "| {dataset} | {budget:.1f}% | {p1:.2f} | {o1:.2f} | {d1:+.2f} | {p3:.2f} | {o3:.2f} | {d3:+.2f} | {pm:.4f} | {om:.4f} | {dm:+.4f} |".format(
                dataset=r["dataset"],
                budget=r["budget_pct"],
                p1=float(r["paper_A@1_pct"] or 0.0),
                o1=float(r["ours_A@1_pct"] or 0.0),
                d1=float(r["delta_A@1_pct"] or 0.0),
                p3=float(r["paper_A@3_pct"] or 0.0),
                o3=float(r["ours_A@3_pct"] or 0.0),
                d3=float(r["delta_A@3_pct"] or 0.0),
                pm=float(r["paper_MRR"] or 0.0),
                om=float(r["ours_MRR"] or 0.0),
                dm=float(r["delta_MRR"] or 0.0),
            )
        )

    lines.extend(
        [
            "",
            "## Output Guide",
            "",
            "- Phase1 sampled traces + kept trace IDs:",
            f"  - reports/analysis/paper-sampled-traces/{args.tag}/<dataset>/<budget_slug>/trace/*.csv",
            f"  - reports/analysis/paper-sampled-traces/{args.tag}/<dataset>/<budget_slug>/kept_trace_ids.txt",
            "- Phase1 combined-style metric file:",
            f"  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-{args.tag}.txt",
            "- Phase2 benchmark table (teacher-facing):",
            f"  - {benchmark_csv}",
            "- Phase2 compare with paper (csv + markdown):",
            f"  - {compare_csv}",
            f"  - {compare_md}",
            "",
            "## Notes",
            "",
            "- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.",
            f"- budget_mode={args.budget_mode}: strict mode enforces exact trace-count budget after simulation.",
            "- Compare-to-paper uses MicroRank row from paper Table 3.",
            "- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.",
        ]
    )
    _write_text(compare_md, "\n".join(lines) + "\n")

    # Output 4: sampling quality key metrics (requested operational indicators).
    quality_csv = root / "reports/compare" / f"sampling-quality-key-metrics-{args.tag}.csv"
    q_fields = list(sampling_quality_rows[0].keys()) if sampling_quality_rows else []
    with quality_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=q_fields)
        w.writeheader()
        for row in sampling_quality_rows:
            w.writerow(row)

    quality_md = root / "reports/compare" / f"sampling-quality-key-metrics-{args.tag}.md"
    q_lines = []
    q_lines.append("# Sampling Quality Key Metrics")
    q_lines.append("")
    q_lines.append(f"tag: {args.tag}")
    q_lines.append(f"budgets_pct: {', '.join(str(b) for b in budgets)}")
    q_lines.append("")
    q_lines.append("| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |")
    q_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in sampling_quality_rows:
        kept_err_txt = (
            f"{int(r['kept_error_traces'])}/{int(r['baseline_error_traces'])} ({float(r['kept_error_traces_pct']):.2f}%)"
            if r["kept_error_traces_pct"] is not None
            else "NA"
        )
        q_lines.append(
            "| {dataset} | {budget:.1f}% | {lat} | {early} | {cov} | {vol} | {step} | {ke} |".format(
                dataset=r["dataset"],
                budget=float(r["budget_pct"]),
                lat=("NA" if r["incident_capture_latency_ms"] is None else f"{float(r['incident_capture_latency_ms']):.1f}"),
                early=("NA" if r["early_incident_retention_pct"] is None else f"{float(r['early_incident_retention_pct']):.2f}"),
                cov=("NA" if r["critical_endpoint_coverage_pct"] is None else f"{float(r['critical_endpoint_coverage_pct']):.2f} ({int(r['critical_endpoint_coverage_kept'])}/{int(r['critical_endpoint_coverage_total'])})"),
                vol=("NA" if r["threshold_volatility_pct"] is None else f"{float(r['threshold_volatility_pct']):.2f}"),
                step=("NA" if r["max_step_change_pct"] is None else f"{float(r['max_step_change_pct']):.2f}"),
                ke=kept_err_txt,
            )
        )
    _write_text(quality_md, "\n".join(q_lines) + "\n")

    print(f"benchmark_csv={benchmark_csv}")
    print(f"compare_csv={compare_csv}")
    print(f"compare_md={compare_md}")
    print(f"quality_csv={quality_csv}")
    print(f"quality_md={quality_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
