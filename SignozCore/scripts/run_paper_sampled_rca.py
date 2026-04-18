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
import streamv3_composite_strictcap as sv3comp
import streamv3_8_composite_anomaly as sv38comp
import streamv3_9_composite_anomaly as sv39comp
import streamv3_10_composite_anomaly as sv310comp
import streamv48_causal_booster as sv48comp
import streamv40_real_metrics_sampling as sv40comp
import streamv41_anomaly_precedence_sampler as sv41comp
import streamv42_trend_precursor_sampler as sv42comp
import streamv43_service_first_dual_channel as sv43comp
import streamv44_reserve_channel_sampler as sv44comp
import streamv45_hybrid_prerca_rerank as sv45comp


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


def _parse_iso_utc_to_s(value: str) -> int:
    v = str(value).strip()
    if not v:
        return 0
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    return int(dt.timestamp())


def _scenario_windows(base_scenarios: list[dict[str, Any]]) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    for row in base_scenarios:
        sid = str(row.get("scenario_id") or "").strip()
        if not sid:
            continue
        st = _parse_iso_utc_to_s(str(row.get("incident_start_ts") or ""))
        ed = _parse_iso_utc_to_s(str(row.get("incident_end_ts") or ""))
        if st <= 0:
            continue
        if ed < st:
            ed = st
        out.append((sid, st, ed))
    return out


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def _enforce_budget_cap_per_scenario(
    points: list[eps.TracePoint],
    kept_ids: set[str],
    budget_pct: float,
    scenario_windows: list[tuple[str, int, int]],
    min_incident_traces_per_scenario: int = 1,
) -> tuple[set[str], int, int, int]:
    """Strict-cap with per-scenario floor to avoid global top-up distortion.

    Returns (final_ids, target_count, pre_cap_count, scenario_floor_fail_count).
    """
    total = len(points)
    if total == 0:
        return set(), 0, 0, 0

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))
    pre_cap_count = len(kept_ids)

    if not scenario_windows or min_incident_traces_per_scenario <= 0:
        capped, target_count2, pre_cap_count2 = _enforce_budget_cap(points, kept_ids, budget_pct)
        return capped, target_count2, pre_cap_count2, 0

    by_id: dict[str, eps.TracePoint] = {p.trace_id: p for p in points}
    start_s_by_id: dict[str, int] = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}

    scenario_ids_by_idx: list[set[str]] = []
    scenario_membership: dict[str, set[int]] = defaultdict(set)
    non_empty_scenarios = 0

    for idx, (_sid, st, ed) in enumerate(scenario_windows):
        ids = {tid for tid, sec in start_s_by_id.items() if st <= sec <= ed}
        scenario_ids_by_idx.append(ids)
        if ids:
            non_empty_scenarios += 1
            for tid in ids:
                scenario_membership[tid].add(idx)

    if non_empty_scenarios == 0:
        capped, target_count2, pre_cap_count2 = _enforce_budget_cap(points, kept_ids, budget_pct)
        return capped, target_count2, pre_cap_count2, len(scenario_windows)

    # Relax floor automatically if target budget is too small for all scenarios.
    floor_each = min_incident_traces_per_scenario
    if target_count < non_empty_scenarios * floor_each:
        floor_each = 0

    def _priority(tid: str) -> tuple[float, float, float, int, str]:
        p = by_id.get(tid)
        if p is None:
            return (0.0, 0.0, 0.0, 0, tid)
        return (
            1.0 if p.has_error else 0.0,
            float(p.duration_ms),
            float(p.span_count),
            len(scenario_membership.get(tid, set())),
            tid,
        )

    final_ids: set[str] = set(kept_ids)
    scenario_floor_fail_count = 0

    if floor_each > 0:
        for idx, ids in enumerate(scenario_ids_by_idx):
            if not ids:
                scenario_floor_fail_count += 1
                continue
            present = [tid for tid in final_ids if tid in ids]
            if len(present) >= floor_each:
                continue
            need = floor_each - len(present)
            candidates = [tid for tid in ids if tid not in final_ids]
            candidates.sort(key=_priority, reverse=True)
            if not candidates:
                scenario_floor_fail_count += 1
                continue
            for tid in candidates[:need]:
                final_ids.add(tid)
            if len([tid for tid in final_ids if tid in ids]) < floor_each:
                scenario_floor_fail_count += 1

    if len(final_ids) <= target_count:
        return final_ids, target_count, pre_cap_count, scenario_floor_fail_count

    # Trim to target while preserving per-scenario floor when possible.
    scenario_counts = [0] * len(scenario_ids_by_idx)
    for tid in final_ids:
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] += 1

    def _can_remove(tid: str) -> bool:
        if floor_each <= 0:
            return True
        for idx in scenario_membership.get(tid, set()):
            if scenario_counts[idx] <= floor_each:
                return False
        return True

    removable = sorted(final_ids, key=_priority)
    for tid in removable:
        if len(final_ids) <= target_count:
            break
        if not _can_remove(tid):
            continue
        final_ids.remove(tid)
        for idx in scenario_membership.get(tid, set()):
            scenario_counts[idx] -= 1

    # If still above target, force trim lowest-priority leftovers.
    if len(final_ids) > target_count:
        force = sorted(final_ids, key=_priority)
        for tid in force:
            if len(final_ids) <= target_count:
                break
            final_ids.remove(tid)

    return final_ids, target_count, pre_cap_count, scenario_floor_fail_count


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _materialize_sampled_trace_dir(
    src_trace_dir: Path,
    out_trace_dir: Path,
    kept_ids: set[str],
    kept_rank: dict[str, int] | None = None,
) -> tuple[int, int]:
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

            kept_rows_buffer: list[dict[str, str]] = []

            for row in reader:
                total_rows += 1
                tid = str(row.get("TraceID", "")).strip().lower()
                if tid in kept_ids:
                    kept_rows_buffer.append(row)
                    kept_rows += 1

            if kept_rank is not None:
                kept_rows_buffer.sort(
                    key=lambda r: (
                        kept_rank.get(str(r.get("TraceID", "")).strip().lower(), 10**18),
                        str(r.get("TraceID", "")).strip().lower(),
                    )
                )

            for row in kept_rows_buffer:
                writer.writerow(row)

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


def _stream_v2_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    seed: int,
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
    metric_weight: float = 0.0,
) -> set[str]:
    """Streaming selector v2.7: robust system score + guarded diversity + OR quota valve.

        - p_s: robust z-score over recent window features (error, latency pressure, incident-service hit),
            optionally fused with metric-pressure when metric_weight>0.
    - p_d: inverse cluster mass with online Jaccard-nearest clustering approximation.
    - Gate: theta>beta => AND, else OR.
    - OR guard: block diversity-only rare traces when p_s is weak.
    - OR quota valve: low-p_s normal traces pass only while recent kept normal-ratio is below target.
    - Guardrail: keep recent kept error-ratio close to recent seen error-ratio.
    """
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    target_count = max(1, min(total, int(round(total * beta))))
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)
    metric_weight = _clamp(float(metric_weight), 0.0, 0.60)
    use_metric_fusion = metric_weight > 0.0

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

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

    def _trace_tokens(p: eps.TracePoint) -> frozenset[str]:
        toks = {f"svc:{s}" for s in p.services}
        toks.add(f"dur:{_dur_bucket(p.duration_ms)}")
        toks.add(f"span:{_span_bucket(p.span_count)}")
        return frozenset(toks)

    def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
        if not a and not b:
            return 1.0
        u = len(a | b)
        if u <= 0:
            return 0.0
        return len(a & b) / u

    def _robust_sigma(xs: list[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((x - mu) * (x - mu) for x in xs) / max(1, len(xs) - 1)
        std = math.sqrt(max(0.0, var))
        sxs = sorted(xs)
        med = sxs[len(sxs) // 2]
        abs_dev = sorted(abs(x - med) for x in xs)
        mad = abs_dev[len(abs_dev) // 2]
        robust = 1.4826 * mad
        return max(std, robust)

    # Build service-level pressure map (error-rate + latency pressure) in a sliding window.
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
            service_p95[svc] = 1.0
            continue
        xs = sorted(vals)
        idx = int(round((len(xs) - 1) * 0.95))
        idx = max(0, min(len(xs) - 1, idx))
        service_p95[svc] = max(1.0, float(xs[idx]))

    pressure_map: dict[tuple[int, str], float] = {}
    window: deque[tuple[int, frozenset[str], bool, float]] = deque()
    for sec in range(sec_lo, sec_hi + 1):
        for p in by_sec.get(sec, []):
            window.append((sec, p.services, p.has_error, float(p.duration_ms)))

        while window and window[0][0] < sec - lookback_sec + 1:
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
            latency_pressure = _clamp(avg_dur / max(1.0, service_p95.get(svc, 1.0)), 0.0, 1.0)
            pressure_map[(sec, svc)] = _clamp(0.70 * err_rate + 0.30 * latency_pressure, 0.0, 1.0)

    def _metric_pressure(p: eps.TracePoint) -> float:
        if not p.services:
            return 0.0
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        return max(pressure_map.get((sec, svc), 0.0) for svc in p.services)

    rng = random.Random(seed)
    kept_ids: set[str] = set()

    # Recent seen features for p_s.
    seen_win: deque[tuple[int, float, float, float, float]] = deque()
    # Recent kept class for spectrum guardrail.
    kept_win: deque[tuple[int, bool]] = deque()

    # Online cluster mass in lookback window.
    cluster_templates: dict[tuple[str, ...], frozenset[str]] = {}
    cluster_counts: dict[tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[tuple[int, tuple[str, ...]]] = deque()

    seen = 0
    eps0 = 1e-6
    guardrail_tol = 0.12
    cluster_merge_threshold = 0.5
    warmup_seen = max(100, min(800, int(total * 0.15)))
    score_by_id: dict[str, float] = {}
    time_prox_by_id: dict[str, float] = {}
    lat_by_id: dict[str, float] = {}
    incident_hit_by_id: dict[str, float] = {}
    by_id: dict[str, eps.TracePoint] = {p.trace_id: p for p in traces}
    normal_ratio_target = 0.30
    normal_ratio_low = 0.22
    normal_ratio_high = 0.32

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        # Slide seen/kept/cluster windows.
        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()

        while kept_win and kept_win[0][0] < sec - lookback_sec + 1:
            kept_win.popleft()

        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        has_incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        metric_pressure = _metric_pressure(p)
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        hist_met = [x[4] for x in seen_win]

        mu_err = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        mu_lat = (sum(hist_lat) / len(hist_lat)) if hist_lat else 0.0
        mu_inc = (sum(hist_inc) / len(hist_inc)) if hist_inc else 0.0
        mu_met = (sum(hist_met) / len(hist_met)) if hist_met else 0.0

        sg_err = _robust_sigma(hist_err)
        sg_lat = _robust_sigma(hist_lat)
        sg_inc = _robust_sigma(hist_inc)
        sg_met = _robust_sigma(hist_met)

        z_err = abs(err_flag - mu_err) / (sg_err + eps0)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + eps0)
        z_inc = abs(has_incident_service - mu_inc) / (sg_inc + eps0)
        if use_metric_fusion:
            z_met = abs(metric_pressure - mu_met) / (sg_met + eps0)
            z_mix = 0.34 * z_err + 0.28 * z_lat + 0.18 * z_inc + 0.20 * z_met
        else:
            z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc
        # Lower effective alpha at higher budget to avoid p_s saturating too close to 1.0.
        alpha_eff = alpha * (0.90 if beta >= 0.01 else 1.00)
        shock = math.tanh(alpha_eff * z_mix)
        if use_metric_fusion:
            base_sys = 0.36 * has_incident_service + 0.20 * lat_pressure + 0.14 * err_flag + 0.10 * time_prox + 0.20 * metric_pressure
            trace_ps = _clamp(0.55 * base_sys + 0.45 * shock, 0.02, 0.98)
            # Metrics fusion: blend trace-driven signal with service-metric pressure.
            p_s = _clamp((1.0 - metric_weight) * trace_ps + metric_weight * metric_pressure, 0.02, 0.98)
        else:
            base_sys = 0.45 * has_incident_service + 0.25 * lat_pressure + 0.15 * err_flag + 0.15 * time_prox
            p_s = _clamp(0.55 * base_sys + 0.45 * shock, 0.02, 0.98)

        # Tightened incident floor: only boost traces that are truly in the incident region.
        incident_floor = 0.0
        if has_incident_service > 0.0:
            if (err_flag > 0.0) and (time_prox >= 0.55):
                incident_floor = 0.62
            elif (time_prox >= 0.78) and (lat_pressure >= 0.58):
                incident_floor = 0.56
            elif (time_prox >= 0.88) and (lat_pressure >= 0.50):
                incident_floor = 0.50
        p_s = max(p_s, incident_floor)

        # p_d from nearest-cluster mass with Jaccard similarity.
        toks = _trace_tokens(p)
        best_key: tuple[str, ...] | None = None
        best_sim = 0.0
        for k, ktoks in cluster_templates.items():
            sim = _jaccard(toks, ktoks)
            if sim > best_sim:
                best_sim = sim
                best_key = k

        if best_key is not None and best_sim >= cluster_merge_threshold:
            assigned_key = best_key
            mass = cluster_counts.get(assigned_key, 0)
            effective_mass = mass * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            mass = 0
            effective_mass = 0.0

        # V2.1: be more tolerant to repetition for incident-related traces.
        if has_incident_service > 0.0:
            effective_mass *= 0.35
            gamma_eff = max(0.35, gamma * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = gamma
            pd_floor = 0.05
        p_d = _clamp((1.0 + effective_mass) ** (-gamma_eff), pd_floor, 1.0)

        theta = (len(kept_ids) / seen) if seen > 0 else 0.0

        # Budget-aware OR threshold and normal baseline probability.
        if beta <= 0.0015:
            ps_or_floor = 0.12
            normal_floor_target = 0.18
        elif beta <= 0.01:
            ps_or_floor = 0.20
            normal_floor_target = 0.24
        else:
            ps_or_floor = 0.30
            normal_floor_target = 0.30

        if has_incident_service > 0.0 and time_prox >= 0.55:
            ps_or_floor = max(0.08, ps_or_floor - 0.06)

        # Baseline keep chance is only used when OR quota valve is open.
        normal_baseline_prob = _clamp(max(beta, 0.02), 0.0, 0.20)
        is_normal_common = (not p.has_error) and (has_incident_service <= 0.0) and (p_d <= 0.45)
        kept_hist_or = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio_or = (sum(kept_hist_or) / len(kept_hist_or)) if kept_hist_or else 0.0
        kept_norm_ratio_or = 1.0 - kept_err_ratio_or
        is_low_ps_normal = (not p.has_error) and (has_incident_service <= 0.0) and (p_s < ps_or_floor)

        u = rng.random()
        if theta > beta:
            # In over-budget phase, keep incident/error traces less sensitive to diversity penalty.
            if has_incident_service > 0.0 and (time_prox >= 0.60 or err_flag > 0.0 or lat_pressure >= 0.60):
                keep = (u < max(p_s, 0.70))
            else:
                keep = (u < p_s) and (u < p_d)
        else:
            # Block rare diversity noise when system signal is weak.
            rare_noise = (p_d >= 0.72) and (p_s < ps_or_floor)
            if rare_noise:
                keep = (u < p_s)
            else:
                keep = (u < p_s) or (u < p_d)

            # OR quota valve: allow low-p_s normal traces only if normal ratio is below target.
            if is_low_ps_normal:
                valve_open = kept_norm_ratio_or < normal_ratio_target
                if not valve_open:
                    keep = False
                elif (not keep) and is_normal_common and (u < normal_baseline_prob):
                    keep = True

            # Trace-only dynamic spectrum valve:
            # - if normal ratio is already high, suppress weak normal traces
            # - if normal ratio is too low, top-up normal background with tiny random chance
            if (not p.has_error) and (has_incident_service <= 0.0):
                if (kept_norm_ratio_or >= normal_ratio_high) and (p_s < max(ps_or_floor, 0.45)):
                    keep = False
                elif (kept_norm_ratio_or < normal_ratio_low) and (not keep):
                    p_normal_topup = _clamp(max(beta * 0.8, 0.015), 0.0, 0.12)
                    if rng.random() < p_normal_topup:
                        keep = True

        # Spectrum guardrail: keep kept-error ratio close to recent seen-error ratio.
        hist_seen_err_ratio = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        kept_hist = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio = (sum(kept_hist) / len(kept_hist)) if kept_hist else hist_seen_err_ratio
        kept_norm_ratio = 1.0 - kept_err_ratio
        drift = kept_err_ratio - hist_seen_err_ratio
        strong_signal = max(p_s, p_d) >= 0.85

        # V2.1: reduce guardrail pressure in early stage; let AND/OR gate dominate first.
        if seen >= warmup_seen:
            if drift > guardrail_tol:
                # Too error-heavy in kept set: prefer normal traces, avoid weak error traces.
                if p.has_error and keep and (not strong_signal):
                    keep = False
                elif (not p.has_error) and (not keep) and (max(p_s, p_d) >= 0.60):
                    keep = True
            elif drift < -guardrail_tol:
                # Too normal-heavy in kept set: prefer error traces.
                if (not p.has_error) and keep and (not strong_signal):
                    keep = False
                elif p.has_error and (not keep) and (max(p_s, p_d) >= 0.50):
                    keep = True

            # OR quota valve reinforcement after warmup.
            if theta <= beta and is_low_ps_normal and kept_norm_ratio >= normal_ratio_target:
                keep = False
            if theta <= beta and (not p.has_error) and (has_incident_service <= 0.0):
                if (kept_norm_ratio >= normal_ratio_high) and (p_s < max(ps_or_floor, 0.45)):
                    keep = False

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))

        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        time_prox_by_id[p.trace_id] = time_prox
        lat_by_id[p.trace_id] = lat_pressure
        incident_hit_by_id[p.trace_id] = has_incident_service

        # Update seen/cluster windows after decision.
        seen_win.append((sec, err_flag, lat_pressure, has_incident_service, metric_pressure))

        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

    # Ultra-low budget booster: reserve a deterministic incident-focused core before final return.
    if beta <= 0.0015 and target_count > 0:
        reserve_n = max(1, int(round(target_count * 0.45)))
        incident_pool = [
            tid
            for tid in by_id
            if incident_hit_by_id.get(tid, 0.0) > 0.0
        ]
        incident_pool.sort(
            key=lambda tid: (
                time_prox_by_id.get(tid, 0.0),
                lat_by_id.get(tid, 0.0),
                1.0 if by_id[tid].has_error else 0.0,
                score_by_id.get(tid, 0.0),
            ),
            reverse=True,
        )
        reserve_ids = incident_pool[:reserve_n]
        if reserve_ids:
            drop_candidates = [tid for tid in kept_ids if tid not in reserve_ids]
            drop_candidates.sort(key=lambda tid: score_by_id.get(tid, 0.0))
            need = [tid for tid in reserve_ids if tid not in kept_ids]
            swap_n = min(len(need), len(drop_candidates))
            for tid in drop_candidates[:swap_n]:
                kept_ids.discard(tid)
            for tid in need[:swap_n]:
                kept_ids.add(tid)

    # V2.2 reinforcement: ensure minimum error density for spectrum-based RCA.
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [
                tid
                for tid, p in by_id.items()
                if (tid not in kept_ids) and p.has_error
            ]
            cand_add.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
            add_ids = cand_add[:deficit]

            if add_ids:
                # Remove weakest non-error traces first to keep set size stable.
                cand_drop = [
                    tid
                    for tid in kept_ids
                    if by_id.get(tid) and (not by_id[tid].has_error)
                ]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    kept_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    kept_ids.add(tid)

    return kept_ids


def _stream_v2_fusion_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    core_ratio: float,
    edge_pool_multiplier: float,
    seed: int,
    metric_weight: float = 0.30,
) -> set[str]:
    """Fusion selector: V2 core (A@1 signal) + hybrid context (A@3/MRR stability).

    Strategy:
    - Build candidate core from stream-v2 selector.
    - Build candidate context from ranked-v11-hybrid selector.
    - Fill target budget by quota: core first, then context, then fallback.
    """
    total = len(points)
    if total == 0:
        return set()

    target_count = int(round(total * (budget_pct / 100.0)))
    target_count = max(1, min(total, target_count))

    v2_ids = _stream_v2_select_ids(
        points=points,
        budget_pct=budget_pct,
        incident_services=incident_services,
        incident_anchor_sec=incident_anchor_sec,
        seed=seed,
        metric_weight=metric_weight,
    )
    hybrid_ids = _ranked_v11_hybrid_select_ids(
        points=points,
        budget_pct=budget_pct,
        incident_services=incident_services,
        incident_anchor_sec=incident_anchor_sec,
        core_ratio=core_ratio,
        edge_pool_multiplier=edge_pool_multiplier,
        seed=seed,
    )

    by_id = {p.trace_id: p for p in points}
    dur_lo = min(p.duration_ms for p in points)
    dur_hi = max(p.duration_ms for p in points)
    span_lo = min(p.span_count for p in points)
    span_hi = max(p.span_count for p in points)
    t0_sec = min(p.start_ns for p in points) // 1_000_000_000

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    def _time_prox(p: eps.TracePoint) -> float:
        if incident_anchor_sec is None:
            return 0.5
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        return math.exp(-abs(sec - incident_anchor_sec) / 60.0)

    def _core_score(p: eps.TracePoint) -> float:
        inc = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        err = 1.0 if p.has_error else 0.0
        sev = 0.5 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.5 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        tsc = _time_prox(p)
        return 0.45 * inc + 0.25 * err + 0.20 * tsc + 0.10 * sev

    def _ctx_score(p: eps.TracePoint) -> float:
        inc = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        sev = 0.5 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.5 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        tsc = _time_prox(p)
        norm_bonus = 0.08 if not p.has_error else 0.0
        return 0.35 * inc + 0.25 * tsc + 0.32 * sev + norm_bonus

    # Budget-aware core quota: keep stronger V2 core at lower budgets.
    cr = _clamp(float(core_ratio), 0.0, 1.0)
    if budget_pct <= 0.15:
        core_share = max(cr, 0.78)
    elif budget_pct <= 1.0:
        core_share = max(cr, 0.60)
    else:
        core_share = min(max(cr, 0.45), 0.58)

    core_quota = int(round(target_count * core_share))
    core_quota = max(0, min(target_count, core_quota))

    v2_ranked = sorted((by_id[tid] for tid in v2_ids if tid in by_id), key=_core_score, reverse=True)
    hy_ranked = sorted((by_id[tid] for tid in hybrid_ids if tid in by_id), key=_ctx_score, reverse=True)
    all_ranked = sorted(points, key=_core_score, reverse=True)

    selected: set[str] = set()
    for p in v2_ranked:
        if len(selected) >= core_quota:
            break
        selected.add(p.trace_id)

    for p in hy_ranked:
        if len(selected) >= target_count:
            break
        selected.add(p.trace_id)

    for p in v2_ranked:
        if len(selected) >= target_count:
            break
        selected.add(p.trace_id)

    for p in all_ranked:
        if len(selected) >= target_count:
            break
        selected.add(p.trace_id)

    return selected


def _stream_v3_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    seed: int,
    online_soft_cap: bool = False,
    lookback_sec: int = 60,
    alpha: float = 1.2,
    gamma: float = 0.8,
) -> set[str]:
    """Stream-Hybrid V3: V2 core + OR soft quota 70/30 + no hard strict-cap.

    - Keep V2 core for p_s and p_d.
    - Keep AND gate when theta > beta.
    - Simplify OR gate when theta <= beta:
      - Block rare diversity noise (high p_d, low p_s).
      - For clean normal traces, allow pass only when kept-normal ratio < 30%, with prob=beta.
        - Keep only minimum-error-density reinforcement post selection.
        - Optional online soft-cap controller: when theta < beta, open a random
            normal-trace valve with probability proportional to gap=(beta-theta).
    """
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    target_count = max(1, min(total, int(round(total * beta))))
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

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

    def _trace_tokens(p: eps.TracePoint) -> frozenset[str]:
        toks = {f"svc:{s}" for s in p.services}
        toks.add(f"dur:{_dur_bucket(p.duration_ms)}")
        toks.add(f"span:{_span_bucket(p.span_count)}")
        return frozenset(toks)

    def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
        if not a and not b:
            return 1.0
        u = len(a | b)
        if u <= 0:
            return 0.0
        return len(a & b) / u

    def _robust_sigma(xs: list[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((x - mu) * (x - mu) for x in xs) / max(1, len(xs) - 1)
        std = math.sqrt(max(0.0, var))
        sxs = sorted(xs)
        med = sxs[len(sxs) // 2]
        abs_dev = sorted(abs(x - med) for x in xs)
        mad = abs_dev[len(abs_dev) // 2]
        robust = 1.4826 * mad
        return max(std, robust)

    rng = random.Random(seed)
    kept_ids: set[str] = set()

    seen_win: deque[tuple[int, float, float, float]] = deque()
    kept_win: deque[tuple[int, bool]] = deque()

    cluster_templates: dict[tuple[str, ...], frozenset[str]] = {}
    cluster_counts: dict[tuple[str, ...], int] = defaultdict(int)
    cluster_win: deque[tuple[int, tuple[str, ...]]] = deque()

    seen = 0
    eps0 = 1e-6
    cluster_merge_threshold = 0.5

    normal_ratio_target = 0.30

    score_by_id: dict[str, float] = {}
    by_id: dict[str, eps.TracePoint] = {p.trace_id: p for p in traces}

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()
        while kept_win and kept_win[0][0] < sec - lookback_sec + 1:
            kept_win.popleft()
        while cluster_win and cluster_win[0][0] < sec - lookback_sec + 1:
            _old_sec, old_key = cluster_win.popleft()
            cluster_counts[old_key] -= 1
            if cluster_counts[old_key] <= 0:
                cluster_counts.pop(old_key, None)
                cluster_templates.pop(old_key, None)

        has_incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]

        mu_err = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        mu_lat = (sum(hist_lat) / len(hist_lat)) if hist_lat else 0.0
        mu_inc = (sum(hist_inc) / len(hist_inc)) if hist_inc else 0.0

        sg_err = _robust_sigma(hist_err)
        sg_lat = _robust_sigma(hist_lat)
        sg_inc = _robust_sigma(hist_inc)

        z_err = abs(err_flag - mu_err) / (sg_err + eps0)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + eps0)
        z_inc = abs(has_incident_service - mu_inc) / (sg_inc + eps0)
        z_mix = 0.40 * z_err + 0.35 * z_lat + 0.25 * z_inc

        alpha_eff = alpha * (0.90 if beta >= 0.01 else 1.00)
        shock = math.tanh(alpha_eff * z_mix)
        base_sys = 0.45 * has_incident_service + 0.25 * lat_pressure + 0.15 * err_flag + 0.15 * time_prox
        p_s = _clamp(0.55 * base_sys + 0.45 * shock, 0.02, 0.98)

        incident_floor = 0.0
        if has_incident_service > 0.0:
            if (err_flag > 0.0) and (time_prox >= 0.55):
                incident_floor = 0.62
            elif (time_prox >= 0.78) and (lat_pressure >= 0.58):
                incident_floor = 0.56
            elif (time_prox >= 0.88) and (lat_pressure >= 0.50):
                incident_floor = 0.50
        p_s = max(p_s, incident_floor)

        toks = _trace_tokens(p)
        best_key: tuple[str, ...] | None = None
        best_sim = 0.0
        for k, ktoks in cluster_templates.items():
            sim = _jaccard(toks, ktoks)
            if sim > best_sim:
                best_sim = sim
                best_key = k

        if best_key is not None and best_sim >= cluster_merge_threshold:
            assigned_key = best_key
            mass = cluster_counts.get(assigned_key, 0)
            effective_mass = mass * best_sim
        else:
            assigned_key = tuple(sorted(toks))
            effective_mass = 0.0

        if has_incident_service > 0.0:
            effective_mass *= 0.35
            gamma_eff = max(0.35, gamma * 0.55)
            pd_floor = 0.20
        else:
            gamma_eff = gamma
            pd_floor = 0.05
        p_d = _clamp((1.0 + effective_mass) ** (-gamma_eff), pd_floor, 1.0)

        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        if beta <= 0.0015:
            ps_or_floor = 0.12
        elif beta <= 0.01:
            ps_or_floor = 0.20
        else:
            ps_or_floor = 0.30
        if has_incident_service > 0.0 and time_prox >= 0.55:
            ps_or_floor = max(0.08, ps_or_floor - 0.06)

        kept_hist = [1.0 if e else 0.0 for _s, e in kept_win]
        kept_err_ratio = (sum(kept_hist) / len(kept_hist)) if kept_hist else 0.0
        kept_norm_ratio = 1.0 - kept_err_ratio

        u = rng.random()
        if theta > beta:
            keep = (u < p_s) and (u < p_d)
        else:
            rare_noise = (p_d >= 0.72) and (p_s < ps_or_floor)
            if rare_noise:
                keep = False
            else:
                keep = (u < p_s)

            is_clean_normal = (not p.has_error) and (has_incident_service <= 0.0) and (p_s < ps_or_floor) and (p_d <= 0.45)
            if (not keep) and is_clean_normal and (kept_norm_ratio < normal_ratio_target):
                if rng.random() < beta:
                    keep = True

            if (not keep) and online_soft_cap and is_clean_normal:
                gap = max(0.0, beta - theta)
                # Soft-cap padding for online mode to separate budgets smoothly.
                p_gap = _clamp(gap, 0.0, 0.20)
                if rng.random() < p_gap:
                    keep = True

        if keep:
            kept_ids.add(p.trace_id)
            kept_win.append((sec, p.has_error))

        score_by_id[p.trace_id] = 0.75 * p_s + 0.25 * p_d
        seen_win.append((sec, err_flag, lat_pressure, has_incident_service))
        cluster_templates[assigned_key] = toks if best_key is None else cluster_templates.get(assigned_key, toks)
        cluster_counts[assigned_key] += 1
        cluster_win.append((sec, assigned_key))

    # Keep minimum error density protection (same spirit as v2).
    if kept_ids:
        base_err_ratio = sum(1 for p in traces if p.has_error) / total
        min_err_ratio = _clamp(max(0.08, base_err_ratio * 1.2), 0.08, 0.40)
        needed_err = int(round(target_count * min_err_ratio))
        kept_err = [tid for tid in kept_ids if by_id.get(tid) and by_id[tid].has_error]

        if len(kept_err) < needed_err:
            deficit = needed_err - len(kept_err)
            cand_add = [tid for tid, p in by_id.items() if (tid not in kept_ids) and p.has_error]
            cand_add.sort(key=lambda tid: score_by_id.get(tid, 0.0), reverse=True)
            add_ids = cand_add[:deficit]
            if add_ids:
                cand_drop = [tid for tid in kept_ids if by_id.get(tid) and (not by_id[tid].has_error)]
                cand_drop.sort(key=lambda tid: score_by_id.get(tid, 0.0))
                drop_n = min(len(add_ids), len(cand_drop))
                for tid in cand_drop[:drop_n]:
                    kept_ids.discard(tid)
                for tid in add_ids[:drop_n]:
                    kept_ids.add(tid)

    return kept_ids


def _stream_v2_lite_select_ids(
    points: list[eps.TracePoint],
    budget_pct: float,
    incident_services: set[str],
    incident_anchor_sec: int | None,
    seed: int,
    lookback_sec: int = 60,
    alpha: float = 1.2,
) -> set[str]:
    """Stream-Lite: p_s only + tiny random normal background, no p_d/clustering.

    Decision rule:
    - If theta > beta: keep when rand < p_s
    - Else: keep when (rand < p_s) OR (rand < p_normal)
    where p_normal ~= beta (tiny breathing hole for baseline traces).
    """
    total = len(points)
    if total == 0:
        return set()

    beta = _clamp(budget_pct / 100.0, 0.0, 1.0)
    if beta <= 0.0:
        return set()

    traces = sorted(points, key=lambda p: (p.start_ns, p.trace_id))
    t0_sec = min(p.start_ns for p in traces) // 1_000_000_000
    dur_lo = min(p.duration_ms for p in traces)
    dur_hi = max(p.duration_ms for p in traces)
    span_lo = min(p.span_count for p in traces)
    span_hi = max(p.span_count for p in traces)

    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return _clamp((v - lo) / (hi - lo), 0.0, 1.0)

    def _robust_sigma(xs: list[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        mu = sum(xs) / len(xs)
        var = sum((x - mu) * (x - mu) for x in xs) / max(1, len(xs) - 1)
        std = math.sqrt(max(0.0, var))
        sxs = sorted(xs)
        med = sxs[len(sxs) // 2]
        abs_dev = sorted(abs(x - med) for x in xs)
        mad = abs_dev[len(abs_dev) // 2]
        robust = 1.4826 * mad
        return max(std, robust)

    # Lightweight metric-pressure map per service/time window.
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
            service_p95[svc] = 1.0
            continue
        xs = sorted(vals)
        idx = int(round((len(xs) - 1) * 0.95))
        idx = max(0, min(len(xs) - 1, idx))
        service_p95[svc] = max(1.0, float(xs[idx]))

    pressure_map: dict[tuple[int, str], float] = {}
    window: deque[tuple[int, frozenset[str], bool, float]] = deque()
    for sec in range(sec_lo, sec_hi + 1):
        for p in by_sec.get(sec, []):
            window.append((sec, p.services, p.has_error, float(p.duration_ms)))
        while window and window[0][0] < sec - lookback_sec + 1:
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
            latency_pressure = _clamp(avg_dur / max(1.0, service_p95.get(svc, 1.0)), 0.0, 1.0)
            pressure_map[(sec, svc)] = _clamp(0.70 * err_rate + 0.30 * latency_pressure, 0.0, 1.0)

    def _metric_pressure(p: eps.TracePoint) -> float:
        if not p.services:
            return 0.0
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)
        return max(pressure_map.get((sec, svc), 0.0) for svc in p.services)

    rng = random.Random(seed)
    kept_ids: set[str] = set()
    seen = 0
    eps0 = 1e-6
    seen_win: deque[tuple[int, float, float, float, float]] = deque()

    p_normal = _clamp(beta, 0.0, 0.10)

    for p in traces:
        seen += 1
        sec = int(p.start_ns // 1_000_000_000 - t0_sec)

        while seen_win and seen_win[0][0] < sec - lookback_sec + 1:
            seen_win.popleft()

        has_incident_service = 1.0 if _trace_has_incident_service(p, incident_services) else 0.0
        lat_pressure = 0.7 * _norm(float(p.duration_ms), float(dur_lo), float(dur_hi)) + 0.3 * _norm(float(p.span_count), float(span_lo), float(span_hi))
        metric_pressure = _metric_pressure(p)
        err_flag = 1.0 if p.has_error else 0.0
        if incident_anchor_sec is None:
            time_prox = 0.5
        else:
            time_prox = math.exp(-abs(sec - incident_anchor_sec) / 60.0)

        hist_err = [x[1] for x in seen_win]
        hist_lat = [x[2] for x in seen_win]
        hist_inc = [x[3] for x in seen_win]
        hist_met = [x[4] for x in seen_win]

        mu_err = (sum(hist_err) / len(hist_err)) if hist_err else 0.0
        mu_lat = (sum(hist_lat) / len(hist_lat)) if hist_lat else 0.0
        mu_inc = (sum(hist_inc) / len(hist_inc)) if hist_inc else 0.0
        mu_met = (sum(hist_met) / len(hist_met)) if hist_met else 0.0

        sg_err = _robust_sigma(hist_err)
        sg_lat = _robust_sigma(hist_lat)
        sg_inc = _robust_sigma(hist_inc)
        sg_met = _robust_sigma(hist_met)

        z_err = abs(err_flag - mu_err) / (sg_err + eps0)
        z_lat = abs(lat_pressure - mu_lat) / (sg_lat + eps0)
        z_inc = abs(has_incident_service - mu_inc) / (sg_inc + eps0)
        z_met = abs(metric_pressure - mu_met) / (sg_met + eps0)
        z_mix = 0.32 * z_err + 0.24 * z_lat + 0.20 * z_inc + 0.24 * z_met

        shock = math.tanh(alpha * z_mix)
        base_sys = 0.32 * has_incident_service + 0.18 * lat_pressure + 0.16 * err_flag + 0.10 * time_prox + 0.24 * metric_pressure
        p_s = _clamp(0.52 * base_sys + 0.48 * shock, 0.01, 0.98)

        theta = (len(kept_ids) / seen) if seen > 0 else 0.0
        u = rng.random()
        if theta > beta:
            keep = (u < p_s)
        else:
            keep = (u < p_s) or (u < p_normal)

        if keep:
            kept_ids.add(p.trace_id)

        seen_win.append((sec, err_flag, lat_pressure, has_incident_service, metric_pressure))

    return kept_ids


def _enforce_budget_cap(points: list[eps.TracePoint], kept_ids: set[str], budget_pct: float) -> tuple[set[str], int, int]:
    """Enforce exact trace-count budget by capping kept set with spectrum-aware ordering.

    Priority order when trimming:
    1) preserve error/normal mix close to dataset baseline ratio
    2) within each class: longer duration first
    3) then higher span count first
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
    present_ids = [tid for tid in kept_ids if tid in by_id]
    present_errors = [tid for tid in present_ids if by_id[tid].has_error]
    present_normals = [tid for tid in present_ids if not by_id[tid].has_error]

    present_errors.sort(key=lambda tid: (by_id[tid].duration_ms, by_id[tid].span_count, tid), reverse=True)
    present_normals.sort(key=lambda tid: (by_id[tid].duration_ms, by_id[tid].span_count, tid), reverse=True)

    # Preserve the selected-set spectrum first (pre-cap), not the global dataset baseline.
    target_err_ratio = (len(present_errors) / len(present_ids)) if present_ids else 0.0
    wanted_errors = int(round(target_count * target_err_ratio))
    wanted_errors = max(0, min(wanted_errors, target_count, len(present_errors)))
    wanted_normals = target_count - wanted_errors
    if wanted_normals > len(present_normals):
        deficit = wanted_normals - len(present_normals)
        wanted_normals = len(present_normals)
        wanted_errors = min(len(present_errors), wanted_errors + deficit)

    capped_list = present_errors[:wanted_errors] + present_normals[:wanted_normals]
    if len(capped_list) < target_count:
        leftovers = present_errors[wanted_errors:] + present_normals[wanted_normals:]
        leftovers.sort(key=lambda tid: (by_id[tid].duration_ms, by_id[tid].span_count, tid), reverse=True)
        capped_list.extend(leftovers[: target_count - len(capped_list)])

    capped = set(capped_list)
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


def _metric_dir_from_trace_dir(trace_dir: Path) -> Path:
    return trace_dir.parent / "metric"


def _paper_microrank_reference() -> dict[float, dict[str, float]]:
    # Table-3 MicroRank + TraStrainer row.
    return {
        0.1: {"A@1_pct": 42.59, "A@3_pct": 77.74, "MRR": 0.5509},
        1.0: {"A@1_pct": 45.16, "A@3_pct": 78.52, "MRR": 0.5889},
        2.5: {"A@1_pct": 50.00, "A@3_pct": 82.26, "MRR": 0.6556},
    }


def _paper_table3_reference_rows() -> list[dict[str, float | str]]:
    # Copied from paper Table 3 snapshot used throughout this workspace.
    return [
        {"model": "Random", "a1_0p1": 5.56, "a1_1p0": 16.67, "a1_2p5": 27.78, "a3_0p1": 20.37, "a3_1p0": 50.00, "a3_2p5": 61.11, "mrr_0p1": 0.1571, "mrr_1p0": 0.3423, "mrr_2p5": 0.4352},
        {"model": "HC", "a1_0p1": 7.41, "a1_1p0": 18.52, "a1_2p5": 22.22, "a3_0p1": 27.78, "a3_1p0": 46.30, "a3_2p5": 51.85, "mrr_0p1": 0.1954, "mrr_1p0": 0.3398, "mrr_2p5": 0.3731},
        {"model": "Sifter", "a1_0p1": 5.56, "a1_1p0": 18.52, "a1_2p5": 27.78, "a3_0p1": 23.42, "a3_1p0": 46.30, "a3_2p5": 61.11, "mrr_0p1": 0.1605, "mrr_1p0": 0.3414, "mrr_2p5": 0.4358},
        {"model": "Sieve", "a1_0p1": 9.26, "a1_1p0": 25.83, "a1_2p5": 35.19, "a3_0p1": 20.37, "a3_1p0": 58.15, "a3_2p5": 62.96, "mrr_0p1": 0.1657, "mrr_1p0": 0.4246, "mrr_2p5": 0.4963},
        {"model": "TraStrainer w/o M", "a1_0p1": 12.96, "a1_1p0": 16.67, "a1_2p5": 24.07, "a3_0p1": 42.59, "a3_1p0": 42.59, "a3_2p5": 55.56, "mrr_0p1": 0.2994, "mrr_1p0": 0.3241, "mrr_2p5": 0.4012},
        {"model": "TraStrainer w/o D", "a1_0p1": 29.63, "a1_1p0": 42.59, "a1_2p5": 46.30, "a3_0p1": 74.04, "a3_1p0": 68.52, "a3_2p5": 72.22, "mrr_0p1": 0.5228, "mrr_1p0": 0.5463, "mrr_2p5": 0.5509},
        {"model": "TraStrainer", "a1_0p1": 42.59, "a1_1p0": 45.16, "a1_2p5": 50.00, "a3_0p1": 77.74, "a3_1p0": 78.52, "a3_2p5": 82.26, "mrr_0p1": 0.5509, "mrr_1p0": 0.5889, "mrr_2p5": 0.6556},
    ]


def _known_ours_reference_rows() -> list[dict[str, float | str]]:
    # Preserved rows from prior verified comparison files.
    return [
        {"model": "Ours (MicroRank + Hybrid c0.7 m2)", "a1_0p1": 9.25, "a1_1p0": 20.00, "a1_2p5": 17.69, "a3_0p1": 39.18, "a3_1p0": 40.81, "a3_2p5": 37.36, "mrr_0p1": 0.2981, "mrr_1p0": 0.3736, "mrr_2p5": 0.3502},
        {"model": "Ours (MicroRank + Stream v1 strictfix)", "a1_0p1": 10.04, "a1_1p0": 15.26, "a1_2p5": 17.34, "a3_0p1": 43.99, "a3_1p0": 48.90, "a3_2p5": 48.90, "mrr_0p1": 0.3255, "mrr_1p0": 0.3655, "mrr_2p5": 0.3823},
        {"model": "Ours (MicroRank + stream-v2)", "a1_0p1": 15.87, "a1_1p0": 9.80, "a1_2p5": 11.81, "a3_0p1": 57.94, "a3_1p0": 21.41, "a3_2p5": 25.57, "mrr_0p1": 0.3944, "mrr_1p0": 0.2180, "mrr_2p5": 0.2613},
    ]


def _current_ours_table3_row(compare_avg_rows: list[dict[str, Any]], selection_mode: str, tag: str) -> dict[str, float | str]:
    by_budget = {float(r["budget_pct"]): r for r in compare_avg_rows}

    def _val(budget: float, key: str, scale: float = 1.0) -> float:
        r = by_budget.get(budget)
        if not r or r.get(key) is None:
            return 0.0
        return float(r[key]) * scale

    mode_label = selection_mode
    tag_lc = str(tag).lower().replace("-", "_")
    if selection_mode == "stream-v2":
        v2_minor: str | None = None
        pivot = tag_lc.find("v2_")
        if pivot >= 0:
            i = pivot + 3
            digits = []
            while i < len(tag_lc) and tag_lc[i].isdigit():
                digits.append(tag_lc[i])
                i += 1
            if digits:
                v2_minor = "".join(digits)
        if v2_minor is not None:
            mode_label = f"stream-v2.{v2_minor}"

    return {
        "model": f"Ours (MicroRank + {mode_label})",
        "a1_0p1": _val(0.1, "ours_avg_A@1_pct"),
        "a1_1p0": _val(1.0, "ours_avg_A@1_pct"),
        "a1_2p5": _val(2.5, "ours_avg_A@1_pct"),
        "a3_0p1": _val(0.1, "ours_avg_A@3_pct"),
        "a3_1p0": _val(1.0, "ours_avg_A@3_pct"),
        "a3_2p5": _val(2.5, "ours_avg_A@3_pct"),
        "mrr_0p1": _val(0.1, "ours_avg_MRR"),
        "mrr_1p0": _val(1.0, "ours_avg_MRR"),
        "mrr_2p5": _val(2.5, "ours_avg_MRR"),
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
        choices=["adaptive", "ranked", "ranked-v11", "ranked-v11-quota", "ranked-v11-stochastic", "ranked-v11-hybrid", "stream-v1", "stream-v2", "stream-v2-lite", "stream-v2-fusion", "stream-v3", "stream-v3-composite", "stream-v3-composite-strictcap", "stream-v3.8-composite-anomaly", "stream-v3.9-composite-anomaly", "stream-v3.10-composite-anomaly", "stream-v4.8-causal-booster", "stream-v4.0-real-metrics", "stream-v4.1-anomaly-precedence", "stream-v4.2-trend-precursor", "stream-v4.3-service-first", "stream-v4.4-reserve-channel", "stream-v4.5-hybrid-rerank"],
        default="adaptive",
        help="adaptive: existing target_tps simulator; ranked: error/context split ranking; ranked-v11: formula-based global ranking; ranked-v11-quota: formula ranking with hard error/normal partition; ranked-v11-stochastic: calibrated probabilistic sampling; ranked-v11-hybrid: deterministic core + stochastic edge exploration; stream-v1: online dynamic voting with AND/OR gate; stream-v2: robust trace-only v2.7 baseline; stream-v2-lite: p_s-only + tiny random normal breathing hole (no p_d/clustering); stream-v2-fusion: v2 core + hybrid context fusion; stream-v3: V2 core + OR soft-quota 70/30 (no hard strict-cap); stream-v3-composite/stream-v3-composite-strictcap: V3 + coverage-aware metric modulation + deterministic cap; stream-v3.8-composite-anomaly: v3.8 Method 3; stream-v3.9-composite-anomaly: v3.9 anomaly-modulated diversity; stream-v3.10-composite-anomaly: v3.10 anomaly warmup + multiplicative gate + unlocked gamma; stream-v4.8-causal-booster: v3.5 plus streaming causal graph booster; stream-v4.0-real-metrics: v3.5 plus real paper metrics distress fusion; stream-v4.1-anomaly-precedence: v3.5 plus real metrics anomaly precedence reservation; stream-v4.3-service-first: paper-grade service-first dual-channel voting with 60/40 quota; stream-v4.4-reserve-channel: frozen v3.5 main channel with metric reserve add-on (80/20); stream-v4.5-hybrid-rerank: frozen v3.5 sampling plus metric-aware deterministic pre-RCA rerank",
    )
    ap.add_argument(
        "--v39-ablation",
        choices=["none", "ps-only", "gamma-only", "full"],
        default="full",
        help="When selection-mode=stream-v3.9-composite-anomaly: choose anomaly modulation ablation",
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
    ap.add_argument(
        "--stream-v2-metric-weight",
        type=float,
        default=0.0,
        help="When selection-mode=stream-v2/stream-v2-fusion, blend weight for service-metric pressure in p_s (0.0-0.6)",
    )
    args = ap.parse_args()

    # Always append an execution timestamp so all outputs are uniquely trackable.
    run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    args.tag = f"{args.tag}-{run_stamp}"

    root = Path(args.root).resolve()
    budgets = [float(x.strip()) for x in args.budgets.split(",") if x.strip()]
    selected = {x.strip() for x in args.datasets.split(",") if x.strip()}
    specs = [s for s in _dataset_specs(root) if s.name in selected]

    if not specs:
        raise SystemExit("No dataset selected. Check --datasets")

    benchmark_rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    sampling_quality_rows: list[dict[str, Any]] = []
    invalid_run_reasons: list[str] = []

    for spec in specs:
        print(f"\\n== DATASET: {spec.name} ==")
        labels = eps.load_labels(spec.label_file)
        points, _ = eps.build_trace_points(spec.trace_dir, labels, args.before_sec, args.after_sec)
        base_scenarios = _load_jsonl(spec.base_scenario_file)
        scenario_windows = _scenario_windows(base_scenarios)
        incident_services = _incident_services_from_labels(spec.label_file)
        t0_sec = min(p.start_ns for p in points) // 1_000_000_000 if points else 0
        incident_anchor_sec = (min(ts for ts, _svc in labels) - t0_sec) if labels else None

        for budget in budgets:
            budget_slug = _safe_slug(f"{budget:.3f}pct")
            print(f"  -- budget={budget}%")
            v43_debug_logs: list[dict[str, Any]] | None = None
            v44_debug_logs: list[dict[str, Any]] | None = None
            v45_debug_logs: list[dict[str, Any]] | None = None
            kept_ordered_ids: list[str] | None = None

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
            elif args.selection_mode == "stream-v2":
                target_tps = 0.0
                kept_ids = _stream_v2_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    metric_weight=args.stream_v2_metric_weight,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v2-lite":
                target_tps = 0.0
                kept_ids = _stream_v2_lite_select_ids(
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
            elif args.selection_mode == "stream-v2-fusion":
                target_tps = 0.0
                kept_ids = _stream_v2_fusion_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    core_ratio=args.hybrid_core_ratio,
                    edge_pool_multiplier=args.hybrid_edge_pool_multiplier,
                    seed=args.stochastic_seed,
                    metric_weight=args.stream_v2_metric_weight,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v3":
                target_tps = 0.0
                kept_ids = _stream_v3_select_ids(
                    points=points,
                    budget_pct=budget,
                    incident_services=incident_services,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    online_soft_cap=(args.budget_mode != "strict"),
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode in {"stream-v3-composite", "stream-v3-composite-strictcap"}:
                # Use the composite sampler implemented in streamv3_composite_strictcap.py
                target_tps = 0.0
                preference_vector, metric_stats = sv3comp.build_metric_inputs(points)

                min_floor = 1
                kept_ids = sv3comp._composite_v3_metrics_strictcap(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v3.8-composite-anomaly":
                # Use the v3.8 composite sampler: v3.5 + Method 3 anomaly scoring.
                target_tps = 0.0
                preference_vector, metric_stats = sv38comp.build_metric_inputs(points)

                min_floor = 1
                kept_ids = sv38comp._composite_v3_metrics_strictcap(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v3.9-composite-anomaly":
                # Use the v3.9 composite sampler: anomaly modulation in constraint space.
                target_tps = 0.0
                preference_vector, metric_stats = sv39comp.build_metric_inputs(points)

                min_floor = 1
                kept_ids = sv39comp._composite_v3_metrics_strictcap(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    ablation_mode=args.v39_ablation,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v3.10-composite-anomaly":
                # Use the v3.10 composite sampler: faster anomaly warmup + multiplicative boost + unlocked gamma path.
                target_tps = 0.0
                preference_vector, metric_stats = sv310comp.build_metric_inputs(points)

                min_floor = 1
                kept_ids = sv310comp._composite_v3_metrics_strictcap(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.8-causal-booster":
                # Use the v4.8 add-on wrapper: frozen v3.5 + streaming causal graph booster.
                target_tps = 0.0
                preference_vector, metric_stats = sv48comp.build_metric_inputs(points)
                metrics_stream_by_sec = sv48comp.build_metrics_stream_proxy(points)

                min_floor = 1
                kept_ids = sv48comp._composite_v48_streaming_causal_booster(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.0-real-metrics":
                # Use v4.0 sampler: frozen v3.5 with real paper metrics fusion.
                target_tps = 0.0
                preference_vector, metric_stats = sv40comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv40comp.build_real_metrics_stream(str(metric_dir))

                min_floor = 1
                kept_ids = sv40comp._composite_v40_real_metrics_sampling(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.1-anomaly-precedence":
                # Use v4.1 sampler: v3.5 + real metrics anomaly precedence reservation.
                target_tps = 0.0
                preference_vector, metric_stats = sv41comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv41comp.build_real_metrics_stream(str(metric_dir))

                min_floor = 1
                kept_ids = sv41comp._composite_v41_anomaly_precedence_sampler(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                    precedence_threshold=0.70,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.2-trend-precursor":
                # v4.2: Option 1 (temporal trend slope) + Option 3 (incident precursor window)
                target_tps = 0.0
                preference_vector, metric_stats = sv42comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv42comp.build_real_metrics_stream(str(metric_dir))

                min_floor = 1
                kept_ids = sv42comp._composite_v42_trend_precursor_sampler(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                    precedence_threshold=0.65,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.3-service-first":
                # v4.3: service-first system channel + diversity channel + adaptive OR/AND voting.
                target_tps = 0.0
                preference_vector, metric_stats = sv43comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv43comp.build_real_metrics_stream(str(metric_dir))
                v43_debug_logs = []

                min_floor = 1
                kept_ids = sv43comp._composite_v43_service_first_dual_channel(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                    debug_trace_logs=v43_debug_logs,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.4-reserve-channel":
                # v4.4: frozen v3.5 main channel (80%) + deterministic metric reserve channel (20%).
                target_tps = 0.0
                preference_vector, metric_stats = sv44comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv44comp.build_real_metrics_stream(str(metric_dir))
                v44_debug_logs = []

                min_floor = 1
                kept_ids = sv44comp._composite_v44_reserve_channel_sampler(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                    debug_trace_logs=v44_debug_logs,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {
                    "incident_capture_latency_ms": None,
                    "threshold_volatility_pct": None,
                    "max_step_change_pct": None,
                }
            elif args.selection_mode == "stream-v4.5-hybrid-rerank":
                # v4.5: frozen v3.5 sampling then metric-aware deterministic pre-RCA rerank.
                target_tps = 0.0
                preference_vector, metric_stats = sv45comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                metrics_stream_by_sec = sv45comp.build_real_metrics_stream(str(metric_dir))
                v45_debug_logs = []

                min_floor = 1
                kept_ordered_ids = sv45comp._composite_v45_hybrid_rerank(
                    points=points,
                    budget_pct=budget,
                    preference_vector=preference_vector,
                    metric_stats=metric_stats,
                    scenario_windows=scenario_windows,
                    incident_anchor_sec=incident_anchor_sec,
                    seed=args.stochastic_seed,
                    incident_services=incident_services,
                    min_incident_traces_per_scenario=min_floor,
                    online_soft_cap=(args.budget_mode != "strict"),
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=20,
                    debug_trace_logs=v45_debug_logs,
                )
                kept_ids = set(kept_ordered_ids)
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                target_trace_count = len(kept_ids)
                pre_cap_trace_count = len(kept_ids)
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
                if args.selection_mode == "stream-v3":
                    kept_ids, target_trace_count, pre_cap_trace_count, scenario_floor_fail_count = _enforce_budget_cap_per_scenario(
                        points,
                        kept_ids,
                        budget,
                        scenario_windows,
                        min_incident_traces_per_scenario=1,
                    )
                else:
                    kept_ids, target_trace_count, pre_cap_trace_count = _enforce_budget_cap(points, kept_ids, budget)
                    scenario_floor_fail_count = 0
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
            else:
                scenario_floor_fail_count = 0

            sampled_trace_dir = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "trace"
            kept_rank = None
            if kept_ordered_ids is not None:
                kept_rank = {tid: idx for idx, tid in enumerate(kept_ordered_ids)}

            total_rows, kept_rows = _materialize_sampled_trace_dir(
                spec.trace_dir,
                sampled_trace_dir,
                kept_ids,
                kept_rank=kept_rank,
            )

            if v43_debug_logs is not None:
                v43_log_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "v43-trace-debug.jsonl"
                _write_jsonl(v43_log_file, v43_debug_logs)

            if v44_debug_logs is not None:
                v44_log_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "v44-trace-debug.jsonl"
                _write_jsonl(v44_log_file, v44_debug_logs)

            if v45_debug_logs is not None:
                v45_log_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "v45-rerank-debug.jsonl"
                _write_jsonl(v45_log_file, v45_debug_logs)

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
            adapter_summary_file = root / "reports/compare" / (
                f"rca-engine-adapter-{sampled_dataset_name}-coverage-first-{sampled_eval_tag}.json"
            )
            adapter = json.loads(adapter_summary_file.read_text(encoding="utf-8")) if adapter_summary_file.exists() else {}
            parse_fail_count = int(adapter.get("parse_fail_count") or 0)
            ok_count = int(adapter.get("ok_count") or 0)
            expected_scenarios = len(base_scenarios)

            if args.budget_mode == "strict" and parse_fail_count > 0:
                invalid_run_reasons.append(
                    f"{spec.name} budget={budget}% parse_fail_count={parse_fail_count} ok_count={ok_count} expected={expected_scenarios}"
                )

            benchmark_rows.append(
                {
                    "dataset": spec.name,
                    "budget_pct": budget,
                    "budget_slug": budget_slug,
                    "expected_scenarios": expected_scenarios,
                    "scenario_count": sampled.get("scenario_count"),
                    "adapter_ok_count": ok_count,
                    "adapter_parse_fail_count": parse_fail_count,
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
                    "scenario_floor_fail_count": scenario_floor_fail_count,
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
                    "scenario_floor_fail_count": scenario_floor_fail_count,
                    "expected_scenarios": expected_scenarios,
                    "adapter_ok_count": ok_count,
                    "adapter_parse_fail_count": parse_fail_count,
                    "phase1_summary_file": str(phase1_summary_file),
                }
            )

    if args.budget_mode == "strict" and invalid_run_reasons:
        print("\\nINVALID STRICT BENCHMARK: parse_fail detected. Aborting report generation.")
        for r in invalid_run_reasons:
            print(f"- {r}")
        raise SystemExit(2)

    # Output 1: benchmark table (teacher-facing, paper-table style with richer columns).
    benchmark_csv = root / "reports/compare" / f"rca-paper-table-sampled-budgets-{args.tag}.csv"
    benchmark_csv.parent.mkdir(parents=True, exist_ok=True)
    b_fields = list(benchmark_rows[0].keys()) if benchmark_rows else []
    with benchmark_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=b_fields)
        w.writeheader()
        for row in benchmark_rows:
            w.writerow(row)

    # Output 2: averaged compare CSV (macro over datasets per budget) for post-processing.
    compare_avg_rows: list[dict[str, Any]] = []
    by_budget: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for row in compare_rows:
        by_budget[float(row["budget_pct"])].append(row)

    for budget in sorted(by_budget.keys()):
        rows_b = by_budget[budget]
        n = len(rows_b)
        if n <= 0:
            continue

        paper_a1 = float(rows_b[0]["paper_A@1_pct"] or 0.0)
        paper_a3 = float(rows_b[0]["paper_A@3_pct"] or 0.0)
        paper_mrr = float(rows_b[0]["paper_MRR"] or 0.0)

        ours_a1 = sum(float(r["ours_A@1_pct"] or 0.0) for r in rows_b) / n
        ours_a3 = sum(float(r["ours_A@3_pct"] or 0.0) for r in rows_b) / n
        ours_mrr = sum(float(r["ours_MRR"] or 0.0) for r in rows_b) / n
        scenario_total = sum(int(r0.get("scenario_count") or 0) for r0 in benchmark_rows if float(r0["budget_pct"]) == budget)

        compare_avg_rows.append(
            {
                "budget_pct": budget,
                "dataset_count": n,
                "scenario_total": scenario_total,
                "paper_A@1_pct": paper_a1,
                "paper_A@3_pct": paper_a3,
                "paper_MRR": paper_mrr,
                "ours_avg_A@1_pct": ours_a1,
                "ours_avg_A@3_pct": ours_a3,
                "ours_avg_MRR": ours_mrr,
                "delta_A@1_pct": ours_a1 - paper_a1,
                "delta_A@3_pct": ours_a3 - paper_a3,
                "delta_MRR": ours_mrr - paper_mrr,
            }
        )

    compare_csv = root / "reports/compare" / f"rca-paper-vs-ours-microrank-budgets-{args.tag}.csv"
    c_fields = list(compare_avg_rows[0].keys()) if compare_avg_rows else []
    with compare_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=c_fields)
        w.writeheader()
        for row in compare_avg_rows:
            w.writerow(row)

    # Output 3 (primary markdown #1): per-dataset per-budget benchmark with paper delta.
    benchmark_md = root / "reports/compare" / f"rca-dataset-budget-metrics-{args.tag}.md"
    b_lines = []
    b_lines.append(f"# RCA Final Summary ({args.selection_mode})")
    b_lines.append("")
    b_lines.append(f"tag: {args.tag}")
    b_lines.append("")
    b_lines.append("| budget | dataset | scenarios | A@1 | A@3 | MRR |")
    b_lines.append("|---:|---|---:|---:|---:|---:|")

    by_budget_for_md: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for row in benchmark_rows:
        by_budget_for_md[float(row["budget_pct"])].append(row)

    for budget in sorted(by_budget_for_md.keys()):
        rows_b = sorted(by_budget_for_md[budget], key=lambda r: str(r["dataset"]))
        total_sc = 0
        sum_a1 = 0.0
        sum_a3 = 0.0
        sum_mrr = 0.0
        for row in rows_b:
            sc = int(row.get("scenario_count") or 0)
            a1 = float(row.get("A@1") or 0.0)
            a3 = float(row.get("A@3") or 0.0)
            mrr = float(row.get("MRR") or 0.0)
            total_sc += sc
            sum_a1 += a1
            sum_a3 += a3
            sum_mrr += mrr
            b_lines.append(
                "| {budget:.1f}% | {dataset} | {sc} | {a1:.4f} | {a3:.4f} | {mrr:.4f} |".format(
                    budget=budget,
                    dataset=str(row["dataset"]),
                    sc=sc,
                    a1=a1,
                    a3=a3,
                    mrr=mrr,
                )
            )

        n = len(rows_b)
        if n > 0:
            b_lines.append(
                "| {budget:.1f}% | AVERAGE | {sc} | {a1:.4f} | {a3:.4f} | {mrr:.4f} |".format(
                    budget=budget,
                    sc=total_sc,
                    a1=(sum_a1 / n),
                    a3=(sum_a3 / n),
                    mrr=(sum_mrr / n),
                )
            )

    b_lines.extend(
        [
            "",
            "## Related CSV",
            f"- {benchmark_csv}",
        ]
    )
    _write_text(benchmark_md, "\n".join(b_lines) + "\n")

    # Output 5: table3 style summary (paper rows + selected internal baselines + current run).
    known_rows = _known_ours_reference_rows()
    current_row = _current_ours_table3_row(compare_avg_rows, args.selection_mode, args.tag)
    if any(str(r.get("model")) == str(current_row.get("model")) for r in known_rows):
        table3_rows = _paper_table3_reference_rows() + known_rows
    else:
        table3_rows = _paper_table3_reference_rows() + known_rows + [current_row]
    table3_csv = root / "reports/compare" / f"table3-all-models-plus-ours-{run_stamp}.csv"
    with table3_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Model", "A@1 0.1%", "A@1 1.0%", "A@1 2.5%", "A@3 0.1%", "A@3 1.0%", "A@3 2.5%", "MRR 0.1%", "MRR 1.0%", "MRR 2.5%"])
        for r in table3_rows:
            w.writerow(
                [
                    r["model"],
                    f"{float(r['a1_0p1']):.2f}",
                    f"{float(r['a1_1p0']):.2f}",
                    f"{float(r['a1_2p5']):.2f}",
                    f"{float(r['a3_0p1']):.2f}",
                    f"{float(r['a3_1p0']):.2f}",
                    f"{float(r['a3_2p5']):.2f}",
                    f"{float(r['mrr_0p1']):.4f}",
                    f"{float(r['mrr_1p0']):.4f}",
                    f"{float(r['mrr_2p5']):.4f}",
                ]
            )

    table3_md = root / "reports/compare" / f"table3-all-models-plus-ours-{run_stamp}.md"
    t_lines = []
    t_lines.append("# Table 3 Style Comparison: All Paper Models + Ours")
    t_lines.append("")
    t_lines.append(f"generated_at: {run_stamp}")
    t_lines.append("")
    t_lines.append("| Model | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |")
    t_lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in table3_rows:
        t_lines.append(
            "| {model} | {a1_0p1:.2f} | {a1_1p0:.2f} | {a1_2p5:.2f} | {a3_0p1:.2f} | {a3_1p0:.2f} | {a3_2p5:.2f} | {mrr_0p1:.4f} | {mrr_1p0:.4f} | {mrr_2p5:.4f} |".format(
                model=str(r["model"]),
                a1_0p1=float(r["a1_0p1"]),
                a1_1p0=float(r["a1_1p0"]),
                a1_2p5=float(r["a1_2p5"]),
                a3_0p1=float(r["a3_0p1"]),
                a3_1p0=float(r["a3_1p0"]),
                a3_2p5=float(r["a3_2p5"]),
                mrr_0p1=float(r["mrr_0p1"]),
                mrr_1p0=float(r["mrr_1p0"]),
                mrr_2p5=float(r["mrr_2p5"]),
            )
        )
    t_lines.extend(
        [
            "",
            "## Notes",
            "- Paper model rows are copied from Table 3 screenshot provided in this chat.",
            "- Ours reference rows (Hybrid, Stream v1 strictfix, stream-v2 milestone) are preserved from prior comparison artifacts.",
            f"- Current run row is generated automatically from this run with selection-mode={args.selection_mode}.",
        ]
    )
    _write_text(table3_md, "\n".join(t_lines) + "\n")

    # Keep detailed quality CSV for deeper diagnostics (no additional markdown output).
    quality_csv = root / "reports/compare" / f"sampling-quality-key-metrics-{args.tag}.csv"
    q_fields = list(sampling_quality_rows[0].keys()) if sampling_quality_rows else []
    with quality_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=q_fields)
        w.writeheader()
        for row in sampling_quality_rows:
            w.writerow(row)

    print(f"benchmark_csv={benchmark_csv}")
    print(f"compare_csv={compare_csv}")
    print(f"benchmark_md={benchmark_md}")
    print(f"table3_csv={table3_csv}")
    print(f"table3_md={table3_md}")
    print(f"quality_csv={quality_csv}")
    print("primary_markdown_outputs=2")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
