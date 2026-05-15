from __future__ import annotations

import csv
import json
import math
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import scripts.outputAndEvaluation.evaluate_paper_sampling as eps
    from scripts.prepareData.build_scenarios_from_trastrainer_labels import build_rows as build_scenario_rows
except ModuleNotFoundError:
    import src.scripts.outputAndEvaluation.evaluate_paper_sampling as eps
    from src.scripts.prepareData.build_scenarios_from_trastrainer_labels import build_rows as build_scenario_rows

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

def _ensure_base_scenario_file(spec: DatasetSpec, before_sec: int, after_sec: int) -> None:
    if spec.base_scenario_file.exists():
        return

    labels = json.loads(spec.label_file.read_text(encoding="utf-8"))
    if not isinstance(labels, dict):
        raise ValueError(
            f"Unsupported label format for dataset {spec.name}: expected object in {spec.label_file}"
        )

    metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
    rows = build_scenario_rows(
        labels=labels,
        dataset_name=spec.name,
        trace_root=str(spec.trace_dir),
        metric_root=str(metric_dir),
        before_sec=before_sec,
        after_sec=after_sec,
    )

    spec.base_scenario_file.parent.mkdir(parents=True, exist_ok=True)
    with spec.base_scenario_file.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(
        f"[DATASET-BOOTSTRAP] generated base scenarios for {spec.name}: {spec.base_scenario_file}"
    )

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
