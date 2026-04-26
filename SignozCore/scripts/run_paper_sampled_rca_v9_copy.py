from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import evaluate_paper_sampling as eps
import streamv39_tuned_rca as sv39tunedcomp
import streamv3942_metric_native_signed_contrast as sv3942comp

# DatasetSpec
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

# build scenario-window from base scenario file in analysis.
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

# load_json 
def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out

# Hanlde budget cap with strict global cap and per-scenario floor.
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

#  Get trace rows for kept IDs and write to output dir, returning total and kept row counts.
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

# build incident services set from label file.
def _incident_services_from_labels(label_file: Path) -> set[str]:
    labels = eps.load_labels(label_file)
    return {svc for _ts, svc in labels}

# Check if trace has any service in the incident services set.
def _trace_has_incident_service(p: eps.TracePoint, incident_services: set[str]) -> bool:
    if not incident_services:
        return False
    return bool(set(p.services) & incident_services)

# Calculate incident service coverage: how many traces with incident services are kept, total, and percentage.
def _incident_service_coverage(points: list[eps.TracePoint], kept_ids: set[str], incident_services: set[str]) -> tuple[int, int, float | None]:
    if not incident_services:
        return 0, 0, None

    base_ids = {p.trace_id for p in points if _trace_has_incident_service(p, incident_services)}
    if not base_ids:
        return 0, 0, None

    kept = len(base_ids & kept_ids)
    pct = (kept / len(base_ids)) * 100.0
    return kept, len(base_ids), pct

# Calculate endpoint retention: count kept traces, total traces, and retention percentage, sorted.
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

# Best-effort endpoint retention table from OperationName.
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


# Calculate average trace rate in traces per second based on start times of trace points.
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
    {
        "model": "Ours (MicroRank-v3.5-metrics-proxy-121427)",
        "a1_0p1": 23.37,
        "a1_1p0": 26.12,
        "a1_2p5": 18.50,
        "a3_0p1": 46.49,
        "a3_1p0": 56.41,
        "a3_2p5": 53.29,
        "mrr_0p1": 0.4105,
        "mrr_1p0": 0.4443,
        "mrr_2p5": 0.3988,
    },
    {
        "model": "Ours (stream-v3.94-consensus-ensemble)",
        "a1_0p1": 25.80,
        "a1_1p0": 25.08,
        "a1_2p5": 20.47,
        "a3_0p1": 47.53,
        "a3_1p0": 53.41,
        "a3_2p5": 51.33,
        "mrr_0p1": 0.4275,
        "mrr_1p0": 0.4342,
        "mrr_2p5": 0.4078,
    },
    {
        "model": "Ours-v3.7-rca-aware",
        "a1_0p1": 24.75,
        "a1_1p0": 25.08,
        "a1_2p5": 16.77,
        "a3_0p1": 46.49,
        "a3_1p0": 54.45,
        "a3_2p5": 49.37,
        "mrr_0p1": 0.4168,
        "mrr_1p0": 0.4325,
        "mrr_2p5": 0.3836,
    },
    {
        "model": "Ours (v3.94.2-metric-native-signed-contrast-1)",
        "a1_0p1": 36.91,
        "a1_1p0": 16.89,
        "a1_2p5": 13.89,
        "a3_0p1": 48.82,
        "a3_1p0": 46.83,
        "a3_2p5": 39.44,
        "mrr_0p1": 0.4946,
        "mrr_1p0": 0.3590,
        "mrr_2p5": 0.3309,
    },
    {
        "model": "Ours (v3.94.2-metric-native-signed-contrast-4)",
        "a1_0p1": 34.35,
        "a1_1p0": 17.93,
        "a1_2p5": 14.81,
        "a3_0p1": 46.73,
        "a3_1p0": 44.53,
        "a3_2p5": 37.13,
        "mrr_0p1": 0.4682,
        "mrr_1p0": 0.3650,
        "mrr_2p5": 0.3310,
    },
]


def _current_ours_table3_row(compare_avg_rows: list[dict[str, Any]], selection_mode: str, tag: str) -> dict[str, float | str]:
    model_name = f"Ours (MicroRank + {selection_mode})"
    if selection_mode == "v52_oracle_metrics_upper_bound":
        model_name = "Ours-v5.2-oracle-metrics-upper-bound"

    by_budget = {float(r["budget_pct"]): r for r in compare_avg_rows}

    def _val(budget: float, key: str, scale: float = 1.0) -> float:
        r = by_budget.get(budget)
        if not r or r.get(key) is None:
            return 0.0
        return float(r[key]) * scale

    mode_label = selection_mode
    if selection_mode == "v52_oracle_metrics_upper_bound":
        mode_label = "Ours-v5.2-oracle-metrics-upper-bound"
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
        "model": model_name,
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
        default="adaptive",
        help="Sampling strategy (includes V13 mode stream-v13-v36-swap-contrast, V12lite mode stream-v12-lite-bias-align, V11 modes stream-v11-rca-aware/stream-v11-no-rca-term/stream-v11-fixed-guard/stream-v11-adaptive-guard/stream-v11-evidence-rescale, and v394.2 mode stream-v3.94.2-metric-native-signed-contrast)",
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
        "--v39-rca-weight",
        type=float,
        default=0.40,
        help="When selection-mode=stream-v3.9-tuned-rca, strength of RCA-contrast boost to p_s",
    )
 
    ap.add_argument(
        "--v3942-min-consensus",
        type=int,
        default=2,
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, minimum seed vote count required for consensus pool",
    )
    ap.add_argument(
        "--v3942-enable-inner-real-metrics",
        action="store_true",
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, enable old inner v3.9 real-metric distress path (default disabled)",
    )
    ap.add_argument(
        "--v3942-enable-swap-refine",
        action="store_true",
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, enable optional bounded swap refine",
    )
    ap.add_argument(
        "--v3942-min-normal-ratio",
        type=float,
        default=None,
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, override minimum normal ratio floor (default budget-adaptive)",
    )
    ap.add_argument(
        "--v3942-min-error-ratio",
        type=float,
        default=None,
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, override minimum error ratio floor (default budget-adaptive)",
    )
    ap.add_argument(
        "--v3942-signed-metric-gain",
        type=float,
        default=None,
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, gain override for signed metric logit modulation (default is budget-adaptive)",
    )
    ap.add_argument(
        "--v3942-metric-tau-mode",
        default="budget-percentile",
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, tau mode for signed calibration (budget-percentile|median|pXX)",
    )
    ap.add_argument(
        "--v3942-metric-scale-mode",
        default="robust",
        help="When selection-mode=stream-v3.94.2-metric-native-signed-contrast, scale mode for signed calibration (robust|mad|iqr|std)",
    )
  
    ap.add_argument(
        "--v8-metric-lookback-sec",
        type=int,
        default=120,
        help="When selection-mode=stream-v8-metric-aware-v35, lookback window for timestamp alignment",
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
         
            kept_ordered_ids: list[str] | None = None

            # Phase 1: calibrate simulator to requested budget and materialize sampled traces.
            target_trace_count = None
            pre_cap_trace_count = None

           
            if args.selection_mode == "stream-v3.94.2-metric-native-signed-contrast":
                target_tps = 0.0
                preference_vector, metric_stats = sv3942comp.build_metric_inputs(points)
                metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
                # metrics_stream_by_sec = sv39tunedcomp.build_real_metrics_stream(str(metric_dir))
                metrics_stream_by_sec = sv3942comp.build_real_metrics_stream(str(metric_dir))

                min_floor = 1
                kept_ids = sv3942comp._composite_v3942_metric_native_signed_contrast(
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
                    rca_weight=args.v39_rca_weight,
                    metrics_stream_by_sec=metrics_stream_by_sec,
                    metric_lookback_sec=args.v8_metric_lookback_sec,
                    min_consensus=args.v3942_min_consensus,
                    enable_swap_refine=args.v3942_enable_swap_refine,
                    enable_inner_real_metrics=args.v3942_enable_inner_real_metrics,
                    min_normal_ratio=args.v3942_min_normal_ratio,
                    min_error_ratio=args.v3942_min_error_ratio,
                    signed_metric_gain=args.v3942_signed_metric_gain,
                    metric_tau_mode=args.v3942_metric_tau_mode,
                    metric_scale_mode=args.v3942_metric_scale_mode,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
                pre_cap_keep_pct = achieved_keep_pct
                sim_metrics = {"incident_capture_latency_ms": None, "threshold_volatility_pct": None, "max_step_change_pct": None}
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
    
    _write_text(table3_md, "\n".join(t_lines) + "\n")

    if args.selection_mode == "v52_oracle_metrics_upper_bound":
        trace_only_row = None
        for r in known_rows:
            if str(r.get("model")) == "Ours (MicroRank + stream-v2)":
                trace_only_row = r
                break
        if trace_only_row is not None:
            def _delta_row(metric_prefix: str) -> tuple[float, float, float]:
                return (
                    float(current_row[f"{metric_prefix}_0p1"]) - float(trace_only_row[f"{metric_prefix}_0p1"]),
                    float(current_row[f"{metric_prefix}_1p0"]) - float(trace_only_row[f"{metric_prefix}_1p0"]),
                    float(current_row[f"{metric_prefix}_2p5"]) - float(trace_only_row[f"{metric_prefix}_2p5"]),
                )

            da1 = _delta_row("a1")
            da3 = _delta_row("a3")
            dmrr = _delta_row("mrr")
            gain_a1_0p1 = da1[0]

            if gain_a1_0p1 > 10.0:
                interpretation = "signal quality is the primary bottleneck"
            elif gain_a1_0p1 < 5.0:
                interpretation = "sampler architecture remains bottlenecked"
            else:
                interpretation = "both signal quality and sampler architecture contribute"

            ablation_md = root / "reports/compare" / f"oracle-upper-bound-ablation-{args.tag}.md"
            a_lines = [
                "# Oracle Metrics Upper-Bound Ablation",
                "",
                f"tag: {args.tag}",
                "",
                "| Variant | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
                "| Trace-only | {a1_0p1:.2f} | {a1_1p0:.2f} | {a1_2p5:.2f} | {a3_0p1:.2f} | {a3_1p0:.2f} | {a3_2p5:.2f} | {mrr_0p1:.4f} | {mrr_1p0:.4f} | {mrr_2p5:.4f} |".format(
                    a1_0p1=float(trace_only_row["a1_0p1"]),
                    a1_1p0=float(trace_only_row["a1_1p0"]),
                    a1_2p5=float(trace_only_row["a1_2p5"]),
                    a3_0p1=float(trace_only_row["a3_0p1"]),
                    a3_1p0=float(trace_only_row["a3_1p0"]),
                    a3_2p5=float(trace_only_row["a3_2p5"]),
                    mrr_0p1=float(trace_only_row["mrr_0p1"]),
                    mrr_1p0=float(trace_only_row["mrr_1p0"]),
                    mrr_2p5=float(trace_only_row["mrr_2p5"]),
                ),
                "| Oracle-metrics upper-bound | {a1_0p1:.2f} | {a1_1p0:.2f} | {a1_2p5:.2f} | {a3_0p1:.2f} | {a3_1p0:.2f} | {a3_2p5:.2f} | {mrr_0p1:.4f} | {mrr_1p0:.4f} | {mrr_2p5:.4f} |".format(
                    a1_0p1=float(current_row["a1_0p1"]),
                    a1_1p0=float(current_row["a1_1p0"]),
                    a1_2p5=float(current_row["a1_2p5"]),
                    a3_0p1=float(current_row["a3_0p1"]),
                    a3_1p0=float(current_row["a3_1p0"]),
                    a3_2p5=float(current_row["a3_2p5"]),
                    mrr_0p1=float(current_row["mrr_0p1"]),
                    mrr_1p0=float(current_row["mrr_1p0"]),
                    mrr_2p5=float(current_row["mrr_2p5"]),
                ),
                "| Delta | {a1_0p1:+.2f} | {a1_1p0:+.2f} | {a1_2p5:+.2f} | {a3_0p1:+.2f} | {a3_1p0:+.2f} | {a3_2p5:+.2f} | {mrr_0p1:+.4f} | {mrr_1p0:+.4f} | {mrr_2p5:+.4f} |".format(
                    a1_0p1=da1[0],
                    a1_1p0=da1[1],
                    a1_2p5=da1[2],
                    a3_0p1=da3[0],
                    a3_1p0=da3[1],
                    a3_2p5=da3[2],
                    mrr_0p1=dmrr[0],
                    mrr_1p0=dmrr[1],
                    mrr_2p5=dmrr[2],
                ),
                "",
                f"Upper-bound gain over trace-only = {gain_a1_0p1:.2f}",
                f"Interpretation: {interpretation}",
            ]
            _write_text(ablation_md, "\n".join(a_lines) + "\n")
            print(f"oracle_ablation_md={ablation_md}")
            print(f"Upper-bound gain over trace-only = {gain_a1_0p1:.2f}")
            print(f"Interpretation: {interpretation}")

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
