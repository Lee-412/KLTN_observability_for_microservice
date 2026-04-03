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
import subprocess
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


def _incident_service_coverage(points: list[eps.TracePoint], kept_ids: set[str], incident_services: set[str]) -> tuple[int, int, float | None]:
    if not incident_services:
        return 0, 0, None

    base_ids = {p.trace_id for p in points if p.root_service in incident_services}
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
            target_tps, kept_ids, achieved_keep_pct = _calibrate_target_tps(points, budget)
            pre_cap_keep_pct = achieved_keep_pct
            target_trace_count = None
            pre_cap_trace_count = None

            if args.budget_mode == "strict":
                kept_ids, target_trace_count, pre_cap_trace_count = _enforce_budget_cap(points, kept_ids, budget)
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0

            _, sim_metrics = eps.simulate_with_metrics(
                points,
                target_tps=target_tps,
                incident_anchor_sec=incident_anchor_sec,
            )

            sampled_trace_dir = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "trace"
            total_rows, kept_rows = _materialize_sampled_trace_dir(spec.trace_dir, sampled_trace_dir, kept_ids)

            kept_ids_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "kept_trace_ids.txt"
            _write_text(kept_ids_file, "\n".join(sorted(kept_ids)) + ("\n" if kept_ids else ""))

            err_total = sum(1 for p in points if p.has_error)
            err_kept = sum(1 for p in points if p.has_error and p.trace_id in kept_ids)

            # Reuse evaluation helper for early-incident retention.
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
