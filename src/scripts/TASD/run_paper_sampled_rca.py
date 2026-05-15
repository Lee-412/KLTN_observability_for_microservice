from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import src.scripts.outputAndEvaluation.evaluate_paper_sampling as eps
from src.scripts.outputAndEvaluation.shell_utils import resolve_bash_executable
from src.scripts.prepareData.build_scenarios_from_trastrainer_labels import build_rows as build_scenario_rows
import src.scripts.TASD.streamv3_composite_strictcap as sv3comp

try:
    from scripts.paper_runner_common import (
        DatasetSpec,
        _run,
        _safe_slug,
        _scenario_windows,
        _load_jsonl,
        _ensure_base_scenario_file,
        _enforce_budget_cap_per_scenario,
        _write_jsonl,
        _write_text,
        _materialize_sampled_trace_dir,
        _incident_services_from_labels,
        _incident_service_coverage,
        _endpoint_retention,
        _format_phase1_combined,
        _paper_microrank_reference,
        _paper_table3_reference_rows,
    )
except ModuleNotFoundError:
    try:
        from src.scripts.paper_runner_common import (
            DatasetSpec,
            _run,
            _safe_slug,
            _scenario_windows,
            _load_jsonl,
            _ensure_base_scenario_file,
            _enforce_budget_cap_per_scenario,
            _write_jsonl,
            _write_text,
            _materialize_sampled_trace_dir,
            _incident_services_from_labels,
            _incident_service_coverage,
            _endpoint_retention,
            _format_phase1_combined,
            _paper_microrank_reference,
            _paper_table3_reference_rows,
        )
    except ModuleNotFoundError:
        from paper_runner_common import (
            DatasetSpec,
            _run,
            _safe_slug,
            _scenario_windows,
            _load_jsonl,
            _ensure_base_scenario_file,
            _enforce_budget_cap_per_scenario,
            _write_jsonl,
            _write_text,
            _materialize_sampled_trace_dir,
            _incident_services_from_labels,
            _incident_service_coverage,
            _endpoint_retention,
            _format_phase1_combined,
            _paper_microrank_reference,
            _paper_table3_reference_rows,
        )


try:
    from scripts.paper_report_writer import write_final_reports
except ModuleNotFoundError:
    try:
        from src.scripts.paper_report_writer import write_final_reports
    except ModuleNotFoundError:
        from paper_report_writer import write_final_reports


V3_SELECTION_MODE = "stream-v3-composite-strictcap"
BASH_EXECUTABLE = resolve_bash_executable()


def _slash_path(value: Path | str) -> str:
    return str(value).replace("\\", "/")


def _early_incident_retention_pct(
    points: list[eps.TracePoint],
    early_windows: list[tuple[int, int]],
    kept_ids: set[str],
) -> float | None:
    if not early_windows:
        return None

    early_total = 0
    early_kept = 0
    for p in points:
        sec = int(p.start_ns // 1_000_000_000)
        if any(st <= sec <= ed for st, ed in early_windows):
            early_total += 1
            if p.trace_id in kept_ids:
                early_kept += 1

    if early_total <= 0:
        return None
    return (early_kept / early_total) * 100.0

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


def _known_ours_reference_rows() -> list[dict[str, float | str]]:
    # Preserved rows from prior verified comparison files.
    return [
        {"model": "Ours (MicroRank + Hybrid c0.7 m2)", "a1_0p1": 9.25, "a1_1p0": 20.00, "a1_2p5": 17.69, "a3_0p1": 39.18, "a3_1p0": 40.81, "a3_2p5": 37.36, "mrr_0p1": 0.2981, "mrr_1p0": 0.3736, "mrr_2p5": 0.3502},
        {"model": "Ours (MicroRank + Stream v1 strictfix)", "a1_0p1": 10.04, "a1_1p0": 15.26, "a1_2p5": 17.34, "a3_0p1": 43.99, "a3_1p0": 48.90, "a3_2p5": 48.90, "mrr_0p1": 0.3255, "mrr_1p0": 0.3655, "mrr_2p5": 0.3823},
        {"model": "Ours (MicroRank + stream-v2)", "a1_0p1": 15.87, "a1_1p0": 9.80, "a1_2p5": 11.81, "a3_0p1": 57.94, "a3_1p0": 21.41, "a3_2p5": 25.57, "mrr_0p1": 0.3944, "mrr_1p0": 0.2180, "mrr_2p5": 0.2613},
    ]


def _current_ours_table3_row(compare_avg_rows: list[dict[str, Any]], selection_mode: str, tag: str) -> dict[str, float | str]:
    model_name = f"Ours (MicroRank + {selection_mode})"

    by_budget = {float(r["budget_pct"]): r for r in compare_avg_rows}

    def _val(budget: float, key: str, scale: float = 1.0) -> float:
        r = by_budget.get(budget)
        if not r or r.get(key) is None:
            return 0.0
        return float(r[key]) * scale

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


def main() -> int:
    ap = argparse.ArgumentParser(description="Run paper datasets through 2-phase sampling->RCA pipeline for multiple budgets")
    ap.add_argument("--root", default=".", help="Path to src root")
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
        choices=[V3_SELECTION_MODE],
        default=V3_SELECTION_MODE,
        help="Only the stream-v3 composite strict-cap sampler is supported by this runner.",
    )
    ap.add_argument(
        "--stochastic-seed",
        type=int,
        default=42,
        help="Random seed passed to the stream-v3 composite sampler.",
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
        _ensure_base_scenario_file(spec, args.before_sec, args.after_sec)
        base_scenarios = _load_jsonl(spec.base_scenario_file)
        scenario_windows = _scenario_windows(base_scenarios)
        incident_services = _incident_services_from_labels(spec.label_file)
        t0_sec = min(p.start_ns for p in points) // 1_000_000_000 if points else 0
        incident_anchor_sec = (min(ts for ts, _svc in labels) - t0_sec) if labels else None
        _, early_windows = eps.build_trace_points(spec.trace_dir, labels, args.before_sec, args.after_sec)

        for budget in budgets:
            budget_slug = _safe_slug(f"{budget:.3f}pct")
            print(f"  -- budget={budget}%")

            # Phase 1: calibrate simulator to requested budget and materialize sampled traces.
            target_trace_count = None
            pre_cap_trace_count = None
            target_tps = 0.0
            preference_vector, metric_stats = sv3comp.build_metric_inputs(points)
            kept_ids = sv3comp._composite_v3_metrics_strictcap(
                points=points,
                budget_pct=budget,
                preference_vector=preference_vector,
                metric_stats=metric_stats,
                scenario_windows=scenario_windows,
                incident_anchor_sec=incident_anchor_sec,
                seed=args.stochastic_seed,
                incident_services=incident_services,
                min_incident_traces_per_scenario=1,
                online_soft_cap=(args.budget_mode != "strict"),
            )
            achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
            pre_cap_keep_pct = achieved_keep_pct
            sim_metrics = {
                "incident_capture_latency_ms": None,
                "threshold_volatility_pct": None,
                "max_step_change_pct": None,
            }

            # Keep exact budget and scenario floor aligned with the reporting pipeline.
            if args.budget_mode == "strict" and target_trace_count is None:
                kept_ids, target_trace_count, pre_cap_trace_count, scenario_floor_fail_count = _enforce_budget_cap_per_scenario(
                    points,
                    kept_ids,
                    budget,
                    scenario_windows,
                    min_incident_traces_per_scenario=1,
                )
                achieved_keep_pct = (len(kept_ids) / len(points) * 100.0) if points else 0.0
            else:
                scenario_floor_fail_count = 0

            sampled_trace_dir = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "trace"

            total_rows, kept_rows = _materialize_sampled_trace_dir(
                spec.trace_dir,
                sampled_trace_dir,
                kept_ids,
                kept_rank=None,
            )

            kept_ids_file = root / "reports/analysis/paper-sampled-traces" / args.tag / spec.name / budget_slug / "kept_trace_ids.txt"
            _write_text(kept_ids_file, "\n".join(sorted(kept_ids)) + ("\n" if kept_ids else ""))

            err_total = sum(1 for p in points if p.has_error)
            err_kept = sum(1 for p in points if p.has_error and p.trace_id in kept_ids)

            phase1_eval = {
                "early_incident_retention_pct": _early_incident_retention_pct(points, early_windows, kept_ids)
            }

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
                    sys.executable,
                    "scripts/outputAndEvaluation/generate_microrank_engine_output.py",
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
                    BASH_EXECUTABLE,
                    "scripts/outputAndEvaluation/run_paper_coverage_first_pipeline.sh",
                    _slash_path(sampled_scenario_file),
                    _slash_path(raw_out_dir),
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

    write_final_reports(
        root=root,
        tag=args.tag,
        run_stamp=run_stamp,
        selection_mode=args.selection_mode,
        benchmark_rows=benchmark_rows,
        compare_rows=compare_rows,
        sampling_quality_rows=sampling_quality_rows,
        paper_table3_reference_rows=_paper_table3_reference_rows(),
        known_ours_reference_rows=_known_ours_reference_rows(),
        current_ours_table3_row_builder=_current_ours_table3_row,
        table3_notes=[
            "- Paper model rows are copied from Table 3 screenshot provided in this chat.",
            "- Ours reference rows (Hybrid, Stream v1 strictfix, stream-v2 milestone) are preserved from prior comparison artifacts.",
            f"- Current run row is generated automatically from this stream-v3 run with selection-mode={args.selection_mode}.",
        ],
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
