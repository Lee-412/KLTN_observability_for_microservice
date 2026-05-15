
from __future__ import annotations

import time
import argparse
import csv
import json
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import scripts.outputAndEvaluation.evaluate_paper_sampling as eps
from scripts.outputAndEvaluation.shell_utils import resolve_bash_executable
import scripts.MESC.streamv3942_metric_native_signed_contrast as sv3942comp
from scripts.prepareData.build_scenarios_from_trastrainer_labels import build_rows as build_scenario_rows
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from scripts.paper_runner_common import (
        DatasetSpec,
        _run,
        _safe_slug,
        _scenario_windows,
        _load_jsonl,
        _ensure_base_scenario_file,
        _write_jsonl,
        _write_text,
        _materialize_sampled_trace_dir,
        _incident_services_from_labels,
        _incident_service_coverage,
        _endpoint_retention,
        _format_phase1_combined,
        _enforce_budget_cap,
        _metric_dir_from_trace_dir,
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
            _write_jsonl,
            _write_text,
            _materialize_sampled_trace_dir,
            _incident_services_from_labels,
            _incident_service_coverage,
            _endpoint_retention,
            _format_phase1_combined,
            _enforce_budget_cap,
            _metric_dir_from_trace_dir,
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
            _write_jsonl,
            _write_text,
            _materialize_sampled_trace_dir,
            _incident_services_from_labels,
            _incident_service_coverage,
            _endpoint_retention,
            _format_phase1_combined,
            _enforce_budget_cap,
            _metric_dir_from_trace_dir,
            _paper_microrank_reference,
            _paper_table3_reference_rows,
        )


try:
    from scripts.paper_report_writer import write_final_reports
except (ImportError, ModuleNotFoundError):
    try:
        from src.scripts.paper_report_writer import write_final_reports
    except (ImportError, ModuleNotFoundError):
        from paper_report_writer import write_final_reports


BASH_EXECUTABLE = resolve_bash_executable()


@dataclass
class Phase2Job:
    dataset: str
    budget_pct: float
    budget_slug: str
    sampled_scenario_file: Path
    raw_out_dir: Path
    sampled_dataset_name: str
    sampled_eval_tag: str
    expected_scenarios: int

    phase1_summary_file: Path
    sampled_trace_dir: Path
    kept_trace_ids_file: Path

    target_tps: float
    budget_mode: str
    achieved_keep_pct: float
    pre_cap_keep_pct: float
    scenario_floor_fail_count: int

    sim_metrics: dict[str, Any]
    phase1_eval: dict[str, Any]

    cov_kept: int
    cov_total: int
    cov_pct: float | None

    err_kept: int
    err_total: int

    


def _resolve_path(root: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return root / path


def _dataset_specs_from_config(root: Path, config_file: Path) -> list[DatasetSpec]:
    config_path = _resolve_path(root, str(config_file))
    if not config_path.exists():
        raise FileNotFoundError(f"Dataset config file not found: {config_path}")

    config = json.loads(config_path.read_text(encoding="utf-8"))
    rows = config.get("datasets", [])

    if not isinstance(rows, list) or not rows:
        raise ValueError(f"Invalid dataset config: missing non-empty 'datasets' list in {config_path}")

    specs: list[DatasetSpec] = []

    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            raise ValueError(f"Invalid dataset config: missing dataset name in {config_path}")

        specs.append(
            DatasetSpec(
                name=name,
                trace_dir=_resolve_path(root, str(row["trace_dir"])),
                label_file=_resolve_path(root, str(row["label_file"])),
                base_scenario_file=_resolve_path(root, str(row["base_scenario_file"])),
                baseline_summary_file=_resolve_path(root, str(row["baseline_summary_file"])),
            )
        )

    return specs


def _validate_dataset_specs(specs: list[DatasetSpec]) -> None:
    errors: list[str] = []

    for spec in specs:
        if not spec.trace_dir.exists():
            errors.append(f"[{spec.name}] trace_dir not found: {spec.trace_dir}")
        if not spec.label_file.exists():
            errors.append(f"[{spec.name}] label_file not found: {spec.label_file}")
    if errors:
        msg = "\n".join(errors)
        raise FileNotFoundError(
            "Dataset path validation failed.\n"
            "Check --root and --dataset-config.\n\n"
            f"{msg}"
        )


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

def _run_phase2_job(job: Phase2Job, root: Path) -> dict[str, Any]:
    job.raw_out_dir.mkdir(parents=True, exist_ok=True)
    scenario_for_bash = str(job.sampled_scenario_file).replace("\\", "/")
    raw_out_for_bash = str(job.raw_out_dir).replace("\\", "/")

    _run(
        [
            sys.executable,
            "scripts/outputAndEvaluation/generate_microrank_engine_output.py",
            "--scenario-file",
            str(job.sampled_scenario_file),
            "--out-dir",
            str(job.raw_out_dir),
        ],
        cwd=root,
    )

    _run(
        [
            BASH_EXECUTABLE,
            "scripts/outputAndEvaluation/run_paper_coverage_first_pipeline.sh",
            scenario_for_bash,
            raw_out_for_bash,
            job.sampled_dataset_name,
            job.sampled_eval_tag,
        ],
        cwd=root,
    )

    sampled_summary_file = root / "reports/compare" / (
        f"rca-benchmark-{job.sampled_dataset_name}-coverage-first-{job.sampled_eval_tag}-summary.json"
    )
    sampled = json.loads(sampled_summary_file.read_text(encoding="utf-8"))

    adapter_summary_file = root / "reports/compare" / (
        f"rca-engine-adapter-{job.sampled_dataset_name}-coverage-first-{job.sampled_eval_tag}.json"
    )
    adapter = json.loads(adapter_summary_file.read_text(encoding="utf-8")) if adapter_summary_file.exists() else {}

    parse_fail_count = int(adapter.get("parse_fail_count") or 0)
    ok_count = int(adapter.get("ok_count") or 0)

    invalid_reason = None
    if job.budget_mode == "strict" and parse_fail_count > 0:
        invalid_reason = (
            f"{job.dataset} budget={job.budget_pct}% "
            f"parse_fail_count={parse_fail_count} "
            f"ok_count={ok_count} "
            f"expected={job.expected_scenarios}"
        )

    paper_ref = _paper_microrank_reference().get(job.budget_pct, {})

    benchmark_row = {
        "dataset": job.dataset,
        "budget_pct": job.budget_pct,
        "budget_slug": job.budget_slug,
        "expected_scenarios": job.expected_scenarios,
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
        "phase1_summary_file": str(job.phase1_summary_file),
        "sampled_trace_dir": str(job.sampled_trace_dir),
        "kept_trace_ids_file": str(job.kept_trace_ids_file),
        "calibrated_target_tps": job.target_tps,
        "budget_mode": job.budget_mode,
        "achieved_keep_pct": job.achieved_keep_pct,
        "pre_cap_keep_pct": job.pre_cap_keep_pct,
        "scenario_floor_fail_count": job.scenario_floor_fail_count,
    }

    compare_row = {
        "dataset": job.dataset,
        "budget_pct": job.budget_pct,
        "budget_slug": job.budget_slug,
        "paper_A@1_pct": paper_ref.get("A@1_pct"),
        "paper_A@3_pct": paper_ref.get("A@3_pct"),
        "paper_MRR": paper_ref.get("MRR"),
        "ours_A@1_pct": (sampled.get("A@1", 0.0) * 100.0) if sampled.get("A@1") is not None else None,
        "ours_A@3_pct": (sampled.get("A@3", 0.0) * 100.0) if sampled.get("A@3") is not None else None,
        "ours_MRR": sampled.get("MRR"),
        "delta_A@1_pct": ((sampled.get("A@1", 0.0) * 100.0) - paper_ref.get("A@1_pct", 0.0)),
        "delta_A@3_pct": ((sampled.get("A@3", 0.0) * 100.0) - paper_ref.get("A@3_pct", 0.0)),
        "delta_MRR": (sampled.get("MRR", 0.0) - paper_ref.get("MRR", 0.0)),
    }

    sampling_quality_row = {
        "dataset": job.dataset,
        "budget_pct": job.budget_pct,
        "budget_slug": job.budget_slug,
        "incident_capture_latency_ms": job.sim_metrics.get("incident_capture_latency_ms"),
        "early_incident_retention_pct": job.phase1_eval.get("early_incident_retention_pct"),
        "critical_endpoint_coverage_pct": job.cov_pct,
        "critical_endpoint_coverage_kept": job.cov_kept,
        "critical_endpoint_coverage_total": job.cov_total,
        "threshold_volatility_pct": job.sim_metrics.get("threshold_volatility_pct"),
        "max_step_change_pct": job.sim_metrics.get("max_step_change_pct"),
        "kept_error_traces": job.err_kept,
        "baseline_error_traces": job.err_total,
        "kept_error_traces_pct": (job.err_kept / job.err_total * 100.0) if job.err_total else None,
        "achieved_keep_pct": job.achieved_keep_pct,
        "pre_cap_keep_pct": job.pre_cap_keep_pct,
        "scenario_floor_fail_count": job.scenario_floor_fail_count,
        "expected_scenarios": job.expected_scenarios,
        "adapter_ok_count": ok_count,
        "adapter_parse_fail_count": parse_fail_count,
        "phase1_summary_file": str(job.phase1_summary_file),
    }

    return {
        "benchmark_row": benchmark_row,
        "compare_row": compare_row,
        "sampling_quality_row": sampling_quality_row,
        "invalid_reason": invalid_reason,
    }

def _write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def main() -> int:
    ap = argparse.ArgumentParser(description="Run paper datasets through 2-phase sampling->RCA pipeline for multiple budgets")
    ap.add_argument("--root", default=".", help="Path to src root")
    ap.add_argument("--tag", default=datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--budgets", default="0.1,1.0,2.5", help="Sampling budgets in percent, comma-separated")
    ap.add_argument("--before-sec", type=int, default=300)
    ap.add_argument("--after-sec", type=int, default=600)
    ap.add_argument(
        "--phase2-workers",
        type=int,
        default=1,
        help="Number of parallel Phase-2 RCA/evaluation jobs. Use 1 for sequential execution.",
    )
    ap.add_argument(
    "--sampling-only",
    action="store_true",
    help="Run sampler and timing only; skip materialization, Phase-1 diagnostics, RCA benchmark, and final RCA reports.",
    )   
    ap.add_argument(
        "--sampling-timing-csv",
        default=None,
        help="Optional output CSV path for sampler-only timing rows.",
    )
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
    ap.add_argument(
        "--skip-phase1-diagnostics",
        action="store_true",
        help="Skip expensive Phase-1 diagnostic reports during benchmark runs.",
    )
    ap.add_argument(
        "--dataset-config",
        default="scripts/configs/datasets.trastrainer.json",
        help="Path to dataset config JSON. Relative paths are resolved from --root.",
    )
    args = ap.parse_args()

    # Always append an execution timestamp so all outputs are uniquely trackable.
    run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    args.tag = f"{args.tag}-{run_stamp}"

    root = Path(args.root).resolve()
    budgets = [float(x.strip()) for x in args.budgets.split(",") if x.strip()]
    selected = {x.strip() for x in args.datasets.split(",") if x.strip()}
    all_specs = _dataset_specs_from_config(root, Path(args.dataset_config))
    specs = [s for s in all_specs if s.name in selected]
    if not specs:
        raise SystemExit("No dataset selected. Check --datasets")
    _validate_dataset_specs(specs)
    for spec in specs:
        _ensure_base_scenario_file(spec, args.before_sec, args.after_sec)

    benchmark_rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    sampling_quality_rows: list[dict[str, Any]] = []
    invalid_run_reasons: list[str] = []
    phase2_jobs: list[Phase2Job] = []
    sampler_timing_rows: list[dict[str, Any]] = []
    for spec in specs:
        print(f"\\n== DATASET: {spec.name} ==")
        labels = eps.load_labels(spec.label_file)
        points, _ = eps.build_trace_points(spec.trace_dir, labels, args.before_sec, args.after_sec)

        base_scenarios = _load_jsonl(spec.base_scenario_file)
        scenario_windows = _scenario_windows(base_scenarios)
        incident_services = _incident_services_from_labels(spec.label_file)
       
        total_trace_count = len(points)
        t0_sec = min(p.start_ns for p in points) // 1_000_000_000 if points else 0
        incident_anchor_sec = (min(ts for ts, _svc in labels) - t0_sec) if labels else None

        v3942_preference_vector = None
        v3942_metric_stats = None
        v3942_metrics_stream_by_sec = None
        v3942_metric_context = None


        v3942_precompute_elapsed_sec = 0.0

        if args.selection_mode == "stream-v3.94.2-metric-native-signed-contrast":
            t_precompute = time.perf_counter()

            v3942_preference_vector, v3942_metric_stats = sv3942comp.build_metric_inputs(points)
            metric_dir = _metric_dir_from_trace_dir(spec.trace_dir)
            v3942_metrics_stream_by_sec = sv3942comp.build_real_metrics_stream(str(metric_dir))
            v3942_metric_context = sv3942comp.build_v3942_metric_context(
                points=points,
                metrics_stream_by_sec=v3942_metrics_stream_by_sec,
                metric_lookback_sec=args.v8_metric_lookback_sec,
            )
            v3942_precompute_elapsed_sec = time.perf_counter() - t_precompute
        else:
            raise SystemExit(
                f"Unsupported selection_mode in this timing path: {args.selection_mode}"
            )
        metric_precompute_amortized_sec_per_budget = (
            v3942_precompute_elapsed_sec / len(budgets)
            if budgets
            else 0.0
        )

        for budget in budgets:
            budget_slug = _safe_slug(f"{budget:.3f}pct")
            print(f"  -- budget={budget}%")
         
            kept_ordered_ids: list[str] | None = None

            # Phase 1: calibrate simulator to requested budget and materialize sampled traces.
            target_tps = 0.0
            target_trace_count = None
            pre_cap_trace_count = None
            
            kept_ids: set[str] = set()
            achieved_keep_pct = 0.0
            pre_cap_keep_pct = 0.0

            sim_metrics = {
                "incident_capture_latency_ms": None,
                "threshold_volatility_pct": None,
                "max_step_change_pct": None,
            }
            
            sampler_core_elapsed_sec = 0.0
            budget_cap_elapsed_sec = 0.0
            sampler_select_total_elapsed_sec = 0.0

            t_select_total = time.perf_counter()

           
            if args.selection_mode == "stream-v3.94.2-metric-native-signed-contrast":
            
                preference_vector = v3942_preference_vector or {}
                metric_stats = v3942_metric_stats or {}
                metrics_stream_by_sec = v3942_metrics_stream_by_sec or {}

                min_floor = 1

                t_sampler_core = time.perf_counter()

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
                    precomputed_metric_context=v3942_metric_context,
                )
                sampler_core_elapsed_sec = time.perf_counter() - t_sampler_core

                achieved_keep_pct = (
                    len(kept_ids) / total_trace_count * 100.0
                    if total_trace_count
                    else 0.0
                )

                pre_cap_keep_pct = achieved_keep_pct
                pre_cap_trace_count = len(kept_ids)
     
            # For non-adaptive modes, enforce strict budget cap here as well.
            if args.budget_mode == "strict" and target_trace_count is None:
                t_budget_cap = time.perf_counter()

                kept_ids, target_trace_count, pre_cap_trace_count = _enforce_budget_cap(
                    points,
                    kept_ids,
                    budget,
                )
                scenario_floor_fail_count = 0

                budget_cap_elapsed_sec = time.perf_counter() - t_budget_cap

                achieved_keep_pct = (
                    len(kept_ids) / total_trace_count * 100.0
                    if total_trace_count
                    else 0.0
                )
            else:
                target_trace_count = (
                    max(
                        1,
                        min(
                            total_trace_count,
                            int(round(total_trace_count * (budget / 100.0))),
                        ),
                    )
                    if total_trace_count
                    else 0
                )
                pre_cap_trace_count = len(kept_ids)
                scenario_floor_fail_count = 0

            sampler_select_total_elapsed_sec = time.perf_counter() - t_select_total

            sampler_core_ms_per_trace = (
                sampler_core_elapsed_sec * 1000.0 / total_trace_count
                if total_trace_count
                else 0.0
            )

            budget_cap_ms_per_trace = (
                budget_cap_elapsed_sec * 1000.0 / total_trace_count
                if total_trace_count
                else 0.0
            )

            sampler_select_total_ms_per_trace = (
                sampler_select_total_elapsed_sec * 1000.0 / total_trace_count
                if total_trace_count
                else 0.0
            )

            seed_count = len(getattr(sv3942comp, "_ENSEMBLE_SEEDS", [])) or 1
            sampler_core_ms_per_member_trace = (
                sampler_core_elapsed_sec * 1000.0 / (total_trace_count * seed_count)
                if total_trace_count
                else 0.0
            )

            select_plus_precompute_amortized_sec = (
                sampler_select_total_elapsed_sec
                + metric_precompute_amortized_sec_per_budget
            )

            select_plus_precompute_amortized_ms_per_trace = (
                select_plus_precompute_amortized_sec * 1000.0 / total_trace_count
                if total_trace_count
                else 0.0
            )

            sampler_timing_rows.append(
                {
                    "dataset": spec.name,
                    "budget_pct": budget,
                    "trace_count": total_trace_count,
                    "kept_count": len(kept_ids),
                    "kept_pct": achieved_keep_pct,
                    "target_trace_count": target_trace_count,
                    "pre_cap_trace_count": pre_cap_trace_count,
                    "pre_cap_keep_pct": pre_cap_keep_pct,
                    "budget_mode": args.budget_mode,
                    "seed_count": seed_count,
                    "min_consensus": args.v3942_min_consensus,
                    "metric_precompute_elapsed_sec_dataset_once": v3942_precompute_elapsed_sec,
                    "metric_precompute_amortized_sec_per_budget": metric_precompute_amortized_sec_per_budget,
                    "sampler_core_elapsed_sec": sampler_core_elapsed_sec,
                    "budget_cap_elapsed_sec": budget_cap_elapsed_sec,
                    "sampler_select_total_elapsed_sec": sampler_select_total_elapsed_sec,
                    "select_plus_precompute_amortized_sec": select_plus_precompute_amortized_sec,
                    "sampler_core_ms_per_trace": sampler_core_ms_per_trace,
                    "budget_cap_ms_per_trace": budget_cap_ms_per_trace,
                    "sampler_select_total_ms_per_trace": sampler_select_total_ms_per_trace,
                    "select_plus_precompute_amortized_ms_per_trace": select_plus_precompute_amortized_ms_per_trace,
                    "sampler_core_ms_per_member_trace": sampler_core_ms_per_member_trace,
                    "selection_mode": args.selection_mode,
                }
            )

            print(
                "[SAMPLER-TIMER] "
                f"dataset={spec.name} "
                f"budget={budget}% "
                f"traces={total_trace_count} "
                f"kept={len(kept_ids)} "
                f"pre_cap={pre_cap_trace_count} "
                f"target={target_trace_count} "
                f"precompute_once_sec={v3942_precompute_elapsed_sec:.6f} "
                f"precompute_amortized_sec={metric_precompute_amortized_sec_per_budget:.6f} "
                f"core_sec={sampler_core_elapsed_sec:.6f} "
                f"cap_sec={budget_cap_elapsed_sec:.6f} "
                f"select_total_sec={sampler_select_total_elapsed_sec:.6f} "
                f"core_ms_per_trace={sampler_core_ms_per_trace:.6f} "
                f"select_total_ms_per_trace={sampler_select_total_ms_per_trace:.6f} "
                f"select_plus_precompute_amortized_ms_per_trace="
                f"{select_plus_precompute_amortized_ms_per_trace:.6f} "
                f"core_ms_per_member_trace={sampler_core_ms_per_member_trace:.6f}"
            )

            if args.sampling_only:
                continue


            # Materialize sampled trace directory
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

           # Phase-1 diagnostics are optional because they do not affect sampling or RCA metrics.
            if args.skip_phase1_diagnostics:
                phase1_eval = {
                    "early_incident_retention_pct": None,
                }
                endpoint_rows = []
            else:
                # Reuse evaluation helper for early-incident retention.
                if args.selection_mode == "ranked":
                    # Recompute early retention directly from selected IDs for ranked mode.
                    labels_local = eps.load_labels(spec.label_file)
                    _pts_local, early_windows = eps.build_trace_points(
                        spec.trace_dir,
                        labels_local,
                        args.before_sec,
                        args.after_sec,
                    )
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

                endpoint_rows = _endpoint_retention(spec.trace_dir, kept_ids)

            cov_kept, cov_total, cov_pct = _incident_service_coverage(points, kept_ids, incident_services)

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
            phase1_summary_text += (
                "\n"
                "== SAMPLER TIMING ==\n"
                f"metric_precompute_elapsed_sec_dataset_once: {v3942_precompute_elapsed_sec:.6f}\n"
                f"metric_precompute_amortized_sec_per_budget: {metric_precompute_amortized_sec_per_budget:.6f}\n"
                f"sampler_core_elapsed_sec: {sampler_core_elapsed_sec:.6f}\n"
                f"budget_cap_elapsed_sec: {budget_cap_elapsed_sec:.6f}\n"
                f"sampler_select_total_elapsed_sec: {sampler_select_total_elapsed_sec:.6f}\n"
                f"select_plus_precompute_amortized_sec: {select_plus_precompute_amortized_sec:.6f}\n"
                f"sampler_core_ms_per_trace: {sampler_core_ms_per_trace:.6f}\n"
                f"budget_cap_ms_per_trace: {budget_cap_ms_per_trace:.6f}\n"
                f"sampler_select_total_ms_per_trace: {sampler_select_total_ms_per_trace:.6f}\n"
                f"select_plus_precompute_amortized_ms_per_trace: {select_plus_precompute_amortized_ms_per_trace:.6f}\n"
                f"sampler_core_ms_per_member_trace: {sampler_core_ms_per_member_trace:.6f}\n"
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
  
            raw_out_dir = root / "reports/compare/rca-engine-raw-microrank-sampled" / spec.name / budget_slug / args.tag

            sampled_dataset_name = f"{spec.name}-sampled-{budget_slug}"
            sampled_eval_tag = f"{args.tag}-microrank-{budget_slug}"
            expected_scenarios = len(base_scenarios)

            phase2_jobs.append(
                Phase2Job(
                    dataset=spec.name,
                    budget_pct=budget,
                    budget_slug=budget_slug,
                    sampled_scenario_file=sampled_scenario_file,
                    raw_out_dir=raw_out_dir,
                    sampled_dataset_name=sampled_dataset_name,
                    sampled_eval_tag=sampled_eval_tag,
                    expected_scenarios=expected_scenarios,
                    phase1_summary_file=phase1_summary_file,
                    sampled_trace_dir=sampled_trace_dir,
                    kept_trace_ids_file=kept_ids_file,
                    target_tps=target_tps,
                    budget_mode=args.budget_mode,
                    achieved_keep_pct=achieved_keep_pct,
                    pre_cap_keep_pct=pre_cap_keep_pct,
                    scenario_floor_fail_count=scenario_floor_fail_count,
                    sim_metrics=sim_metrics,
                    phase1_eval=phase1_eval,
                    cov_kept=cov_kept,
                    cov_total=cov_total,
                    cov_pct=cov_pct,
                    err_kept=err_kept,
                    err_total=err_total,
                )
            )


    sampler_timing_csv = (
        Path(args.sampling_timing_csv)
        if args.sampling_timing_csv
        else root / "reports/compare" / f"sampler-timing-{args.tag}.csv"
    )

    _write_csv_rows(sampler_timing_csv, sampler_timing_rows)
    print(f"sampler_timing_csv={sampler_timing_csv}")

    if args.sampling_only:
        if sampler_timing_rows:
            total_traces_for_timing = sum(
                int(r["trace_count"]) for r in sampler_timing_rows
            )

            total_core_sec = sum(
                float(r["sampler_core_elapsed_sec"]) for r in sampler_timing_rows
            )

            total_select_sec = sum(
                float(r.get("sampler_select_total_elapsed_sec", 0.0))
                for r in sampler_timing_rows
            )

            total_select_plus_precompute_sec = sum(
                float(r.get("select_plus_precompute_amortized_sec", 0.0))
                for r in sampler_timing_rows
            )

            weighted_core_ms_per_trace = (
                total_core_sec * 1000.0 / total_traces_for_timing
                if total_traces_for_timing
                else 0.0
            )

            weighted_select_ms_per_trace = (
                total_select_sec * 1000.0 / total_traces_for_timing
                if total_traces_for_timing
                else 0.0
            )

            weighted_select_plus_precompute_ms_per_trace = (
                total_select_plus_precompute_sec * 1000.0 / total_traces_for_timing
                if total_traces_for_timing
                else 0.0
            )

            max_core_ms_per_trace = max(
                float(r["sampler_core_ms_per_trace"])
                for r in sampler_timing_rows
            )

            max_select_ms_per_trace = max(
                float(r.get("sampler_select_total_ms_per_trace", 0.0))
                for r in sampler_timing_rows
            )

            max_select_plus_precompute_ms_per_trace = max(
                float(r.get("select_plus_precompute_amortized_ms_per_trace", 0.0))
                for r in sampler_timing_rows
            )

            print(f"sampler_core_weighted_avg_ms_per_trace={weighted_core_ms_per_trace:.6f}")
            print(f"sampler_select_weighted_avg_ms_per_trace={weighted_select_ms_per_trace:.6f}")
            print(
                "sampler_select_plus_precompute_weighted_avg_ms_per_trace="
                f"{weighted_select_plus_precompute_ms_per_trace:.6f}"
            )

            print(f"sampler_core_max_ms_per_trace={max_core_ms_per_trace:.6f}")
            print(f"sampler_select_max_ms_per_trace={max_select_ms_per_trace:.6f}")
            print(
                "sampler_select_plus_precompute_max_ms_per_trace="
                f"{max_select_plus_precompute_ms_per_trace:.6f}"
            )

        return 0
    phase2_results: list[dict[str, Any]] = []
    phase2_workers = max(1, int(args.phase2_workers))

    if phase2_workers == 1:
        for job in phase2_jobs:
            phase2_results.append(_run_phase2_job(job, root))
    else:
        with ThreadPoolExecutor(max_workers=phase2_workers) as executor:
            future_to_job = {
                executor.submit(_run_phase2_job, job, root): job
                for job in phase2_jobs
            }

            for future in as_completed(future_to_job):
                job = future_to_job[future]
                try:
                    phase2_results.append(future.result())
                except Exception as exc:
                    raise RuntimeError(
                        f"Phase2 job failed: dataset={job.dataset}, budget={job.budget_pct}"
                    ) from exc

    for result in phase2_results:
        benchmark_rows.append(result["benchmark_row"])
        compare_rows.append(result["compare_row"])
        sampling_quality_rows.append(result["sampling_quality_row"])

        if result.get("invalid_reason"):
            invalid_run_reasons.append(str(result["invalid_reason"]))

    benchmark_rows.sort(key=lambda r: (float(r["budget_pct"]), str(r["dataset"])))
    compare_rows.sort(key=lambda r: (float(r["budget_pct"]), str(r["dataset"])))
    sampling_quality_rows.sort(key=lambda r: (float(r["budget_pct"]), str(r["dataset"])))
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
        table3_notes=None,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
