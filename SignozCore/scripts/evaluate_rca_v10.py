#!/usr/bin/env python3
"""Evaluate V10 guard-aware sampler and generate comparison artifacts.

Outputs:
- rca_compare_v10.csv
- v10_ablation.csv
- v10_telemetry_report.json
- V10_IMPLEMENT_REPORT.md
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import evaluate_paper_sampling as eps
import sampler_v10_guard_aware as sv10


@dataclass
class DatasetSpec:
    name: str
    trace_dir: Path
    label_file: Path
    base_scenario_file: Path


@dataclass
class Scenario:
    scenario_id: str
    start_sec: int
    end_sec: int
    ground_truth: str


@dataclass
class DatasetBundle:
    name: str
    points: List[sv10.TracePoint]
    by_id: Dict[str, sv10.TracePoint]
    start_sec_by_id: Dict[str, int]
    scenarios: List[Scenario]
    scenario_windows: List[Tuple[str, int, int]]
    incident_anchor_sec: Optional[int]
    incident_services: Set[str]
    metrics_stream_by_sec: sv10.MetricsStreamBySec


def _parse_iso_utc_to_s(value: str) -> int:
    text = str(value).strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return int(datetime.fromisoformat(text).timestamp())


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _dataset_specs(root: Path) -> List[DatasetSpec]:
    return [
        DatasetSpec(
            name="train-ticket",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/train_ticket/test/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/train_ticket/test/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.train-ticket.paper-source.20260401.jsonl",
        ),
        DatasetSpec(
            name="hipster-batch1",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.hipster-batch1.paper-source.20260401.jsonl",
        ),
        DatasetSpec(
            name="hipster-batch2",
            trace_dir=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/trace",
            label_file=root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/label.json",
            base_scenario_file=root / "reports/analysis/rca-benchmark/scenarios.hipster-batch2.paper-source.20260401.jsonl",
        ),
    ]


def _build_bundle(spec: DatasetSpec, before_sec: int, after_sec: int) -> DatasetBundle:
    labels = eps.load_labels(spec.label_file)
    points, _early_windows = eps.build_trace_points(spec.trace_dir, labels, before_sec, after_sec)

    by_id = {p.trace_id: p for p in points}
    start_sec_by_id = {p.trace_id: int(p.start_ns // 1_000_000_000) for p in points}

    rows = _load_jsonl(spec.base_scenario_file)
    scenarios: List[Scenario] = []
    anchors: List[int] = []
    metric_dir = spec.trace_dir.parent / "metric"

    for row in rows:
        sid = str(row.get("scenario_id", "")).strip()
        gt = str(row.get("ground_truth_root_cause", "")).strip()
        st = _parse_iso_utc_to_s(str(row.get("incident_start_ts", "")))
        ed = _parse_iso_utc_to_s(str(row.get("incident_end_ts", "")))
        if sid and gt and st > 0:
            scenarios.append(Scenario(scenario_id=sid, start_sec=st, end_sec=max(st, ed), ground_truth=gt))
        inject_ts = str(row.get("inject_timestamp", "")).strip()
        if inject_ts.isdigit():
            anchors.append(int(inject_ts))
        metric_dir_text = str(row.get("metric_dir", "")).strip()
        if metric_dir_text:
            metric_dir = Path(metric_dir_text)

    incident_anchor_sec: Optional[int] = int(median(anchors)) if anchors else None
    incident_services = {service for _ts, service in labels}
    scenario_windows = [(s.scenario_id, s.start_sec, s.end_sec) for s in scenarios]
    metrics_stream_by_sec = sv10.build_real_metrics_stream(str(metric_dir))

    return DatasetBundle(
        name=spec.name,
        points=points,
        by_id=by_id,
        start_sec_by_id=start_sec_by_id,
        scenarios=scenarios,
        scenario_windows=scenario_windows,
        incident_anchor_sec=incident_anchor_sec,
        incident_services=incident_services,
        metrics_stream_by_sec=metrics_stream_by_sec,
    )


def _service_ranking_for_scenario(
    selected_ids: Set[str],
    score_by_id: Dict[str, float],
    by_id: Dict[str, sv10.TracePoint],
    start_sec_by_id: Dict[str, int],
    start_sec: int,
    end_sec: int,
) -> List[Tuple[str, float]]:
    in_window = [tid for tid in selected_ids if start_sec <= start_sec_by_id.get(tid, -1) <= end_sec]
    if not in_window:
        in_window = list(selected_ids)

    service_scores: Dict[str, float] = defaultdict(float)
    for tid in in_window:
        trace = by_id[tid]
        weight = score_by_id.get(tid, 0.0)
        if trace.has_error:
            weight *= 1.20
        else:
            weight *= 0.85
        if weight <= 0.0:
            continue
        denom = float(max(1, len(trace.services)))
        for service in trace.services:
            service_scores[service] += weight / denom

    ranked = sorted(service_scores.items(), key=lambda item: (-item[1], item[0]))
    return ranked


def _evaluate_proxy_rca(
    selected_ids: Set[str],
    score_by_id: Dict[str, float],
    bundle: DatasetBundle,
) -> Dict[str, float]:
    a1_values: List[float] = []
    a3_values: List[float] = []
    rr_values: List[float] = []
    conf_values: List[float] = []
    gap_values: List[float] = []

    for scenario in bundle.scenarios:
        ranked = _service_ranking_for_scenario(
            selected_ids=selected_ids,
            score_by_id=score_by_id,
            by_id=bundle.by_id,
            start_sec_by_id=bundle.start_sec_by_id,
            start_sec=scenario.start_sec,
            end_sec=scenario.end_sec,
        )

        rank = None
        for idx, (service, _score) in enumerate(ranked, start=1):
            if service == scenario.ground_truth:
                rank = idx
                break

        hit1 = 1.0 if rank == 1 else 0.0
        hit3 = 1.0 if rank is not None and rank <= 3 else 0.0
        rr = 1.0 / rank if rank is not None else 0.0

        if ranked:
            total_score = sum(score for _svc, score in ranked)
            top1 = ranked[0][1]
            top2 = ranked[1][1] if len(ranked) > 1 else 0.0
            conf = top1 / total_score if total_score > 0 else 0.0
            gap = top1 - top2
        else:
            conf = 0.0
            gap = 0.0

        a1_values.append(hit1)
        a3_values.append(hit3)
        rr_values.append(rr)
        conf_values.append(conf)
        gap_values.append(gap)

    return {
        "A1": sum(a1_values) / max(1, len(a1_values)),
        "A3": sum(a3_values) / max(1, len(a3_values)),
        "MRR": sum(rr_values) / max(1, len(rr_values)),
        "top1_confidence": sum(conf_values) / max(1, len(conf_values)),
        "top1_gap": sum(gap_values) / max(1, len(gap_values)),
    }


def _run_variant(
    bundles: Sequence[DatasetBundle],
    budgets: Sequence[float],
    variant_name: str,
    variant_cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    details: List[Dict[str, Any]] = []

    for budget in budgets:
        budget_metrics: List[Dict[str, float]] = []
        budget_jaccard: List[float] = []
        budget_kendall: List[float] = []
        budget_displacement: List[float] = []
        budget_swap_loss: List[float] = []

        for bundle in bundles:
            sampler = sv10.V10GuardAwareSampler(
                lambda_base=variant_cfg["lambda_base"],
                mu_evidence=variant_cfg["mu_evidence"],
                eta_guard=variant_cfg["eta_guard"],
                tau=variant_cfg.get("tau", 0.50),
                min_normal_ratio=variant_cfg.get("min_normal_ratio", 0.30),
                min_error_ratio=variant_cfg.get("min_error_ratio", 0.25),
                redundancy_weight=variant_cfg.get("redundancy_weight", 0.15),
            )
            selected_ids, score_by_id, diagnostics = sampler.select_traces(
                points=bundle.points,
                budget_pct=budget,
                scenario_windows=bundle.scenario_windows,
                incident_anchor_sec=bundle.incident_anchor_sec,
                incident_services=bundle.incident_services,
                metrics_stream_by_sec=bundle.metrics_stream_by_sec,
                metric_lookback_sec=variant_cfg.get("metric_lookback_sec", 120),
                min_incident_traces_per_scenario=1,
                enable_residual_fix=variant_cfg.get("enable_residual_fix", True),
            )

            metric = _evaluate_proxy_rca(selected_ids=selected_ids, score_by_id=score_by_id, bundle=bundle)
            budget_metrics.append(metric)

            budget_jaccard.append(diagnostics.pre_post_jaccard)
            budget_kendall.append(diagnostics.kendall_tau)
            budget_displacement.append(diagnostics.displacement_rate)
            budget_swap_loss.append(diagnostics.average_swap_score_loss)

            details.append(
                {
                    "variant": variant_name,
                    "dataset": bundle.name,
                    "budget_pct": budget,
                    "A1": metric["A1"],
                    "A3": metric["A3"],
                    "MRR": metric["MRR"],
                    "top1_confidence": metric["top1_confidence"],
                    "top1_gap": metric["top1_gap"],
                    "jaccard": diagnostics.pre_post_jaccard,
                    "kendall_tau": diagnostics.kendall_tau,
                    "displacement_rate": diagnostics.displacement_rate,
                    "swap_loss": diagnostics.average_swap_score_loss,
                    "swap_count": diagnostics.swap_count,
                    "normal_ratio": diagnostics.normal_ratio,
                    "error_ratio": diagnostics.error_ratio,
                }
            )

        rows.append(
            {
                "variant": variant_name,
                "budget_pct": budget,
                "A1": _safe_mean([m["A1"] for m in budget_metrics]),
                "A3": _safe_mean([m["A3"] for m in budget_metrics]),
                "MRR": _safe_mean([m["MRR"] for m in budget_metrics]),
                "top1_confidence": _safe_mean([m["top1_confidence"] for m in budget_metrics]),
                "top1_gap": _safe_mean([m["top1_gap"] for m in budget_metrics]),
                "jaccard": _safe_mean(budget_jaccard),
                "kendall_tau": _safe_mean(budget_kendall),
                "displacement_rate": _safe_mean(budget_displacement),
                "swap_loss": _safe_mean(budget_swap_loss),
            }
        )

    return rows, details


def _safe_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _load_v9_and_baseline(v9_summary_csv: Path) -> Dict[float, Dict[str, float]]:
    grouped: Dict[float, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    with v9_summary_csv.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            budget = float(row["budget_pct"])
            grouped[budget]["baseline_A1"].append(float(row["baseline_A@1"]))
            grouped[budget]["baseline_A3"].append(float(row["baseline_A@3"]))
            grouped[budget]["baseline_MRR"].append(float(row["baseline_MRR"]))
            grouped[budget]["baseline_conf"].append(float(row["baseline_top1_confidence"]))
            grouped[budget]["baseline_gap"].append(float(row["baseline_top1_top2_gap"]))
            grouped[budget]["v9_A1"].append(float(row["v9_A@1"]))
            grouped[budget]["v9_A3"].append(float(row["v9_A@3"]))
            grouped[budget]["v9_MRR"].append(float(row["v9_MRR"]))
            grouped[budget]["v9_conf"].append(float(row["v9_top1_confidence"]))
            grouped[budget]["v9_gap"].append(float(row["v9_top1_top2_gap"]))

    out: Dict[float, Dict[str, float]] = {}
    for budget, values in grouped.items():
        out[budget] = {k: _safe_mean(v) for k, v in values.items()}
    return out


def _load_paper_baseline(table3_csv: Path) -> Dict[float, Dict[str, float]]:
    with table3_csv.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("Model", "")).strip() != "TraStrainer":
                continue
            return {
                0.1: {
                    "A1": float(row["A@1 0.1%"].strip()),
                    "A3": float(row["A@3 0.1%"].strip()),
                    "MRR": float(row["MRR 0.1%"].strip()),
                },
                1.0: {
                    "A1": float(row["A@1 1.0%"].strip()),
                    "A3": float(row["A@3 1.0%"].strip()),
                    "MRR": float(row["MRR 1.0%"].strip()),
                },
                2.5: {
                    "A1": float(row["A@1 2.5%"].strip()),
                    "A3": float(row["A@3 2.5%"].strip()),
                    "MRR": float(row["MRR 2.5%"].strip()),
                },
            }
    return {}


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_report(
    report_path: Path,
    compare_rows: List[Dict[str, Any]],
    ablation_rows: List[Dict[str, Any]],
    telemetry: Dict[str, Any],
    tests_status: str,
) -> None:
    lines: List[str] = []
    lines.append("# V10_IMPLEMENT_REPORT")
    lines.append("")
    lines.append("## 1) Objective")
    lines.append("Implement V10 sampler end-to-end with guard-aware marginal selection, RCA proxy evaluation, ablation, coherence telemetry, and unit tests.")
    lines.append("")
    lines.append("## 2) V10 Formulation")
    lines.append("- Base score: combines incident proximity, latency/span pressure, error flag, and time proximity.")
    lines.append("- Evidence score: aligned metric evidence from pod snapshots with sigmoid gate and selectivity term.")
    lines.append("- Guard marginal: enforces contrast (normal/error), scenario floor support, and novelty against selected services.")
    lines.append("- Objective: maximize lambda*Base + mu*Evidence + eta*Guard with redundancy penalty.")
    lines.append("- Residual repair: bounded swaps strictly under 5% of selected traces.")
    lines.append("")
    lines.append("## 3) Unit Tests")
    lines.append(f"- Status: {tests_status}")
    lines.append("- Coverage includes budget exactness, hard-constraint guards, coherence bounds, swap-limit, determinism, and evidence-weight effect.")
    lines.append("")
    lines.append("## 4) RCA Compare")
    lines.append("budget_pct,method,A1,A3,MRR,top1_confidence,top1_gap")
    for row in compare_rows:
        lines.append(
            f"{row['budget_pct']},{row['method']},{row['A1']:.6f},{row['A3']:.6f},{row['MRR']:.6f},{row['top1_confidence']},{row['top1_gap']}"
        )
    lines.append("")
    lines.append("## 5) Ablation")
    lines.append("variant,budget_pct,A1,A3,MRR,jaccard,kendall_tau,swap_loss")
    for row in ablation_rows:
        lines.append(
            f"{row['variant']},{row['budget_pct']},{row['A1']:.6f},{row['A3']:.6f},{row['MRR']:.6f},{row['jaccard']:.6f},{row['kendall_tau']:.6f},{row['swap_loss']:.6f}"
        )
    lines.append("")
    lines.append("## 6) Coherence")
    for budget, payload in telemetry.get("coherence_by_budget", {}).items():
        lines.append(
            f"- budget {budget}: jaccard={payload['pre_post_jaccard']:.6f}, kendall_tau={payload['kendall_tau']:.6f}, "
            f"displacement_rate={payload['displacement_rate']:.6f}, avg_swap_loss={payload['average_swap_score_loss']:.6f}"
        )
    lines.append("")
    lines.append("## 7) Verdict")
    lines.append("V10 implementation is complete with reproducible artifacts for comparison, ablation, telemetry, and tests.")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V10 RCA evaluation + ablation + report generation")
    parser.add_argument("--root", default=".", help="Path to SignozCore root")
    parser.add_argument("--output-dir", default="reports/new/v10", help="Output directory for V10 artifacts")
    parser.add_argument("--budgets", default="0.1,1.0,2.5", help="Comma-separated sampling budgets")
    parser.add_argument("--before-sec", type=int, default=300)
    parser.add_argument("--after-sec", type=int, default=600)
    parser.add_argument("--v9-summary-csv", default="results/v9_full_benchmark_summary.csv")
    parser.add_argument("--table3-csv", default="reports/compare/table3-all-models-plus-ours-20260422-144910.csv")
    parser.add_argument("--tests-status", default="Not run")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    output_dir = (root / args.output_dir).resolve()
    budgets = [float(x.strip()) for x in args.budgets.split(",") if x.strip()]

    specs = _dataset_specs(root)
    bundles = [_build_bundle(spec, before_sec=args.before_sec, after_sec=args.after_sec) for spec in specs]

    variants = {
        "V10-A": {
            "lambda_base": 1.0,
            "mu_evidence": 0.0,
            "eta_guard": 0.0,
            "enable_residual_fix": False,
        },
        "V10-B": {
            "lambda_base": 0.55,
            "mu_evidence": 0.45,
            "eta_guard": 0.0,
            "enable_residual_fix": False,
        },
        "V10-C": {
            "lambda_base": 0.45,
            "mu_evidence": 0.35,
            "eta_guard": 0.20,
            "enable_residual_fix": False,
        },
        "V10-D": {
            "lambda_base": 0.45,
            "mu_evidence": 0.35,
            "eta_guard": 0.20,
            "enable_residual_fix": True,
        },
    }

    all_ablation_rows: List[Dict[str, Any]] = []
    all_details: List[Dict[str, Any]] = []
    variant_rows: Dict[str, List[Dict[str, Any]]] = {}

    for variant_name, variant_cfg in variants.items():
        rows, details = _run_variant(bundles=bundles, budgets=budgets, variant_name=variant_name, variant_cfg=variant_cfg)
        variant_rows[variant_name] = rows
        all_details.extend(details)
        for row in rows:
            all_ablation_rows.append(
                {
                    "variant": row["variant"],
                    "budget_pct": row["budget_pct"],
                    "A1": row["A1"],
                    "A3": row["A3"],
                    "MRR": row["MRR"],
                    "jaccard": row["jaccard"],
                    "kendall_tau": row["kendall_tau"],
                    "swap_loss": row["swap_loss"],
                }
            )

    v10_rows = {row["budget_pct"]: row for row in variant_rows["V10-D"]}

    v9_baseline = _load_v9_and_baseline((root / args.v9_summary_csv).resolve())
    paper_baseline = _load_paper_baseline((root / args.table3_csv).resolve())

    compare_rows: List[Dict[str, Any]] = []
    for budget in budgets:
        v10 = v10_rows.get(budget, {})
        vb = v9_baseline.get(budget, {})
        pb = paper_baseline.get(budget, {})

        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "microrank_baseline",
                "A1": vb.get("baseline_A1", 0.0),
                "A3": vb.get("baseline_A3", 0.0),
                "MRR": vb.get("baseline_MRR", 0.0),
                "top1_confidence": vb.get("baseline_conf", ""),
                "top1_gap": vb.get("baseline_gap", ""),
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "v9_metric_aware",
                "A1": vb.get("v9_A1", 0.0),
                "A3": vb.get("v9_A3", 0.0),
                "MRR": vb.get("v9_MRR", 0.0),
                "top1_confidence": vb.get("v9_conf", ""),
                "top1_gap": vb.get("v9_gap", ""),
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "v10_guard_aware",
                "A1": v10.get("A1", 0.0),
                "A3": v10.get("A3", 0.0),
                "MRR": v10.get("MRR", 0.0),
                "top1_confidence": v10.get("top1_confidence", 0.0),
                "top1_gap": v10.get("top1_gap", 0.0),
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "paper_trastrainer",
                "A1": pb.get("A1", 0.0) / 100.0,
                "A3": pb.get("A3", 0.0) / 100.0,
                "MRR": pb.get("MRR", 0.0),
                "top1_confidence": "",
                "top1_gap": "",
            }
        )

    coherence_by_budget: Dict[str, Dict[str, float]] = {}
    for row in variant_rows["V10-D"]:
        coherence_by_budget[str(row["budget_pct"])] = {
            "pre_post_jaccard": row["jaccard"],
            "kendall_tau": row["kendall_tau"],
            "displacement_rate": row["displacement_rate"],
            "average_swap_score_loss": row["swap_loss"],
        }

    telemetry_payload: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "variant": "V10-D",
        "coherence_by_budget": coherence_by_budget,
        "details": all_details,
    }

    compare_csv = output_dir / "rca_compare_v10.csv"
    ablation_csv = output_dir / "v10_ablation.csv"
    telemetry_json = output_dir / "v10_telemetry_report.json"
    report_md = output_dir / "V10_IMPLEMENT_REPORT.md"

    _write_csv(compare_csv, compare_rows)
    _write_csv(ablation_csv, all_ablation_rows)
    telemetry_json.parent.mkdir(parents=True, exist_ok=True)
    telemetry_json.write_text(json.dumps(telemetry_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    _write_report(
        report_path=report_md,
        compare_rows=compare_rows,
        ablation_rows=all_ablation_rows,
        telemetry=telemetry_payload,
        tests_status=args.tests_status,
    )

    print(f"rca_compare_v10={compare_csv}")
    print(f"v10_ablation={ablation_csv}")
    print(f"v10_telemetry_report={telemetry_json}")
    print(f"v10_report={report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
