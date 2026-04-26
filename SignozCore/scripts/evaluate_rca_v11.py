#!/usr/bin/env python3
"""Evaluate V11 RCA-aware sampler and generate comparison artifacts.

Outputs:
- rca_compare_v11.csv
- v11_ablation.csv
- v11_telemetry_report.json
- V11_IMPLEMENT_REPORT.md
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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import evaluate_paper_sampling as eps
import sampler_v10_guard_aware as sv10
import sampler_v11_rca_aware as sv11


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
    points: List[sv11.TracePoint]
    by_id: Dict[str, sv11.TracePoint]
    start_sec_by_id: Dict[str, int]
    scenarios: List[Scenario]
    scenario_windows: List[Tuple[str, int, int]]
    incident_anchor_sec: Optional[int]
    incident_services: Set[str]
    metrics_stream_by_sec: sv11.MetricsStreamBySec


def _safe_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _safe_std(values: Sequence[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mu = _safe_mean(values)
    var = sum((x - mu) * (x - mu) for x in values) / float(len(values))
    return float(var ** 0.5)


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
    metrics_stream_by_sec = sv11.build_real_metrics_stream(str(metric_dir))

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
    by_id: Dict[str, sv11.TracePoint],
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
            weight *= 1.15
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

        a1_values.append(1.0 if rank == 1 else 0.0)
        a3_values.append(1.0 if rank is not None and rank <= 3 else 0.0)
        rr_values.append(1.0 / rank if rank is not None else 0.0)

        if ranked:
            total_score = sum(score for _svc, score in ranked)
            top1 = ranked[0][1]
            top2 = ranked[1][1] if len(ranked) > 1 else 0.0
            conf = top1 / total_score if total_score > 0 else 0.0
            gap = top1 - top2
        else:
            conf = 0.0
            gap = 0.0

        conf_values.append(conf)
        gap_values.append(gap)

    return {
        "A1": _safe_mean(a1_values),
        "A3": _safe_mean(a3_values),
        "MRR": _safe_mean(rr_values),
        "top1_confidence": _safe_mean(conf_values),
        "top1_gap": _safe_mean(gap_values),
    }


def _run_variant(
    bundles: Sequence[DatasetBundle],
    budgets: Sequence[float],
    variant_name: str,
    sampler_kind: str,
    cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    details: List[Dict[str, Any]] = []

    for budget in budgets:
        budget_metrics: List[Dict[str, float]] = []
        budget_jaccard: List[float] = []
        budget_kendall: List[float] = []
        budget_displacement: List[float] = []
        budget_swap_loss: List[float] = []
        budget_score_std: List[float] = []

        for bundle in bundles:
            if sampler_kind == "v10":
                sampler = sv10.V10GuardAwareSampler(
                    lambda_base=cfg.get("lambda_base", 0.45),
                    mu_evidence=cfg.get("mu_evidence", 0.35),
                    eta_guard=cfg.get("eta_guard", 0.20),
                    tau=cfg.get("tau", 0.50),
                    min_normal_ratio=cfg.get("min_normal_ratio", 0.30),
                    min_error_ratio=cfg.get("min_error_ratio", 0.25),
                    redundancy_weight=cfg.get("redundancy_weight", 0.15),
                )
            else:
                sampler = sv11.V11RCAAwareSampler(
                    lambda_base=cfg.get("lambda_base", 0.40),
                    mu_evidence=cfg.get("mu_evidence", 0.25),
                    lambda_rca=cfg.get("lambda_rca", 0.25),
                    eta_guard=cfg.get("eta_guard", 0.10),
                    tau=cfg.get("tau", 0.50),
                    min_normal_ratio=cfg.get("min_normal_ratio", 0.30),
                    min_error_ratio=cfg.get("min_error_ratio", 0.25),
                    redundancy_weight=cfg.get("redundancy_weight", 0.15),
                    guard_floor_factor=cfg.get("guard_floor_factor", 0.20),
                )

            selected_ids, score_by_id, diagnostics = sampler.select_traces(
                points=bundle.points,
                budget_pct=budget,
                scenario_windows=bundle.scenario_windows,
                incident_anchor_sec=bundle.incident_anchor_sec,
                incident_services=bundle.incident_services,
                metrics_stream_by_sec=bundle.metrics_stream_by_sec,
                metric_lookback_sec=cfg.get("metric_lookback_sec", 120),
                min_incident_traces_per_scenario=1,
                enable_residual_fix=cfg.get("enable_residual_fix", True),
            )

            metric = _evaluate_proxy_rca(selected_ids=selected_ids, score_by_id=score_by_id, bundle=bundle)
            budget_metrics.append(metric)
            budget_jaccard.append(diagnostics.pre_post_jaccard)
            budget_kendall.append(diagnostics.kendall_tau)
            budget_displacement.append(diagnostics.displacement_rate)
            budget_swap_loss.append(diagnostics.average_swap_score_loss)
            budget_score_std.append(_safe_std(list(score_by_id.values())))

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
                    "score_std": _safe_std(list(score_by_id.values())),
                    "score_mean": _safe_mean(list(score_by_id.values())),
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
                "score_std": _safe_mean(budget_score_std),
            }
        )

    return rows, details


def _load_v9_and_baseline(v9_summary_csv: Path) -> Dict[float, Dict[str, float]]:
    grouped: Dict[float, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    with v9_summary_csv.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            budget = float(row["budget_pct"])
            grouped[budget]["baseline_A1"].append(float(row["baseline_A@1"]))
            grouped[budget]["baseline_A3"].append(float(row["baseline_A@3"]))
            grouped[budget]["baseline_MRR"].append(float(row["baseline_MRR"]))
            grouped[budget]["v9_A1"].append(float(row["v9_A@1"]))
            grouped[budget]["v9_A3"].append(float(row["v9_A@3"]))
            grouped[budget]["v9_MRR"].append(float(row["v9_MRR"]))

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
    path: Path,
    compare_rows: List[Dict[str, Any]],
    ablation_rows: List[Dict[str, Any]],
    telemetry_payload: Dict[str, Any],
    tests_status: str,
) -> None:
    lines: List[str] = []
    lines.append("# V11_IMPLEMENT_REPORT")
    lines.append("")
    lines.append("## 1) Why V11")
    lines.append("- Guard term in V10 could dominate RCA discriminative signal at low budget.")
    lines.append("- Confidence values were not comparable across legacy outputs and proxy outputs due scale mismatch.")
    lines.append("- V11 adds RCA-aware relevance to align objective with A@1 optimization.")
    lines.append("")
    lines.append("## 2) Objective")
    lines.append("- V11 score: lambda_base * base + mu_evidence * evidence + lambda_rca * rca_relevance.")
    lines.append("- Guard is annealed after quota deficits approach zero.")
    lines.append("- Evidence uses log1p scaling instead of sigmoid compression to preserve score dynamics.")
    lines.append("")
    lines.append("## 3) Tests")
    lines.append(f"- Status: {tests_status}")
    lines.append("")
    lines.append("## 4) RCA Compare")
    lines.append("budget_pct,method,A1,A3,MRR,top1_confidence_proxy,top1_gap_proxy,confidence_scale_note")
    for row in compare_rows:
        lines.append(
            f"{row['budget_pct']},{row['method']},{row['A1']:.6f},{row['A3']:.6f},{row['MRR']:.6f},"
            f"{row['top1_confidence_proxy']},{row['top1_gap_proxy']},{row['confidence_scale_note']}"
        )
    lines.append("")
    lines.append("## 5) Ablation")
    lines.append("variant,budget_pct,A1,A3,MRR,jaccard,kendall_tau,swap_loss,score_std")
    for row in ablation_rows:
        lines.append(
            f"{row['variant']},{row['budget_pct']},{row['A1']:.6f},{row['A3']:.6f},{row['MRR']:.6f},"
            f"{row['jaccard']:.6f},{row['kendall_tau']:.6f},{row['swap_loss']:.6f},{row['score_std']:.6f}"
        )
    lines.append("")
    lines.append("## 6) Score-Scale Hypothesis")
    for budget, payload in telemetry_payload.get("score_scale_hypothesis", {}).items():
        lines.append(
            f"- budget {budget}: std_ratio_v11_over_v10={payload['std_ratio_v11_over_v10']:.6f}, "
            f"A1_delta_v11_minus_v10={payload['A1_delta_v11_minus_v10']:.6f}"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V11 RCA-aware evaluation")
    parser.add_argument("--root", default=".", help="Path to SignozCore root")
    parser.add_argument("--output-dir", default="reports/new/v11", help="Output directory for V11 artifacts")
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

    bundles = [_build_bundle(spec, before_sec=args.before_sec, after_sec=args.after_sec) for spec in _dataset_specs(root)]

    variants: Dict[str, Tuple[str, Dict[str, Any]]] = {
        "V10-REF": (
            "v10",
            {
                "lambda_base": 0.45,
                "mu_evidence": 0.35,
                "eta_guard": 0.20,
                "enable_residual_fix": True,
            },
        ),
        "V11-A": (
            "v11",
            {
                "lambda_base": 1.0,
                "mu_evidence": 0.0,
                "lambda_rca": 0.0,
                "eta_guard": 0.0,
                "enable_residual_fix": False,
            },
        ),
        "V11-B": (
            "v11",
            {
                "lambda_base": 0.55,
                "mu_evidence": 0.45,
                "lambda_rca": 0.0,
                "eta_guard": 0.0,
                "enable_residual_fix": False,
            },
        ),
        "V11-C": (
            "v11",
            {
                "lambda_base": 0.45,
                "mu_evidence": 0.20,
                "lambda_rca": 0.35,
                "eta_guard": 0.0,
                "enable_residual_fix": False,
            },
        ),
        "V11-D": (
            "v11",
            {
                "lambda_base": 0.40,
                "mu_evidence": 0.25,
                "lambda_rca": 0.25,
                "eta_guard": 0.10,
                "guard_floor_factor": 0.20,
                "enable_residual_fix": True,
            },
        ),
    }

    variant_rows: Dict[str, List[Dict[str, Any]]] = {}
    details: List[Dict[str, Any]] = []
    ablation_rows: List[Dict[str, Any]] = []

    for variant_name, (sampler_kind, cfg) in variants.items():
        rows, variant_details = _run_variant(
            bundles=bundles,
            budgets=budgets,
            variant_name=variant_name,
            sampler_kind=sampler_kind,
            cfg=cfg,
        )
        variant_rows[variant_name] = rows
        details.extend(variant_details)
        if variant_name.startswith("V11-"):
            for row in rows:
                ablation_rows.append(
                    {
                        "variant": row["variant"],
                        "budget_pct": row["budget_pct"],
                        "A1": row["A1"],
                        "A3": row["A3"],
                        "MRR": row["MRR"],
                        "jaccard": row["jaccard"],
                        "kendall_tau": row["kendall_tau"],
                        "swap_loss": row["swap_loss"],
                        "score_std": row["score_std"],
                    }
                )

    v10_ref = {row["budget_pct"]: row for row in variant_rows["V10-REF"]}
    v11_full = {row["budget_pct"]: row for row in variant_rows["V11-D"]}

    legacy = _load_v9_and_baseline((root / args.v9_summary_csv).resolve())
    paper = _load_paper_baseline((root / args.table3_csv).resolve())

    compare_rows: List[Dict[str, Any]] = []
    for budget in budgets:
        leg = legacy.get(budget, {})
        v10 = v10_ref.get(budget, {})
        v11 = v11_full.get(budget, {})
        pap = paper.get(budget, {})

        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "microrank_baseline",
                "A1": leg.get("baseline_A1", 0.0),
                "A3": leg.get("baseline_A3", 0.0),
                "MRR": leg.get("baseline_MRR", 0.0),
                "top1_confidence_proxy": "",
                "top1_gap_proxy": "",
                "confidence_scale_note": "legacy_unscaled",
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "v9_metric_aware",
                "A1": leg.get("v9_A1", 0.0),
                "A3": leg.get("v9_A3", 0.0),
                "MRR": leg.get("v9_MRR", 0.0),
                "top1_confidence_proxy": "",
                "top1_gap_proxy": "",
                "confidence_scale_note": "legacy_unscaled",
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "v10_guard_aware_proxy",
                "A1": v10.get("A1", 0.0),
                "A3": v10.get("A3", 0.0),
                "MRR": v10.get("MRR", 0.0),
                "top1_confidence_proxy": v10.get("top1_confidence", 0.0),
                "top1_gap_proxy": v10.get("top1_gap", 0.0),
                "confidence_scale_note": "proxy_ratio",
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "v11_rca_aware",
                "A1": v11.get("A1", 0.0),
                "A3": v11.get("A3", 0.0),
                "MRR": v11.get("MRR", 0.0),
                "top1_confidence_proxy": v11.get("top1_confidence", 0.0),
                "top1_gap_proxy": v11.get("top1_gap", 0.0),
                "confidence_scale_note": "proxy_ratio",
            }
        )
        compare_rows.append(
            {
                "budget_pct": budget,
                "method": "paper_trastrainer",
                "A1": pap.get("A1", 0.0) / 100.0,
                "A3": pap.get("A3", 0.0) / 100.0,
                "MRR": pap.get("MRR", 0.0),
                "top1_confidence_proxy": "",
                "top1_gap_proxy": "",
                "confidence_scale_note": "paper_reported",
            }
        )

    score_scale_hypothesis: Dict[str, Dict[str, float]] = {}
    for budget in budgets:
        v10 = v10_ref.get(budget, {})
        v11 = v11_full.get(budget, {})
        denom = max(1e-9, float(v10.get("score_std", 0.0)))
        score_scale_hypothesis[str(budget)] = {
            "std_ratio_v11_over_v10": float(v11.get("score_std", 0.0)) / denom,
            "A1_delta_v11_minus_v10": float(v11.get("A1", 0.0)) - float(v10.get("A1", 0.0)),
        }

    telemetry_payload: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "variants": {
            "V10-REF": variant_rows["V10-REF"],
            "V11-D": variant_rows["V11-D"],
        },
        "score_scale_hypothesis": score_scale_hypothesis,
        "details": details,
    }

    compare_csv = output_dir / "rca_compare_v11.csv"
    ablation_csv = output_dir / "v11_ablation.csv"
    telemetry_json = output_dir / "v11_telemetry_report.json"
    report_md = output_dir / "V11_IMPLEMENT_REPORT.md"

    _write_csv(compare_csv, compare_rows)
    _write_csv(ablation_csv, ablation_rows)
    telemetry_json.parent.mkdir(parents=True, exist_ok=True)
    telemetry_json.write_text(json.dumps(telemetry_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    _write_report(
        path=report_md,
        compare_rows=compare_rows,
        ablation_rows=ablation_rows,
        telemetry_payload=telemetry_payload,
        tests_status=args.tests_status,
    )

    print(f"rca_compare_v11={compare_csv}")
    print(f"v11_ablation={ablation_csv}")
    print(f"v11_telemetry_report={telemetry_json}")
    print(f"v11_report={report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
