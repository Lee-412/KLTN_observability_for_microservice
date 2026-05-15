from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

Table3Row = dict[str, float | str]
CurrentRowBuilder = Callable[[list[dict[str, Any]], str, str], Table3Row]


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_dict_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_compare_avg_rows(
    *,
    compare_rows: list[dict[str, Any]],
    benchmark_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
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
        scenario_total = sum(
            int(r0.get("scenario_count") or 0)
            for r0 in benchmark_rows
            if float(r0["budget_pct"]) == budget
        )

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

    return compare_avg_rows


def _write_benchmark_markdown(
    *,
    path: Path,
    benchmark_rows: list[dict[str, Any]],
    benchmark_csv: Path,
    selection_mode: str,
    tag: str,
) -> None:
    lines: list[str] = []
    lines.append(f"# RCA Final Summary ({selection_mode})")
    lines.append("")
    lines.append(f"tag: {tag}")
    lines.append("")
    lines.append("| budget | dataset | scenarios | A@1 | A@3 | MRR |")
    lines.append("|---:|---|---:|---:|---:|---:|")

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
            lines.append(
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
            lines.append(
                "| {budget:.1f}% | AVERAGE | {sc} | {a1:.4f} | {a3:.4f} | {mrr:.4f} |".format(
                    budget=budget,
                    sc=total_sc,
                    a1=(sum_a1 / n),
                    a3=(sum_a3 / n),
                    mrr=(sum_mrr / n),
                )
            )

    lines.extend(
        [
            "",
            "## Related CSV",
            f"- {benchmark_csv}",
        ]
    )
    _write_text(path, "\n".join(lines) + "\n")


def _write_table3_outputs(
    *,
    root: Path,
    run_stamp: str,
    compare_avg_rows: list[dict[str, Any]],
    selection_mode: str,
    tag: str,
    paper_table3_reference_rows: list[Table3Row],
    known_ours_reference_rows: list[Table3Row],
    current_ours_table3_row_builder: CurrentRowBuilder,
    table3_notes: list[str] | None,
) -> tuple[Path, Path]:
    current_row = current_ours_table3_row_builder(compare_avg_rows, selection_mode, tag)
    if any(str(r.get("model")) == str(current_row.get("model")) for r in known_ours_reference_rows):
        table3_rows = paper_table3_reference_rows + known_ours_reference_rows
    else:
        table3_rows = paper_table3_reference_rows + known_ours_reference_rows + [current_row]

    table3_csv = root / "reports/compare" / f"table3-all-models-plus-ours-{run_stamp}.csv"
    table3_csv.parent.mkdir(parents=True, exist_ok=True)
    with table3_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Model",
                "A@1 0.1%",
                "A@1 1.0%",
                "A@1 2.5%",
                "A@3 0.1%",
                "A@3 1.0%",
                "A@3 2.5%",
                "MRR 0.1%",
                "MRR 1.0%",
                "MRR 2.5%",
            ]
        )
        for row in table3_rows:
            writer.writerow(
                [
                    row["model"],
                    f"{float(row['a1_0p1']):.2f}",
                    f"{float(row['a1_1p0']):.2f}",
                    f"{float(row['a1_2p5']):.2f}",
                    f"{float(row['a3_0p1']):.2f}",
                    f"{float(row['a3_1p0']):.2f}",
                    f"{float(row['a3_2p5']):.2f}",
                    f"{float(row['mrr_0p1']):.4f}",
                    f"{float(row['mrr_1p0']):.4f}",
                    f"{float(row['mrr_2p5']):.4f}",
                ]
            )

    table3_md = root / "reports/compare" / f"table3-all-models-plus-ours-{run_stamp}.md"
    lines: list[str] = []
    lines.append("# Table 3 Style Comparison: All Paper Models + Ours")
    lines.append("")
    lines.append(f"generated_at: {run_stamp}")
    lines.append("")
    lines.append(
        "| Model | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in table3_rows:
        lines.append(
            "| {model} | {a1_0p1:.2f} | {a1_1p0:.2f} | {a1_2p5:.2f} | {a3_0p1:.2f} | {a3_1p0:.2f} | {a3_2p5:.2f} | {mrr_0p1:.4f} | {mrr_1p0:.4f} | {mrr_2p5:.4f} |".format(
                model=str(row["model"]),
                a1_0p1=float(row["a1_0p1"]),
                a1_1p0=float(row["a1_1p0"]),
                a1_2p5=float(row["a1_2p5"]),
                a3_0p1=float(row["a3_0p1"]),
                a3_1p0=float(row["a3_1p0"]),
                a3_2p5=float(row["a3_2p5"]),
                mrr_0p1=float(row["mrr_0p1"]),
                mrr_1p0=float(row["mrr_1p0"]),
                mrr_2p5=float(row["mrr_2p5"]),
            )
        )

    if table3_notes:
        lines.extend(["", "## Notes", *table3_notes])

    _write_text(table3_md, "\n".join(lines) + "\n")
    return table3_csv, table3_md


def write_final_reports(
    *,
    root: Path,
    tag: str,
    run_stamp: str,
    selection_mode: str,
    benchmark_rows: list[dict[str, Any]],
    compare_rows: list[dict[str, Any]],
    sampling_quality_rows: list[dict[str, Any]],
    paper_table3_reference_rows: list[Table3Row],
    known_ours_reference_rows: list[Table3Row],
    current_ours_table3_row_builder: CurrentRowBuilder,
    table3_notes: list[str] | None = None,
    print_outputs: bool = True,
) -> dict[str, Any]:
  
    compare_dir = root / "reports/compare"
    compare_dir.mkdir(parents=True, exist_ok=True)

    benchmark_csv = compare_dir / f"rca-paper-table-sampled-budgets-{tag}.csv"
    _write_dict_csv(benchmark_csv, benchmark_rows)

    compare_avg_rows = _build_compare_avg_rows(
        compare_rows=compare_rows,
        benchmark_rows=benchmark_rows,
    )

    compare_csv = compare_dir / f"rca-paper-vs-ours-microrank-budgets-{tag}.csv"
    _write_dict_csv(compare_csv, compare_avg_rows)

    benchmark_md = compare_dir / f"rca-dataset-budget-metrics-{tag}.md"
    _write_benchmark_markdown(
        path=benchmark_md,
        benchmark_rows=benchmark_rows,
        benchmark_csv=benchmark_csv,
        selection_mode=selection_mode,
        tag=tag,
    )

    table3_csv, table3_md = _write_table3_outputs(
        root=root,
        run_stamp=run_stamp,
        compare_avg_rows=compare_avg_rows,
        selection_mode=selection_mode,
        tag=tag,
        paper_table3_reference_rows=paper_table3_reference_rows,
        known_ours_reference_rows=known_ours_reference_rows,
        current_ours_table3_row_builder=current_ours_table3_row_builder,
        table3_notes=table3_notes,
    )

    quality_csv = compare_dir / f"sampling-quality-key-metrics-{tag}.csv"
    _write_dict_csv(quality_csv, sampling_quality_rows)

    outputs = {
        "benchmark_csv": benchmark_csv,
        "compare_csv": compare_csv,
        "benchmark_md": benchmark_md,
        "table3_csv": table3_csv,
        "table3_md": table3_md,
        "quality_csv": quality_csv,
        "compare_avg_rows": compare_avg_rows,
    }

    if print_outputs:
        print(f"benchmark_csv={benchmark_csv}")
        print(f"compare_csv={compare_csv}")
        print(f"benchmark_md={benchmark_md}")
        print(f"table3_csv={table3_csv}")
        print(f"table3_md={table3_md}")
        print(f"quality_csv={quality_csv}")
        print("primary_markdown_outputs=2")

    return outputs
