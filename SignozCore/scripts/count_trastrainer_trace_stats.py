#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def count_trace_dir(trace_dir: Path) -> dict[str, int]:
    unique_ids: set[str] = set()
    csv_rows = 0
    csv_files = 0

    for path in sorted(trace_dir.glob("*.csv")):
        csv_files += 1
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                csv_rows += 1
                tid = str(row.get("TraceID", "")).strip().lower()
                if tid:
                    unique_ids.add(tid)

    return {
        "csv_files": csv_files,
        "csv_rows": csv_rows,
        "unique_trace_ids": len(unique_ids),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Path to SignozCore root")
    ap.add_argument(
        "--out",
        default="reports/compare/trastrainer-trace-stats.csv",
        help="Output CSV path",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()

    datasets = {
        "train-ticket": root / "data/paper-source/TraStrainer/data/dataset/train_ticket/test/trace",
        "hipster-batch1": root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/trace",
        "hipster-batch2": root / "data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/trace",
    }

    rows = []
    for name, trace_dir in datasets.items():
        stats = count_trace_dir(trace_dir)
        rows.append(
            {
                "dataset": name,
                "trace_dir": str(trace_dir),
                **stats,
            }
        )

    total = {
        "dataset": "TOTAL",
        "trace_dir": "",
        "csv_files": sum(r["csv_files"] for r in rows),
        "csv_rows": sum(r["csv_rows"] for r in rows),
        "unique_trace_ids": sum(r["unique_trace_ids"] for r in rows),
    }
    rows.append(total)

    out = root / args.out
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8", newline="") as f:
        fields = ["dataset", "trace_dir", "csv_files", "csv_rows", "unique_trace_ids"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"trace_stats_csv={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())