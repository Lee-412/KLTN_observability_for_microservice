#!/usr/bin/env python3
"""Build paper-format single-row CSV from evaluator summary JSON."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


def _ci_to_str(v: Any) -> str:
    if isinstance(v, list) and len(v) == 2:
        return f"[{v[0]:.4f},{v[1]:.4f}]"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert evaluator summary to paper table row")
    ap.add_argument("--summary", required=True, help="Path to evaluator summary JSON")
    ap.add_argument("--dataset", required=True, help="Dataset name label")
    ap.add_argument("--profile", default="coverage-first", help="Profile label")
    ap.add_argument("--out-csv", required=True, help="Output CSV path")
    ap.add_argument("--append", action="store_true", help="Append row if file exists")
    args = ap.parse_args()

    summary_path = Path(args.summary).resolve()
    out_csv = Path(args.out_csv).resolve()

    s: Dict[str, Any] = json.loads(summary_path.read_text(encoding="utf-8"))

    fields: List[str] = [
        "dataset",
        "profile",
        "scenario_count",
        "A@1",
        "A@3",
        "MRR",
        "A@1_CI95",
        "A@3_CI95",
        "MRR_CI95",
        "summary_file",
    ]

    row = {
        "dataset": args.dataset,
        "profile": args.profile,
        "scenario_count": s.get("scenario_count", ""),
        "A@1": s.get("A@1", ""),
        "A@3": s.get("A@3", ""),
        "MRR": s.get("MRR", ""),
        "A@1_CI95": _ci_to_str(s.get("A@1_CI95")),
        "A@3_CI95": _ci_to_str(s.get("A@3_CI95")),
        "MRR_CI95": _ci_to_str(s.get("MRR_CI95")),
        "summary_file": str(summary_path),
    }

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    exists = out_csv.exists()
    mode = "a" if args.append and exists else "w"

    with out_csv.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if mode == "w":
            w.writeheader()
        w.writerow(row)

    print(f"out_csv={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
