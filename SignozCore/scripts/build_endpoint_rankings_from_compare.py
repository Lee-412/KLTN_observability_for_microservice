#!/usr/bin/env python3
"""Build endpoint ranking JSON from one-click compare report.

This script extracts the "KEPT TRACE ROUTES" table and ranks endpoints by kept rate.
It outputs a JSON compatible with scripts/evaluate_rca_ranking.py.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def extract_endpoint_rows(text: str):
    # Match rows like:
    # /payment 1311/3125 41.95% 1311 3125
    pattern = re.compile(
        r"(?P<endpoint>/[A-Za-z0-9_\-]+)\s+"
        r"(?P<kept>\d+)/(?P<baseline>\d+)\s+"
        r"(?P<rate>\d+(?:\.\d+)?)%"
    )

    seen = {}
    for m in pattern.finditer(text):
        endpoint = m.group("endpoint")
        # Deduplicate if the report has repeated sections.
        if endpoint in seen:
            continue
        seen[endpoint] = {
            "endpoint": endpoint,
            "kept": int(m.group("kept")),
            "baseline": int(m.group("baseline")),
            "rate": float(m.group("rate")),
        }

    return list(seen.values())


def main() -> int:
    parser = argparse.ArgumentParser(description="Build endpoint ranking from compare report")
    parser.add_argument("--compare-report", required=True, help="Path to combined one-click report")
    parser.add_argument("--out", required=True, help="Output ranking JSON path")
    args = parser.parse_args()

    report_path = Path(args.compare_report).resolve()
    out_path = Path(args.out).resolve()

    if not report_path.exists():
        raise SystemExit(f"Compare report not found: {report_path}")

    text = report_path.read_text(encoding="utf-8", errors="ignore")
    rows = extract_endpoint_rows(text)
    if not rows:
        raise SystemExit("No endpoint rows found in compare report")

    rows.sort(key=lambda r: (-r["rate"], r["endpoint"]))

    ranked_candidates = []
    for idx, row in enumerate(rows, start=1):
        ranked_candidates.append(
            {
                "candidate_id": row["endpoint"],
                "rank": idx,
                "score": round(row["rate"] / 100.0, 6),
                "meta": {
                    "kept": row["kept"],
                    "baseline": row["baseline"],
                    "kept_rate_pct": row["rate"],
                },
            }
        )

    result = {
        "method": "endpoint-kept-rate-from-compare-report",
        "source_compare_report": str(report_path),
        "ranked_candidates": ranked_candidates,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"wrote_ranking={out_path}")
    print(f"candidate_count={len(ranked_candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
