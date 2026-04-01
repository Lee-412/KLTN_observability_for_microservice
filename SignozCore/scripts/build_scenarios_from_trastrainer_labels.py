#!/usr/bin/env python3
"""Build evaluator scenario JSONL from TraStrainer label.json.

Input label format (TraStrainer):
{
  "11": [
    {
      "inject_time": "2023-01-30 11:51:46",
      "inject_timestamp": "1675079506",
      "inject_pod": "ts-contacts-service-866bd68c97-dzqgd",
      "inject_type": "network_delay"
    }
  ]
}

Output per row (JSONL):
- scenario_id
- incident_start_ts
- incident_end_ts
- ground_truth_root_cause
- inject_type
- trace_glob
- metric_dir
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any


def _to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_pod_to_service(pod_name: str) -> str:
    # Example: ts-contacts-service-866bd68c97-dzqgd -> ts-contacts-service
    parts = pod_name.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return pod_name


def build_rows(
    labels: Dict[str, List[Dict[str, Any]]],
    dataset_name: str,
    trace_root: str,
    metric_root: str,
    before_sec: int,
    after_sec: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    idx = 0

    for bucket in sorted(labels.keys(), key=lambda x: int(x)):
        for item in labels[bucket]:
            idx += 1
            ts = int(str(item["inject_timestamp"]).strip())
            start_iso = _to_iso_utc(ts - before_sec)
            end_iso = _to_iso_utc(ts + after_sec)
            service = _strip_pod_to_service(str(item.get("inject_pod", "")).strip())
            scn_id = f"scn-{dataset_name}-{bucket}-{idx:03d}"

            rows.append(
                {
                    "scenario_id": scn_id,
                    "incident_start_ts": start_iso,
                    "incident_end_ts": end_iso,
                    "ground_truth_root_cause": service,
                    "inject_type": item.get("inject_type"),
                    "inject_pod": item.get("inject_pod"),
                    "inject_timestamp": str(item.get("inject_timestamp")),
                    "trace_glob": f"{trace_root}/*.csv",
                    "metric_dir": metric_root,
                }
            )

    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Build scenarios from TraStrainer labels")
    ap.add_argument("--label-file", required=True, help="Path to label.json")
    ap.add_argument("--dataset-name", required=True, help="Name tag, e.g. train-ticket")
    ap.add_argument("--trace-root", required=True, help="Path to trace folder for this dataset")
    ap.add_argument("--metric-root", required=True, help="Path to metric folder for this dataset")
    ap.add_argument("--out", required=True, help="Output .jsonl path")
    ap.add_argument("--before-sec", type=int, default=300, help="Seconds before inject time")
    ap.add_argument("--after-sec", type=int, default=600, help="Seconds after inject time")
    args = ap.parse_args()

    label_path = Path(args.label_file).resolve()
    out_path = Path(args.out).resolve()

    if not label_path.exists():
        raise SystemExit(f"Label file not found: {label_path}")

    labels = json.loads(label_path.read_text(encoding="utf-8"))
    if not isinstance(labels, dict):
        raise SystemExit("Unsupported label format: expected object")

    rows = build_rows(
        labels=labels,
        dataset_name=args.dataset_name,
        trace_root=args.trace_root,
        metric_root=args.metric_root,
        before_sec=args.before_sec,
        after_sec=args.after_sec,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"scenario_count={len(rows)}")
    print(f"out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
