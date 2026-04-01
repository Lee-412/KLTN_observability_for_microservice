#!/usr/bin/env python3
"""Prepare paper-source scenarios and mapping manifest from TraStrainer datasets.

Datasets (public in TraStrainer):
- train_ticket/test
- hipster/batches/batch1
- hipster/batches/batch2

Outputs:
- one scenario .jsonl per dataset
- one manifest .json summarizing paths and scenario counts
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_pod_to_service(pod_name: str) -> str:
    parts = pod_name.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return pod_name


def _load_labels(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Label format invalid: {path}")
    return obj


def _build_rows(
    labels: Dict[str, List[Dict[str, Any]]],
    dataset_name: str,
    trace_root: Path,
    metric_root: Path,
    before_sec: int,
    after_sec: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    idx = 0

    for bucket in sorted(labels.keys(), key=lambda x: int(x)):
        items = labels[bucket]
        if not isinstance(items, list):
            continue
        for item in items:
            idx += 1
            ts = int(str(item["inject_timestamp"]).strip())
            start_iso = _to_iso_utc(ts - before_sec)
            end_iso = _to_iso_utc(ts + after_sec)
            inject_pod = str(item.get("inject_pod", "")).strip()
            service = _strip_pod_to_service(inject_pod)
            scn_id = f"scn-{dataset_name}-{bucket}-{idx:03d}"

            rows.append(
                {
                    "scenario_id": scn_id,
                    "incident_start_ts": start_iso,
                    "incident_end_ts": end_iso,
                    "ground_truth_root_cause": service,
                    "inject_type": item.get("inject_type"),
                    "inject_pod": inject_pod,
                    "inject_timestamp": str(item.get("inject_timestamp")),
                    "trace_glob": str(trace_root / "*.csv"),
                    "metric_dir": str(metric_root),
                }
            )

    return rows


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Prepare TraStrainer paper-source scenarios")
    ap.add_argument("--trastrainer-root", required=True, help="Path to TraStrainer repository root")
    ap.add_argument(
        "--out-scenario-dir",
        default="reports/analysis/rca-benchmark",
        help="Output directory for scenario jsonl files",
    )
    ap.add_argument(
        "--out-manifest",
        default="reports/analysis/rca-benchmark/paper-source-map.20260401.json",
        help="Output manifest json path",
    )
    ap.add_argument("--tag", default="20260401", help="Date tag for output scenario filenames")
    ap.add_argument("--before-sec", type=int, default=300, help="Seconds before inject timestamp")
    ap.add_argument("--after-sec", type=int, default=600, help="Seconds after inject timestamp")
    args = ap.parse_args()

    root = Path(args.trastrainer_root).resolve()
    out_scenario_dir = Path(args.out_scenario_dir).resolve()
    out_manifest = Path(args.out_manifest).resolve()

    specs = [
        {
            "dataset": "train-ticket",
            "label": root / "data/dataset/train_ticket/test/label.json",
            "trace": root / "data/dataset/train_ticket/test/trace",
            "metric": root / "data/dataset/train_ticket/test/metric",
        },
        {
            "dataset": "hipster-batch1",
            "label": root / "data/dataset/hipster/batches/batch1/label.json",
            "trace": root / "data/dataset/hipster/batches/batch1/trace",
            "metric": root / "data/dataset/hipster/batches/batch1/metric",
        },
        {
            "dataset": "hipster-batch2",
            "label": root / "data/dataset/hipster/batches/batch2/label.json",
            "trace": root / "data/dataset/hipster/batches/batch2/trace",
            "metric": root / "data/dataset/hipster/batches/batch2/metric",
        },
    ]

    manifest: Dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trastrainer_root": str(root),
        "datasets": [],
    }

    for s in specs:
        label_file = s["label"]
        trace_dir = s["trace"]
        metric_dir = s["metric"]
        dataset = s["dataset"]

        if not label_file.exists():
            manifest["datasets"].append(
                {
                    "dataset": dataset,
                    "status": "missing_label",
                    "label_file": str(label_file),
                }
            )
            continue

        labels = _load_labels(label_file)
        rows = _build_rows(
            labels=labels,
            dataset_name=dataset,
            trace_root=trace_dir,
            metric_root=metric_dir,
            before_sec=args.before_sec,
            after_sec=args.after_sec,
        )

        out_file = out_scenario_dir / f"scenarios.{dataset}.paper-source.{args.tag}.jsonl"
        _write_jsonl(out_file, rows)

        manifest["datasets"].append(
            {
                "dataset": dataset,
                "status": "ok",
                "scenario_count": len(rows),
                "label_file": str(label_file),
                "trace_dir": str(trace_dir),
                "metric_dir": str(metric_dir),
                "scenario_file": str(out_file),
            }
        )

    out_manifest.parent.mkdir(parents=True, exist_ok=True)
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("== PREPARE TRASTRAINER PAPER INPUTS ==")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
