#!/usr/bin/env python3
"""Run the 2-pass HotROD baseline vs model-sampling experiment.

This script automates the exact manual steps you listed:
1) Baseline run (no model sampling):
   - Update otel-collector-config.yaml so traces pipeline does NOT include signoz_tail_sampling
   - Exporter file/traces writes to otel-export/traces.json
   - Recreate otel-collector
   - Run generator/hotrod for N seconds
   - Snapshot otel-export/traces.json -> otel-export/baseline2/<ts>-traces.json

2) Model run (with model sampling):
   - Update otel-collector-config.yaml so traces pipeline includes signoz_tail_sampling
   - Exporter file/traces writes to otel-export/traces.model.json
   - Recreate otel-collector with OTELCOL_TAG override (default: kltn-model)
   - Run generator/hotrod for N seconds
   - Snapshot otel-export/traces.model.json -> otel-export/model1/<ts>-traces.model.json

3) Compare + save report:
   - Calls scripts/run_compare_and_save.py with explicit snapshot paths

Notes:
- This compares *aggregates* (spans/traceIds/error spans) because baseline and model runs
  are separate executions (traceIds won't match 1:1).
- Assumes the SigNoz stack is already up; the script only recreates otel-collector.

Usage:
  python3 scripts/run_hotrod_baseline_model.py

Optional:
  python3 scripts/run_hotrod_baseline_model.py --hotrod-seconds 90 --model-tag kltn-model
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run(cmd: List[str], *, cwd: Optional[Path] = None, env: Optional[dict] = None) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env:
        merged_env.update({k: str(v) for k, v in env.items()})

    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=merged_env, text=True)


def _must_ok(proc: subprocess.CompletedProcess, what: str) -> None:
    if proc.returncode != 0:
        raise SystemExit(f"ERROR: {what} (exit={proc.returncode})")


def _docker_compose_cmd() -> List[str]:
    # Prefer 'docker compose' when available; fall back to docker-compose.
    proc = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True)
    if proc.returncode == 0:
        return ["docker", "compose"]
    return ["docker-compose"]


def _patch_collector_config(
    *,
    path: Path,
    processors_list: str,
    exporter_path_value: str,
) -> None:
    text = path.read_text(encoding="utf-8", errors="replace").splitlines(True)

    in_pipelines = False
    pipelines_indent = None
    in_traces_pipeline = False
    traces_indent = None

    in_exporters = False
    exporters_indent = None
    in_file_traces = False
    file_traces_indent = None

    def indent_len(line: str) -> int:
        return len(line) - len(line.lstrip(" "))

    for i, line in enumerate(text):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        ind = indent_len(line)

        # service/pipelines/traces processors
        if stripped == "pipelines:":
            in_pipelines = True
            pipelines_indent = ind
            in_traces_pipeline = False
            traces_indent = None
            continue

        if in_pipelines and pipelines_indent is not None and ind <= pipelines_indent and stripped != "pipelines:":
            in_pipelines = False
            in_traces_pipeline = False
            traces_indent = None

        if in_pipelines and stripped == "traces:":
            in_traces_pipeline = True
            traces_indent = ind
            continue

        if in_traces_pipeline and traces_indent is not None and ind <= traces_indent and stripped != "traces:":
            in_traces_pipeline = False
            traces_indent = None

        if in_traces_pipeline and traces_indent is not None:
            target_indent = traces_indent + 2
            if line.startswith(" " * target_indent + "processors:"):
                text[i] = " " * target_indent + f"processors: {processors_list}\n"

        # exporters/file/traces path
        if stripped == "exporters:":
            in_exporters = True
            exporters_indent = ind
            in_file_traces = False
            file_traces_indent = None
            continue

        if in_exporters and exporters_indent is not None and ind <= exporters_indent and stripped != "exporters:":
            in_exporters = False
            in_file_traces = False
            file_traces_indent = None

        if in_exporters and stripped == "file/traces:":
            in_file_traces = True
            file_traces_indent = ind
            continue

        if in_file_traces and file_traces_indent is not None and ind <= file_traces_indent and stripped != "file/traces:":
            in_file_traces = False
            file_traces_indent = None

        if in_file_traces and file_traces_indent is not None:
            target_indent = file_traces_indent + 2
            if line.startswith(" " * target_indent + "path:"):
                text[i] = " " * target_indent + f"path: {exporter_path_value}\n"

    path.write_text("".join(text), encoding="utf-8")


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _copy_snapshot(src: Path, dst_dir: Path, dst_name: str) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / dst_name
    shutil.copyfile(src, dst)
    return dst


def main() -> int:
    root = _workspace_root()

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--collector-config",
        default="signoz/deploy/docker/otel-collector-config.yaml",
        help="Collector config file to edit in-place",
    )
    ap.add_argument(
        "--compose-dir",
        default="signoz/deploy/docker",
        help="Directory containing docker-compose.yaml for the SigNoz stack",
    )
    ap.add_argument(
        "--hotrod-dir",
        default="signoz/deploy/docker/generator/hotrod",
        help="Directory containing docker-compose.yaml for hotrod generator",
    )
    ap.add_argument("--hotrod-seconds", type=int, default=90, help="How long to run hotrod workload")
    ap.add_argument(
        "--post-hotrod-sleep",
        type=int,
        default=10,
        help="Sleep after stopping hotrod to allow collector/exporter flush",
    )

    ap.add_argument("--baseline-export", default="signoz/deploy/docker/otel-export/traces.json")
    ap.add_argument("--model-export", default="signoz/deploy/docker/otel-export/traces.model.json")

    ap.add_argument("--baseline-snapshot-dir", default="signoz/deploy/docker/otel-export/baseline2")
    ap.add_argument("--model-snapshot-dir", default="signoz/deploy/docker/otel-export/model1")

    ap.add_argument(
        "--baseline-tag",
        default=None,
        help="Optional OTELCOL_TAG to use for baseline run (default: use compose/.env)",
    )
    ap.add_argument("--model-tag", default="kltn-model", help="OTELCOL_TAG to use for model run")

    ap.add_argument(
        "--keep-config",
        action="store_true",
        help="Do not restore otel-collector-config.yaml at the end (default: restore)",
    )

    ap.add_argument("--skip-compare", action="store_true")
    args = ap.parse_args()

    cfg_path = (root / args.collector_config).resolve()
    compose_dir = (root / args.compose_dir).resolve()
    hotrod_dir = (root / args.hotrod_dir).resolve()

    baseline_export = (root / args.baseline_export).resolve()
    model_export = (root / args.model_export).resolve()

    baseline_snap_dir = (root / args.baseline_snapshot_dir).resolve()
    model_snap_dir = (root / args.model_snapshot_dir).resolve()

    compose = _docker_compose_cmd()

    # Backup config so this script is safe to run.
    original_cfg = cfg_path.read_text(encoding="utf-8", errors="replace")

    # Reset exports (matches your manual rm -f traces*.json)
    for p in (baseline_export, model_export):
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists():
                p.unlink()

    baseline_snapshot = None
    model_snapshot = None

    try:
        # 1) Baseline
        print("\n== PASS 1: BASELINE (no model sampling) ==")
        _patch_collector_config(
            path=cfg_path,
            processors_list="[signozspanmetrics/delta, batch]",
            exporter_path_value="/var/signoz/otel-export/traces.json",
        )

        baseline_env = {"OTELCOL_TAG": args.baseline_tag} if args.baseline_tag else None
        proc = _run(compose + ["up", "-d", "--force-recreate", "otel-collector"], cwd=compose_dir, env=baseline_env)
        _must_ok(proc, "recreate otel-collector (baseline)")

        proc = _run(compose + ["up", "-d"], cwd=hotrod_dir)
        _must_ok(proc, "start hotrod generator")
        time.sleep(max(0, int(args.hotrod_seconds)))
        proc = _run(compose + ["down"], cwd=hotrod_dir)
        _must_ok(proc, "stop hotrod generator")

        time.sleep(max(0, int(args.post_hotrod_sleep)))

        if not baseline_export.exists():
            raise SystemExit(f"Baseline export not found: {baseline_export}")

        ts = _timestamp()
        baseline_snapshot = _copy_snapshot(baseline_export, baseline_snap_dir, f"{ts}-traces.json")
        print(f"Baseline snapshot: {baseline_snapshot}")

        # 2) Model
        print("\n== PASS 2: MODEL (linear + adaptive sampling) ==")
        _patch_collector_config(
            path=cfg_path,
            processors_list="[signozspanmetrics/delta, signoz_tail_sampling, batch]",
            exporter_path_value="/var/signoz/otel-export/traces.model.json",
        )

        proc = _run(
            compose + ["up", "-d", "--force-recreate", "otel-collector"],
            cwd=compose_dir,
            env={"OTELCOL_TAG": args.model_tag},
        )
        _must_ok(proc, "recreate otel-collector (model)")

        proc = _run(compose + ["up", "-d"], cwd=hotrod_dir)
        _must_ok(proc, "start hotrod generator")
        time.sleep(max(0, int(args.hotrod_seconds)))
        proc = _run(compose + ["down"], cwd=hotrod_dir)
        _must_ok(proc, "stop hotrod generator")

        time.sleep(max(0, int(args.post_hotrod_sleep)))

        if not model_export.exists():
            raise SystemExit(f"Model export not found: {model_export}")

        ts = _timestamp()
        model_snapshot = _copy_snapshot(model_export, model_snap_dir, f"{ts}-traces.model.json")
        print(f"Model snapshot: {model_snapshot}")

        # 3) Compare
        if not args.skip_compare:
            print("\n== COMPARE + SAVE REPORT ==")
            proc = _run(
                [
                    sys.executable,
                    str(root / "scripts/run_compare_and_save.py"),
                    "--base",
                    str(baseline_snapshot),
                    "--test",
                    str(model_snapshot),
                ],
                cwd=root,
            )
            _must_ok(proc, "compare and save")
    finally:
        if not args.keep_config:
            cfg_path.write_text(original_cfg, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
