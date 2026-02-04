#!/usr/bin/env python3
"""Run OTLP trace comparison and save the output under reports/compare.

This is a convenience wrapper around scripts/compare_otlp_traces.py.

Default behavior (no args):
- auto-pick newest trace export in:
  - signoz/deploy/docker/otel-export/baseline2
  - signoz/deploy/docker/otel-export/model1
- write a timestamped report file to:
  - reports/compare/

Examples:
  # Auto-pick newest baseline2 + model1 files
  python3 scripts/run_compare_and_save.py

  # Use explicit files
  python3 scripts/run_compare_and_save.py \
    --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
    --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json

  # Use directories (auto-pick newest from each)
  python3 scripts/run_compare_and_save.py \
    --base-dir signoz/deploy/docker/otel-export/baseline2 \
    --test-dir signoz/deploy/docker/otel-export/model1
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def _workspace_root() -> Path:
    # scripts/<this_file> -> repo root
    return Path(__file__).resolve().parents[1]


def _pick_newest_trace_file(directory: Path) -> Path:
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Directory not found: {directory}")

    # Accept both baseline and model naming styles.
    candidates = sorted(directory.glob("*traces*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No '*traces*.json' files found in: {directory}")
    return candidates[-1]


def _resolve_path(root: Path, maybe_rel: str) -> Path:
    p = Path(maybe_rel)
    return p if p.is_absolute() else (root / p)


def main() -> int:
    root = _workspace_root()

    ap = argparse.ArgumentParser()
    ap.add_argument("--base", help="Baseline traces JSON (line-delimited OTLP)")
    ap.add_argument("--test", help="Test/model traces JSON (line-delimited OTLP)")
    ap.add_argument("--base-dir", help="Directory to auto-pick newest baseline traces JSON")
    ap.add_argument("--test-dir", help="Directory to auto-pick newest test/model traces JSON")
    ap.add_argument("--no-details", action="store_true", help="Do not pass --details to compare script")
    args = ap.parse_args()

    default_base_dir = root / "signoz/deploy/docker/otel-export/baseline2"
    default_test_dir = root / "signoz/deploy/docker/otel-export/model1"

    base_path: Optional[Path] = _resolve_path(root, args.base) if args.base else None
    test_path: Optional[Path] = _resolve_path(root, args.test) if args.test else None

    if base_path is None:
        base_dir = _resolve_path(root, args.base_dir) if args.base_dir else default_base_dir
        base_path = _pick_newest_trace_file(base_dir)

    if test_path is None:
        test_dir = _resolve_path(root, args.test_dir) if args.test_dir else default_test_dir
        test_path = _pick_newest_trace_file(test_dir)

    compare_script = root / "scripts/compare_otlp_traces.py"
    if not compare_script.exists():
        raise FileNotFoundError(f"Compare script not found: {compare_script}")

    out_dir = root / "reports/compare"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_base = base_path.name.replace(" ", "_")
    safe_test = test_path.name.replace(" ", "_")
    out_path = out_dir / f"{ts}-{safe_base}_VS_{safe_test}.txt"

    cmd = [
        sys.executable,
        str(compare_script),
        "--base",
        str(base_path),
        "--test",
        str(test_path),
    ]
    if not args.no_details:
        cmd.append("--details")

    proc = subprocess.run(cmd, capture_output=True, text=True)

    # Write a single report that includes the command and output (stdout+stderr).
    report = []
    report.append(f"command: {' '.join(cmd)}")
    report.append(f"exit_code: {proc.returncode}")
    report.append(f"base: {base_path}")
    report.append(f"test: {test_path}")
    report.append("")
    report.append("---- stdout ----")
    report.append(proc.stdout.rstrip("\n"))
    report.append("")
    report.append("---- stderr ----")
    report.append(proc.stderr.rstrip("\n"))
    report.append("")

    out_path.write_text("\n".join(report) + "\n")

    # Print a short summary to console.
    print(f"Saved report: {out_path}")
    if proc.returncode != 0:
        print("Compare exited non-zero; check the report for stderr.")
        return proc.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
