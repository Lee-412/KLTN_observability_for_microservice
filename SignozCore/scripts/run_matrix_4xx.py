#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CONFIGS = [
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor00_target50.yaml",
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target20.yaml",
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target35.yaml",
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target50.yaml",
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target70.yaml",
    "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor20_target50.yaml",
]


def run_cmd_stream(cmd: list[str], log_path: Path) -> str:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            lf.write(line)
        code = proc.wait()
    if code != 0:
        raise RuntimeError(f"Command failed ({code}): {' '.join(cmd)}\nSee log: {log_path}")
    return log_path.read_text(encoding="utf-8", errors="replace")


def parse_metrics(report_path: Path) -> dict[str, str]:
    text = report_path.read_text(encoding="utf-8", errors="replace")
    keys = {
        "kept_traces": r"kept traces\s*:\s*([^\n]+)",
        "kept_spans_rate": r"kept spans rate:\s*([^\n]+)",
        "kept_error_traces": r"kept error traces\s*:\s*([^\n]+)",
        "kept_non_error_traces": r"kept non-error traces:\s*([^\n]+)",
        "critical_cov": r"critical_endpoint_coverage_pct:\s*([^\n]+)",
        "early_retention": r"early_incident_retention_pct:\s*([^\n]+)",
    }
    out: dict[str, str] = {}
    for k, pat in keys.items():
        m = re.search(pat, text)
        out[k] = m.group(1).strip() if m else "NA"
    return out


def load_completed_variants(csv_path: Path) -> set[str]:
    done: set[str] = set()
    if not csv_path.exists():
        return done
    with csv_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            v = (row.get("variant") or "").strip()
            if v:
                done.add(v)
    return done


def append_row(csv_path: Path, fields: list[str], row: dict[str, str]) -> None:
    exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace-count", type=int, default=10000, help="Per-variant trace count to reduce machine pressure")
    ap.add_argument("--cooldown-seconds", type=float, default=20.0, help="Sleep between variants to avoid resource spikes")
    ap.add_argument("--max-runs", type=int, default=0, help="Run at most N variants this invocation (0 = all)")
    ap.add_argument("--csv", default="", help="Output CSV path; if set and exists, runner can resume")
    ap.add_argument("--resume", action="store_true", help="Skip variants already present in CSV")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_csv = ROOT / (args.csv if args.csv else f"reports/compare/matrix-4xx-{ts}.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    logs_dir = ROOT / f"reports/compare/matrix-4xx-logs-{ts}"
    logs_dir.mkdir(parents=True, exist_ok=True)

    for cfg in CONFIGS:
        variant = Path(cfg).stem.replace("otel-collector-config.", "")
        if args.resume and variant in load_completed_variants(out_csv):
            print(f"skip {variant} (already in CSV)", flush=True)
            continue
        print(f"=== RUN {variant} ===", flush=True)
        cmd = [
            "python3",
            "scripts/one_click_sampling_proof.py",
            "--trace-count",
            str(args.trace_count),
            "--endpoint-profile",
            "mixed-4xx",
            "--model-config",
            cfg,
        ]
        log_path = logs_dir / f"{variant}.log"
        out = run_cmd_stream(cmd, log_path)
        m = re.search(r"^retention=(.+)$", out, re.MULTILINE)
        if not m:
            raise RuntimeError(f"Cannot find retention path for {variant}. See log: {log_path}")
        report = Path(m.group(1).strip())
        metrics = parse_metrics(report)
        row = {
            "variant": variant,
            "report": str(report),
            **metrics,
        }
        fields = [
            "variant",
            "kept_traces",
            "kept_spans_rate",
            "kept_error_traces",
            "kept_non_error_traces",
            "critical_cov",
            "early_retention",
            "report",
        ]
        append_row(out_csv, fields, row)
        print(f"done {variant}: {metrics['kept_traces']} / {metrics['kept_error_traces']}", flush=True)
        if args.cooldown_seconds > 0:
            time.sleep(args.cooldown_seconds)
        if args.max_runs > 0:
            args.max_runs -= 1
            if args.max_runs == 0:
                break

    print(f"CSV={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
