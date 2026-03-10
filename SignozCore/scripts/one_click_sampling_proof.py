#!/usr/bin/env python3
"""One-click baseline vs model sampling proof runner.

What it does:
1) Run baseline collector config and generate deterministic traces.
2) Snapshot baseline output into otel-export/baseline2.
3) Run model collector config and generate the SAME deterministic traces.
4) Snapshot model output into otel-export/model1.
5) Run compare + retention reports and write a final sampler-proof file.

Usage examples:
  python3 scripts/one_click_sampling_proof.py --trace-count 5000
  python3 scripts/one_click_sampling_proof.py --duration-seconds 120 --rps 40
  python3 scripts/one_click_sampling_proof.py --trace-count 8000 --model-config signoz/deploy/docker/otel-collector-config.model.yaml
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print("$", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def nanos(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000_000))


def gen_trace_id(rng: random.Random) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(32))


def gen_span_id(rng: random.Random) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(16))


def send_deterministic_traces(
    endpoint: str,
    run_id: str,
    trace_count: int,
    seed: int,
    sleep_ms: int,
    endpoint_profile: str,
) -> None:
    rng = random.Random(seed)
    sent = 0
    errors = 0

    for i in range(trace_count):
        trace_id = gen_trace_id(rng)
        is_error = (i % 12 == 0)
        if is_error:
            errors += 1

        if i % 4 == 0:
            duration_ms = rng.randint(900, 2600)
            test_case = "slow"
            span_count = rng.randint(16, 32)
        else:
            duration_ms = rng.randint(8, 140)
            test_case = "fast"
            span_count = rng.randint(4, 14)

        endpoint_route = None
        endpoint_tier = None
        if endpoint_profile == "mixed":
            if test_case == "slow":
                critical_pool = ["/checkout", "/orders", "/payment", "/pay"]
                endpoint_route = critical_pool[(i // 4) % len(critical_pool)]
                endpoint_tier = "critical"
            else:
                non_critical_pool = ["/home", "/search", "/catalog", "/product"]
                endpoint_route = non_critical_pool[i % len(non_critical_pool)]
                endpoint_tier = "non-critical"
        elif endpoint_profile == "priority-proof":
            endpoint_slot = i % 20
            if endpoint_slot in (0, 1, 2, 3):
                critical_pool = ["/payment", "/pay", "/checkout", "/orders"]
                endpoint_route = critical_pool[endpoint_slot]
                endpoint_tier = "critical"

                test_case = "fast"
                duration_ms = rng.randint(8, 70)
                span_count = rng.randint(4, 10)
                is_error = False
            else:
                non_critical_pool = ["/search", "/catalog", "/product", "/home"]
                endpoint_route = non_critical_pool[endpoint_slot % len(non_critical_pool)]
                endpoint_tier = "non-critical"

        start = datetime.utcnow()
        root_span_id = gen_span_id(rng)
        spans = []

        for j in range(span_count):
            st = start + timedelta(milliseconds=(duration_ms * j / span_count))
            et = start + timedelta(milliseconds=(duration_ms * (j + 1) / span_count))
            span_id = root_span_id if j == 0 else gen_span_id(rng)

            attrs = [
                {"key": "service.name", "value": {"stringValue": "thesis-proof-gen"}},
                {"key": "test.run_id", "value": {"stringValue": run_id}},
                {"key": "test.case", "value": {"stringValue": test_case}},
                {"key": "http.status_code", "value": {"intValue": "500" if is_error else "200"}},
                {"key": "test.duration_ms", "value": {"intValue": str(duration_ms)}},
                {"key": "test.span_count", "value": {"intValue": str(span_count)}},
            ]
            if endpoint_route is not None:
                attrs.append({"key": "http.route", "value": {"stringValue": endpoint_route}})
                attrs.append({"key": "http.target", "value": {"stringValue": endpoint_route}})
            if endpoint_tier is not None:
                attrs.append({"key": "test.endpoint_tier", "value": {"stringValue": endpoint_tier}})

            span = {
                "traceId": trace_id,
                "spanId": span_id,
                "name": "root" if j == 0 else f"child-{j}",
                "startTimeUnixNano": nanos(st),
                "endTimeUnixNano": nanos(et),
                "kind": 1,
                "attributes": attrs,
            }
            if j > 0:
                span["parentSpanId"] = root_span_id
            if is_error and j == 0:
                span["status"] = {"code": 2, "message": "synthetic-error"}
            spans.append(span)

        payload = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "thesis-proof-gen"}}
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "thesis-proof", "version": "1.0"},
                            "spans": spans,
                        }
                    ],
                }
            ]
        }

        req = urllib.request.Request(
            endpoint,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            resp.read()

        sent += 1
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    print(f"done run_id={run_id} traces={sent} error_traces={errors}")


def extract_eval_summary(eval_text: str) -> list[str]:
    wanted_prefixes = (
        "kept traces    :",
        "dropped traces :",
        "kept spans rate:",
        "kept duration_ms p50/p95/p99:",
        "kept duration_ms range",
        "estimated keep threshold duration_ms",
        "non-error separation:",
    )

    lines = [ln.rstrip() for ln in eval_text.splitlines()]
    out: list[str] = []
    in_kept_bucket = False

    for ln in lines:
        if any(ln.startswith(prefix) for prefix in wanted_prefixes):
            out.append(ln)

        if ln.strip() == "kept     duration buckets:":
            in_kept_bucket = True
            out.append(ln)
            continue

        if in_kept_bucket:
            if ln.startswith("  <10ms") or ln.startswith("  10-100ms") or ln.startswith("  100-500ms") or ln.startswith("  >=500ms"):
                out.append(ln)
                continue
            in_kept_bucket = False

    if not out:
        return ["(summary unavailable)"]
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace-count", type=int, default=5000)
    parser.add_argument("--duration-seconds", type=int, default=0, help="If >0, overrides trace-count with duration*rps")
    parser.add_argument("--rps", type=float, default=40.0)
    parser.add_argument("--sleep-ms", type=int, default=0, help="Optional delay after each sent trace")
    parser.add_argument("--seed", type=int, default=20260302)
    parser.add_argument("--run-id", default="", help="Optional explicit run_id")
    parser.add_argument(
        "--endpoint-profile",
        choices=["none", "mixed", "priority-proof"],
        default="none",
        help=(
            "Inject endpoint attrs into traces. "
            "'mixed' adds basic critical/non-critical routes, "
            "'priority-proof' makes critical endpoints low-score to test rescue by priority/floor policies."
        ),
    )

    parser.add_argument("--endpoint", default="http://localhost:4318/v1/traces")
    parser.add_argument("--flush-wait", type=int, default=12)

    parser.add_argument("--baseline-config", default="signoz/deploy/docker/otel-collector-config.baseline.yaml")
    parser.add_argument("--model-config", default="signoz/deploy/docker/otel-collector-config.model.yaml")
    parser.add_argument("--active-config", default="signoz/deploy/docker/otel-collector-config.yaml")
    parser.add_argument("--docker-dir", default="signoz/deploy/docker")

    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    docker_dir = root / args.docker_dir
    baseline_cfg = root / args.baseline_config
    model_cfg = root / args.model_config
    active_cfg = root / args.active_config

    if args.duration_seconds > 0:
        trace_count = int(args.duration_seconds * args.rps)
    else:
        trace_count = int(args.trace_count)

    if trace_count <= 0:
        raise SystemExit("trace_count must be > 0")

    run_id = args.run_id or f"thesis-proof-{now_ts()}"

    otel_export = docker_dir / "otel-export"
    traces_base = otel_export / "traces.json"
    traces_model = otel_export / "traces.model.json"

    baseline_snap_dir = otel_export / "baseline2"
    model_snap_dir = otel_export / "model1"
    baseline_snap_dir.mkdir(parents=True, exist_ok=True)
    model_snap_dir.mkdir(parents=True, exist_ok=True)

    for p in (traces_base, traces_model):
        p.touch(exist_ok=True)

    run(["chmod", "666", str(traces_base), str(traces_model)])

    print("\\n== PASS 1: baseline ==")
    shutil.copyfile(baseline_cfg, active_cfg)
    run(["docker-compose", "up", "-d", "--force-recreate", "otel-collector"], cwd=docker_dir)
    time.sleep(6)
    traces_base.write_text("", encoding="utf-8")
    send_deterministic_traces(args.endpoint, run_id, trace_count, args.seed, args.sleep_ms, args.endpoint_profile)
    time.sleep(args.flush_wait)
    base_snap = baseline_snap_dir / f"{now_ts()}-traces.json"
    shutil.copyfile(traces_base, base_snap)

    print("\\n== PASS 2: model ==")
    shutil.copyfile(model_cfg, active_cfg)
    run(["docker-compose", "up", "-d", "--force-recreate", "otel-collector"], cwd=docker_dir)
    time.sleep(6)
    traces_model.write_text("", encoding="utf-8")
    send_deterministic_traces(args.endpoint, run_id, trace_count, args.seed, args.sleep_ms, args.endpoint_profile)
    time.sleep(args.flush_wait)
    model_snap = model_snap_dir / f"{now_ts()}-traces.model.json"
    shutil.copyfile(traces_model, model_snap)

    print("\\n== PASS 3: compare + retention ==")
    compare_cmd = [
        "python3",
        "scripts/run_compare_and_save.py",
        "--base",
        str(base_snap),
        "--test",
        str(model_snap),
    ]
    compare_proc = subprocess.run(
        compare_cmd,
        cwd=str(root),
        capture_output=True,
        text=True,
        check=True,
    )

    compare_report_path: Path | None = None
    for line in compare_proc.stdout.splitlines():
        if line.startswith("Saved report:"):
            maybe_path = line.split(":", 1)[1].strip()
            if maybe_path:
                compare_report_path = Path(maybe_path)
                break

    eval_proc = subprocess.run(
        [
            "python3",
            "scripts/evaluate_ab_sampling.py",
            "--baseline",
            str(base_snap),
            "--sampled",
            str(model_snap),
            "--run-id",
            run_id,
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=True,
    )

    reports_compare_dir = root / "reports" / "compare"
    reports_compare_dir.mkdir(parents=True, exist_ok=True)
    combined_out = reports_compare_dir / f"{now_ts()}-combined-proof-{run_id}.txt"

    compare_report_text = ""
    if compare_report_path and compare_report_path.exists():
        compare_report_text = compare_report_path.read_text(encoding="utf-8")
    else:
        compare_report_text = (
            "command: " + " ".join(compare_cmd) + "\n"
            + f"exit_code: {compare_proc.returncode}\n\n"
            + "---- stdout ----\n"
            + compare_proc.stdout
            + "\n---- stderr ----\n"
            + compare_proc.stderr
            + "\n"
        )

    eval_summary = extract_eval_summary(eval_proc.stdout)

    combined_out.write_text(
        "\n".join(
            [
                "=== ONE-CLICK SAMPLING REPORT ===",
                f"generated_at: {datetime.now().isoformat()}",
                f"run_id: {run_id}",
                f"trace_count: {trace_count}",
                f"seed: {args.seed}",
                f"endpoint_profile: {args.endpoint_profile}",
                f"baseline_file: {base_snap}",
                f"model_file: {model_snap}",
                f"compare_report_file: {compare_report_path if compare_report_path else '(inline-only)'}",
                "",
                "=== QUICK SUMMARY (KEEP + THRESHOLD) ===",
                *eval_summary,
                "",
                "=== COMPARE OUTPUT ===",
                compare_report_text.rstrip("\n"),
                "",
                "=== EVALUATE OUTPUT ===",
                eval_proc.stdout.rstrip("\n"),
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    retention_out = combined_out

    proof_out = root / "reports" / "sampler-proof" / f"{now_ts()}-oneclick-proof.txt"
    proof_out.parent.mkdir(parents=True, exist_ok=True)
    proof_out.write_text(
        "\n".join(
            [
                f"run_id: {run_id}",
                f"trace_count: {trace_count}",
                f"seed: {args.seed}",
                f"endpoint_profile: {args.endpoint_profile}",
                f"baseline_file: {base_snap}",
                f"model_file: {model_snap}",
                f"retention_file: {retention_out}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print("\\n=== DONE ===")
    print(f"run_id={run_id}")
    print(f"baseline={base_snap}")
    print(f"model={model_snap}")
    print(f"retention={retention_out}")
    print(f"proof={proof_out}")
    print("\\nIf you update model logic/config, rerun this command with same arguments to compare consistently.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
