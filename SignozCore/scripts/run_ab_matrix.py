#!/usr/bin/env python3
"""Run a 2-variant A/B sampling experiment driven by a JSON config.

What this does for each variant:
- (Optional) build a collector Docker image from a git ref
- restart only the docker-compose service `otel-collector` with OTELCOL_TAG override
- delete previous baseline/sampled export JSON files
- run the synthetic trace generator with a unique run_id
- run the evaluator + detailed compare and save reports under reports/ab_matrix/

Usage:
  python3 scripts/run_ab_matrix.py --config scripts/ab_matrix.example.json

Notes:
- This script assumes the SigNoz stack is already up (clickhouse, signoz, etc.).
- It only recreates the otel-collector container.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run(cmd: List[str], *, cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env:
        merged_env.update({k: str(v) for k, v in env.items()})

    # Print commands in a copy-paste friendly form.
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=merged_env, text=True, capture_output=True)


def _must(proc: subprocess.CompletedProcess, *, what: str) -> None:
    if proc.returncode == 0:
        return
    sys.stderr.write(f"\nERROR: {what} (exit={proc.returncode})\n")
    if proc.stdout:
        sys.stderr.write("---- stdout ----\n")
        sys.stderr.write(proc.stdout)
        sys.stderr.write("\n")
    if proc.stderr:
        sys.stderr.write("---- stderr ----\n")
        sys.stderr.write(proc.stderr)
        sys.stderr.write("\n")
    raise SystemExit(proc.returncode)


def _safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "variant"


@dataclass(frozen=True)
class ComposeCfg:
    workdir: Path
    files: List[str]
    service: str


def _compose_cmd(compose: ComposeCfg) -> List[str]:
    cmd = ["docker", "compose"]
    for f in compose.files:
        cmd += ["-f", f]
    return cmd


def _delete_if_exists(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except TypeError:
        # Python <3.8 compatibility (not expected here, but harmless).
        if path.exists():
            path.unlink()


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _build_collector_image(
    *,
    root: Path,
    repo_rel: str,
    git_ref: str,
    image: str,
    tag: str,
    targetos: str,
    targetarch: str,
) -> None:
    repo = root / repo_rel
    if not repo.exists():
        raise SystemExit(f"Collector repo not found: {repo}")

    # Use a worktree for non-HEAD refs, so we don't dirty the current workspace.
    build_dir = repo
    worktree_dir: Optional[Path] = None

    if git_ref and git_ref != "HEAD":
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        wt_root = Path("/tmp") / "kltn_ab_worktrees"
        wt_root.mkdir(parents=True, exist_ok=True)
        worktree_dir = wt_root / f"signoz-otel-collector_{_safe_slug(git_ref)}_{ts}"

        proc = _run(["git", "-C", str(repo), "rev-parse", "--verify", git_ref])
        _must(proc, what=f"verify git ref {git_ref}")

        proc = _run(["git", "-C", str(repo), "worktree", "add", "--detach", str(worktree_dir), git_ref])
        _must(proc, what=f"git worktree add {git_ref}")
        build_dir = worktree_dir

    try:
        # Build binaries expected by Dockerfile.
        proc = _run(["make", "GOOS=linux", "GOARCH=amd64", "build"], cwd=build_dir)
        _must(proc, what="make build")

        # Build image using the correct Dockerfile.
        proc = _run(
            [
                "docker",
                "build",
                "-f",
                "cmd/signozotelcollector/Dockerfile",
                "-t",
                f"{image}:{tag}",
                "--build-arg",
                f"TARGETOS={targetos}",
                "--build-arg",
                f"TARGETARCH={targetarch}",
                ".",
            ],
            cwd=build_dir,
        )
        _must(proc, what=f"docker build {image}:{tag}")
    finally:
        if worktree_dir is not None:
            # Best-effort cleanup; do not fail experiment if this fails.
            _run(["git", "-C", str(repo), "worktree", "remove", "--force", str(worktree_dir)])


def _run_variant(
    *,
    root: Path,
    compose: ComposeCfg,
    exports_baseline: Path,
    exports_sampled: Path,
    generator: Dict[str, Any],
    post_load_sleep_seconds: float,
    variant: Dict[str, Any],
    reports_dir: Path,
) -> Path:
    name = variant.get("name") or "variant"
    slug = _safe_slug(name)
    run_id = f"ab-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug}"

    # (Optional) build
    build_cfg = variant.get("build") or {}
    if bool(build_cfg.get("enabled")):
        _build_collector_image(
            root=root,
            repo_rel=str(build_cfg.get("collector_repo") or "signoz-otel-collector"),
            git_ref=str(build_cfg.get("git_ref") or "HEAD"),
            image=str(build_cfg.get("image") or "signoz/signoz-otel-collector"),
            tag=str(build_cfg.get("tag") or slug),
            targetos=str(build_cfg.get("targetos") or "linux"),
            targetarch=str(build_cfg.get("targetarch") or "amd64"),
        )

    # Restart collector with env override.
    runtime_env = {k: str(v) for k, v in (variant.get("runtime") or {}).items()}
    compose_cmd = _compose_cmd(compose)

    proc = _run(compose_cmd + ["up", "-d", "--force-recreate", compose.service], cwd=compose.workdir, env=runtime_env)
    _must(proc, what=f"docker compose up {compose.service}")

    # Clean exports
    _ensure_parent_dir(exports_baseline)
    _ensure_parent_dir(exports_sampled)
    _delete_if_exists(exports_baseline)
    _delete_if_exists(exports_sampled)

    # Generate traces
    py = str(generator.get("python") or sys.executable)
    script = str(generator.get("script") or "scripts/tracegen_latency_mix.py")
    endpoint = str(generator.get("endpoint") or "127.0.0.1:4317")

    cmd = [py, script, "--endpoint", endpoint]
    if bool(generator.get("insecure")):
        cmd.append("--insecure")

    def add_num(flag: str, key: str) -> None:
        v = generator.get(key)
        if v is None:
            return
        cmd.extend([flag, str(v)])

    def add_pair(flag: str, key: str) -> None:
        v = generator.get(key)
        if not v:
            return
        cmd.extend([flag, str(v[0]), str(v[1])])

    add_num("--seconds", "seconds")
    add_num("--rps", "rps")
    add_num("--concurrency", "concurrency")
    add_num("--slow-ratio", "slow_ratio")
    add_pair("--fast-ms", "fast_ms")
    add_pair("--slow-ms", "slow_ms")
    add_pair("--spans", "spans")
    add_num("--error-ratio", "error_ratio")
    cmd.extend(["--run-id", run_id])

    proc = _run(cmd, cwd=root)
    _must(proc, what="run trace generator")

    time.sleep(max(0.0, float(post_load_sleep_seconds)))

    # Evaluate
    evaluator = root / "scripts/evaluate_ab_sampling.py"
    compare = root / "scripts/compare_otlp_traces.py"

    eval_cmd = [sys.executable, str(evaluator), "--baseline", str(exports_baseline), "--sampled", str(exports_sampled), "--run-id", run_id]
    eval_proc = _run(eval_cmd, cwd=root)

    compare_cmd = [sys.executable, str(compare), "--base", str(exports_baseline), "--test", str(exports_sampled), "--details"]
    cmp_proc = _run(compare_cmd, cwd=root)

    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slug}.txt"

    report_parts = []
    report_parts.append(f"variant: {name}")
    report_parts.append(f"run_id: {run_id}")
    report_parts.append(f"OTELCOL_TAG: {runtime_env.get('OTELCOL_TAG', '(not set)')}")
    report_parts.append(f"baseline_export: {exports_baseline}")
    report_parts.append(f"sampled_export: {exports_sampled}")
    report_parts.append("")

    report_parts.append("==== evaluator command ====")
    report_parts.append("command: " + " ".join(eval_cmd))
    report_parts.append(f"exit_code: {eval_proc.returncode}")
    report_parts.append("---- stdout ----")
    report_parts.append((eval_proc.stdout or "").rstrip("\n"))
    report_parts.append("---- stderr ----")
    report_parts.append((eval_proc.stderr or "").rstrip("\n"))
    report_parts.append("")

    report_parts.append("==== compare command ====")
    report_parts.append("command: " + " ".join(compare_cmd))
    report_parts.append(f"exit_code: {cmp_proc.returncode}")
    report_parts.append("---- stdout ----")
    report_parts.append((cmp_proc.stdout or "").rstrip("\n"))
    report_parts.append("---- stderr ----")
    report_parts.append((cmp_proc.stderr or "").rstrip("\n"))
    report_parts.append("")

    out_path.write_text("\n".join(report_parts) + "\n")

    # If evaluator failed, still keep the report file and raise at the end.
    if eval_proc.returncode != 0:
        raise SystemExit(eval_proc.returncode)

    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to JSON config (workspace-relative or absolute)")
    args = ap.parse_args()

    root = _workspace_root()
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path

    cfg = json.loads(cfg_path.read_text())

    compose_raw = cfg.get("compose") or {}
    compose = ComposeCfg(
        workdir=(root / str(compose_raw.get("workdir") or "signoz/deploy/docker")),
        files=list(compose_raw.get("files") or ["docker-compose.yaml"]),
        service=str(compose_raw.get("service") or "otel-collector"),
    )

    exports_raw = cfg.get("exports") or {}
    exports_baseline = root / str(exports_raw.get("baseline") or "signoz/deploy/docker/otel-export/latencytest_baseline/traces.baseline.json")
    exports_sampled = root / str(exports_raw.get("sampled") or "signoz/deploy/docker/otel-export/latencytest_sampled/traces.sampled.json")

    generator = cfg.get("generator") or {}
    timing = cfg.get("timing") or {}
    post_sleep = float(timing.get("post_load_sleep_seconds") or 8)

    variants = list(cfg.get("variants") or [])
    if len(variants) != 2:
        raise SystemExit("Config must contain exactly 2 variants in variants[].")

    reports_dir = root / "reports/ab_matrix"

    saved: List[Path] = []
    for v in variants:
        print("\n==============================")
        print(f"Running variant: {v.get('name')}")
        print("==============================")
        out = _run_variant(
            root=root,
            compose=compose,
            exports_baseline=exports_baseline,
            exports_sampled=exports_sampled,
            generator=generator,
            post_load_sleep_seconds=post_sleep,
            variant=v,
            reports_dir=reports_dir,
        )
        saved.append(out)
        print(f"Saved report: {out}")

    print("\nAll done.")
    for p in saved:
        print(f"- {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
