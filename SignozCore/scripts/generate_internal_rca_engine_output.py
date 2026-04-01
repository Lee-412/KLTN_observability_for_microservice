#!/usr/bin/env python3
"""Generate raw RCA engine outputs per scenario from trace CSV windows.

This is an internal heuristic RCA engine to unblock paper-style benchmarking.
Input scenario rows should include:
- scenario_id
- incident_start_ts / incident_end_ts (ISO-8601 UTC)
- trace_glob

Output per scenario:
- <out_dir>/<scenario_id>.json with ranked_candidates
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _parse_iso_utc(s: str) -> int:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return int(dt.astimezone(timezone.utc).timestamp())


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _service_from_pod(pod: str) -> str:
    parts = pod.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return pod


def _build_ranking(trace_files: List[str], start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
    agg: Dict[str, Dict[str, float]] = {}

    for tf in trace_files:
        with open(tf, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                st_raw = str(r.get("StartTimeUnixNano", "")).strip()
                if not st_raw:
                    continue
                try:
                    st = int(st_raw[:10])
                except Exception:
                    continue
                if st < start_ts or st > end_ts:
                    continue

                pod = str(r.get("PodName", "")).strip()
                if not pod:
                    continue
                svc = _service_from_pod(pod)

                dur_raw = str(r.get("Duration", "0")).strip()
                try:
                    dur_ms = float(dur_raw) / 1_000_000.0
                except Exception:
                    dur_ms = 0.0

                rec = agg.setdefault(svc, {"count": 0.0, "sum_ms": 0.0, "max_ms": 0.0})
                rec["count"] += 1.0
                rec["sum_ms"] += dur_ms
                if dur_ms > rec["max_ms"]:
                    rec["max_ms"] = dur_ms

    # Heuristic score: emphasize heavy and long services in the incident window.
    scored: List[tuple[str, float]] = []
    for svc, rec in agg.items():
        score = rec["sum_ms"] + 2.0 * rec["max_ms"] + 5.0 * rec["count"]
        scored.append((svc, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    ranked: List[Dict[str, Any]] = []
    for i, (svc, score) in enumerate(scored, start=1):
        ranked.append({"candidate_id": svc, "rank": i, "score": round(score, 6)})
    return ranked


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate internal RCA engine raw outputs")
    ap.add_argument("--scenario-file", required=True, help="Scenario .jsonl file")
    ap.add_argument("--out-dir", required=True, help="Output dir for raw engine JSON files")
    args = ap.parse_args()

    scenario_file = Path(args.scenario_file).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    scenarios = _load_jsonl(scenario_file)
    ok = 0
    empty = 0

    for s in scenarios:
        sid = str(s.get("scenario_id", "")).strip()
        if not sid:
            continue
        start_ts = _parse_iso_utc(str(s.get("incident_start_ts", "")))
        end_ts = _parse_iso_utc(str(s.get("incident_end_ts", "")))
        trace_glob = str(s.get("trace_glob", "")).strip()
        files = sorted(glob.glob(trace_glob))

        t0 = time.time()
        ranked = _build_ranking(files, start_ts, end_ts)
        infer_ms = (time.time() - t0) * 1000.0

        out = {
            "method": "internal-heuristic-rca",
            "scenario_id": sid,
            "inference_time_ms": round(infer_ms, 3),
            "ranked_candidates": ranked,
        }
        (out_dir / f"{sid}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        if ranked:
            ok += 1
        else:
            empty += 1

    print(f"scenario_count={len(scenarios)}")
    print(f"ok_count={ok}")
    print(f"empty_count={empty}")
    print(f"out_dir={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
