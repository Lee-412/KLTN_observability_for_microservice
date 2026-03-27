#!/usr/bin/env python3
"""Evaluate RCA ranking quality from scenario ground truth and ranking outputs.

Inputs:
- scenario file (.json or .jsonl) with at least:
  scenario_id, ground_truth_root_cause, ranking_file
- ranking file (.json) with at least one of:
  - {"ranked_candidates": [{"candidate_id": "svc-a", "rank": 1, "score": 0.9}, ...]}
  - [{"candidate_id": "svc-a", "rank": 1, "score": 0.9}, ...]

Outputs:
- Console summary with A@1, A@3, MRR and CI
- Optional per-scenario CSV and summary JSON
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Tuple


def _load_json_or_jsonl(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    data = json.loads(text)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "scenarios" in data and isinstance(data["scenarios"], list):
            return data["scenarios"]
        return [data]
    raise ValueError(f"Unsupported scenario file format: {path}")


def _normalize_candidates(ranking_obj: Any) -> List[Dict[str, Any]]:
    if isinstance(ranking_obj, dict):
        if "ranked_candidates" in ranking_obj:
            ranking_obj = ranking_obj["ranked_candidates"]
        elif "candidates" in ranking_obj:
            ranking_obj = ranking_obj["candidates"]

    if not isinstance(ranking_obj, list):
        return []

    candidates: List[Dict[str, Any]] = []
    for i, item in enumerate(ranking_obj, start=1):
        if isinstance(item, str):
            candidates.append({"candidate_id": item, "rank": i, "score": None})
            continue

        if not isinstance(item, dict):
            continue

        cid = (
            item.get("candidate_id")
            or item.get("candidate")
            or item.get("id")
            or item.get("service")
            or item.get("endpoint")
        )
        if cid is None:
            continue

        rank = item.get("rank")
        if rank is None:
            rank = i

        candidates.append(
            {
                "candidate_id": str(cid),
                "rank": int(rank),
                "score": item.get("score"),
            }
        )

    candidates.sort(key=lambda x: x["rank"])
    return candidates


def _load_ranking(path: Path) -> Tuple[List[Dict[str, Any]], float | None]:
    obj = json.loads(path.read_text(encoding="utf-8"))

    inference_ms = None
    if isinstance(obj, dict):
        inference_ms = obj.get("inference_time_ms")

    cands = _normalize_candidates(obj)
    return cands, inference_ms


def _rank_of_ground_truth(candidates: List[Dict[str, Any]], ground_truth: str) -> int | None:
    target = ground_truth.strip()
    for item in candidates:
        if str(item["candidate_id"]).strip() == target:
            return int(item["rank"])
    return None


def _bootstrap_ci(values: List[float], rounds: int, seed: int) -> Tuple[float, float]:
    if not values:
        return (math.nan, math.nan)
    if len(values) == 1:
        v = values[0]
        return (v, v)

    rng = random.Random(seed)
    n = len(values)
    means = []
    for _ in range(rounds):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        means.append(mean(sample))
    means.sort()

    lo_idx = int(0.025 * (rounds - 1))
    hi_idx = int(0.975 * (rounds - 1))
    return means[lo_idx], means[hi_idx]


def evaluate(
    scenarios: Iterable[Dict[str, Any]],
    base_dir: Path,
    bootstrap_rounds: int,
    seed: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    per_scenario: List[Dict[str, Any]] = []
    a1_values: List[float] = []
    a3_values: List[float] = []
    rr_values: List[float] = []
    latencies: List[float] = []

    for row in scenarios:
        sid = str(row.get("scenario_id", "")).strip()
        gt = str(row.get("ground_truth_root_cause", "")).strip()
        ranking_file = str(row.get("ranking_file", "")).strip()

        if not sid or not gt or not ranking_file:
            continue

        rpath = (base_dir / ranking_file).resolve() if not Path(ranking_file).is_absolute() else Path(ranking_file)
        if not rpath.exists():
            per_scenario.append(
                {
                    "scenario_id": sid,
                    "ground_truth_root_cause": gt,
                    "ranking_file": str(rpath),
                    "rank": None,
                    "hit_at_1": 0,
                    "hit_at_3": 0,
                    "reciprocal_rank": 0.0,
                    "inference_time_ms": None,
                    "status": "missing_ranking_file",
                }
            )
            a1_values.append(0.0)
            a3_values.append(0.0)
            rr_values.append(0.0)
            continue

        candidates, inference_ms = _load_ranking(rpath)
        rank = _rank_of_ground_truth(candidates, gt)

        hit1 = 1.0 if rank == 1 else 0.0
        hit3 = 1.0 if rank is not None and rank <= 3 else 0.0
        rr = 1.0 / rank if rank is not None and rank > 0 else 0.0

        a1_values.append(hit1)
        a3_values.append(hit3)
        rr_values.append(rr)
        if inference_ms is not None:
            latencies.append(float(inference_ms))

        per_scenario.append(
            {
                "scenario_id": sid,
                "ground_truth_root_cause": gt,
                "ranking_file": str(rpath),
                "rank": rank,
                "hit_at_1": int(hit1),
                "hit_at_3": int(hit3),
                "reciprocal_rank": rr,
                "inference_time_ms": inference_ms,
                "status": "ok",
            }
        )

    summary: Dict[str, Any] = {
        "scenario_count": len(per_scenario),
        "A@1": mean(a1_values) if a1_values else math.nan,
        "A@3": mean(a3_values) if a3_values else math.nan,
        "MRR": mean(rr_values) if rr_values else math.nan,
        "A@1_CI95": _bootstrap_ci(a1_values, bootstrap_rounds, seed),
        "A@3_CI95": _bootstrap_ci(a3_values, bootstrap_rounds, seed),
        "MRR_CI95": _bootstrap_ci(rr_values, bootstrap_rounds, seed),
    }

    if latencies:
        sl = sorted(latencies)
        p50 = sl[int(0.50 * (len(sl) - 1))]
        p95 = sl[int(0.95 * (len(sl) - 1))]
        summary["rca_inference_time_ms_p50"] = p50
        summary["rca_inference_time_ms_p95"] = p95

    return per_scenario, summary


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate RCA ranking metrics A@1/A@3/MRR")
    parser.add_argument("--scenario-file", required=True, help="Path to scenario .json/.jsonl")
    parser.add_argument("--base-dir", default=".", help="Base directory for relative ranking files")
    parser.add_argument("--out-summary", default="", help="Optional output summary json path")
    parser.add_argument("--out-scenarios", default="", help="Optional output per-scenario csv path")
    parser.add_argument("--bootstrap-rounds", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=20260326)
    args = parser.parse_args()

    scenario_file = Path(args.scenario_file).resolve()
    if not scenario_file.exists():
        raise SystemExit(f"Scenario file not found: {scenario_file}")

    scenarios = _load_json_or_jsonl(scenario_file)
    per_scenario, summary = evaluate(
        scenarios=scenarios,
        base_dir=Path(args.base_dir).resolve(),
        bootstrap_rounds=args.bootstrap_rounds,
        seed=args.seed,
    )

    print("== RCA BENCHMARK SUMMARY ==")
    print(f"scenario_count: {summary['scenario_count']}")
    print(f"A@1: {summary['A@1']:.4f}  CI95={summary['A@1_CI95']}")
    print(f"A@3: {summary['A@3']:.4f}  CI95={summary['A@3_CI95']}")
    print(f"MRR: {summary['MRR']:.4f}  CI95={summary['MRR_CI95']}")
    if "rca_inference_time_ms_p50" in summary:
        print(
            "rca_inference_time_ms_p50/p95: "
            f"{summary['rca_inference_time_ms_p50']:.2f}/{summary['rca_inference_time_ms_p95']:.2f}"
        )

    if args.out_scenarios:
        _write_csv(Path(args.out_scenarios).resolve(), per_scenario)
        print(f"per_scenario_csv={Path(args.out_scenarios).resolve()}")

    if args.out_summary:
        out_summary = Path(args.out_summary).resolve()
        out_summary.parent.mkdir(parents=True, exist_ok=True)
        out_summary.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"summary_json={out_summary}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
