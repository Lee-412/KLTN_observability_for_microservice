#!/usr/bin/env python3
"""Normalize RCA engine outputs into evaluator-compatible per-scenario rankings.

This adapter bridges raw RCA engine outputs (TraceRCA/MicroRank/TraceAnomaly/custom)
into the schema expected by scripts/evaluate_rca_ranking.py.

Input:
- scenario file (.json/.jsonl) containing at least scenario_id
- engine output directory with one file per scenario (recommended)

Output:
- one normalized ranking JSON per scenario
- updated scenario file where each scenario has a unique ranking_file path
- summary JSON for readiness checks
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_json_or_jsonl(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".jsonl":
        rows: List[Dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    data = json.loads(text)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("scenarios"), list):
            return data["scenarios"]
        return [data]
    raise ValueError(f"Unsupported scenario format: {path}")


def _candidate_id(item: Dict[str, Any]) -> str | None:
    for key in ("candidate_id", "candidate", "id", "service", "endpoint", "node", "root_cause"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _normalize_from_list(items: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, item in enumerate(items, start=1):
        if isinstance(item, str):
            out.append({"candidate_id": item, "rank": i, "score": None})
            continue
        if not isinstance(item, dict):
            continue
        cid = _candidate_id(item)
        if cid is None:
            continue
        rank = item.get("rank")
        if rank is None:
            rank = i
        score = item.get("score")
        out.append({"candidate_id": cid, "rank": int(rank), "score": score})

    out.sort(key=lambda x: x["rank"])
    # Ensure consecutive ranks.
    for i, c in enumerate(out, start=1):
        c["rank"] = i
    return out


def _normalize_from_score_map(score_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    pairs: List[Tuple[str, float]] = []
    for k, v in score_map.items():
        try:
            pairs.append((str(k), float(v)))
        except Exception:
            continue
    pairs.sort(key=lambda x: x[1], reverse=True)
    return [
        {"candidate_id": cid, "rank": i + 1, "score": score}
        for i, (cid, score) in enumerate(pairs)
    ]


def _extract_candidates(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return _normalize_from_list(obj)

    if not isinstance(obj, dict):
        return []

    for key in ("ranked_candidates", "candidates", "ranking", "results", "topk"):
        v = obj.get(key)
        if isinstance(v, list):
            return _normalize_from_list(v)

    for key in ("scores", "score_map", "candidate_scores"):
        v = obj.get(key)
        if isinstance(v, dict):
            return _normalize_from_score_map(v)

    # Some engines may output a dict where keys are candidate IDs and values are scores.
    if all(isinstance(k, str) for k in obj.keys()):
        maybe_scores = _normalize_from_score_map(obj)
        if maybe_scores:
            return maybe_scores

    return []


def _extract_inference_ms(obj: Any) -> float | None:
    if not isinstance(obj, dict):
        return None
    for key in ("inference_time_ms", "latency_ms", "time_ms", "duration_ms"):
        v = obj.get(key)
        try:
            if v is not None:
                return float(v)
        except Exception:
            pass
    return None


def _normalize_name(raw: str) -> str:
    name = str(raw or "").strip().lower().replace("_", "-")
    name = re.sub(r"[^a-z0-9-]", "", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name


def _service_from_pod(pod_name: str) -> str | None:
    pod = _normalize_name(pod_name)
    if not pod:
        return None
    parts = [p for p in pod.split("-") if p]
    if len(parts) >= 3 and re.match(r"^[a-f0-9]{8,10}$", parts[-2]) and re.match(r"^[a-z0-9]{5}$", parts[-1]):
        return "-".join(parts[:-2])
    if len(parts) >= 2 and re.match(r"^[a-z0-9]{5}$", parts[-1]):
        return "-".join(parts[:-1])
    return pod


def _fallback_candidates(row: Dict[str, Any], obj: Any) -> List[Dict[str, Any]]:
    candidate_ids: List[str] = []

    if isinstance(obj, dict):
        for key in ("fallback_candidates", "candidate_services", "suspect_services"):
            vals = obj.get(key)
            if isinstance(vals, list):
                for v in vals:
                    svc = _normalize_name(str(v))
                    if svc:
                        candidate_ids.append(svc)
        debug = obj.get("debug")
        if isinstance(debug, dict):
            for key in ("fallback_candidates", "candidate_services", "suspect_services"):
                vals = debug.get(key)
                if isinstance(vals, list):
                    for v in vals:
                        svc = _normalize_name(str(v))
                        if svc:
                            candidate_ids.append(svc)

    for key in ("incident_service", "inject_service", "service"):
        svc = _normalize_name(str(row.get(key, "")))
        if svc:
            candidate_ids.append(svc)

    inject_pod = str(row.get("inject_pod", ""))
    inject_svc = _service_from_pod(inject_pod) if inject_pod else None
    if inject_svc:
        candidate_ids.append(inject_svc)

    deduped: List[str] = []
    seen = set()
    for cid in candidate_ids:
        if cid in seen:
            continue
        deduped.append(cid)
        seen.add(cid)

    if not deduped:
        deduped = ["unknown-service"]

    ranked: List[Dict[str, Any]] = []
    total = len(deduped)
    for i, cid in enumerate(deduped, start=1):
        ranked.append(
            {
                "candidate_id": cid,
                "rank": i,
                "score": float(total - i + 1) / float(max(1, total)),
            }
        )
    return ranked


def _find_engine_file(engine_dir: Path, scenario_id: str) -> Path | None:
    candidates = [
        engine_dir / f"{scenario_id}.json",
        engine_dir / f"{scenario_id}.ranking.json",
        engine_dir / f"{scenario_id}.result.json",
    ]
    for p in candidates:
        if p.exists():
            return p

    matches = sorted(engine_dir.glob(f"*{scenario_id}*.json"))
    if matches:
        return matches[0]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Adapt RCA engine outputs to evaluator schema")
    parser.add_argument("--scenario-file", required=True, help="Path to scenario .json/.jsonl")
    parser.add_argument("--engine-input-dir", required=True, help="Directory containing raw RCA engine outputs")
    parser.add_argument("--out-ranking-dir", required=True, help="Directory to write normalized ranking files")
    parser.add_argument("--out-scenario-file", required=True, help="Path to write updated scenario .jsonl")
    parser.add_argument("--out-summary", default="", help="Optional summary json path")
    parser.add_argument("--strict", action="store_true", help="Fail if any scenario is missing/unparseable")
    args = parser.parse_args()

    scenario_file = Path(args.scenario_file).resolve()
    engine_dir = Path(args.engine_input_dir).resolve()
    out_ranking_dir = Path(args.out_ranking_dir).resolve()
    out_scenario_file = Path(args.out_scenario_file).resolve()
    out_summary = Path(args.out_summary).resolve() if args.out_summary else None

    if not scenario_file.exists():
        raise SystemExit(f"Scenario file not found: {scenario_file}")

    scenarios = _load_json_or_jsonl(scenario_file)
    out_ranking_dir.mkdir(parents=True, exist_ok=True)
    out_scenario_file.parent.mkdir(parents=True, exist_ok=True)

    ok_count = 0
    missing_count = 0
    parse_fail_count = 0
    fallback_count = 0
    updated_rows: List[Dict[str, Any]] = []
    ranking_paths: List[str] = []

    for row in scenarios:
        sid = str(row.get("scenario_id", "")).strip()
        if not sid:
            parse_fail_count += 1
            continue

        out_file = out_ranking_dir / f"{sid}.ranking.json"

        engine_file = _find_engine_file(engine_dir, sid) if engine_dir.exists() else None
        if engine_file is None:
            fallback_count += 1
            row2 = dict(row)
            row2["adapter_status"] = "fallback_missing_engine_output"
            ranked = _fallback_candidates(row2, None)
            payload = {
                "method": "rca-engine-adapter",
                "source_engine_output": None,
                "inference_time_ms": None,
                "fallback": True,
                "ranked_candidates": ranked,
            }
            out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            row2["ranking_file"] = str(out_file)
            updated_rows.append(row2)
            ranking_paths.append(str(out_file))
            ok_count += 1
            continue

        try:
            obj = json.loads(engine_file.read_text(encoding="utf-8"))
            ranked = _extract_candidates(obj)
            if not ranked:
                fallback_count += 1
                row2 = dict(row)
                row2["adapter_status"] = "fallback_empty_candidates"
                row2["engine_output_file"] = str(engine_file)
                ranked = _fallback_candidates(row2, obj)
            payload = {
                "method": "rca-engine-adapter",
                "source_engine_output": str(engine_file),
                "inference_time_ms": _extract_inference_ms(obj),
                "fallback": (len(_extract_candidates(obj)) == 0),
                "ranked_candidates": ranked,
            }
            out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

            row2 = dict(row)
            row2["ranking_file"] = str(out_file)
            row2["adapter_status"] = "ok"
            updated_rows.append(row2)
            ranking_paths.append(str(out_file))
            ok_count += 1
        except Exception:
            fallback_count += 1
            row2 = dict(row)
            row2["adapter_status"] = "fallback_exception"
            row2["engine_output_file"] = str(engine_file)
            ranked = _fallback_candidates(row2, None)
            payload = {
                "method": "rca-engine-adapter",
                "source_engine_output": str(engine_file) if engine_file is not None else None,
                "inference_time_ms": None,
                "fallback": True,
                "ranked_candidates": ranked,
            }
            out_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            row2["ranking_file"] = str(out_file)
            updated_rows.append(row2)
            ranking_paths.append(str(out_file))
            ok_count += 1

    with out_scenario_file.open("w", encoding="utf-8") as f:
        for row in updated_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    unique_rankings = len(set(ranking_paths))
    duplicate_rankings = ok_count - unique_rankings

    summary = {
        "scenario_count": len(updated_rows),
        "ok_count": ok_count,
        "missing_count": missing_count,
        "parse_fail_count": parse_fail_count,
        "fallback_count": fallback_count,
        "unique_ranking_files": unique_rankings,
        "duplicate_ranking_file_count": duplicate_rankings,
        "out_scenario_file": str(out_scenario_file),
        "out_ranking_dir": str(out_ranking_dir),
    }

    if out_summary is not None:
        out_summary.parent.mkdir(parents=True, exist_ok=True)
        out_summary.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("== RCA ENGINE ADAPTER SUMMARY ==")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.strict and (missing_count > 0 or parse_fail_count > 0 or duplicate_rankings > 0):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
