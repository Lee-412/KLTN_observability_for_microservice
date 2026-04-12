"""
Test script for StreamV3 Composite StrictCap Sampler
- Load paper-format trace CSV
- Load/generate preference vector (metrics anomaly)
- Load scenario windows
- Run composite sampler
- Print summary stats
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple
from streamv3_composite_strictcap import TracePoint, _composite_v3_metrics_strictcap

# --- Config ---
TRACE_CSV = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/paper_traces.csv"
SCENARIO_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/scenario.json"
PREF_VECTOR_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/preference_vector.json"
BUDGET_PCT = 1.0
SEED = 42

# --- Helpers ---
def load_tracepoints(csv_path: str) -> List[TracePoint]:
    import csv
    points = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append(TracePoint(
                trace_id=row["trace_id"],
                start_ns=int(row["start_ns"]),
                duration_ms=float(row["duration_ms"]),
                span_count=int(row["span_count"]),
                has_error=(row.get("has_error", "0") in ("1", "true", "True")),
                services=frozenset(s.strip() for s in row["services"].split("|") if s.strip()),
            ))
    return points

def load_scenario_windows(json_path: str) -> List[Tuple[str, int, int]]:
    with open(json_path, "r", encoding="utf-8") as f:
        scenarios = json.load(f)
    return [(row["id"], int(row["start_s"]), int(row["end_s"])) for row in scenarios]

def load_preference_vector(json_path: str) -> Dict[str, float]:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    print("Loading tracepoints...")
    points = load_tracepoints(TRACE_CSV)
    print(f"Loaded {len(points)} traces")

    print("Loading scenario windows...")
    scenario_windows = load_scenario_windows(SCENARIO_JSON)
    print(f"Loaded {len(scenario_windows)} scenarios")

    print("Loading/generating preference vector...")
    try:
        preference_vector = load_preference_vector(PREF_VECTOR_JSON)
        print(f"Loaded preference vector for {len(preference_vector)} services")
    except Exception:
        print("No preference vector found, using uniform (all 0.0)")
        preference_vector = {}

    print("Running composite sampler...")
    kept_ids = _composite_v3_metrics_strictcap(
        points=points,
        budget_pct=BUDGET_PCT,
        preference_vector=preference_vector,
        scenario_windows=scenario_windows,
        incident_anchor_sec=None,
        seed=SEED,
        min_incident_traces_per_scenario=1,
        online_soft_cap=False,
        lookback_sec=60,
        alpha=1.2,
        gamma=0.8,
        anomaly_weight=0.7,
    )
    print(f"Kept {len(kept_ids)} traces out of {len(points)} ({len(kept_ids)/len(points)*100:.2f}%)")
    # Optionally: print kept_ids or write to file
    # with open("kept_ids.txt", "w") as f:
    #     for tid in kept_ids:
    #         f.write(f"{tid}\n")

if __name__ == "__main__":
    main()
