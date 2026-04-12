# Save kept_ids for RCA pipeline
import csv
from streamv3_composite_strictcap import TracePoint, _composite_v3_metrics_strictcap
import json

TRACE_CSV = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/paper_traces.csv"
SCENARIO_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/scenario.json"
PREF_VECTOR_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/preference_vector.json"
OUT_IDS = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/kept_ids.txt"

# Load tracepoints
def load_tracepoints(csv_path):
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

def load_scenario_windows(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        scenarios = json.load(f)
    return [(row["id"], int(row["start_s"]), int(row["end_s"])) for row in scenarios]

def load_preference_vector(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

points = load_tracepoints(TRACE_CSV)
scenario_windows = load_scenario_windows(SCENARIO_JSON)
try:
    preference_vector = load_preference_vector(PREF_VECTOR_JSON)
except Exception:
    preference_vector = {}

kept_ids = _composite_v3_metrics_strictcap(
    points=points,
    budget_pct=1.0,
    preference_vector=preference_vector,
    scenario_windows=scenario_windows,
    incident_anchor_sec=None,
    seed=42,
    min_incident_traces_per_scenario=1,
    online_soft_cap=False,
    lookback_sec=60,
    alpha=1.2,
    gamma=0.8,
    anomaly_weight=0.7,
)

with open(OUT_IDS, "w") as f:
    for tid in kept_ids:
        f.write(f"{tid}\n")
print(f"Wrote {len(kept_ids)} kept_ids to {OUT_IDS}")
