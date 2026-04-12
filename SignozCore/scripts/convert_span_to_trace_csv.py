"""
Convert span-level trace CSVs to trace-level summary CSV for sampler input
- Input: folder of span-level CSVs (one per time window)
- Output: paper_traces.csv (trace_id, start_ns, duration_ms, span_count, has_error, services)
"""
import os
import csv
from collections import defaultdict

# Input/output paths
TRACE_DIR = "/home/leeduc/Desktop/KLTN/SignozCore/data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/trace"
OUT_CSV = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/paper_traces.csv"

# Aggregate all spans by trace_id
data = defaultdict(lambda: {
    "start_ns": None,
    "end_ns": None,
    "span_count": 0,
    "has_error": False,
    "services": set(),
})

for fname in sorted(os.listdir(TRACE_DIR)):
    if not fname.endswith(".csv"): continue
    with open(os.path.join(TRACE_DIR, fname), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tid = row["TraceID"]
            start = int(row["StartTimeUnixNano"])
            end = int(row["EndTimeUnixNano"])
            svc = row["PodName"].split("-")[0] if row["PodName"] else ""
            # Update trace summary
            d = data[tid]
            d["start_ns"] = start if d["start_ns"] is None else min(d["start_ns"], start)
            d["end_ns"] = end if d["end_ns"] is None else max(d["end_ns"], end)
            d["span_count"] += 1
            d["services"].add(svc)
            # Error detection (if OperationName or other field marks error, add logic here)
            # For now, mark has_error=False for all

# Write output
with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["trace_id", "start_ns", "duration_ms", "span_count", "has_error", "services"])
    for tid, d in data.items():
        duration_ms = (d["end_ns"] - d["start_ns"]) / 1_000_000
        writer.writerow([
            tid,
            d["start_ns"],
            duration_ms,
            d["span_count"],
            0,  # has_error (all 0 for now)
            "|".join(sorted(d["services"]))
        ])
print(f"Wrote {len(data)} traces to {OUT_CSV}")
