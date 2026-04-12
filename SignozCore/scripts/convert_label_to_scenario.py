"""
Convert label.json to scenario.json for sampler input
- Input: label.json (per-batch)
- Output: scenario.json (list of {id, start_s, end_s})
"""
import json

LABEL_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/label.json"
OUT_JSON = "/home/leeduc/Desktop/KLTN/SignozCore/data/streamv3test/scenario.json"

with open(LABEL_JSON, "r", encoding="utf-8") as f:
    label = json.load(f)

scenarios = []
for hour, events in label.items():
    for idx, ev in enumerate(events):
        # Use inject_timestamp as start_s, add 600s window (10min) as end_s
        start_s = int(ev["inject_timestamp"])
        end_s = start_s + 600
        scenarios.append({
            "id": f"{hour}-{idx+1}",
            "start_s": start_s,
            "end_s": end_s
        })

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(scenarios, f, indent=2)
print(f"Wrote {len(scenarios)} scenarios to {OUT_JSON}")
