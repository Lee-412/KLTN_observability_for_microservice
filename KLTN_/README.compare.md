# Script to compare logs and generate a report
# 1) Goal

- Provide a repeatable way to compare two OTLP trace export JSON files (baseline vs model).
- Save comparison output as a timestamped report under `reports/compare/`.

---

## 2) Inputs and outputs

### 2.1) Trace input files

The trace files are **line-delimited JSON** produced by the collector `fileexporter` (each line contains an object with `resourceSpans`).

Example snapshots in this workspace:
- Baseline: `signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json`
- Model: `signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json`

### 2.2) Saved reports

Reports are saved to:
- `reports/compare/`

Report filenames include a timestamp to avoid overwriting.

---

## 3) Script 1: direct compare (prints to console)

Script:
- `scripts/compare_otlp_traces.py`

Run:

```bash
cd /KLTN/KLTN_
python3 scripts/compare_otlp_traces.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json \
  --details
```

Primary outputs:
- `records(lines)`, `spans`, `unique traceIds`
- `error spans` (% error)
- Top `service.name`
- Top route/method (best-effort: `http.route`, `http.target`, `rpc.method`)

---

## 4) Script 2: compare and auto-save a report

Script:
- `scripts/run_compare_and_save.py`

### 4.1) Auto mode (recommended)

Auto-picks the newest `*traces*.json` from:
- `signoz/deploy/docker/otel-export/baseline2/`
- `signoz/deploy/docker/otel-export/model1/`

Then runs the comparison and saves a timestamped report.

```bash
cd /KLTN/KLTN_
python3 scripts/run_compare_and_save.py
ls -lh reports/compare | tail -n 5
```

### 4.2) Explicit files

```bash
python3 scripts/run_compare_and_save.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json
```

### 4.3) Explicit directories

```bash
python3 scripts/run_compare_and_save.py \
  --base-dir signoz/deploy/docker/otel-export/baseline2 \
  --test-dir signoz/deploy/docker/otel-export/model1
```

---

## 5) Troubleshooting

- If no files are found, ensure the directory contains `*traces*.json`.
- If log exports are empty, the workload (HotROD) typically emits traces but not OTLP logs.
