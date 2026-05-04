# MESC v3.94.2 Usage Guide

## 1. Problem Overview

Microservice systems continuously generate large volumes of distributed traces. Under a strict storage or processing budget, retaining all raw traces is often infeasible. The sampled trace set, however, must still preserve enough diagnostic evidence for Root Cause Analysis (RCA), where the objective is to identify the faulty service or component behind an incident.

MESC v3.94.2 addresses this setting by selecting a small subset of traces under a predefined budget while prioritizing traces that are expected to be useful for the downstream RCA stage.

## 2. Method Summary

The recommended selection mode is:

```text
stream-v3.94.2-metric-native-signed-contrast
```

The method combines two main signal groups:

- **Trace signals**: trace timestamp, latency, span count, error flag, involved services, and incident-window relevance.
- **Metric signals**: CPU, memory, latency, workload, success rate, and service/pod metrics mapped back to trace-level evidence.

At a high level, the sampler performs the following steps:

1. Preprocess trace and metric data for each dataset.
2. Build metric context for each trace.
3. Run multiple member samplers with different seeds.
4. Aggregate selections using consensus voting.
5. Rank traces using RCA-aware, metric-aware, and contrast-aware scores.
6. Apply budget enforcement and normal/error balance guards.
7. Materialize the retained trace set for MicroRank/RCA evaluation in phase 2.

Main files:

```text
scripts/MESC/run_paper_sampled_rca_v9_contrast.py
scripts/MESC/streamv3942_metric_native_signed_contrast.py
```

## 3. Environment Requirements

### 3.1. Minimum Requirements

- Python 3.10 or later. Python 3.10 or 3.11 is recommended.
- `pip` for installing Python packages.
- `numpy`.
- A shell capable of running Bash scripts for phase 2.
- TraStrainer data placed according to the dataset configuration used by the repository.

On Windows, the following are recommended:

- PowerShell for running Python commands.
- Git Bash or an equivalent Bash environment for running `.sh` scripts in phase 2.

### 3.2. Required Dataset Structure

The runner reads the dataset configuration from the following default file:

```text
scripts/configs/datasets.trastrainer.json
```

The main datasets are typically:

```text
train-ticket
hipster-batch1
hipster-batch2
```

Each dataset should provide at least:

- a trace CSV directory;
- a `label.json` file;
- a metric CSV directory for the metric-native branch;
- a base scenario file, or enough label data for the runner to bootstrap scenarios automatically.

If the base scenario file is missing, the runner can generate it from `label.json` using `--before-sec` and `--after-sec`.

## 4. Installation

The commands below assume that the current working directory is the repository `src` directory.

### 4.1. Create a Virtual Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

On Linux/macOS or Git Bash:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 4.2. Install the Minimum Dependency

```powershell
pip install numpy
```

### 4.3. Verify the Dependency

```powershell
python -c "import numpy; print(numpy.__version__)"
```

When using the Windows virtual environment explicitly:

```powershell
.\.venv\Scripts\python.exe -c "import numpy; print(numpy.__version__)"
```

## 5. Running the Sampler

The following commands assume that you are inside the `src` directory and therefore use `--root .`.

### 5.1. Quick Sampler-Only Run

This command runs only the sampling phase and writes a timing CSV. RCA phase 2 is skipped.

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-sanity `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --datasets hipster-batch1 `
  --budgets 0.1 `
  --before-sec 300 `
  --after-sec 600 `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-only `
  --sampling-timing-csv .\reports\compare\sampler-timing-v3942-sanity.csv
```

Use this mode to:

- verify that the sampler runs successfully;
- validate dataset paths;
- measure sampling latency;
- confirm that the retained trace count matches the requested budget.

### 5.2. Full Pipeline for One Dataset

Example: run the complete sampling and RCA pipeline for `train-ticket` at the `1.0%` budget.

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-train-ticket-1pct `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --datasets train-ticket `
  --budgets 1.0 `
  --budget-mode strict `
  --v3942-min-consensus 2
```

### 5.3. Full Pipeline for Three Datasets and Three Budgets

If `--datasets` and `--budgets` are omitted, the runner uses the following defaults:

```text
--datasets train-ticket,hipster-batch1,hipster-batch2
--budgets 0.1,1.0,2.5
```

Full command:

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-full `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-timing-csv .\reports\compare\sampler-timing-v3942-full.csv
```

### 5.4. Parallel Phase-2 Execution

If the machine has sufficient resources, phase 2 can be parallelized:

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-full-workers `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --phase2-workers 3
```

## 6. Important Parameters

| Parameter | Description |
|---|---|
| `--root` | Path to the root directory containing `scripts`, `data`, and `reports`. Use `--root .` when running from `src`. |
| `--tag` | Run label. The runner appends a timestamp automatically to avoid output collisions. |
| `--selection-mode` | Sampling strategy. For MESC v3.94.2, use `stream-v3.94.2-metric-native-signed-contrast`. |
| `--datasets` | Comma-separated dataset list. |
| `--budgets` | Comma-separated sampling budgets in percent, for example `0.1,1.0,2.5`. |
| `--before-sec` | Number of seconds before the incident timestamp used to construct the scenario window. |
| `--after-sec` | Number of seconds after the incident timestamp used to construct the scenario window. |
| `--budget-mode strict` | Enforces the exact trace-count budget after sampling. |
| `--v3942-min-consensus` | Minimum vote count required for a trace to enter the consensus pool. The recommended value is `2`. |
| `--v3942-enable-inner-real-metrics` | Enables the older internal metric-distress branch. Disabled by default. |
| `--v3942-enable-swap-refine` | Enables bounded swap refinement. Disabled by default. |
| `--v3942-min-normal-ratio` | Overrides the minimum normal-trace floor. |
| `--v3942-min-error-ratio` | Overrides the minimum error-trace floor. |
| `--v3942-signed-metric-gain` | Overrides the strength of signed metric modulation. |
| `--sampling-only` | Runs only the sampling stage and skips full materialization and RCA phase 2. |
| `--sampling-timing-csv` | Path to the sampler timing CSV. |
| `--phase2-workers` | Number of parallel phase-2 workers. |
| `--dataset-config` | Dataset configuration file. Defaults to `scripts/configs/datasets.trastrainer.json`. |

## 7. Checking Results

### 7.1. After a Sampler-Only Run

Check the console log for lines starting with:

```text
[SAMPLER-TIMER]
```

This line reports:

- dataset;
- budget;
- total trace count;
- retained trace count;
- target trace count;
- metric precomputation time;
- sampler core time;
- strict-cap time;
- per-trace selection latency.

Check the timing CSV, for example:

```text
reports/compare/sampler-timing-v3942-sanity.csv
```

Inspect these columns first:

```text
dataset
budget_pct
trace_count
kept_count
kept_pct
target_trace_count
seed_count
min_consensus
sampler_core_ms_per_trace
sampler_select_total_ms_per_trace
select_plus_precompute_amortized_ms_per_trace
```

### 7.2. After a Full Pipeline Run

The runner prints the main output paths, typically in this form:

```text
sampler_timing_csv=...
benchmark_csv=...
compare_csv=...
benchmark_md=...
table3_csv=...
table3_md=...
quality_csv=...
```

Outputs are usually written under:

```text
reports/compare/
reports/analysis/paper-sampled-traces/
reports/analysis/rca-benchmark/
```

Key files:

| Output | Purpose |
|---|---|
| `sampler-timing-*.csv` | Sampling latency and timing statistics. |
| `rca-paper-table-sampled-budgets-*.csv` | RCA results per dataset and budget. |
| `rca-paper-vs-ours-microrank-budgets-*.csv` | Macro comparison against the paper baseline by budget. |
| `rca-dataset-budget-metrics-*.md` | Human-readable markdown summary per dataset/budget. |
| `table3-all-models-plus-ours-*.csv` | Table-3-style comparison. |
| `sampling-quality-key-metrics-*.csv` | Sampling quality and coverage diagnostics. |

### 7.3. RCA Metrics to Inspect

In the summary or markdown report, inspect these metrics first:

```text
scenario_count
A@1
A@3
MRR
```

Definitions:

- `A@1`: fraction of scenarios where the root cause is ranked first.
- `A@3`: fraction of scenarios where the root cause is ranked in the top three.
- `MRR`: Mean Reciprocal Rank, measuring the average ranking quality of the root cause.

If phase 2 runs correctly, the adapter summary should report:

```text
parse_fail_count = 0
ok_count = scenario_count
```

When `--budget-mode strict` is used, any `parse_fail_count > 0` invalidates the strict benchmark and stops aggregate report generation.

## 8. Recommended Execution Workflow

For a first-time MESC v3.94.2 run, use the following workflow:

1. Run `sampling-only` on a small dataset, for example `hipster-batch1` at `0.1%`.
2. Check `sampler_timing_csv`, `kept_count`, `target_trace_count`, and the `[SAMPLER-TIMER]` log.
3. Run the full pipeline on one dataset at `1.0%`.
4. Check `scenario_count`, `A@1`, `A@3`, `MRR`, `parse_fail_count`, and `ok_count`.
5. Once the setup is stable, run all three datasets with budgets `0.1,1.0,2.5`.

## 9. Common Issues

### 9.1. Missing `numpy`

Symptom:

```text
ModuleNotFoundError: No module named 'numpy'
```

Fix:

```powershell
pip install numpy
```

When using a virtual environment, install it into the exact Python used by the runner:

```powershell
.\.venv\Scripts\python.exe -m pip install numpy
```

### 9.2. Incorrect `--root`

If running from `src`, use:

```text
--root .
```

If running from the repository root and the scripts/data are inside `src`, use:

```text
--root .\src
```

### 9.3. Phase 2 Fails on Windows

Phase 2 requires Bash to run `.sh` scripts. Install Git Bash and ensure `bash` is available in `PATH`.

Verify with:

```powershell
bash --version
```

### 9.4. Dataset Not Found

Check:

```text
scripts/configs/datasets.trastrainer.json
```

Confirm that `trace_dir`, `label_file`, `base_scenario_file`, and `baseline_summary_file` exist under the configured `--root`.

## 10. Experimental Notes

- Use `--budget-mode strict` for reportable results under an exact trace-count budget.
- Store `--sampling-timing-csv` for every major run to support latency analysis.
- Avoid enabling multiple refinement options at once unless the run is part of a controlled ablation.
- For the main reported configuration, keep `--v3942-min-consensus 2` for consistency with the recommended setup.
