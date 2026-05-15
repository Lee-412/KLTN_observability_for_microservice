# TASD v3 

## 1. Problem Overview

Microservice systems produce a large volume of distributed traces. When the storage or processing budget is limited, retaining every trace is usually infeasible. The objective is to select a small trace subset that satisfies the sampling budget while preserving enough diagnostic evidence for downstream Root Cause Analysis (RCA).

In this repository, TASD v3 is used as an adaptive trace sampling method. It is designed to retain failure-relevant evidence, preserve structural diversity, and support stable RCA metrics such as `A@1`, `A@3`, and `MRR`.

## 2. Method Summary

The current TASD v3 runner supports the following selection mode:

```text
stream-v3-composite-strictcap
```

The method combines the following signal groups:

- trace proximity to the incident window;
- severity based on latency and span count;
- trace-level error signal;
- service diversity and trace-cluster mass control;
- strict budget-cap enforcement;
- scenario-level floor protection to avoid completely dropping incident scenarios.

The experimental pipeline performs these stages:

1. load trace data and incident labels from the TraStrainer dataset;
2. bootstrap base scenarios from `label.json` when scenario files are missing;
3. run the `stream-v3-composite-strictcap` sampler;
4. materialize retained traces for phase 2;
5. run the MicroRank-based RCA pipeline;
6. aggregate benchmark results by dataset and budget.

Main files:

```text
src/scripts/TASD/run_paper_sampled_rca.py
src/scripts/TASD/streamv3_composite_strictcap.py
```

## 3. Environment Requirements

### 3.1. Minimum Requirements

- Python 3;
- `pip`;
- `numpy`;
- a shell environment with `bash` available for phase 2;
- TraStrainer data under `src/data/paper-source/TraStrainer/...`.

On Windows, Git Bash is recommended. The `bash` command should be available from the terminal used to run the experiment.

### 3.2. Expected Dataset Layout

The runner supports three default datasets:

```text
train-ticket
hipster-batch1
hipster-batch2
```

If the TraStrainer paper dataset is not available locally yet, clone the paper repository into the expected data directory from the repository root:

```powershell
git clone https://github.com/IntelligentDDS/TraStrainer.git .\src\data\paper-source\TraStrainer
```

If it already exists, update it with:

```powershell
git -C .\src\data\paper-source\TraStrainer pull
```

Expected input locations under `src`:

```text
data/paper-source/TraStrainer/data/dataset/train_ticket/test/

data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/

data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/
```

Each dataset should provide at least:

```text
label.json
trace/
metric/
```

If a base scenario file is missing, the runner can generate it automatically from `label.json` using the configured `--before-sec` and `--after-sec` windows.

## 4. Installation

The commands below assume that the current working directory is the repository root.

### 4.1. Create a Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

Linux, macOS, or Git Bash:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 4.2. Install the Required Dependency

```powershell
pip install numpy
```

### 4.3. Verify the Python Environment

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -c "import numpy; print(numpy.__version__)"
```

If the `numpy` import succeeds, the Python environment is ready for TASD v3.

## 5. Running TASD v3

The commands below assume that they are executed from the repository root with `--root .\src`.

### 5.1. Smoke Test on One Dataset

Example: run `train-ticket` at `1.0%`.

```powershell
.\.venv\Scripts\python.exe -m src.scripts.TASD.run_paper_sampled_rca `
  --root .\src `
  --tag tasd-v3-train-ticket-1pct `
  --datasets train-ticket `
  --budgets 1.0 `
  --budget-mode strict `
  --selection-mode stream-v3-composite-strictcap `
  --stochastic-seed 42
```

This command validates the main execution path:

- Python module imports;
- trace and incident-label loading;
- scenario bootstrap when needed;
- sampled trace materialization;
- phase-2 RCA execution;
- final report generation.

### 5.2. Full Pipeline on Three Datasets and Three Budgets

If `--datasets` and `--budgets` are omitted, the runner uses these defaults:

```text
--datasets train-ticket,hipster-batch1,hipster-batch2
--budgets 0.1,1.0,2.5
```

Full command:

```powershell
.\.venv\Scripts\python.exe -m src.scripts.TASD.run_paper_sampled_rca `
  --root .\src `
  --tag tasd-v3-full `
  --budget-mode strict `
  --selection-mode stream-v3-composite-strictcap `
  --stochastic-seed 42
```

### 5.3. Run a Custom Budget Set

Example: run `train-ticket` and `hipster-batch1` at `0.1%` and `1.0%`.

```powershell
.\.venv\Scripts\python.exe -m src.scripts.TASD.run_paper_sampled_rca `
  --root .\src `
  --tag tasd-v3-custom `
  --datasets train-ticket,hipster-batch1 `
  --budgets "0.1,1.0" `
  --budget-mode strict `
  --selection-mode stream-v3-composite-strictcap `
  --stochastic-seed 42
```

In PowerShell, keep the comma-separated budget list quoted when spaces may be introduced.

## 6. Important Parameters

| Parameter | Description |
|---|---|
| `--root` | Path to the `src` directory. From the repository root, use `--root .\src`. |
| `--tag` | Run label. The runner appends a timestamp automatically to keep outputs unique. |
| `--datasets` | Comma-separated dataset list. Default: `train-ticket,hipster-batch1,hipster-batch2`. |
| `--budgets` | Comma-separated sampling budgets in percent. Default: `0.1,1.0,2.5`. |
| `--before-sec` | Pre-incident window in seconds. Default: `300`. |
| `--after-sec` | Post-incident window in seconds. Default: `600`. |
| `--budget-mode strict` | Enforces the exact trace-count budget. Recommended for reportable benchmark results. |
| `--budget-mode approx` | Keeps the approximate sampler output without final exact-cap enforcement. |
| `--selection-mode` | Sampler mode. This runner currently supports only `stream-v3-composite-strictcap`. |
| `--stochastic-seed` | Random seed passed to the sampler. Default: `42`. |

## 7. Checking Results

### 7.1. Terminal Output

A successful run usually prints the following output paths:

```text
benchmark_csv=...
compare_csv=...
benchmark_md=...
table3_csv=...
table3_md=...
quality_csv=...
primary_markdown_outputs=2
```

If the process exits with code `0` and prints `table3_md=...`, the main RCA artifacts have been generated successfully.

### 7.2. Output Locations

Outputs are usually written under:

```text
src/reports/compare/
src/reports/analysis/paper-sampled-traces/
src/reports/analysis/rca-benchmark/
```

Key files:

| Output | Purpose |
|---|---|
| `rca-paper-table-sampled-budgets-*.csv` | RCA results per dataset and budget. |
| `rca-paper-vs-ours-microrank-budgets-*.csv` | Macro comparison against the reference baseline by budget. |
| `rca-dataset-budget-metrics-*.md` | Human-readable summary by dataset and budget. |
| `table3-all-models-plus-ours-*.csv` | Table-3-style comparison in CSV format. |
| `table3-all-models-plus-ours-*.md` | Table-3-style comparison in markdown format. |
| `sampling-quality-key-metrics-*.csv` | Sampling quality, retention, and coverage diagnostics. |

### 7.3. Metrics to Inspect First

Inspect these fields first in the summary JSON or markdown output:

```text
scenario_count
A@1
A@3
MRR
parse_fail_count
ok_count
```

Definitions:

- `A@1`: fraction of scenarios where the root cause is ranked first;
- `A@3`: fraction of scenarios where the root cause is ranked in the top three;
- `MRR`: Mean Reciprocal Rank of the true root cause;
- `parse_fail_count`: number of phase-2 parse failures;
- `ok_count`: number of scenarios evaluated successfully.

For a valid strict run, the expected condition is:

```text
parse_fail_count = 0
ok_count = scenario_count
```

## 8. Recommended Execution Workflow

For a first-time TASD v3 run, use the following sequence:

1. run `train-ticket` at budget `1.0%` as a smoke test;
2. check `scenario_count`, `A@1`, `A@3`, `MRR`, `parse_fail_count`, and `ok_count`;
3. confirm that sampled traces and markdown summaries were written under `src/reports`;
4. once the environment is stable, run all three datasets with budgets `0.1,1.0,2.5`.

## 9. Common Issues

### 9.1. Missing `numpy`

Symptom:

```text
ModuleNotFoundError: No module named 'numpy'
```

Fix:

```powershell
.\.venv\Scripts\python.exe -m pip install numpy
```

### 9.2. Incorrect `--root`

When running from the repository root, use:

```text
--root .\src
```

If `--root .` is used from the repository root, the runner may look for `data/` and `reports/` in the wrong location.

### 9.3. Phase 2 Fails on Windows

Phase 2 uses Bash scripts. Install Git Bash and ensure that `bash` is available in `PATH`.

Verify with:

```powershell
bash --version
```

### 9.4. Missing Label, Trace, or Scenario Inputs

Check that each dataset contains:

```text
label.json
trace/
metric/
```

The runner can bootstrap base scenarios, but it still requires valid labels and trace data.

## 10. Experimental Notes

- This runner is designed for TASD v3 and supports only `stream-v3-composite-strictcap`.
- Use `--budget-mode strict` for reportable or comparable benchmark results.
- Assign a clear `--tag` to every reported run, even though the runner appends a timestamp automatically.
- Start with a single-dataset smoke test before launching the full benchmark.
