# KLTN - Observability for Microservices

This repository is maintained as a graduation thesis (KLTN) workspace for offline trace sampling and RCA benchmarking on microservice datasets.

This README is intentionally focused on running two sampler versions only:

- TASD `v3`
- MESC `v3942`

---

### 1. Problem Statement

Microservice systems generate a large number of traces, so it is usually impossible to retain everything under a tight storage and processing budget. The thesis problem addressed in this repository is:

- keep fewer traces,
- but preserve the most informative ones,
- so that the downstream RCA stage can still identify root causes effectively.

### 2. Two Methods

#### TASD v3

- this is the baseline sampler track,
- it focuses on trace-level signals, error signals, diversity, and strict budget control,
- it is used as the main baseline for comparison.

Main files:

- `src/scripts/TASD/run_paper_sampled_rca.py`
- `src/scripts/TASD/streamv3_composite_strictcap.py`

#### MESC v3942

- this is the more advanced sampler track,
- it combines trace signals with metric-native context,
- it uses signed metric modulation, seed ensemble, and consensus to prioritize RCA-useful traces.

Main files:

- `src/scripts/MESC/run_paper_sampled_rca_v9_contrast.py`
- `src/scripts/MESC/streamv3942_metric_native_signed_contrast.py`

### 3. Requirements and Setup

#### Minimum requirements

- Python 3
- `numpy`
- Git Bash or any environment with `bash` for phase 2 on Windows
- TraStrainer data under `src/data/paper-source/TraStrainer/...`

#### Recommended setup

Run from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install numpy
```

Quick verification:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -c "import numpy; print(numpy.__version__)"
```

#### Input data

If the TraStrainer paper dataset is not available locally yet, clone the paper repository first:

```powershell
git clone https://github.com/IntelligentDDS/TraStrainer.git .\src\data\paper-source\TraStrainer
```

If the target directory already exists, update it instead:

```powershell
git -C .\src\data\paper-source\TraStrainer pull
```

After cloning, the repository expects these dataset directories to exist:

- `src/data/paper-source/TraStrainer/data/dataset/train_ticket/test/`
- `src/data/paper-source/TraStrainer/data/dataset/hipster/batches/batch1/`
- `src/data/paper-source/TraStrainer/data/dataset/hipster/batches/batch2/`

Each dataset should contain at least:

- `label.json`
- `trace/`
- `metric/`

Quick check:

```powershell
Get-ChildItem .\src\data\paper-source\TraStrainer\data\dataset
```

Note:

- if `scenarios.*.paper-source.*.jsonl` files are missing, the current runners can bootstrap them from `label.json`.
- this project only reads the TraStrainer data from the cloned paper repository; the sampling and RCA outputs are still written into this repository under `src/reports/...`.

### 4. Run Scripts

#### 4.1 Run TASD v3

Example smoke test for `train-ticket` at `1.0%` from the repository root:

```powershell
.\.venv\Scripts\python.exe -m src.scripts.TASD.run_paper_sampled_rca `
  --root .\src `
  --tag tasd-v3-train-ticket-1pct `
  --datasets train-ticket `
  --budgets 1.0 `
  --budget-mode strict `
  --selection-mode stream-v3-composite-strictcap
```

Example full 3-dataset run:

```powershell
.\.venv\Scripts\python.exe -m src.scripts.TASD.run_paper_sampled_rca `
  --root .\src `
  --tag tasd-v3-full `
  --budget-mode strict
```

Current TASD runner defaults:

- `--datasets`: `train-ticket,hipster-batch1,hipster-batch2`
- `--budgets`: `0.1,1.0,2.5`
- `--selection-mode`: `stream-v3-composite-strictcap`

#### 4.2 Run MESC v3942

Example sampler-only sanity run from the repository root:

```powershell
.\.venv\Scripts\python.exe .\src\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root .\src `
  --tag v3942-sanity `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --datasets hipster-batch1 `
  --budgets 0.1 `
  --before-sec 300 `
  --after-sec 600 `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-only `
  --sampling-timing-csv .\src\reports\compare\sampler-timing-v3942-sanity.csv
```

Example full 3-dataset run:

```powershell
.\.venv\Scripts\python.exe .\src\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root .\src `
  --tag v3942-full `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-timing-csv .\src\reports\compare\sampler-timing-v3942-full.csv
```

Current MESC runner defaults:

- `--datasets`: `train-ticket,hipster-batch1,hipster-batch2`
- `--budgets`: `0.1,1.0,2.5`
- `--dataset-config`: `scripts/configs/datasets.trastrainer.json`

### 5. How To Check Results

After a full run, inspect first:

- `src/reports/compare/`
- `src/reports/analysis/rca-benchmark/`

Most important output files:

- `rca-dataset-budget-metrics-<tag>.md`
- `table3-all-models-plus-ours-<timestamp>.md`
- `rca-paper-table-sampled-budgets-<tag>.csv`
- `sampling-quality-key-metrics-<tag>.csv`

Metrics to check first:

- `scenario_count`
- `A@1`
- `A@3`
- `MRR`
- `parse_fail_count`
- `ok_count`

On a successful run, the terminal usually prints:

```text
benchmark_csv=...
compare_csv=...
benchmark_md=...
table3_csv=...
table3_md=...
quality_csv=...
primary_markdown_outputs=2
```

For `sampling-only`, also inspect the timing CSV, for example:

- `src/reports/compare/sampler-timing-v3942-sanity.csv`

---

## Author

**Lee**  
Bachelor Thesis (KLTN) - Observability for Microservices  
Vietnam National University

---

## License

This project is maintained for academic and research purposes.
