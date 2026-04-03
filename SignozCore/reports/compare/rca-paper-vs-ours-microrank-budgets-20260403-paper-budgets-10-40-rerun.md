# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260403-paper-budgets-10-40-rerun
budgets_pct: 10.0, 20.0, 30.0, 40.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 10.0% | 0.00 | 11.76 | +11.76 | 0.00 | 41.18 | +41.18 | 0.0000 | 0.3296 | +0.3296 |
| train-ticket | 20.0% | 0.00 | 11.76 | +11.76 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2680 | +0.2680 |
| train-ticket | 30.0% | 0.00 | 11.76 | +11.76 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2758 | +0.2758 |
| train-ticket | 40.0% | 0.00 | 5.88 | +5.88 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2464 | +0.2464 |
| hipster-batch1 | 10.0% | 0.00 | 16.67 | +16.67 | 0.00 | 54.17 | +54.17 | 0.0000 | 0.3814 | +0.3814 |
| hipster-batch1 | 20.0% | 0.00 | 16.67 | +16.67 | 0.00 | 41.67 | +41.67 | 0.0000 | 0.3700 | +0.3700 |
| hipster-batch1 | 30.0% | 0.00 | 20.83 | +20.83 | 0.00 | 45.83 | +45.83 | 0.0000 | 0.4012 | +0.4012 |
| hipster-batch1 | 40.0% | 0.00 | 20.83 | +20.83 | 0.00 | 45.83 | +45.83 | 0.0000 | 0.3991 | +0.3991 |
| hipster-batch2 | 10.0% | 0.00 | 21.88 | +21.88 | 0.00 | 50.00 | +50.00 | 0.0000 | 0.4159 | +0.4159 |
| hipster-batch2 | 20.0% | 0.00 | 21.88 | +21.88 | 0.00 | 46.88 | +46.88 | 0.0000 | 0.4105 | +0.4105 |
| hipster-batch2 | 30.0% | 0.00 | 21.88 | +21.88 | 0.00 | 46.88 | +46.88 | 0.0000 | 0.4094 | +0.4094 |
| hipster-batch2 | 40.0% | 0.00 | 18.75 | +18.75 | 0.00 | 43.75 | +43.75 | 0.0000 | 0.3948 | +0.3948 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260403-paper-budgets-10-40-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260403-paper-budgets-10-40-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260403-paper-budgets-10-40-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260403-paper-budgets-10-40-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260403-paper-budgets-10-40-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260403-paper-budgets-10-40-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
