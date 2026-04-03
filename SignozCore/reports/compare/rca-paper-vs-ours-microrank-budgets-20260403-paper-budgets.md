# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260403-paper-budgets
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 15.38 | -27.21 | 77.74 | 30.77 | -46.97 | 0.5509 | 0.3272 | -0.2237 |
| train-ticket | 1.0% | 45.16 | 17.65 | -27.51 | 78.52 | 41.18 | -37.34 | 0.5889 | 0.3437 | -0.2452 |
| train-ticket | 2.5% | 50.00 | 11.76 | -38.24 | 82.26 | 52.94 | -29.32 | 0.6556 | 0.3325 | -0.3231 |
| hipster-batch1 | 0.1% | 42.59 | 33.33 | -9.26 | 77.74 | 50.00 | -27.74 | 0.5509 | 0.4931 | -0.0578 |
| hipster-batch1 | 1.0% | 45.16 | 7.14 | -38.02 | 78.52 | 42.86 | -35.66 | 0.5889 | 0.3136 | -0.2753 |
| hipster-batch1 | 2.5% | 50.00 | 16.67 | -33.33 | 82.26 | 50.00 | -32.26 | 0.6556 | 0.3870 | -0.2686 |
| hipster-batch2 | 0.1% | 42.59 | 14.29 | -28.30 | 77.74 | 57.14 | -20.60 | 0.5509 | 0.3571 | -0.1938 |
| hipster-batch2 | 1.0% | 45.16 | 17.65 | -27.51 | 78.52 | 35.29 | -43.23 | 0.5889 | 0.3783 | -0.2106 |
| hipster-batch2 | 2.5% | 50.00 | 21.88 | -28.12 | 82.26 | 53.12 | -29.14 | 0.6556 | 0.4198 | -0.2358 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260403-paper-budgets/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260403-paper-budgets/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260403-paper-budgets.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260403-paper-budgets.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260403-paper-budgets.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260403-paper-budgets.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
