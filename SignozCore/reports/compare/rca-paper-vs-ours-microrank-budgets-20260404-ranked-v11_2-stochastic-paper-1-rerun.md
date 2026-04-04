# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-ranked-v11_2-stochastic-paper-1-rerun
budgets_pct: 1.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 1.0% | 45.16 | 0.00 | -45.16 | 78.52 | 17.65 | -60.87 | 0.5889 | 0.1324 | -0.4565 |
| hipster-batch1 | 1.0% | 45.16 | 8.33 | -36.83 | 78.52 | 33.33 | -45.19 | 0.5889 | 0.2952 | -0.2937 |
| hipster-batch2 | 1.0% | 45.16 | 9.38 | -35.78 | 78.52 | 34.38 | -44.14 | 0.5889 | 0.3187 | -0.2702 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-stochastic-paper-1-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-stochastic-paper-1-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-ranked-v11_2-stochastic-paper-1-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-ranked-v11_2-stochastic-paper-1-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-stochastic-paper-1-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-stochastic-paper-1-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
