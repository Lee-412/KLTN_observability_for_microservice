# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun
budgets_pct: 1.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 1.0% | 45.16 | 11.76 | -33.40 | 78.52 | 29.41 | -49.11 | 0.5889 | 0.2748 | -0.3141 |
| hipster-batch1 | 1.0% | 45.16 | 16.67 | -28.49 | 78.52 | 37.50 | -41.02 | 0.5889 | 0.3692 | -0.2197 |
| hipster-batch2 | 1.0% | 45.16 | 12.50 | -32.66 | 78.52 | 46.88 | -31.64 | 0.5889 | 0.3462 | -0.2427 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-hybrid-c0p8-m2-paper-1-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
