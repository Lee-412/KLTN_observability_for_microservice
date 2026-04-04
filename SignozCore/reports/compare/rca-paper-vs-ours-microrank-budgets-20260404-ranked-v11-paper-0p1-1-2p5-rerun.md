# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-ranked-v11-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 0.00 | -42.59 | 77.74 | 27.27 | -50.47 | 0.5509 | 0.1989 | -0.3520 |
| train-ticket | 1.0% | 45.16 | 0.00 | -45.16 | 78.52 | 23.53 | -54.99 | 0.5889 | 0.1735 | -0.4154 |
| train-ticket | 2.5% | 50.00 | 0.00 | -50.00 | 82.26 | 23.53 | -58.73 | 0.6556 | 0.1855 | -0.4701 |
| hipster-batch1 | 0.1% | 42.59 | 14.29 | -28.30 | 77.74 | 42.86 | -34.88 | 0.5509 | 0.3372 | -0.2137 |
| hipster-batch1 | 1.0% | 45.16 | 20.83 | -24.33 | 78.52 | 50.00 | -28.52 | 0.5889 | 0.3991 | -0.1898 |
| hipster-batch1 | 2.5% | 50.00 | 16.67 | -33.33 | 82.26 | 45.83 | -36.43 | 0.6556 | 0.3604 | -0.2952 |
| hipster-batch2 | 0.1% | 42.59 | 12.90 | -29.69 | 77.74 | 45.16 | -32.58 | 0.5509 | 0.3208 | -0.2301 |
| hipster-batch2 | 1.0% | 45.16 | 15.62 | -29.53 | 78.52 | 46.88 | -31.64 | 0.5889 | 0.3612 | -0.2277 |
| hipster-batch2 | 2.5% | 50.00 | 21.88 | -28.12 | 82.26 | 50.00 | -32.26 | 0.6556 | 0.4031 | -0.2525 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-ranked-v11-paper-0p1-1-2p5-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-ranked-v11-paper-0p1-1-2p5-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11-paper-0p1-1-2p5-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11-paper-0p1-1-2p5-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
