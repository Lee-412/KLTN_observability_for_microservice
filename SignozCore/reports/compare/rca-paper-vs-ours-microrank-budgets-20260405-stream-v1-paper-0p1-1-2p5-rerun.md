# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260405-stream-v1-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 0.00 | -42.59 | 77.74 | 35.29 | -42.45 | 0.5509 | 0.2211 | -0.3298 |
| train-ticket | 1.0% | 45.16 | 0.00 | -45.16 | 78.52 | 35.29 | -43.23 | 0.5889 | 0.2211 | -0.3678 |
| train-ticket | 2.5% | 50.00 | 0.00 | -50.00 | 82.26 | 35.29 | -46.97 | 0.6556 | 0.2211 | -0.4345 |
| hipster-batch1 | 0.1% | 42.59 | 8.33 | -34.26 | 77.74 | 29.17 | -48.57 | 0.5509 | 0.2886 | -0.2623 |
| hipster-batch1 | 1.0% | 45.16 | 8.33 | -36.83 | 78.52 | 29.17 | -49.35 | 0.5889 | 0.2886 | -0.3003 |
| hipster-batch1 | 2.5% | 50.00 | 8.33 | -41.67 | 82.26 | 29.17 | -53.09 | 0.6556 | 0.2886 | -0.3670 |
| hipster-batch2 | 0.1% | 42.59 | 15.62 | -26.97 | 77.74 | 37.50 | -40.24 | 0.5509 | 0.3542 | -0.1967 |
| hipster-batch2 | 1.0% | 45.16 | 15.62 | -29.53 | 78.52 | 37.50 | -41.02 | 0.5889 | 0.3542 | -0.2347 |
| hipster-batch2 | 2.5% | 50.00 | 15.62 | -34.38 | 82.26 | 37.50 | -44.76 | 0.6556 | 0.3542 | -0.3014 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-stream-v1-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-stream-v1-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-stream-v1-paper-0p1-1-2p5-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-stream-v1-paper-0p1-1-2p5-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v1-paper-0p1-1-2p5-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v1-paper-0p1-1-2p5-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
