# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260405-stream-v1-strictfix-paper-0p1-1-2p5
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 7.69 | -34.90 | 77.74 | 30.77 | -46.97 | 0.5509 | 0.2951 | -0.2558 |
| train-ticket | 1.0% | 45.16 | 17.65 | -27.51 | 78.52 | 52.94 | -25.58 | 0.5889 | 0.3703 | -0.2186 |
| train-ticket | 2.5% | 50.00 | 17.65 | -32.35 | 82.26 | 52.94 | -29.32 | 0.6556 | 0.3823 | -0.2733 |
| hipster-batch1 | 0.1% | 42.59 | 9.09 | -33.50 | 77.74 | 54.55 | -23.19 | 0.5509 | 0.3167 | -0.2342 |
| hipster-batch1 | 1.0% | 45.16 | 12.50 | -32.66 | 78.52 | 50.00 | -28.52 | 0.5889 | 0.3527 | -0.2362 |
| hipster-batch1 | 2.5% | 50.00 | 12.50 | -37.50 | 82.26 | 50.00 | -32.26 | 0.6556 | 0.3546 | -0.3010 |
| hipster-batch2 | 0.1% | 42.59 | 13.33 | -29.26 | 77.74 | 46.67 | -31.07 | 0.5509 | 0.3648 | -0.1861 |
| hipster-batch2 | 1.0% | 45.16 | 15.62 | -29.53 | 78.52 | 43.75 | -34.77 | 0.5889 | 0.3735 | -0.2154 |
| hipster-batch2 | 2.5% | 50.00 | 21.88 | -28.12 | 82.26 | 43.75 | -38.51 | 0.6556 | 0.4101 | -0.2455 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-stream-v1-strictfix-paper-0p1-1-2p5/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-stream-v1-strictfix-paper-0p1-1-2p5/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-stream-v1-strictfix-paper-0p1-1-2p5.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-stream-v1-strictfix-paper-0p1-1-2p5.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v1-strictfix-paper-0p1-1-2p5.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v1-strictfix-paper-0p1-1-2p5.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
