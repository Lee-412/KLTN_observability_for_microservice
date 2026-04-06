# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260405-stream-v2-paper-0p1-1-2p5
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 0.00 | -42.59 | 77.74 | 10.00 | -67.74 | 0.5509 | 0.0986 | -0.4523 |
| train-ticket | 1.0% | 45.16 | 5.88 | -39.28 | 78.52 | 11.76 | -66.76 | 0.5889 | 0.1448 | -0.4441 |
| train-ticket | 2.5% | 50.00 | 0.00 | -50.00 | 82.26 | 11.76 | -70.50 | 0.6556 | 0.0924 | -0.5632 |
| hipster-batch1 | 0.1% | 42.59 | 13.33 | -29.26 | 77.74 | 33.33 | -44.41 | 0.5509 | 0.2611 | -0.2898 |
| hipster-batch1 | 1.0% | 45.16 | 4.17 | -40.99 | 78.52 | 29.17 | -49.35 | 0.5889 | 0.2585 | -0.3304 |
| hipster-batch1 | 2.5% | 50.00 | 16.67 | -33.33 | 82.26 | 33.33 | -48.93 | 0.6556 | 0.3389 | -0.3167 |
| hipster-batch2 | 0.1% | 42.59 | 8.33 | -34.26 | 77.74 | 25.00 | -52.74 | 0.5509 | 0.1806 | -0.3703 |
| hipster-batch2 | 1.0% | 45.16 | 9.38 | -35.78 | 78.52 | 37.50 | -41.02 | 0.5889 | 0.3230 | -0.2659 |
| hipster-batch2 | 2.5% | 50.00 | 18.75 | -31.25 | 82.26 | 37.50 | -44.76 | 0.6556 | 0.3735 | -0.2821 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-stream-v2-paper-0p1-1-2p5/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-stream-v2-paper-0p1-1-2p5/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-stream-v2-paper-0p1-1-2p5.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-stream-v2-paper-0p1-1-2p5.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v2-paper-0p1-1-2p5.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v2-paper-0p1-1-2p5.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
