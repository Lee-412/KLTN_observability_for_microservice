# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

## Ours Only (for screenshot)

| dataset | budget | ours A@1(%) | ours A@3(%) | ours MRR |
|---|---:|---:|---:|---:|
| train-ticket | 0.1% | 0.00 | 21.43 | 0.1985 |
| train-ticket | 1.0% | 23.53 | 41.18 | 0.3702 |
| train-ticket | 2.5% | 17.65 | 23.53 | 0.2865 |
| hipster-batch1 | 0.1% | 17.39 | 47.83 | 0.3603 |
| hipster-batch1 | 1.0% | 20.83 | 37.50 | 0.3866 |
| hipster-batch1 | 2.5% | 16.67 | 41.67 | 0.3724 |
| hipster-batch2 | 0.1% | 10.34 | 48.28 | 0.3356 |
| hipster-batch2 | 1.0% | 15.62 | 43.75 | 0.3641 |
| hipster-batch2 | 2.5% | 18.75 | 46.88 | 0.3917 |

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | 42.59 | 0.00 | -42.59 | 77.74 | 21.43 | -56.31 | 0.5509 | 0.1985 | -0.3524 |
| train-ticket | 1.0% | 45.16 | 23.53 | -21.63 | 78.52 | 41.18 | -37.34 | 0.5889 | 0.3702 | -0.2187 |
| train-ticket | 2.5% | 50.00 | 17.65 | -32.35 | 82.26 | 23.53 | -58.73 | 0.6556 | 0.2865 | -0.3691 |
| hipster-batch1 | 0.1% | 42.59 | 17.39 | -25.20 | 77.74 | 47.83 | -29.91 | 0.5509 | 0.3603 | -0.1906 |
| hipster-batch1 | 1.0% | 45.16 | 20.83 | -24.33 | 78.52 | 37.50 | -41.02 | 0.5889 | 0.3866 | -0.2023 |
| hipster-batch1 | 2.5% | 50.00 | 16.67 | -33.33 | 82.26 | 41.67 | -40.59 | 0.6556 | 0.3724 | -0.2832 |
| hipster-batch2 | 0.1% | 42.59 | 10.34 | -32.25 | 77.74 | 48.28 | -29.46 | 0.5509 | 0.3356 | -0.2153 |
| hipster-batch2 | 1.0% | 45.16 | 15.62 | -29.53 | 78.52 | 43.75 | -34.77 | 0.5889 | 0.3641 | -0.2248 |
| hipster-batch2 | 2.5% | 50.00 | 18.75 | -31.25 | 82.26 | 46.88 | -35.39 | 0.6556 | 0.3917 | -0.2639 |




## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
