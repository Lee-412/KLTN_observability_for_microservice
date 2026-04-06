# MicroRank RCA Metrics by Dataset and Budget

tag: stream-v2_3-paper-0p1-1-2p5-md2-20260405-225041
selection_mode: stream-v2
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | scenarios | ours A@1(%) | ours A@3(%) | ours MRR | paper A@1(%) | delta A@1 | paper A@3(%) | delta A@3 | paper MRR | delta MRR | achieved keep(%) | pre-cap keep(%) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hipster-batch1 | 0.1% | 6 | 33.33 | 50.00 | 0.4931 | 42.59 | -9.26 | 77.74 | -27.74 | 0.5509 | -0.0578 | 0.10 | 46.15 |
| hipster-batch1 | 1.0% | 20 | 5.00 | 25.00 | 0.2069 | 45.16 | -40.16 | 78.52 | -53.52 | 0.5889 | -0.3820 | 1.00 | 46.15 |
| hipster-batch1 | 2.5% | 24 | 16.67 | 33.33 | 0.3322 | 50.00 | -33.33 | 82.26 | -48.93 | 0.6556 | -0.3234 | 2.50 | 46.15 |
| hipster-batch2 | 0.1% | 7 | 14.29 | 57.14 | 0.3429 | 42.59 | -28.30 | 77.74 | -20.60 | 0.5509 | -0.2080 | 0.10 | 44.23 |
| hipster-batch2 | 1.0% | 27 | 14.81 | 37.04 | 0.2789 | 45.16 | -30.35 | 78.52 | -41.48 | 0.5889 | -0.3100 | 1.00 | 44.23 |
| hipster-batch2 | 2.5% | 32 | 18.75 | 37.50 | 0.3564 | 50.00 | -31.25 | 82.26 | -44.76 | 0.6556 | -0.2992 | 2.50 | 44.23 |
| train-ticket | 0.1% | 6 | 0.00 | 66.67 | 0.3472 | 42.59 | -42.59 | 77.74 | -11.07 | 0.5509 | -0.2037 | 0.10 | 36.80 |
| train-ticket | 1.0% | 17 | 5.88 | 5.88 | 0.1533 | 45.16 | -39.28 | 78.52 | -72.64 | 0.5889 | -0.4356 | 1.00 | 36.80 |
| train-ticket | 2.5% | 17 | 0.00 | 5.88 | 0.0965 | 50.00 | -50.00 | 82.26 | -76.38 | 0.6556 | -0.5591 | 2.50 | 36.80 |

## Notes
- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.

## Related CSV
- /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-stream-v2_3-paper-0p1-1-2p5-md2-20260405-225041.csv
- /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-stream-v2_3-paper-0p1-1-2p5-md2-20260405-225041.csv
