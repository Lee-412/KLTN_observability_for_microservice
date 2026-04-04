# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-paper-budgets-50-70
budgets_pct: 50.0, 60.0, 70.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 50.0% | 0.00 | 5.88 | +5.88 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2463 | +0.2463 |
| train-ticket | 60.0% | 0.00 | 5.88 | +5.88 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2457 | +0.2457 |
| train-ticket | 70.0% | 0.00 | 5.88 | +5.88 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2449 | +0.2449 |
| hipster-batch1 | 50.0% | 0.00 | 20.83 | +20.83 | 0.00 | 45.83 | +45.83 | 0.0000 | 0.3991 | +0.3991 |
| hipster-batch1 | 60.0% | 0.00 | 16.67 | +16.67 | 0.00 | 33.33 | +33.33 | 0.0000 | 0.3473 | +0.3473 |
| hipster-batch1 | 70.0% | 0.00 | 16.67 | +16.67 | 0.00 | 33.33 | +33.33 | 0.0000 | 0.3465 | +0.3465 |
| hipster-batch2 | 50.0% | 0.00 | 6.25 | +6.25 | 0.00 | 37.50 | +37.50 | 0.0000 | 0.3178 | +0.3178 |
| hipster-batch2 | 60.0% | 0.00 | 6.25 | +6.25 | 0.00 | 34.38 | +34.38 | 0.0000 | 0.2948 | +0.2948 |
| hipster-batch2 | 70.0% | 0.00 | 6.25 | +6.25 | 0.00 | 34.38 | +34.38 | 0.0000 | 0.2948 | +0.2948 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-paper-budgets-50-70/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-paper-budgets-50-70/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-paper-budgets-50-70.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-paper-budgets-50-70.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-paper-budgets-50-70.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-paper-budgets-50-70.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
