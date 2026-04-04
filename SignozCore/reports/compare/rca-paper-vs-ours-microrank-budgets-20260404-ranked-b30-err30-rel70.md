# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-ranked-b30-err30-rel70
budgets_pct: 30.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 30.0% | 0.00 | 5.88 | +5.88 | 0.00 | 35.29 | +35.29 | 0.0000 | 0.2453 | +0.2453 |
| hipster-batch1 | 30.0% | 0.00 | 16.67 | +16.67 | 0.00 | 37.50 | +37.50 | 0.0000 | 0.3406 | +0.3406 |
| hipster-batch2 | 30.0% | 0.00 | 18.75 | +18.75 | 0.00 | 40.62 | +40.62 | 0.0000 | 0.3573 | +0.3573 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-ranked-b30-err30-rel70/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-ranked-b30-err30-rel70/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-ranked-b30-err30-rel70.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-ranked-b30-err30-rel70.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-b30-err30-rel70.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-b30-err30-rel70.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
