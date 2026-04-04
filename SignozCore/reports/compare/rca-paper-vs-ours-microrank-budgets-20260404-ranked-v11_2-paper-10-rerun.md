# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260404-ranked-v11_2-paper-10-rerun
budgets_pct: 10.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 10.0% | 0.00 | 5.88 | +5.88 | 0.00 | 17.65 | +17.65 | 0.0000 | 0.1852 | +0.1852 |
| hipster-batch1 | 10.0% | 0.00 | 20.83 | +20.83 | 0.00 | 37.50 | +37.50 | 0.0000 | 0.3849 | +0.3849 |
| hipster-batch2 | 10.0% | 0.00 | 18.75 | +18.75 | 0.00 | 50.00 | +50.00 | 0.0000 | 0.3950 | +0.3950 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-paper-10-rerun/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260404-ranked-v11_2-paper-10-rerun/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260404-ranked-v11_2-paper-10-rerun.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260404-ranked-v11_2-paper-10-rerun.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-paper-10-rerun.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260404-ranked-v11_2-paper-10-rerun.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
