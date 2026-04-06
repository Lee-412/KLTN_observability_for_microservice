# MicroRank: Paper vs Ours (Averaged Across Datasets)

tag: stream-v2_3-budget0p1-20260405-220801
budgets_pct: 0.1

| budget | datasets | scenarios | paper A@1(%) | ours avg A@1(%) | delta | paper A@3(%) | ours avg A@3(%) | delta | paper MRR | ours avg MRR | delta |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.1% | 3 | 19 | 42.59 | 15.87 | -26.72 | 77.74 | 57.94 | -19.80 | 0.5509 | 0.3944 | -0.1565 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/stream-v2_3-budget0p1-20260405-220801/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/stream-v2_3-budget0p1-20260405-220801/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-stream-v2_3-budget0p1-20260405-220801.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-stream-v2_3-budget0p1-20260405-220801.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-stream-v2_3-budget0p1-20260405-220801.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-stream-v2_3-budget0p1-20260405-220801.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
