# MicroRank: Paper vs Ours (Averaged Across Datasets)

tag: 20260405-output-format-check
budgets_pct: 0.1, 1.0, 2.5

| budget | datasets | scenarios | paper A@1(%) | ours avg A@1(%) | delta | paper A@3(%) | ours avg A@3(%) | delta | paper MRR | ours avg MRR | delta |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.1% | 3 | 36 | 42.59 | 3.03 | -39.56 | 77.74 | 40.91 | -36.83 | 0.5509 | 0.2483 | -0.3026 |
| 1.0% | 3 | 73 | 45.16 | 9.03 | -36.13 | 78.52 | 25.57 | -52.95 | 0.5889 | 0.2529 | -0.3360 |
| 2.5% | 3 | 73 | 50.00 | 7.52 | -42.48 | 82.26 | 26.61 | -55.65 | 0.6556 | 0.2496 | -0.4060 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-output-format-check/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-output-format-check/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-output-format-check.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-output-format-check.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-output-format-check.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-output-format-check.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
