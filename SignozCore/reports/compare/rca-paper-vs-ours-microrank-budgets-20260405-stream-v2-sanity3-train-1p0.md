# MicroRank: Paper vs Ours (All Datasets, Multi-Budget)

tag: 20260405-stream-v2-sanity3-train-1p0
budgets_pct: 1.0

| dataset | budget | paper A@1(%) | ours A@1(%) | delta | paper A@3(%) | ours A@3(%) | delta | paper MRR | ours MRR | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 1.0% | 45.16 | 5.88 | -39.28 | 78.52 | 11.76 | -66.76 | 0.5889 | 0.1448 | -0.4441 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/20260405-stream-v2-sanity3-train-1p0/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/20260405-stream-v2-sanity3-train-1p0/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-20260405-stream-v2-sanity3-train-1p0.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-20260405-stream-v2-sanity3-train-1p0.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v2-sanity3-train-1p0.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-20260405-stream-v2-sanity3-train-1p0.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
