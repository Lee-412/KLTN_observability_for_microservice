# MicroRank: Paper vs Ours (Averaged Across Datasets)

tag: stream-v2_1-sanity-train-1p0-20260405-215656
budgets_pct: 1.0

| budget | datasets | scenarios | paper A@1(%) | ours avg A@1(%) | delta | paper A@3(%) | ours avg A@3(%) | delta | paper MRR | ours avg MRR | delta |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1.0% | 1 | 17 | 45.16 | 0.00 | -45.16 | 78.52 | 0.00 | -78.52 | 0.5889 | 0.0983 | -0.4906 |

## Output Guide

- Phase1 sampled traces + kept trace IDs:
  - reports/analysis/paper-sampled-traces/stream-v2_1-sanity-train-1p0-20260405-215656/<dataset>/<budget_slug>/trace/*.csv
  - reports/analysis/paper-sampled-traces/stream-v2_1-sanity-train-1p0-20260405-215656/<dataset>/<budget_slug>/kept_trace_ids.txt
- Phase1 combined-style metric file:
  - reports/compare/paper-sampling-phase1-<dataset>-<budget_slug>-stream-v2_1-sanity-train-1p0-20260405-215656.txt
- Phase2 benchmark table (teacher-facing):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-stream-v2_1-sanity-train-1p0-20260405-215656.csv
- Phase2 compare with paper (csv + markdown):
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-stream-v2_1-sanity-train-1p0-20260405-215656.csv
  - /home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-vs-ours-microrank-budgets-stream-v2_1-sanity-train-1p0-20260405-215656.md

## Notes

- Budgets are calibrated by binary-searching target_tps to approximate requested keep ratio.
- budget_mode=strict: strict mode enforces exact trace-count budget after simulation.
- Compare-to-paper uses MicroRank row from paper Table 3.
- A@1/A@3 in paper are percentage points; internal evaluator outputs are ratios and converted to % here.
