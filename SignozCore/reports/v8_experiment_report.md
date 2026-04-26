# V8 Metric-Aware Sampling from v3.5: Full Benchmark Report

## 1) Implementation Summary
- Created new sampler: `scripts/streamv8_metric_aware_sampling_from_v35.py` from v3.5 baseline with metric-aware probability fusion and telemetry.
- Created isolated runner copy: `scripts/run_paper_sampled_rca_v8.py` with a new mode `stream-v8-metric-aware-v35` (no changes to original runner).
- Executed 9 baseline runs (v3.5 strictcap) and 9 v8 runs (strict mode, guard disabled via runtime flags `--v8-min-normal-ratio 0.0 --v8-min-error-ratio 0.0`).

## 2) Benchmark Table (Baseline vs V8)
| dataset | budget | A@1 (base) | A@1 (v8) | ΔA@1 (pp) | A@3 (base) | A@3 (v8) | A@5 (base) | A@5 (v8) | MRR (base) | MRR (v8) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hipster-batch1 | 0.1% | 0.3333 | 0.3750 | +4.17 | 0.5417 | 0.5417 | 0.7917 | 0.7917 | 0.4856 | 0.5117 |
| hipster-batch1 | 1.0% | 0.2083 | 0.2083 | +0.00 | 0.5417 | 0.5417 | 0.7500 | 0.7500 | 0.4149 | 0.4152 |
| hipster-batch1 | 2.5% | 0.2500 | 0.2083 | -4.17 | 0.5417 | 0.5417 | 0.7500 | 0.7917 | 0.4432 | 0.4237 |
| hipster-batch2 | 0.1% | 0.2500 | 0.2500 | +0.00 | 0.5000 | 0.5312 | 0.7812 | 0.7812 | 0.4315 | 0.4341 |
| hipster-batch2 | 1.0% | 0.2812 | 0.2500 | -3.12 | 0.5625 | 0.5625 | 0.7500 | 0.7500 | 0.4661 | 0.4401 |
| hipster-batch2 | 2.5% | 0.1875 | 0.2188 | +3.12 | 0.4688 | 0.4688 | 0.7500 | 0.7500 | 0.3943 | 0.4167 |
| train-ticket | 0.1% | 0.1176 | 0.1176 | +0.00 | 0.3529 | 0.3529 | 0.5882 | 0.5882 | 0.3142 | 0.3142 |
| train-ticket | 1.0% | 0.2941 | 0.2941 | +0.00 | 0.5882 | 0.5294 | 0.5882 | 0.5294 | 0.4520 | 0.4384 |
| train-ticket | 2.5% | 0.1176 | 0.1176 | +0.00 | 0.5882 | 0.5294 | 0.6471 | 0.7059 | 0.3591 | 0.3559 |

## 3) Uplift Summary
- Macro average ΔA@1: +0.00 pp
- Macro average ΔA@3: -0.96 pp
- Macro average ΔA@5: +0.46 pp
- Macro average ΔMRR: -0.0012
- Budget-level ΔA@1 averages:
  - 0.1%: +1.39 pp
  - 1.0%: -1.04 pp
  - 2.5%: -0.35 pp
- Dataset-level ΔA@1 averages:
  - hipster-batch1: +0.00 pp
  - hipster-batch2: +0.00 pp
  - train-ticket: +0.00 pp

## 4) Stability Analysis (std of A@1 across budgets)
| dataset | baseline std(A@1) | v8 std(A@1) | Δstd |
|---|---:|---:|---:|
| hipster-batch1 | 0.0520 | 0.0786 | +0.0266 |
| hipster-batch2 | 0.0390 | 0.0147 | -0.0242 |
| train-ticket | 0.0832 | 0.0832 | +0.0000 |

## 5) Failure Cases
- Cases with negative ΔA@1:
  - hipster-batch1 @ 2.5%: -4.17 pp
  - hipster-batch2 @ 1.0%: -3.12 pp
- Initial strict v8 run with non-zero contrast guard failed at 0.1% (empty candidates on hipster scenarios). Re-run with guard disabled produced fully valid 9/9 runs.

## 6) Telemetry Observations (v8 logs)
- Mean probability uplift (`p_final_mean - p_trace_mean`): 0.1859
- Mean service-to-pod mapping coverage ratio: 0.9660
- Mean mapping confidence: 0.8481
- Top1 confidence and top1-top2 gap were computed from adapted ranking JSON files and included in the CSV output.

## 7) Recommendation
- Candidate for partial migration into the 3.94 line, but only after adding a safe contrast guard policy that does not create empty-candidate scenarios at 0.1% budgets.

## Artifacts
- results CSV: `/home/leeduc/Desktop/KLTN/SignozCore/results/v8_full_benchmark_summary.csv`
- report: `/home/leeduc/Desktop/KLTN/SignozCore/reports/v8_experiment_report.md`
- baseline run CSV: `/home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-v8exp-baseline-20260422-132707.csv`
- v8 run CSV: `/home/leeduc/Desktop/KLTN/SignozCore/reports/compare/rca-paper-table-sampled-budgets-v8exp-v8noguard-20260422-135713.csv`
