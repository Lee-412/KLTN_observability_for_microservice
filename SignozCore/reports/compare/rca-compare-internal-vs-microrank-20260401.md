# RCA Comparison: Internal Heuristic vs MicroRank (Coverage-First)

Date: 2026-04-01

## Scope
- Profile: coverage-first
- Datasets: train-ticket, hipster-batch1, hipster-batch2
- Baseline: internal heuristic engine outputs (tag 20260401)
- Candidate: MicroRank offline engine outputs (tag 20260401-microrank)

## Side-by-side Metrics

| dataset | scenarios | A@1 internal | A@1 microrank | delta | A@3 internal | A@3 microrank | delta | MRR internal | MRR microrank | delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 17 | 0.1176 | 0.1176 | +0.0000 | 0.1176 | 0.5294 | +0.4118 | 0.1949 | 0.3172 | +0.1223 |
| hipster-batch1 | 24 | 0.0417 | 0.1250 | +0.0833 | 0.2917 | 0.3333 | +0.0417 | 0.2638 | 0.3226 | +0.0588 |
| hipster-batch2 | 32 | 0.1250 | 0.1562 | +0.0312 | 0.3125 | 0.4375 | +0.1250 | 0.3233 | 0.3767 | +0.0533 |

## Inference Time Comparison (p50 ms)

| dataset | internal p50 | microrank p50 | delta (microrank - internal) |
|---|---:|---:|---:|
| train-ticket | 1618.589 | 455.869 | -1162.720 |
| hipster-batch1 | 8564.536 | 24297.230 | +15732.694 |
| hipster-batch2 | 12394.284 | 24157.284 | +11763.000 |

## Source Summary Files

Internal:
- reports/compare/rca-benchmark-train-ticket-coverage-first-20260401-summary.json
- reports/compare/rca-benchmark-hipster-batch1-coverage-first-20260401-summary.json
- reports/compare/rca-benchmark-hipster-batch2-coverage-first-20260401-summary.json

MicroRank:
- reports/compare/rca-benchmark-train-ticket-coverage-first-20260401-microrank-summary.json
- reports/compare/rca-benchmark-hipster-batch1-coverage-first-20260401-microrank-summary.json
- reports/compare/rca-benchmark-hipster-batch2-coverage-first-20260401-microrank-summary.json

## Conclusion
- MicroRank improves ranking quality on all three datasets in A@3 and MRR.
- A@1 is improved on hipster-batch1 and hipster-batch2, and unchanged on train-ticket.
- Runtime trade-off is dataset-dependent: faster on train-ticket, slower on both hipster batches.
