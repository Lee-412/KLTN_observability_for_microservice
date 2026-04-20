# Oracle Metrics Upper-Bound Ablation

tag: v52-oracle-metrics-upper-bound-20260419-201131

| Variant | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Trace-only | 15.87 | 9.80 | 11.81 | 57.94 | 21.41 | 25.57 | 0.3944 | 0.2180 | 0.2613 |
| Oracle-metrics upper-bound | 11.79 | 13.07 | 8.33 | 36.42 | 34.80 | 30.07 | 0.2821 | 0.3115 | 0.2743 |
| Delta | -4.08 | +3.27 | -3.48 | -21.52 | +13.39 | +4.50 | -0.1123 | +0.0935 | +0.0130 |

Upper-bound gain over trace-only = -4.08
Interpretation: sampler architecture remains bottlenecked
