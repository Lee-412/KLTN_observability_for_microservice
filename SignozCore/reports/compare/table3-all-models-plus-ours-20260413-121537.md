# Table 3 Style Comparison: All Paper Models + Ours

generated_at: 20260413-121537

| Model | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Random | 5.56 | 16.67 | 27.78 | 20.37 | 50.00 | 61.11 | 0.1571 | 0.3423 | 0.4352 |
| HC | 7.41 | 18.52 | 22.22 | 27.78 | 46.30 | 51.85 | 0.1954 | 0.3398 | 0.3731 |
| Sifter | 5.56 | 18.52 | 27.78 | 23.42 | 46.30 | 61.11 | 0.1605 | 0.3414 | 0.4358 |
| Sieve | 9.26 | 25.83 | 35.19 | 20.37 | 58.15 | 62.96 | 0.1657 | 0.4246 | 0.4963 |
| TraStrainer w/o M | 12.96 | 16.67 | 24.07 | 42.59 | 42.59 | 55.56 | 0.2994 | 0.3241 | 0.4012 |
| TraStrainer w/o D | 29.63 | 42.59 | 46.30 | 74.04 | 68.52 | 72.22 | 0.5228 | 0.5463 | 0.5509 |
| TraStrainer | 42.59 | 45.16 | 50.00 | 77.74 | 78.52 | 82.26 | 0.5509 | 0.5889 | 0.6556 |
| Ours (MicroRank + Hybrid c0.7 m2) | 9.25 | 20.00 | 17.69 | 39.18 | 40.81 | 37.36 | 0.2981 | 0.3736 | 0.3502 |
| Ours (MicroRank + Stream v1 strictfix) | 10.04 | 15.26 | 17.34 | 43.99 | 48.90 | 48.90 | 0.3255 | 0.3655 | 0.3823 |
| Ours (MicroRank + stream-v2) | 15.87 | 9.80 | 11.81 | 57.94 | 21.41 | 25.57 | 0.3944 | 0.2180 | 0.2613 |
| Ours (MicroRank + stream-v3-composite-strictcap) | 23.37 | 22.65 | 16.07 | 47.53 | 52.94 | 53.29 | 0.4151 | 0.4219 | 0.3861 |

## Notes
- Paper model rows are copied from Table 3 screenshot provided in this chat.
- Ours reference rows (Hybrid, Stream v1 strictfix, stream-v2 milestone) are preserved from prior comparison artifacts.
- Current run row is generated automatically from this run with selection-mode=stream-v3-composite-strictcap.
