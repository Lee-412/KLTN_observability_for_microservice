# Table 3 Style Comparison: All Paper Models + Ours

generated_at: 20260412-184436

| Model | A@1 0.1% | A@1 1.0% | A@1 2.5% | A@3 0.1% | A@3 1.0% | A@3 2.5% | MRR 0.1% | MRR 1.0% | MRR 2.5% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Random | 5.56 | 16.67 | 27.78 | 20.37 | 50.00 | 61.11 | 0.1571 | 0.3423 | 0.4352 |
| HC | 7.41 | 18.52 | 22.22 | 27.78 | 46.30 | 51.85 | 0.1954 | 0.3398 | 0.3731 |
| Sifter | 5.56 | 18.52 | 27.78 | 23.42 | 46.30 | 61.11 | 0.1605 | 0.3414 | 0.4358 |
| Sieve | 9.26 | 25.83 | 35.19 | 20.37 | 58.15 | 62.96 | 0.1657 | 0.4246 | 0.4963 |
| TraStrainer w/o M | 12.96 | 16.67 | 24.07 | 42.59 | 42.59 | 55.56 | 0.2994 | 0.3241 | 0.4012 |
| TraStrainer w/o D | 29.63 | 42.59 | 46.30 | 74.04 | 68.52 | 72.22 | 0.5228 | 0.5463 | 0.5509 |
| TraStrainer | 42.59 | 45.16 | 50.00 | 77.74 | 78.52 | 82.26 | 0.5509 | 0.5889 | 0.6556 |
 Ours (MicroRank + stream-v3) | 14.81 | 18.16 | 10.99 | 39.20 | 47.86 | 49.02 | 0.3469 | 0.3898 | 0.3477 |
| Ours (MicroRank + stream-v3-composite-strictcap) | 14.63 | 20.81 | 18.23 | 38.56 | 50.32 | 51.06 | 0.3352 | 0.4069 | 0.3966 |

## Notes
- Paper model rows are copied from Table 3 screenshot provided in this chat.
- Ours reference rows (Hybrid, Stream v1 strictfix, stream-v2 milestone) are preserved from prior comparison artifacts.
- Current run row is generated automatically from this run with selection-mode=stream-v3-composite-strictcap.
