# Table 3 (MicroRank): Paper vs Ours

This table keeps only the MicroRank block from the paper table and adds one row for our current result.

| RCA Approach | Sampling Approach | A@1 (0.1%) | A@1 (1.0%) | A@1 (2.5%) | A@3 (0.1%) | A@3 (1.0%) | A@3 (2.5%) | MRR (0.1%) | MRR (1.0%) | MRR (2.5%) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| MicroRank | TraStrainer (paper) | 42.59 | 45.16 | 50.00 | 77.74 | 78.52 | 82.26 | 0.5509 | 0.5889 | 0.6556 |
| MicroRank | Ours (current sampling, single setting) | 9.60 | 9.60 | 9.60 | 34.33 | 34.33 | 34.33 | 0.2972 | 0.2972 | 0.2972 |

## Notes

- Paper row is copied from Table 3 image (MicroRank + TraStrainer row).
- Ours row is macro-average across 3 paper datasets from:
  - reports/compare/rca-paper-sampled-vs-baseline-20260403-paper-sampled.csv
- Ours currently has only one sampling setting (not three budget points 0.1/1.0/2.5).
- To keep table layout identical to paper, the same Ours value is repeated across the three budget columns.
