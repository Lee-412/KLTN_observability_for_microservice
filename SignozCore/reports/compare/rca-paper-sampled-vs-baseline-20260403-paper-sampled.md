# RCA on Sampled Paper Traces vs Baseline

tag: 20260403-paper-sampled
target_tps: 0.2

| dataset | sampled row rate | A@1 base -> sampled | A@3 base -> sampled | MRR base -> sampled |
|---|---:|---:|---:|---:|
| train-ticket | 68.74% | 0.1176 -> 0.0588 (-0.0588) | 0.5294 -> 0.3529 (-0.1765) | 0.3172 -> 0.2464 (-0.0708) |
| hipster-batch1 | 68.60% | 0.1250 -> 0.1667 (+0.0417) | 0.3333 -> 0.3333 (+0.0000) | 0.3226 -> 0.3477 (+0.0252) |
| hipster-batch2 | 60.78% | 0.1562 -> 0.0625 (-0.0938) | 0.4375 -> 0.3438 (-0.0938) | 0.3767 -> 0.2974 (-0.0793) |

## Notes

- Baseline here is existing paper-source RCA benchmark (raw traces, MicroRank).
- Sampled run uses current adaptive simulation policy from evaluate_paper_sampling.py.
- This is a pragmatic comparison against your current method, not an exact replication of the paper budget protocol.
