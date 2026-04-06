# Sampling Quality Key Metrics

tag: stream-v2_3-paper-0p1-1-2p5-20260405-222114
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 0.15 (27/18355) | NA | NA | 4/2536 (0.16%) |
| train-ticket | 1.0% | NA | 60.00 | 1.46 (268/18355) | NA | NA | 44/2536 (1.74%) |
| train-ticket | 2.5% | NA | 60.00 | 3.59 (659/18355) | NA | NA | 109/2536 (4.30%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 88/61663 (0.14%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 887/61663 (1.44%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 2219/61663 (3.60%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 125/83120 (0.15%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 1246/83120 (1.50%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 3116/83120 (3.75%) |
