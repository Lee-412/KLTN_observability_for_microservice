# Sampling Quality Key Metrics

tag: 20260404-ranked-v11-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | NA | NA | NA | 14/2536 (0.55%) |
| train-ticket | 1.0% | NA | 60.00 | NA | NA | NA | 147/2536 (5.80%) |
| train-ticket | 2.5% | NA | 60.00 | NA | NA | NA | 353/2536 (13.92%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 90/61663 (0.15%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 1076/61663 (1.74%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 2717/61663 (4.41%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 127/83120 (0.15%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 1545/83120 (1.86%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 3893/83120 (4.68%) |
