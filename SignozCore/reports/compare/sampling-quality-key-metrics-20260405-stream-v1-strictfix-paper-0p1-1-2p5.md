# Sampling Quality Key Metrics

tag: 20260405-stream-v1-strictfix-paper-0p1-1-2p5
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 0.15 (27/18355) | NA | NA | 27/2536 (1.06%) |
| train-ticket | 1.0% | NA | 60.00 | 1.49 (274/18355) | NA | NA | 274/2536 (10.80%) |
| train-ticket | 2.5% | NA | 60.00 | 3.73 (684/18355) | NA | NA | 684/2536 (26.97%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 121/61663 (0.20%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 1214/61663 (1.97%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 3036/61663 (4.92%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 178/83120 (0.21%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 1779/83120 (2.14%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 4449/83120 (5.35%) |
