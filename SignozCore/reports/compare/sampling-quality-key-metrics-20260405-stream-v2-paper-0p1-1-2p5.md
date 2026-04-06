# Sampling Quality Key Metrics

tag: 20260405-stream-v2-paper-0p1-1-2p5
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 0.14 (26/18355) | NA | NA | 4/2536 (0.16%) |
| train-ticket | 1.0% | NA | 60.00 | 1.37 (252/18355) | NA | NA | 36/2536 (1.42%) |
| train-ticket | 2.5% | NA | 60.00 | 3.30 (606/18355) | NA | NA | 91/2536 (3.59%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 61/61663 (0.10%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 615/61663 (1.00%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 1538/61663 (2.49%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 83/83120 (0.10%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 833/83120 (1.00%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 2082/83120 (2.50%) |
