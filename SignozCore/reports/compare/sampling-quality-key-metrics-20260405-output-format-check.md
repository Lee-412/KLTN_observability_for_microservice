# Sampling Quality Key Metrics

tag: 20260405-output-format-check
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 0.15 (27/18355) | NA | NA | 4/2536 (0.16%) |
| train-ticket | 1.0% | NA | 60.00 | 1.48 (271/18355) | NA | NA | 41/2536 (1.62%) |
| train-ticket | 2.5% | NA | 60.00 | 3.63 (667/18355) | NA | NA | 102/2536 (4.02%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 60/61663 (0.10%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 597/61663 (0.97%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 1493/61663 (2.42%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 82/83120 (0.10%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 821/83120 (0.99%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 2054/83120 (2.47%) |
