# Sampling Quality Key Metrics

tag: 20260405-ranked-v11_2-hybrid-c0p7-m2-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 0.15 (27/18355) | NA | NA | 27/2536 (1.06%) |
| train-ticket | 1.0% | NA | 60.00 | 1.49 (274/18355) | NA | NA | 219/2536 (8.64%) |
| train-ticket | 2.5% | NA | 60.00 | 3.73 (684/18355) | NA | NA | 514/2536 (20.27%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 0.10 (121/121449) | NA | NA | 108/61663 (0.18%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 1.00 (1214/121449) | NA | NA | 1077/61663 (1.75%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 2.50 (3036/121449) | NA | NA | 2727/61663 (4.42%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 0.10 (178/177945) | NA | NA | 159/83120 (0.19%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 1.00 (1779/177945) | NA | NA | 1562/83120 (1.88%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 2.50 (4449/177945) | NA | NA | 3962/83120 (4.77%) |
