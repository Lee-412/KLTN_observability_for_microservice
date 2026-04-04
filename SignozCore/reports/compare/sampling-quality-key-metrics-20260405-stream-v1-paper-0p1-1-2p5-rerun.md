# Sampling Quality Key Metrics

tag: 20260405-stream-v1-paper-0p1-1-2p5-rerun
budgets_pct: 0.1, 1.0, 2.5

| dataset | budget | incident_capture_latency_ms | early_incident_retention_pct | critical_endpoint_coverage_pct | threshold_volatility_pct | max_step_change_pct | kept error traces |
|---|---:|---:|---:|---:|---:|---:|---:|
| train-ticket | 0.1% | NA | 60.00 | 33.65 (6176/18355) | NA | NA | 977/2536 (38.53%) |
| train-ticket | 1.0% | NA | 60.00 | 33.65 (6176/18355) | NA | NA | 977/2536 (38.53%) |
| train-ticket | 2.5% | NA | 60.00 | 33.65 (6176/18355) | NA | NA | 977/2536 (38.53%) |
| hipster-batch1 | 0.1% | NA | 62.87 | 12.84 (15593/121449) | NA | NA | 7669/61663 (12.44%) |
| hipster-batch1 | 1.0% | NA | 62.87 | 12.84 (15593/121449) | NA | NA | 7669/61663 (12.44%) |
| hipster-batch1 | 2.5% | NA | 62.87 | 12.84 (15593/121449) | NA | NA | 7669/61663 (12.44%) |
| hipster-batch2 | 0.1% | NA | 65.77 | 12.66 (22527/177945) | NA | NA | 10402/83120 (12.51%) |
| hipster-batch2 | 1.0% | NA | 65.77 | 12.66 (22527/177945) | NA | NA | 10402/83120 (12.51%) |
| hipster-batch2 | 2.5% | NA | 65.77 | 12.66 (22527/177945) | NA | NA | 10402/83120 (12.51%) |
