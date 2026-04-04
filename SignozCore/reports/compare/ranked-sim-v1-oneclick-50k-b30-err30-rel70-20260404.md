# Ranked Adaptive Simulation (One-click Data)

input: signoz/deploy/docker/otel-export/model1/20260404-182208-traces.model.json
run_id: thesis-proof-20260404-181933
total_traces: 14699
incident_start_sec: 2
error_budget_ratio: 0.3

| budget | kept traces | selected error/context | kept error traces | early incident retention | critical endpoint coverage | incident_capture_latency_ms |
|---:|---:|---:|---:|---:|---:|---:|
| 30% | 4410/14699 (30.00%) | 1323/3087 | 1323/3258 (40.61%) | 39.54 | 45.15 (4410/9767) | 0.0 |
