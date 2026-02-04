# KLTN_ — Custom SigNoz sampling assets

This folder contains the customizations and experiment artifacts used to compare baseline tail sampling and model-based tail sampling in SigNoz.

Environment installation and SigNoz deployment instructions are intentionally out of scope for this README.

---

## 1) Scope

- Custom tail-sampling policy implementation (`type: model`) in the collector.
- Runtime configuration changes used for the experiment.
- Offline trace export and comparison scripts.
- Example exports and saved reports produced in this workspace.

---

## 2) Repository contents

- `signoz/`: SigNoz repository and Docker Compose deployment.
- `signoz-otel-collector/`: collector source with sampling changes.
- `scripts/`: offline comparison tooling.
- `reports/compare/`: saved comparison reports.
- `signoz/deploy/docker/otel-export/`: example export snapshots (baseline and model).

---

## 3) Custom sampling policy: `model`

A new tail-sampling policy type `model` is added. The sampler implements a linear scoring rule:

```
score = intercept + Σ(weight_i × feature_i)
```

Sampling decision:

```
score ≥ threshold  => Sampled
score < threshold  => NotSampled
```

Supported features:
- `duration_ms`: trace duration (earliest start to latest end)
- `span_count`: number of spans
- `has_error`: 1 if any span is error or `http.status_code >= 500`, else 0

Configuration validation is enforced for `type: model` policies (including sub-policies).

---

## 4) Experiment artifacts

- Offline trace exports are written as line-delimited JSON via `fileexporter`.
- Comparison output is saved as timestamped reports under `reports/compare/`.

---

## 5) Documentation

- Sampling implementation and runtime changes: `README.sampling.md`
- Offline comparison workflow: `README.compare.md`
