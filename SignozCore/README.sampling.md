# 1) Goal

- Preserve existing SigNoz tail sampling behavior.
- Add a new policy type **"model"** to perform score-based sampling (linear scoring) using trace-derived features.
- Wire it into the full SigNoz Docker Compose flow and validate via UI + offline JSON diff.

---

## 2) Code changes

### 2.1) New policy type: `model`

Repo: `signoz-otel-collector`

- File: `signoz-otel-collector/processor/signoztailsampler/config.go`
  - Added `PolicyType = "model"`.
  - Added `ModelCfg` and `ModelCfg *ModelCfg` in `BasePolicy` (`mapstructure: "model"`).

Config meaning:
- `model.type`: currently implemented `linear`.
- `model.threshold`: sampling decision threshold.
- `model.intercept`: bias term.
- `model.weights`: map of feature -> weight.

### 2.2) Evaluator routing

- File: `signoz-otel-collector/processor/signoztailsampler/evaluator.go`
  - When `policyCfg.Type == model`, the evaluator builds and uses the linear model sampler.
  - Missing `model` config safely falls back to `NeverSample`.

### 2.3) New sampler implementation (linear model)

- File: `signoz-otel-collector/processor/signoztailsampler/internal/sampling/model_sampler.go`

Logic summary:
- Extract features:
  - `duration_ms` (earliest start to latest end across spans)
  - `span_count`
  - `has_error` (span status ERROR or `http.status_code >= 500`)
- Score:
  - `score = intercept + Σ(weights[feature] * feature_value)`
- Decision:
  - `score >= threshold` => Sampled

Implementation note:
- Avoided an import cycle by passing primitive config values into the sampler constructor.

### 2.4) Unit tests

- File: `signoz-otel-collector/processor/signoztailsampler/internal/sampling/model_sampler_test.go`

Covers short/long/error traces.

---

## 3) Runtime / deployment changes (SigNoz docker)

### 3.1) Enable the processor in collector config

- File: `signoz/deploy/docker/otel-collector-config.yaml`
  - Added `processors.signoz_tail_sampling` with a `type: model` policy.
  - Inserted `signoz_tail_sampling` into `service.pipelines.traces.processors`.

### 3.2) JSON export for offline diff

- File: `signoz/deploy/docker/otel-collector-config.yaml`
  - Added `fileexporter` exporters.

To avoid overwriting baseline exports, the model run writes to:
- `otel-export/traces.model.json`
- `otel-export/logs.model.json`

### 3.3) Use the custom collector image

- File: `signoz/deploy/docker/.env`
  - `OTELCOL_TAG=kltn-model`

Also split schema migrator tag into `SCHEMA_MIGRATOR_TAG` in:
- `signoz/deploy/docker/docker-compose.yaml`
- `signoz/deploy/docker/.env`

And removed an invalid `--feature-gates` flag from the compose command to prevent collector crashes.
