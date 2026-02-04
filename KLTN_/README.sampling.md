# Sampling changes (VI + EN)

Tài liệu này ghi rõ các thay đổi mình đã implement trong phần tail sampling của `signoz-otel-collector`, và cách cấu hình để kích hoạt chúng trong SigNoz stack.

---

## (VI) 1) Mục tiêu thay đổi

- Giữ nguyên hệ thống tail sampling hiện có của SigNoz (không phá các policy cũ).
- Thêm một policy mới kiểu **"model"** để có thể sampling theo điểm số (linear score) dựa trên feature của trace.
- Gắn vào full flow (SigNoz docker compose) để test end-to-end và so sánh offline bằng fileexporter JSON.

---

## (VI) 2) Thay đổi code cụ thể

### 2.1) Thêm policy type mới: `model`

Trong repo `signoz-otel-collector`:

- File: `signoz-otel-collector/processor/signoztailsampler/config.go`
  - Thêm `PolicyType = "model"`.
  - Thêm `ModelCfg` và field `ModelCfg *ModelCfg` trong `BasePolicy` (mapstructure key: `model`).

Ý nghĩa config:
- `model.type`: hiện implement `linear`.
- `model.threshold`: ngưỡng quyết định sample.
- `model.intercept`: hệ số tự do.
- `model.weights`: map feature -> weight.

### 2.2) Route evaluator sang model sampler

- File: `signoz-otel-collector/processor/signoztailsampler/evaluator.go`
  - Khi `policyCfg.Type == model`, evaluator tạo sampler mới (linear model sampler).
  - Nếu thiếu `model` config thì fallback về `NeverSample` để tránh crash.

### 2.3) Implement sampler mới (linear model)

- File: `signoz-otel-collector/processor/signoztailsampler/internal/sampling/model_sampler.go`

Logic (tóm tắt):
- Trích feature từ trace:
  - `duration_ms`: thời gian trace (earliest start -> latest end) tính theo ms.
  - `span_count`: tổng số spans.
  - `has_error`: có span lỗi (status ERROR) hoặc `http.status_code >= 500`.
- Tính điểm:
  - `score = intercept + Σ(weights[feature] * feature_value)`
- Quyết định:
  - Nếu `score >= threshold` => Sampled.

Ghi chú kỹ thuật:
- Đã tránh import-cycle bằng cách constructor của sampler nhận primitive values (threshold/intercept/weights) thay vì import config type.

### 2.4) Unit tests

- File: `signoz-otel-collector/processor/signoztailsampler/internal/sampling/model_sampler_test.go`
- Có test cho:
  - Trace ngắn (NotSampled)
  - Trace dài hơn (có thể Sampled tuỳ weights/threshold)
  - Trace có lỗi (Sampled với trọng số `has_error` đủ lớn)

---

## (VI) 3) Thay đổi cấu hình runtime (SigNoz docker)

### 3.1) Bật processor trong collector config

- File: `signoz/deploy/docker/otel-collector-config.yaml`

Đã thêm:
- `processors.signoz_tail_sampling` với policy `type: model`
- Insert `signoz_tail_sampling` vào pipeline `service.pipelines.traces.processors`

Ví dụ các tham số model đang dùng trong workspace này:
- `threshold: 2.0`
- `weights`:
  - `duration_ms: 0.01`
  - `has_error: 2.0`
  - `span_count: 0.05`

### 3.2) Export JSON để diff

- File: `signoz/deploy/docker/otel-collector-config.yaml`
  - `exporters.file/traces` và `exporters.file/logs`

Trong quá trình thí nghiệm, để không ghi đè baseline, output model dùng file riêng:
- `otel-export/traces.model.json`
- `otel-export/logs.model.json`

### 3.3) Dùng image collector custom

- File: `signoz/deploy/docker/.env`
  - `OTELCOL_TAG=kltn-model`

Fix thêm để schema migrator không bị kéo nhầm tag:
- File: `signoz/deploy/docker/docker-compose.yaml`
  - schema migrator dùng `SCHEMA_MIGRATOR_TAG`
- File: `signoz/deploy/docker/.env`
  - `SCHEMA_MIGRATOR_TAG=v0.129.13`

Ngoài ra, đã remove flag feature gate không hợp lệ khỏi `docker-compose.yaml`.

---

## (EN) 4) Goal

- Preserve existing SigNoz tail sampling behavior.
- Add a new policy type **"model"** to perform score-based sampling (linear scoring) using trace-derived features.
- Wire it into the full SigNoz Docker Compose flow and validate via UI + offline JSON diff.

---

## (EN) 5) Code changes

### 5.1) New policy type: `model`

Repo: `signoz-otel-collector`

- File: `signoz-otel-collector/processor/signoztailsampler/config.go`
  - Added `PolicyType = "model"`.
  - Added `ModelCfg` and `ModelCfg *ModelCfg` in `BasePolicy` (`mapstructure: "model"`).

Config meaning:
- `model.type`: currently implemented `linear`.
- `model.threshold`: sampling decision threshold.
- `model.intercept`: bias term.
- `model.weights`: map of feature -> weight.

### 5.2) Evaluator routing

- File: `signoz-otel-collector/processor/signoztailsampler/evaluator.go`
  - When `policyCfg.Type == model`, the evaluator builds and uses the linear model sampler.
  - Missing `model` config safely falls back to `NeverSample`.

### 5.3) New sampler implementation (linear model)

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

### 5.4) Unit tests

- File: `signoz-otel-collector/processor/signoztailsampler/internal/sampling/model_sampler_test.go`

Covers short/long/error traces.

---

## (EN) 6) Runtime / deployment changes (SigNoz docker)

### 6.1) Enable the processor in collector config

- File: `signoz/deploy/docker/otel-collector-config.yaml`
  - Added `processors.signoz_tail_sampling` with a `type: model` policy.
  - Inserted `signoz_tail_sampling` into `service.pipelines.traces.processors`.

### 6.2) JSON export for offline diff

- File: `signoz/deploy/docker/otel-collector-config.yaml`
  - Added `fileexporter` exporters.

To avoid overwriting baseline exports, the model run writes to:
- `otel-export/traces.model.json`
- `otel-export/logs.model.json`

### 6.3) Use the custom collector image

- File: `signoz/deploy/docker/.env`
  - `OTELCOL_TAG=kltn-model`

Also split schema migrator tag into `SCHEMA_MIGRATOR_TAG` in:
- `signoz/deploy/docker/docker-compose.yaml`
- `signoz/deploy/docker/.env`

And removed an invalid `--feature-gates` flag from the compose command to prevent collector crashes.
