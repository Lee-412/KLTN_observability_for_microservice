# Compare traces (VI + EN)

Tài liệu này hướng dẫn cách **so sánh 2 file OTLP traces JSON** (baseline vs model) và **tự động lưu log so sánh** vào thư mục `reports/compare/`.

## (VI) 1) Mục tiêu

- Có cách so sánh **repeatable** giữa 2 lần chạy sampling.
- Lưu output so sánh thành file report để:
  - đính kèm vào báo cáo,
  - theo dõi lịch sử thay đổi,
  - không phải copy/paste thủ công.

## (VI) 2) Input/Output đang dùng trong workspace

### 2.1) Input file traces

Các file traces là **line-delimited JSON** do collector `fileexporter` ghi ra (mỗi dòng là một object chứa `resourceSpans`).

Trong workspace này, các snapshot ví dụ:
- Baseline: `signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json`
- Model: `signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json`

### 2.2) Output report (log compare)

Mọi report được lưu ở:
- `reports/compare/`

Tên file report có timestamp để không bị ghi đè.

## (VI) 3) Script 1: compare trực tiếp (in ra console)

Script:
- `scripts/compare_otlp_traces.py`

Chạy:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/compare_otlp_traces.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json \
  --details
```

Output chính:
- `records(lines)`
- `spans`
- `unique traceIds`
- `error spans` (% error)
- top `service.name`
- top route/method (best-effort: `http.route`, `http.target`, `rpc.method`)

## (VI) 4) Script 2: 1 lệnh là compare + tự lưu report

Script:
- `scripts/run_compare_and_save.py`

### 4.1) Chạy auto (khuyên dùng)

Mặc định script sẽ tự chọn file mới nhất trong:
- `signoz/deploy/docker/otel-export/baseline2/`
- `signoz/deploy/docker/otel-export/model1/`

Sau đó chạy compare và tự lưu report.

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/run_compare_and_save.py
```

Kiểm tra report:

```bash
ls -lh reports/compare | tail -n 5
```

### 4.2) Chỉ định file cụ thể

```bash
python3 scripts/run_compare_and_save.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json
```

### 4.3) Chỉ định folder (script tự pick file mới nhất)

```bash
python3 scripts/run_compare_and_save.py \
  --base-dir signoz/deploy/docker/otel-export/baseline2 \
  --test-dir signoz/deploy/docker/otel-export/model1
```

## (VI) 5) Troubleshooting

- Nếu script báo không tìm thấy file: kiểm tra folder có `*traces*.json`.
- Nếu output logs rỗng: workload (HotROD) thường chỉ sinh traces, không sinh OTLP logs.

---

## (EN) 1) Goal

- Provide a repeatable way to compare **two OTLP trace export JSON** files.
- Automatically save the comparison output as a timestamped report under `reports/compare/`.

## (EN) 2) Inputs/Outputs in this workspace

### 2.1) Trace input files

The trace files are **line-delimited JSON** produced by the collector `fileexporter`.

Example snapshots:
- Baseline: `signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json`
- Model: `signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json`

### 2.2) Saved reports

Reports are saved to:
- `reports/compare/`

## (EN) 3) Script 1: direct compare (prints to console)

Script:
- `scripts/compare_otlp_traces.py`

Run:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/compare_otlp_traces.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json \
  --details
```

## (EN) 4) Script 2: compare + auto-save report

Script:
- `scripts/run_compare_and_save.py`

### 4.1) Auto mode (recommended)

Auto-picks the newest `*traces*.json` from:
- `signoz/deploy/docker/otel-export/baseline2/`
- `signoz/deploy/docker/otel-export/model1/`

Then saves a timestamped report.

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/run_compare_and_save.py
ls -lh reports/compare | tail -n 5
```

### 4.2) Explicit files

```bash
python3 scripts/run_compare_and_save.py \
  --base <baseline.json> \
  --test <model.json>
```

### 4.3) Explicit directories

```bash
python3 scripts/run_compare_and_save.py \
  --base-dir <dir> \
  --test-dir <dir>
```
