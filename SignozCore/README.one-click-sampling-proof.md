# One-click Sampling Proof (Baseline vs Model)

Hướng dẫn chạy test baseline và adaptive model
1) chạy baseline, 2) chạy model, 3) snapshot ra 2 file riêng, 4) compare + retention.

## 1) Script 

- Script: [scripts/one_click_sampling_proof.py](scripts/one_click_sampling_proof.py)
- Chạy từ folder: `SignozCore`

## 2) Chuẩn bị

- Folder deploy:
  - `cd signoz/deploy/docker`
- Up stack docker (bao gồm signoz, signoz otel collector):
  - `docker-compose up -d`

Nếu collector custom, kiểm tra `.env`:
- [signoz/deploy/docker/.env](signoz/deploy/docker/.env)

## 3) Run test

Từ folder `SignozCore`:

- Theo số trace:
  - `python3 scripts/one_click_sampling_proof.py --trace-count 5000`

- Theo thời gian + tốc độ:
  - `python3 scripts/one_click_sampling_proof.py --duration-seconds 120 --rps 40`

## 4) Các tham số  có thể  thay đổi

- `--trace-count`: số trace cần test.
- `--duration-seconds`: nếu > 0 thì script tự tính `trace_count = duration_seconds * rps`.
- `--rps`: tốc độ phát trace.
- `--seed`: giữ deterministic để so sánh công bằng giữa baseline/model.
- `--model-config`: trỏ tới config model bạn vừa update.
  - Ví dụ: `--model-config signoz/deploy/docker/otel-collector-config.model.yaml`

## 5) Viec cần thực hiện khi update model sampling thì làm gì?

1. Sửa code/config model.
2. Build/restart collector theo flow hiện tại.
3. Chạy lại lệnh one-click (giữ seed và trace-count như cũ để so sánh công bằng).

## 6) Kết quả được ghi ở đâu

Sau mỗi lần chạy script sẽ tạo:

- Baseline snapshot:
  - `signoz/deploy/docker/otel-export/baseline2/<timestamp>-traces.json`
- Model snapshot:
  - `signoz/deploy/docker/otel-export/model1/<timestamp>-traces.model.json`
- Compare report:
  - `reports/compare/<timestamp>-...-traces.json_VS_...-traces.model.json.txt`
- Retention report theo `run_id`:
  - `reports/compare/<timestamp>-retention-<run_id>.txt`
- Proof tổng hợp:
  - `reports/sampler-proof/<timestamp>-oneclick-proof.txt`

## 7) Tổng hợp

### Step A — vào đúng folder
- `/KLTN/SignozCore`

### Step B — chạy test baseline vs model
- `python3 scripts/one_click_sampling_proof.py --trace-count 5000`

### Step C — mở kết quả
- `ls -lt reports/sampler-proof | head`
- `ls -lt reports/compare | head`

## 8) Ghi chú đánh giá

- Dùng file `retention-<run_id>.txt` để chứng minh kept/dropped theo trace_id.
- Dùng file compare để chứng minh chênh lệch aggregate (spans/traceIds/error spans).
- Nếu cần tăng độ tin cậy: chạy 3 lần với cùng tham số và so sánh xu hướng.
