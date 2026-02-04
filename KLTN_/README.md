# KLTN_ — Hướng dẫn dựng SigNoz + thí nghiệm Tail Sampling (VI)

Tài liệu này ghi lại đầy đủ các bước mình đã làm để:
1) Clone repo, dựng SigNoz stack (Docker Compose) chạy được trên Linux.
2) Bật export OTLP ra JSON để so sánh offline.
3) Build và thay collector bằng bản custom (có thêm policy sampling kiểu "model").
4) Chạy thí nghiệm baseline vs model và so sánh kết quả.

> Repo trong workspace này:
> - `signoz/` (SigNoz + docker compose deploy)
> - `signoz-otel-collector/` (collector custom, code Go)

---

## 0) Yêu cầu môi trường

- OS: Linux (đã chạy trên Ubuntu 22.04)
- Docker Engine cài sẵn
- Docker Compose CLI kiểu `docker-compose` (trong quá trình làm đang dùng `docker-compose`, không dùng `docker compose` plugin)
- Git
- Python 3 (để chạy script so sánh)

Kiểm tra nhanh:

```bash
git --version

docker --version

docker-compose --version

python3 --version
```

---

## 1) Clone source

Tại thư mục workspace (ví dụ):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_

# SigNoz
git clone https://github.com/SigNoz/signoz.git

# SigNoz OpenTelemetry Collector (custom)
git clone https://github.com/SigNoz/signoz-otel-collector.git
```

---

## 2) Chạy SigNoz stack (base)

### 2.1) Up stack

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d

docker-compose ps
```

### 2.2) Check UI

- Mở: http://localhost:8080

### 2.3) Kiểm tra container quan trọng

Trong `docker-compose ps` cần thấy ít nhất:
- `signoz` (UI/backend)
- `clickhouse`
- `zookeeper-1`
- `signoz-otel-collector`
- `schema-migrator-sync` (thường exit sau khi sync xong)

---

## 3) Bật export OTLP ra file JSON để diff

Mục tiêu: traces/logs đi qua collector vẫn vào ClickHouse (để UI hoạt động), đồng thời ghi thêm ra file JSON trên host để so sánh.

### 3.1) Sửa collector config

File: `signoz/deploy/docker/otel-collector-config.yaml`

Đã thêm exporters:
- `file/traces`
- `file/logs`

Và thêm các exporters này vào pipelines tương ứng.

### 3.2) Mount thư mục export ra host

File: `signoz/deploy/docker/docker-compose.yaml`

Đã mount:
- `./otel-export:/var/signoz/otel-export`

Tạo thư mục export trên host:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export
```

### 3.3) Recreate collector để apply config

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate otel-collector

docker logs --since=2m signoz-otel-collector | tail -n 80
```

---

## 4) Generate dữ liệu traces (baseline)

Dùng HotROD generator có sẵn:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker/generator/hotrod

docker-compose up -d
sleep 45
docker-compose down
```

Kiểm tra file export (baseline):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
ls -lh otel-export/
```

Snapshot baseline (ví dụ folder `baseline2/` đã có sẵn trong workspace này):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export/baseline2

ts=$(date +%Y%m%d-%H%M%S)
cp -f otel-export/traces.json otel-export/baseline2/${ts}-traces.json
cp -f otel-export/logs.json   otel-export/baseline2/${ts}-logs.json
```

Ghi chú: HotROD thường chỉ sinh traces, nên logs.json có thể rỗng.

---

## 5) Build collector custom và thay vào stack

### 5.1) Code thay đổi nằm ở đâu?

Chi tiết xem file: `README.sampling.md`.

### 5.2) Build image collector custom

Trong quá trình làm, mình đã build image:
- `signoz/signoz-otel-collector:kltn-model`

(Phần build cụ thể phụ thuộc vào máy, docker builder, v.v. Nếu cần mình có thể chuẩn hoá lại thành 1 command/script.)

### 5.3) Cho compose dùng image custom

File: `signoz/deploy/docker/.env`

Đặt:
- `OTELCOL_TAG=kltn-model`

Và để schema migrator vẫn dùng tag upstream:
- `SCHEMA_MIGRATOR_TAG=v0.129.13`

### 5.4) Fix 2 lỗi runtime (đã xử lý trong workspace)

1) Compose ban đầu kéo nhầm `signoz/signoz-schema-migrator:kltn-model` (không tồn tại) vì dùng chung biến tag.
   - Giải pháp: tách biến tag migrator riêng.

2) Collector crash vì flag:
   - `--feature-gates=-pkg.translator.prometheus.NormalizeName`
   - Gate này đã stable nên không disable được.
   - Giải pháp: remove flag khỏi compose command.

### 5.5) Recreate collector

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate schema-migrator-sync otel-collector

docker-compose ps schema-migrator-sync otel-collector
```

Check image đang chạy:

```bash
docker-compose ps otel-collector
```

---

## 6) Bật model sampling trong config và chạy “after/model”

### 6.1) Bật processor tail sampling

File: `signoz/deploy/docker/otel-collector-config.yaml`

- Có `processors.signoz_tail_sampling` với policy `type: model`.
- `service.pipelines.traces.processors` chứa `signoz_tail_sampling` (đứng trước `batch`).

### 6.2) Đổi file output để không đè baseline

Mình đã chuyển output sang file mới:
- `otel-export/traces.model.json`
- `otel-export/logs.model.json`

### 6.3) Recreate collector + chạy HotROD 45s

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker

docker-compose up -d --force-recreate otel-collector

cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker/generator/hotrod

docker-compose up -d
sleep 45
docker-compose down

cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
ls -lh otel-export/traces.model.json otel-export/logs.model.json
```

Snapshot “after/model” (workspace hiện có `otel-export/model1/`):

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_/signoz/deploy/docker
mkdir -p otel-export/model1

ts=$(date +%Y%m%d-%H%M%S)
cp -f otel-export/traces.model.json otel-export/model1/${ts}-traces.model.json
cp -f otel-export/logs.model.json   otel-export/model1/${ts}-logs.model.json
```

---

## 7) So sánh baseline vs model bằng script

Chi tiết về compare + auto-save report: xem `README.compare.md`.

Script nằm ở: `scripts/compare_otlp_traces.py`

Ví dụ so sánh đúng snapshot đã tạo trong workspace:

```bash
cd /home/leeduc/Desktop/KLTN/KLTN_
python3 scripts/compare_otlp_traces.py \
  --base signoz/deploy/docker/otel-export/baseline2/20260202-225522-traces.json \
  --test signoz/deploy/docker/otel-export/model1/20260202-230513-traces.model.json \
  --details
```

---

## 8) Check trên UI

- Mở UI: http://localhost:8080
- Khi model sampling bật, số lượng traces ingest (và hiển thị) giảm đáng kể so với baseline.

---

## 9) Ghi chú / Known limitations

- `logs.json` / `logs.model.json` có thể rỗng vì HotROD không phát OTLP logs.
  Nếu mục tiêu bắt buộc phải so sánh logs, cần thêm log generator (OTLP logs) vào stack.
