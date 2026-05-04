# Hướng dẫn sử dụng MESC v3.94.2

## 1. Giới thiệu bài toán

Trong hệ thống microservice, lượng trace được sinh ra liên tục và có thể rất lớn. Khi ngân sách lưu trữ hoặc xử lý trace bị giới hạn, hệ thống không thể giữ lại toàn bộ trace gốc. Tuy nhiên, tập trace sau lấy mẫu vẫn cần bảo toàn đủ tín hiệu để phục vụ bài toán Root Cause Analysis (RCA), tức xác định dịch vụ hoặc thành phần gây ra sự cố.

MESC v3.94.2 được xây dựng cho bối cảnh này: giảm số lượng trace cần giữ lại theo một ngân sách định trước, nhưng vẫn ưu tiên các trace có giá trị cao cho bước RCA phía sau.

## 2. Tóm tắt phương pháp

Phiên bản khuyến nghị hiện tại là:

```text
stream-v3.94.2-metric-native-signed-contrast
```

Phương pháp kết hợp hai nhóm tín hiệu chính:

- **Tín hiệu trace**: thời điểm trace, độ trễ, số span, lỗi, tập dịch vụ xuất hiện trong trace, mức độ liên quan đến kịch bản sự cố.
- **Tín hiệu metric**: CPU, memory, latency, workload, success rate và các tín hiệu metric được ánh xạ từ pod/service về trace.

Ở mức tổng quát, sampler thực hiện các bước sau:

1. Tiền xử lý trace và metric theo từng dataset.
2. Xây dựng ngữ cảnh metric cho từng trace.
3. Chạy nhiều member sampler với các seed khác nhau.
4. Tổng hợp lựa chọn bằng cơ chế consensus voting.
5. Xếp hạng trace theo điểm RCA-aware, metric-aware và contrast-aware.
6. Áp dụng ràng buộc ngân sách và guard cân bằng normal/error trace.
7. Ghi tập trace được giữ lại để chạy MicroRank/RCA ở phase 2.

Các file chính:

```text
scripts/MESC/run_paper_sampled_rca_v9_contrast.py
scripts/MESC/streamv3942_metric_native_signed_contrast.py
```

## 3. Yêu cầu môi trường

### 3.1. Yêu cầu tối thiểu

- Python 3.10 trở lên, khuyến nghị Python 3.10 hoặc 3.11.
- `pip` để cài package Python.
- `numpy`.
- Shell có thể chạy script Bash cho phase 2.
- Dữ liệu TraStrainer được đặt đúng theo cây thư mục cấu hình trong repo.

Trên Windows, nên có thêm:

- PowerShell để chạy lệnh Python.
- Git Bash hoặc môi trường Bash tương đương để chạy các script `.sh` trong phase 2.

### 3.2. Cấu trúc dữ liệu cần có

Runner mặc định đọc cấu hình dataset tại:

```text
scripts/configs/datasets.trastrainer.json
```

Các dataset chính thường gồm:

```text
train-ticket
hipster-batch1
hipster-batch2
```

Mỗi dataset cần có tối thiểu:

- thư mục trace CSV;
- file `label.json`;
- thư mục metric CSV nếu chạy nhánh metric-native;
- file scenario gốc hoặc đủ dữ liệu label để runner tự bootstrap scenario.

Nếu thiếu base scenario file, runner có thể tự tạo từ `label.json` bằng các tham số `--before-sec` và `--after-sec`.

## 4. Cài đặt

Giả sử đang đứng tại thư mục `src` của repo.

### 4.1. Tạo virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

Trên Linux/macOS hoặc Git Bash:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 4.2. Cài dependency tối thiểu

```powershell
pip install numpy
```

### 4.3. Kiểm tra dependency

```powershell
python -c "import numpy; print(numpy.__version__)"
```

Nếu dùng đúng Python trong virtual environment trên Windows:

```powershell
.\.venv\Scripts\python.exe -c "import numpy; print(numpy.__version__)"
```

## 5. Chạy sampler

Các lệnh dưới đây giả định bạn đang đứng trong thư mục `src` và dùng `--root .`.

### 5.1. Chạy nhanh sampler-only

Lệnh này chỉ chạy phase sampling và ghi timing CSV. RCA phase 2 chưa được chạy.

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-sanity `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --datasets hipster-batch1 `
  --budgets 0.1 `
  --before-sec 300 `
  --after-sec 600 `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-only `
  --sampling-timing-csv .\reports\compare\sampler-timing-v3942-sanity.csv
```

Mục đích:

- kiểm tra sampler có chạy được hay không;
- kiểm tra đường dẫn dataset;
- đo thời gian sampling;
- xác nhận số trace được giữ lại theo ngân sách.

### 5.2. Chạy full pipeline cho một dataset

Ví dụ chạy đầy đủ sampling và RCA cho `train-ticket` ở budget `1.0%`:

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-train-ticket-1pct `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --datasets train-ticket `
  --budgets 1.0 `
  --budget-mode strict `
  --v3942-min-consensus 2
```

### 5.3. Chạy full pipeline cho ba dataset và ba budget

Nếu không truyền `--datasets` và `--budgets`, runner dùng mặc định:

```text
--datasets train-ticket,hipster-batch1,hipster-batch2
--budgets 0.1,1.0,2.5
```

Lệnh chạy đầy đủ:

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-full `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --sampling-timing-csv .\reports\compare\sampler-timing-v3942-full.csv
```

### 5.4. Chạy phase 2 song song

Nếu máy có đủ tài nguyên, có thể tăng số worker cho phase 2:

```powershell
python .\scripts\MESC\run_paper_sampled_rca_v9_contrast.py `
  --root . `
  --tag v3942-full-workers `
  --selection-mode stream-v3.94.2-metric-native-signed-contrast `
  --budget-mode strict `
  --v3942-min-consensus 2 `
  --phase2-workers 3
```

## 6. Các tham số quan trọng

| Tham số | Ý nghĩa |
|---|---|
| `--root` | Đường dẫn tới thư mục gốc chứa `scripts`, `data`, `reports`. Nếu chạy trong `src`, dùng `--root .`. |
| `--tag` | Nhãn cho lần chạy. Runner sẽ tự gắn thêm timestamp để tránh ghi đè output. |
| `--selection-mode` | Chọn thuật toán sampling. Với MESC v3.94.2, dùng `stream-v3.94.2-metric-native-signed-contrast`. |
| `--datasets` | Danh sách dataset, phân tách bằng dấu phẩy. |
| `--budgets` | Danh sách budget theo phần trăm, ví dụ `0.1,1.0,2.5`. |
| `--before-sec` | Số giây lấy trước thời điểm sự cố để tạo scenario window. |
| `--after-sec` | Số giây lấy sau thời điểm sự cố để tạo scenario window. |
| `--budget-mode strict` | Ép số trace giữ lại theo đúng ngân sách trace-count. |
| `--v3942-min-consensus` | Số phiếu tối thiểu để một trace vào consensus pool. Giá trị mặc định khuyến nghị là `2`. |
| `--v3942-enable-inner-real-metrics` | Bật nhánh metric distress cũ bên trong sampler. Mặc định tắt. |
| `--v3942-enable-swap-refine` | Bật bước refine bằng swap có giới hạn. Mặc định tắt. |
| `--v3942-min-normal-ratio` | Ghi đè floor tối thiểu cho normal trace. |
| `--v3942-min-error-ratio` | Ghi đè floor tối thiểu cho error trace. |
| `--v3942-signed-metric-gain` | Ghi đè độ mạnh của signed metric modulation. |
| `--sampling-only` | Chỉ chạy sampling, không materialize đầy đủ và không chạy RCA phase 2. |
| `--sampling-timing-csv` | Đường dẫn file CSV ghi timing sampler. |
| `--phase2-workers` | Số worker chạy phase 2 song song. |
| `--dataset-config` | File cấu hình dataset. Mặc định là `scripts/configs/datasets.trastrainer.json`. |

## 7. Kiểm tra kết quả

### 7.1. Sau khi chạy sampler-only

Kiểm tra log console, đặc biệt dòng:

```text
[SAMPLER-TIMER]
```

Dòng này cho biết:

- dataset;
- budget;
- tổng số trace;
- số trace được giữ lại;
- số trace mục tiêu;
- thời gian precompute metric;
- thời gian sampler core;
- thời gian strict cap;
- thời gian chọn trace trên mỗi trace.

Kiểm tra file timing CSV, ví dụ:

```text
reports/compare/sampler-timing-v3942-sanity.csv
```

Các cột nên kiểm tra trước:

```text
dataset
budget_pct
trace_count
kept_count
kept_pct
target_trace_count
seed_count
min_consensus
sampler_core_ms_per_trace
sampler_select_total_ms_per_trace
select_plus_precompute_amortized_ms_per_trace
```

### 7.2. Sau khi chạy full pipeline

Runner sẽ in ra các output chính dạng:

```text
sampler_timing_csv=...
benchmark_csv=...
compare_csv=...
benchmark_md=...
table3_csv=...
table3_md=...
quality_csv=...
```

Các output thường nằm trong:

```text
reports/compare/
reports/analysis/paper-sampled-traces/
reports/analysis/rca-benchmark/
```

Các file quan trọng:

| Output | Vai trò |
|---|---|
| `sampler-timing-*.csv` | Thống kê thời gian sampling. |
| `rca-paper-table-sampled-budgets-*.csv` | Kết quả RCA theo từng dataset và budget. |
| `rca-paper-vs-ours-microrank-budgets-*.csv` | So sánh trung bình với baseline paper theo từng budget. |
| `rca-dataset-budget-metrics-*.md` | Báo cáo markdown dễ đọc cho từng dataset/budget. |
| `table3-all-models-plus-ours-*.csv` | Bảng so sánh kiểu Table 3. |
| `sampling-quality-key-metrics-*.csv` | Chỉ số chất lượng sampling và coverage. |

### 7.3. Chỉ số RCA cần kiểm tra

Trong summary hoặc markdown, kiểm tra trước các chỉ số:

```text
scenario_count
A@1
A@3
MRR
```

Ý nghĩa:

- `A@1`: tỷ lệ scenario mà root cause đứng hạng 1.
- `A@3`: tỷ lệ scenario mà root cause nằm trong top 3.
- `MRR`: Mean Reciprocal Rank, phản ánh vị trí trung bình của root cause trong ranking.

Nếu phase 2 chạy đúng, adapter summary nên có:

```text
parse_fail_count = 0
ok_count = scenario_count
```

Với `--budget-mode strict`, nếu có `parse_fail_count > 0`, runner sẽ đánh dấu benchmark không hợp lệ và dừng sinh báo cáo tổng hợp.

## 8. Quy trình chạy khuyến nghị

Với người mới chạy MESC v3.94.2, nên dùng thứ tự sau:

1. Chạy `sampling-only` trên một dataset nhỏ, ví dụ `hipster-batch1` với budget `0.1%`.
2. Kiểm tra `sampler_timing_csv`, `kept_count`, `target_trace_count` và log `[SAMPLER-TIMER]`.
3. Chạy full pipeline cho một dataset ở budget `1.0%`.
4. Kiểm tra `scenario_count`, `A@1`, `A@3`, `MRR`, `parse_fail_count` và `ok_count`.
5. Sau khi mọi thứ ổn định, chạy full ba dataset với ba budget `0.1,1.0,2.5`.

## 9. Lỗi thường gặp

### 9.1. Thiếu `numpy`

Hiện tượng:

```text
ModuleNotFoundError: No module named 'numpy'
```

Cách xử lý:

```powershell
pip install numpy
```

Nếu dùng virtual environment, cần chắc chắn đang chạy đúng Python:

```powershell
.\.venv\Scripts\python.exe -m pip install numpy
```

### 9.2. Sai `--root`

Nếu chạy trong thư mục `src`, dùng:

```text
--root .
```

Nếu chạy từ root repo và dữ liệu/script nằm trong `src`, dùng:

```text
--root .\src
```

### 9.3. Không chạy được phase 2 trên Windows

Phase 2 cần Bash để chạy script `.sh`. Cài Git Bash và đảm bảo lệnh `bash` có trong `PATH`.

Kiểm tra:

```powershell
bash --version
```

### 9.4. Không tìm thấy dataset

Kiểm tra file:

```text
scripts/configs/datasets.trastrainer.json
```

và xác nhận các đường dẫn `trace_dir`, `label_file`, `base_scenario_file`, `baseline_summary_file` tương ứng tồn tại dưới `--root`.

## 10. Ghi chú thực nghiệm

- Nên dùng `--budget-mode strict` khi cần báo cáo kết quả theo đúng ngân sách trace-count.
- Nên lưu `--sampling-timing-csv` cho mỗi lần chạy để phục vụ phân tích latency.
- Không nên bật đồng thời nhiều tùy chọn refine nếu chưa có ablation cụ thể.
- Với kết quả báo cáo chính, nên giữ `--v3942-min-consensus 2` để thống nhất với cấu hình khuyến nghị.
