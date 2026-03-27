#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/leeduc/Desktop/KLTN/SignozCore"
MATRIX="$ROOT/reports/compare/20260327-run-matrix.csv"
LOG_DIR="$ROOT/reports/compare/20260327-runlogs"
mkdir -p "$LOG_DIR"

# Usage:
#   bash scripts/run_matrix_20260327.sh            # run all rows
#   bash scripts/run_matrix_20260327.sh 1 9        # run rows 1..9 only
#   bash scripts/run_matrix_20260327.sh 10 18      # run cost-first block
START_ROW="${1:-1}"
END_ROW="${2:-999}"

while IFS=, read -r run_order run_id profile model_config seed trace_count endpoint_profile notes; do
  if [[ "$run_order" == "run_order" ]]; then
    continue
  fi

  if (( run_order < START_ROW || run_order > END_ROW )); then
    continue
  fi

  ts="$(date +%Y%m%d-%H%M%S)"
  log_file="$LOG_DIR/${ts}-${run_order}-${profile}.log"

  echo "[INFO] Running row=$run_order run_id=$run_id profile=$profile seed=$seed trace_count=$trace_count"
  (
    cd "$ROOT"
    python3 scripts/one_click_sampling_proof.py \
      --trace-count "$trace_count" \
      --seed "$seed" \
      --run-id "$run_id" \
      --endpoint-profile "$endpoint_profile" \
      --baseline-config signoz/deploy/docker/otel-collector-config.baseline.yaml \
      --model-config "$model_config"
  ) | tee "$log_file"

done < "$MATRIX"

echo "[DONE] Matrix execution finished for selected rows. Logs: $LOG_DIR"
