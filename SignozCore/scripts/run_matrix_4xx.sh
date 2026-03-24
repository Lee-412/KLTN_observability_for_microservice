#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TS="$(date +%Y%m%d-%H%M%S)"
OUT="reports/compare/matrix-4xx-${TS}.csv"
LOG_DIR="reports/compare/matrix-4xx-logs-${TS}"
mkdir -p "$LOG_DIR"

echo "variant,report" > "$OUT"

TRACE_COUNT="${TRACE_COUNT:-10000}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-20}"
MAX_RUNS="${MAX_RUNS:-0}"
RESUME_FROM_CSV="${RESUME_FROM_CSV:-}"

CONFIGS=(
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor00_target50.yaml"
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target20.yaml"
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target35.yaml"
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target50.yaml"
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor10_target70.yaml"
  "signoz/deploy/docker/exp-matrix-4xx/otel-collector-config.floor20_target50.yaml"
)

for cfg in "${CONFIGS[@]}"; do
  variant="$(basename "$cfg" .yaml | sed 's/otel-collector-config.//')"
  log_file="${LOG_DIR}/${variant}.log"

  if [[ -n "$RESUME_FROM_CSV" ]] && [[ -f "$RESUME_FROM_CSV" ]] && grep -q "^${variant}," "$RESUME_FROM_CSV"; then
    echo "=== SKIP ${variant} (already in ${RESUME_FROM_CSV}) ==="
    continue
  fi

  echo "=== RUN ${variant} ==="
  python3 scripts/one_click_sampling_proof.py \
    --trace-count "$TRACE_COUNT" \
    --endpoint-profile mixed-4xx \
    --model-config "$cfg" | tee "$log_file"

  report_path="$(grep -E '^retention=' "$log_file" | tail -n1 | sed 's/^retention=//')"
  if [[ -z "$report_path" ]]; then
    echo "ERROR: missing report path for ${variant}" >&2
    exit 1
  fi

  echo "${variant},${report_path}" >> "$OUT"

  if [[ "$COOLDOWN_SECONDS" != "0" ]]; then
    sleep "$COOLDOWN_SECONDS"
  fi

  if [[ "$MAX_RUNS" != "0" ]]; then
    MAX_RUNS="$((MAX_RUNS - 1))"
    if [[ "$MAX_RUNS" -le 0 ]]; then
      break
    fi
  fi
done

echo "DONE: ${OUT}"
cat "$OUT"
