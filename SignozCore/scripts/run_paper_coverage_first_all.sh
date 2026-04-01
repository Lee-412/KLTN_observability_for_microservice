#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TAG="${1:-$(date +%Y%m%d)}"

run_one() {
  local scenario_file="$1"
  local engine_dir="$2"
  local dataset="$3"

  echo "=== ${dataset} ==="
  ./scripts/run_paper_coverage_first_pipeline.sh "$scenario_file" "$engine_dir" "$dataset" "$TAG"
}

run_one \
  "reports/analysis/rca-benchmark/scenarios.train-ticket.paper-source.20260401.jsonl" \
  "reports/compare/rca-engine-raw/train-ticket" \
  "train-ticket"

run_one \
  "reports/analysis/rca-benchmark/scenarios.hipster-batch1.paper-source.20260401.jsonl" \
  "reports/compare/rca-engine-raw/hipster-batch1" \
  "hipster-batch1"

run_one \
  "reports/analysis/rca-benchmark/scenarios.hipster-batch2.paper-source.20260401.jsonl" \
  "reports/compare/rca-engine-raw/hipster-batch2" \
  "hipster-batch2"

echo "ALL DONE"
