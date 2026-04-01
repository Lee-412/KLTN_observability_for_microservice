#!/usr/bin/env bash
set -euo pipefail

# One-shot coverage-first paper-oriented pipeline:
# 1) adapt engine raw outputs into evaluator schema
# 2) evaluate A@1/A@3/MRR + CI95
# 3) export paper-format table row CSV

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <scenario_file> <engine_input_dir> [dataset_name] [tag]" >&2
  echo "Example: $0 reports/analysis/rca-benchmark/scenarios.train-ticket.paper-source.20260401.jsonl reports/compare/rca-engine-raw/train-ticket train-ticket 20260401" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SCENARIO_FILE="$1"
ENGINE_INPUT_DIR="$2"
DATASET_NAME="${3:-paper-dataset}"
TAG="${4:-$(date +%Y%m%d)}"
PROFILE="coverage-first"

OUT_SCENARIO="reports/analysis/rca-benchmark/scenarios.${DATASET_NAME}.${PROFILE}.engine-adapted.${TAG}.jsonl"
OUT_RANK_DIR="reports/compare/rca-engine-adapted-rankings/${DATASET_NAME}/${PROFILE}/${TAG}"
OUT_ADAPTER_SUMMARY="reports/compare/rca-engine-adapter-${DATASET_NAME}-${PROFILE}-${TAG}.json"
OUT_EVAL_SUMMARY="reports/compare/rca-benchmark-${DATASET_NAME}-${PROFILE}-${TAG}-summary.json"
OUT_EVAL_SCENARIOS="reports/compare/rca-benchmark-${DATASET_NAME}-${PROFILE}-${TAG}-scenarios.csv"
OUT_PAPER_TABLE="reports/compare/rca-paper-table-${PROFILE}-${TAG}.csv"

echo "== STEP 1: ADAPTER =="
python3 scripts/adapter_rca_engine_output.py \
  --scenario-file "$SCENARIO_FILE" \
  --engine-input-dir "$ENGINE_INPUT_DIR" \
  --out-ranking-dir "$OUT_RANK_DIR" \
  --out-scenario-file "$OUT_SCENARIO" \
  --out-summary "$OUT_ADAPTER_SUMMARY"

echo "== STEP 2: EVALUATOR =="
python3 scripts/evaluate_rca_ranking.py \
  --scenario-file "$OUT_SCENARIO" \
  --base-dir "$ROOT" \
  --out-summary "$OUT_EVAL_SUMMARY" \
  --out-scenarios "$OUT_EVAL_SCENARIOS" \
  --seed 20260401

echo "== STEP 3: PAPER TABLE ROW =="
python3 scripts/build_paper_table_from_summary.py \
  --summary "$OUT_EVAL_SUMMARY" \
  --dataset "$DATASET_NAME" \
  --profile "$PROFILE" \
  --out-csv "$OUT_PAPER_TABLE"

echo "DONE"
echo "adapter_summary=$OUT_ADAPTER_SUMMARY"
echo "eval_summary=$OUT_EVAL_SUMMARY"
echo "eval_scenarios=$OUT_EVAL_SCENARIOS"
echo "paper_table=$OUT_PAPER_TABLE"
