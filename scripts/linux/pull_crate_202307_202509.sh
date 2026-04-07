#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

source telemetry/helpers/sh/telemetry.sh

PIPELINE_NAME="crate_ingestion_backfill"
STEP_NAME="pull_crate_202307_202509"

RUN_ID="${RUN_ID:-$(telemetry_new_run_id)}"
TRACE_ID="${TRACE_ID:-$(telemetry_new_trace_id)}"
BATCH_SPAN_ID="$(telemetry_new_span_id)"
EMPTY_RESULT_EXIT_CODE="${EMPTY_RESULT_EXIT_CODE:-10}"

CITIES=("apba" "torrepacheco")
POLLUTANTS=("co" "no2" "o3" "so2")

START_MONTH="${START_MONTH:-2023-07}"
END_MONTH="${END_MONTH:-2025-09}"

month_cursor="${START_MONTH}"
batch_warning="0"
warning_count="0"
failure_message=""

next_month() {
  python3 - "$1" <<'PY'
import sys
from datetime import datetime

month = sys.argv[1]
dt = datetime.strptime(month, "%Y-%m")
year = dt.year + (dt.month // 12)
month_num = 1 if dt.month == 12 else dt.month + 1
print(f"{year:04d}-{month_num:02d}")
PY
}

finish_batch() {
  local status="$1"
  local message="$2"
  local exit_code="$3"

  telemetry_emit_metric_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${BATCH_SPAN_ID}" \
    "empty_result_warnings_total" \
    "${warning_count}" \
    "count" \
    "" \
    "" \
    ""

  telemetry_emit_span_event \
    "span_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${BATCH_SPAN_ID}" \
    "" \
    "${STEP_NAME}" \
    "${status}" \
    "" \
    "" \
    ""

  telemetry_emit_run_event \
    "run_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${PIPELINE_NAME}" \
    "${status}" \
    "${message}"

  trap - ERR

  if [ "${warning_count}" -gt 0 ]; then
    echo "batch pull completed: run_id=${RUN_ID} trace_id=${TRACE_ID} status=${status} warning_count=${warning_count}"
  else
    echo "batch pull completed: run_id=${RUN_ID} trace_id=${TRACE_ID} status=${status}"
  fi

  exit "${exit_code}"
}

fail_batch() {
  local rc="$1"
  local error_type="$2"
  local error_message="$3"

  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${BATCH_SPAN_ID}" \
    "${STEP_NAME}" \
    "${error_type}" \
    "${error_message}"

  finish_batch "failed" "${error_message}" "${rc}"
}

on_batch_error() {
  local rc="$1"
  local message="${failure_message:-batch runner failed with exit code ${rc}}"

  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${BATCH_SPAN_ID}" \
    "${STEP_NAME}" \
    "batch_failed" \
    "${message}"

  telemetry_emit_span_event \
    "span_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${BATCH_SPAN_ID}" \
    "" \
    "${STEP_NAME}" \
    "failed" \
    "" \
    "" \
    ""

  telemetry_emit_run_event \
    "run_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${PIPELINE_NAME}" \
    "failed" \
    "${message}"

  trap - ERR
  exit "${rc}"
}

trap 'rc=$?; on_batch_error "$rc"' ERR

telemetry_emit_run_event \
  "run_start" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${PIPELINE_NAME}" \
  "running" \
  "batch pull start"

telemetry_emit_span_event \
  "span_start" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${BATCH_SPAN_ID}" \
  "" \
  "${STEP_NAME}" \
  "running" \
  "" \
  "" \
  ""

while true; do
  for city in "${CITIES[@]}"; do
    for pollutant in "${POLLUTANTS[@]}"; do
      echo "city=${city} pollutant=${pollutant} month=${month_cursor}"

      child_rc=0
      RUN_ID="${RUN_ID}" \
      TRACE_ID="${TRACE_ID}" \
      PARENT_SPAN_ID="${BATCH_SPAN_ID}" \
      EMPTY_RESULT_EXIT_CODE="${EMPTY_RESULT_EXIT_CODE}" \
      bash scripts/linux/pull_crate_month.sh "${city}" "${pollutant}" "${month_cursor}" || child_rc=$?

      if [ "${child_rc}" -eq 0 ]; then
        continue
      fi

      if [ "${child_rc}" -eq "${EMPTY_RESULT_EXIT_CODE}" ]; then
        batch_warning="1"
        warning_count="$((warning_count + 1))"
        echo "warning: empty result for city=${city} pollutant=${pollutant} month=${month_cursor}" >&2
        continue
      fi

      failure_message="child pull failed: city=${city} pollutant=${pollutant} month=${month_cursor}"
      fail_batch "1" "child_pull_failed" "${failure_message}"
    done
  done

  if [ "${month_cursor}" = "${END_MONTH}" ]; then
    break
  fi

  month_cursor="$(next_month "${month_cursor}")"
done

if [ "${batch_warning}" = "1" ]; then
  finish_batch \
    "warning" \
    "batch pull completed with ${warning_count} empty_result warning(s)" \
    "0"
fi

finish_batch "success" "batch pull success" "0"