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

CITIES=("apba" "torrepacheco")
POLLUTANTS=("co" "no2" "o3" "so2")

START_MONTH="2023-07"
END_MONTH="2025-09"

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

month_cursor="${START_MONTH}"
batch_failed="0"
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

while true; do
  for city in "${CITIES[@]}"; do
    for pollutant in "${POLLUTANTS[@]}"; do
      echo "city=${city} pollutant=${pollutant} month=${month_cursor}"

      if ! RUN_ID="${RUN_ID}" TRACE_ID="${TRACE_ID}" \
        bash scripts/linux/pull_crate_month.sh "${city}" "${pollutant}" "${month_cursor}"; then
        batch_failed="1"
        failure_message="child pull failed: city=${city} pollutant=${pollutant} month=${month_cursor}"

        telemetry_emit_error_event \
          "${RUN_ID}" \
          "${TRACE_ID}" \
          "${BATCH_SPAN_ID}" \
          "${STEP_NAME}" \
          "child_pull_failed" \
          "${failure_message}"

        break 3
      fi
    done
  done

  if [ "${month_cursor}" = "${END_MONTH}" ]; then
    break
  fi

  month_cursor="$(next_month "${month_cursor}")"
done

if [ "${batch_failed}" = "1" ]; then
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
    "${failure_message}"

  exit 1
fi

telemetry_emit_span_event \
  "span_end" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${BATCH_SPAN_ID}" \
  "" \
  "${STEP_NAME}" \
  "success" \
  "" \
  "" \
  ""

telemetry_emit_run_event \
  "run_end" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${PIPELINE_NAME}" \
  "success" \
  "batch pull success"

echo "batch pull completed: run_id=${RUN_ID} trace_id=${TRACE_ID}"
