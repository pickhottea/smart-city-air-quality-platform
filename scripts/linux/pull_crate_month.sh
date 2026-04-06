#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

source telemetry/helpers/sh/telemetry.sh

BASE_URL="https://torrepacheco-opendata.hopu.eu/api/datasources/proxy/1/_sql"

CITY="${1:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
POLLUTANT="${2:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
MONTH="${3:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"

case "$CITY" in
  apba) SERVICEPATH="/apba" ;;
  torrepacheco) SERVICEPATH="/torrepacheco" ;;
  *) echo "Unsupported city: $CITY" >&2; exit 1 ;;
esac

case "$POLLUTANT" in
  co|no2|o3|so2) ;;
  *) echo "Unsupported pollutant: $POLLUTANT" >&2; exit 1 ;;
esac

OUTPUT_DIR="data/bronze/crate/${CITY}/${POLLUTANT}"
OUTPUT_FILE="${OUTPUT_DIR}/${MONTH}.csv"
TMP_FILE="$(mktemp)"
STEP_NAME="pull_crate_month"
PIPELINE_NAME="crate_ingestion_monthly"

RUN_ID="${RUN_ID:-$(telemetry_new_run_id)}"
TRACE_ID="${TRACE_ID:-$(telemetry_new_trace_id)}"
SPAN_ID="$(telemetry_new_span_id)"

cleanup() {
  rm -f "${TMP_FILE}"
}

on_error() {
  local rc="$1"
  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${STEP_NAME}" \
    "script_failed" \
    "pull failed with exit code ${rc}"

  telemetry_emit_span_event \
    "span_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "" \
    "${STEP_NAME}" \
    "failed" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"

  telemetry_emit_run_event \
    "run_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${PIPELINE_NAME}" \
    "failed" \
    "pull failed"

  cleanup
  exit "${rc}"
}

trap 'rc=$?; on_error "$rc"' ERR

mkdir -p "${OUTPUT_DIR}"

telemetry_emit_run_event \
  "run_start" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${PIPELINE_NAME}" \
  "running" \
  "pull start"

telemetry_emit_span_event \
  "span_start" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${SPAN_ID}" \
  "" \
  "${STEP_NAME}" \
  "running" \
  "${CITY}" \
  "${POLLUTANT}" \
  "${MONTH}"

# --------------------------------------------------
# 把你原本的下載邏輯放在這裡
# 目標：把結果寫到 ${TMP_FILE}
# --------------------------------------------------

YEAR_MONTH_START="${MONTH}-01"

SQL="SELECT * FROM \"${SERVICEPATH}/${POLLUTANT}\" WHERE date_trunc('month', observed_at) = date_trunc('month', timestamp '${YEAR_MONTH_START}')"
ENCODED_SQL="$(python3 - <<PY
import urllib.parse
print(urllib.parse.quote("""${SQL}"""))
PY
)"

curl -fsSL "${BASE_URL}?sql=${ENCODED_SQL}" -o "${TMP_FILE}"

# --------------------------------------------------
# 下載後統計
# --------------------------------------------------

FILE_BYTES="$(wc -c < "${TMP_FILE}" | tr -d ' ')"

if [ -s "${TMP_FILE}" ]; then
  ROW_COUNT="$(awk 'NR>1' "${TMP_FILE}" | wc -l | tr -d ' ')"
else
  ROW_COUNT="0"
fi

mv "${TMP_FILE}" "${OUTPUT_FILE}"

telemetry_emit_metric_event \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${SPAN_ID}" \
  "rows_downloaded_total" \
  "${ROW_COUNT}" \
  "count" \
  "${CITY}" \
  "${POLLUTANT}" \
  "${MONTH}"

telemetry_emit_metric_event \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${SPAN_ID}" \
  "file_bytes_written" \
  "${FILE_BYTES}" \
  "bytes" \
  "${CITY}" \
  "${POLLUTANT}" \
  "${MONTH}"

telemetry_emit_span_event \
  "span_end" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${SPAN_ID}" \
  "" \
  "${STEP_NAME}" \
  "success" \
  "${CITY}" \
  "${POLLUTANT}" \
  "${MONTH}"

telemetry_emit_run_event \
  "run_end" \
  "${RUN_ID}" \
  "${TRACE_ID}" \
  "${PIPELINE_NAME}" \
  "success" \
  "pull success"

trap - ERR
cleanup

echo "Wrote ${OUTPUT_FILE}"
