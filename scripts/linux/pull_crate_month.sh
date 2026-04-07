#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

source telemetry/helpers/sh/telemetry.sh

BASE_URL="https://torrepacheco-opendata.hopu.eu/api/datasources/proxy/1/_sql"
TABLE_NAME="mtairquality.etairqualityobserved"

CITY="${1:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
POLLUTANT="${2:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
MONTH="${3:?Usage: bash scripts/linux/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"

STEP_NAME="pull_crate_month"
PIPELINE_NAME="crate_ingestion_monthly"

RUN_ID="${RUN_ID:-$(telemetry_new_run_id)}"
TRACE_ID="${TRACE_ID:-$(telemetry_new_trace_id)}"
SPAN_ID="$(telemetry_new_span_id)"
PARENT_SPAN_ID="${PARENT_SPAN_ID:-}"
EMPTY_RESULT_EXIT_CODE="${EMPTY_RESULT_EXIT_CODE:-10}"

# Bronze ingestion stays minimally filtered on value thresholds.
# Negative and extreme measurements remain in Bronze so Silver QC can flag them explicitly.

IS_NESTED="0"
if [ -n "${PARENT_SPAN_ID}" ]; then
  IS_NESTED="1"
fi

case "${CITY}" in
  apba) SERVICEPATH="/apba" ;;
  torrepacheco) SERVICEPATH="/torrepacheco" ;;
  *)
    echo "Unsupported city: ${CITY}" >&2
    exit 1
    ;;
esac

case "${POLLUTANT}" in
  co|no2|o3|so2) ;;
  *)
    echo "Unsupported pollutant: ${POLLUTANT}" >&2
    exit 1
    ;;
esac

RAW_DIR="data/raw/crate/${CITY}/${POLLUTANT}"
BRONZE_DIR="data/bronze/crate/${CITY}/${POLLUTANT}"
RAW_FILE="${RAW_DIR}/${MONTH}.json"
OUTPUT_FILE="${BRONZE_DIR}/${MONTH}.csv"

TMP_JSON="$(mktemp)"
TMP_CSV="$(mktemp)"
TMP_BODY="$(mktemp)"

cleanup() {
  rm -f "${TMP_JSON}" "${TMP_CSV}" "${TMP_BODY}"
}

emit_run_start_if_root() {
  if [ "${IS_NESTED}" = "0" ]; then
    telemetry_emit_run_event \
      "run_start" \
      "${RUN_ID}" \
      "${TRACE_ID}" \
      "${PIPELINE_NAME}" \
      "running" \
      "pull start"
  fi
}

emit_run_end_if_root() {
  local status="$1"
  local message="$2"

  if [ "${IS_NESTED}" = "0" ]; then
    telemetry_emit_run_event \
      "run_end" \
      "${RUN_ID}" \
      "${TRACE_ID}" \
      "${PIPELINE_NAME}" \
      "${status}" \
      "${message}"
  fi
}

emit_span_start() {
  telemetry_emit_span_event \
    "span_start" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${PARENT_SPAN_ID}" \
    "${STEP_NAME}" \
    "running" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"
}

emit_span_end() {
  local status="$1"

  telemetry_emit_span_event \
    "span_end" \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${PARENT_SPAN_ID}" \
    "${STEP_NAME}" \
    "${status}" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"
}

emit_metric_events() {
  local rows_downloaded="$1"
  local raw_response_bytes="$2"
  local file_bytes_written="$3"

  telemetry_emit_metric_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "rows_downloaded_total" \
    "${rows_downloaded}" \
    "count" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"

  telemetry_emit_metric_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "raw_response_bytes" \
    "${raw_response_bytes}" \
    "bytes" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"

  telemetry_emit_metric_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "file_bytes_written" \
    "${file_bytes_written}" \
    "bytes" \
    "${CITY}" \
    "${POLLUTANT}" \
    "${MONTH}"
}

fail_with_error() {
  local rc="$1"
  local error_type="$2"
  local error_message="$3"
  local run_message="$4"

  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${STEP_NAME}" \
    "${error_type}" \
    "${error_message}"

  emit_span_end "failed"
  emit_run_end_if_root "failed" "${run_message}"

  trap - ERR
  cleanup
  exit "${rc}"
}

finish_empty_result_warning() {
  local raw_bytes="$1"

  emit_metric_events "0" "${raw_bytes}" "0"

  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${STEP_NAME}" \
    "empty_result" \
    "no rows returned from source"

  emit_span_end "warning"
  emit_run_end_if_root "warning" "empty result"

  trap - ERR
  cleanup
  echo "warning: empty result for city=${CITY} pollutant=${POLLUTANT} month=${MONTH}" >&2
  exit "${EMPTY_RESULT_EXIT_CODE}"
}

on_unexpected_error() {
  local rc="$1"
  fail_with_error \
    "${rc}" \
    "script_failed" \
    "pull failed with exit code ${rc}" \
    "pull failed"
}

trap 'rc=$?; on_unexpected_error "$rc"' ERR

mkdir -p "${RAW_DIR}" "${BRONZE_DIR}"

emit_run_start_if_root
emit_span_start

MONTH_START="${MONTH}-01 00:00:00"
NEXT_MONTH_START="$(python3 - "${MONTH}" <<'PY'
import sys
from datetime import datetime

month = datetime.strptime(sys.argv[1], "%Y-%m")
if month.month == 12:
    next_month = datetime(month.year + 1, 1, 1)
else:
    next_month = datetime(month.year, month.month + 1, 1)
print(next_month.strftime("%Y-%m-%d 00:00:00"))
PY
)"

SQL="SELECT
  time_index,
  dateobserved AS observed_at,
  '${CITY}' AS city,
  '${POLLUTANT}' AS pollutant,
  ${POLLUTANT} AS value,
  fiware_servicepath,
  refpointofinterest,
  name,
  operationalstatus,
  reliability,
  airqualityindex,
  airqualitylevel
FROM ${TABLE_NAME}
WHERE fiware_servicepath = '${SERVICEPATH}'
  AND time_index >= TIMESTAMP '${MONTH_START}'
  AND time_index < TIMESTAMP '${NEXT_MONTH_START}'
  AND ${POLLUTANT} IS NOT NULL
ORDER BY time_index"

python3 - "${SQL}" > "${TMP_BODY}" <<'PY'
import json
import sys
print(json.dumps({"stmt": sys.argv[1]}))
PY

MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-15}"
CURL_MAX_TIME="${CURL_MAX_TIME:-120}"

HTTP_OK="0"
attempt=1
max_attempts="${MAX_ATTEMPTS}"

while [ "${attempt}" -le "${max_attempts}" ]; do
  echo "info: curl attempt=${attempt} city=${CITY} pollutant=${POLLUTANT} month=${MONTH}" >&2

  set +e
  curl -fsSL \
    --connect-timeout "${CURL_CONNECT_TIMEOUT}" \
    --max-time "${CURL_MAX_TIME}" \
    -H 'Content-Type: application/json' \
    -X POST \
    --data @"${TMP_BODY}" \
    "${BASE_URL}" \
    -o "${TMP_JSON}"
  curl_rc="$?"
  set -e

  if [ "${curl_rc}" -eq 0 ]; then
    HTTP_OK="1"
    echo "info: curl success on attempt=${attempt} city=${CITY} pollutant=${POLLUTANT} month=${MONTH}" >&2
    cp "${TMP_JSON}" "${RAW_FILE}"
    break
  fi

  telemetry_emit_error_event \
    "${RUN_ID}" \
    "${TRACE_ID}" \
    "${SPAN_ID}" \
    "${STEP_NAME}" \
    "source_retryable_failure" \
    "curl failed attempt=${attempt} rc=${curl_rc} city=${CITY} pollutant=${POLLUTANT} month=${MONTH}"

  if [ "${attempt}" -lt "${max_attempts}" ]; then
    case "${attempt}" in
      1) sleep 5 ;;
      2) sleep 15 ;;
      *) sleep 30 ;;
    esac
  fi

  attempt="$((attempt + 1))"
done

if [ "${HTTP_OK}" != "1" ]; then
  fail_with_error \
    "1" \
    "source_timeout_or_http_failure" \
    "curl failed after ${max_attempts} attempts" \
    "pull failed after retries"
fi

  
ROW_COUNT="$(python3 - "${TMP_JSON}" "${TMP_CSV}" <<'PY'
import csv
import json
import sys
from pathlib import Path

payload_path = Path(sys.argv[1])
csv_path = Path(sys.argv[2])

payload = json.loads(payload_path.read_text())
rows = payload.get("rows", [])
cols = payload.get("cols", [])
header = [c[0] if isinstance(c, list) and c else str(c) for c in cols]

with csv_path.open("w", newline="") as fh:
    writer = csv.writer(fh)
    if header:
        writer.writerow(header)
    writer.writerows(rows)

print(len(rows))
PY
)"

RAW_BYTES="$(wc -c < "${RAW_FILE}" | tr -d ' ')"

if [ "${ROW_COUNT}" = "0" ]; then
  finish_empty_result_warning "${RAW_BYTES}"
fi

mv "${TMP_CSV}" "${OUTPUT_FILE}"

CSV_BYTES="$(wc -c < "${OUTPUT_FILE}" | tr -d ' ')"

emit_metric_events "${ROW_COUNT}" "${RAW_BYTES}" "${CSV_BYTES}"
emit_span_end "success"
emit_run_end_if_root "success" "pull success"

trap - ERR
cleanup

echo "Wrote ${OUTPUT_FILE}"