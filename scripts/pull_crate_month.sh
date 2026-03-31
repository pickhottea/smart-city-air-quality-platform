#!/usr/bin/env bash

set -euo pipefail

BASE_URL="https://torrepacheco-opendata.hopu.eu/api/datasources/proxy/1/_sql"

CITY="${1:?Usage: bash scripts/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
POLLUTANT="${2:?Usage: bash scripts/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"
MONTH="${3:?Usage: bash scripts/pull_crate_month.sh <apba|torrepacheco> <co|no2|o3|so2> <YYYY-MM>}"

case "$CITY" in
  apba) SERVICEPATH="/apba" ;;
  torrepacheco) SERVICEPATH="/torrepacheco" ;;
  *) echo "Unsupported city"; exit 1 ;;
esac

case "$POLLUTANT" in
  co|no2|o3|so2) ;;
  *) echo "Unsupported pollutant"; exit 1 ;;
esac

START_TS=$(date -d "${MONTH}-01 00:00:00 UTC" +%s000)
END_TS=$(date -d "${MONTH}-01 +1 month 00:00:00 UTC" +%s000)

RAW_DIR="data/raw/crate/${CITY}/${POLLUTANT}"
BRONZE_DIR="data/bronze/crate/${CITY}/${POLLUTANT}"
TMP_DIR="tmp"

mkdir -p "$RAW_DIR" "$BRONZE_DIR" "$TMP_DIR"

RAW_FILE="${RAW_DIR}/${MONTH}.json"
CSV_FILE="${BRONZE_DIR}/${MONTH}.csv"
BODY_FILE="${TMP_DIR}/${CITY}_${POLLUTANT}_${MONTH}.tmp"

echo "city=$CITY pollutant=$POLLUTANT month=$MONTH"

SQL="SELECT time_index, ${POLLUTANT} AS value FROM mtairquality.etairqualityobserved WHERE fiware_servicepath = '${SERVICEPATH}' AND time_index >= ${START_TS} AND time_index < ${END_TS} AND ${POLLUTANT} IS NOT NULL ORDER BY time_index ASC"

PAYLOAD=$(jq -nc --arg stmt "$SQL" '{stmt:$stmt}')

HTTP_CODE=$(curl -sS -o "$BODY_FILE" -w "%{http_code}" \
  -X POST "$BASE_URL" \
  -H 'content-type: application/json' \
  --data-raw "$PAYLOAD")

cp "$BODY_FILE" "$RAW_FILE"

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "HTTP error $HTTP_CODE"
  sed -n '1,10p' "$RAW_FILE"
  exit 1
fi

ROWCOUNT=$(jq -r '.rowcount // 0' "$RAW_FILE")

{
  echo "time_index,value"
  if [[ "$ROWCOUNT" != "0" ]]; then
    jq -r '.rows[] | [.[0], .[1]] | @csv' "$RAW_FILE"
  fi
} > "$CSV_FILE"

echo "rows=$ROWCOUNT → $CSV_FILE"
