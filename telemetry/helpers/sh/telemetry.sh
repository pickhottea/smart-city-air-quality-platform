#!/usr/bin/env bash
set -euo pipefail

TELEMETRY_ROOT="${TELEMETRY_ROOT:-data/telemetry}"
RUNTIME_DIR="${TELEMETRY_ROOT}/runtime"

RUN_EVENTS_FILE="${RUNTIME_DIR}/run_events.jsonl"
SPAN_EVENTS_FILE="${RUNTIME_DIR}/span_events.jsonl"
METRIC_EVENTS_FILE="${RUNTIME_DIR}/metric_events.jsonl"
ERROR_EVENTS_FILE="${RUNTIME_DIR}/error_events.jsonl"

mkdir -p "${RUNTIME_DIR}"

telemetry_now_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

telemetry_hostname() {
  hostname 2>/dev/null || echo "unknown-host"
}

telemetry_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr 'A-Z' 'a-z'
  else
    printf '%s-%s\n' "$(date +%s)" "$RANDOM"
  fi
}

telemetry_json_escape() {
  local s="${1:-}"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

telemetry_append_jsonl() {
  local file="$1"
  local line="$2"
  mkdir -p "$(dirname "$file")"
  printf '%s\n' "$line" >> "$file"
}

telemetry_emit_run_event() {
  local event_type="$1"      # run_start | run_end
  local run_id="$2"
  local trace_id="$3"
  local pipeline_name="$4"
  local status="${5:-}"
  local message="${6:-}"

  local ts host
  ts="$(telemetry_now_utc)"
  host="$(telemetry_hostname)"

  telemetry_append_jsonl "${RUN_EVENTS_FILE}" \
    "{\"event_ts\":\"$(telemetry_json_escape "$ts")\",\"event_type\":\"$(telemetry_json_escape "$event_type")\",\"run_id\":\"$(telemetry_json_escape "$run_id")\",\"trace_id\":\"$(telemetry_json_escape "$trace_id")\",\"pipeline_name\":\"$(telemetry_json_escape "$pipeline_name")\",\"status\":\"$(telemetry_json_escape "$status")\",\"message\":\"$(telemetry_json_escape "$message")\",\"host\":\"$(telemetry_json_escape "$host")\"}"
}

telemetry_emit_span_event() {
  local event_type="$1"      # span_start | span_end
  local run_id="$2"
  local trace_id="$3"
  local span_id="$4"
  local parent_span_id="$5"
  local step_name="$6"
  local status="${7:-}"
  local city="${8:-}"
  local pollutant="${9:-}"
  local batch_month="${10:-}"

  local ts host
  ts="$(telemetry_now_utc)"
  host="$(telemetry_hostname)"

  telemetry_append_jsonl "${SPAN_EVENTS_FILE}" \
    "{\"event_ts\":\"$(telemetry_json_escape "$ts")\",\"event_type\":\"$(telemetry_json_escape "$event_type")\",\"run_id\":\"$(telemetry_json_escape "$run_id")\",\"trace_id\":\"$(telemetry_json_escape "$trace_id")\",\"span_id\":\"$(telemetry_json_escape "$span_id")\",\"parent_span_id\":\"$(telemetry_json_escape "$parent_span_id")\",\"step_name\":\"$(telemetry_json_escape "$step_name")\",\"status\":\"$(telemetry_json_escape "$status")\",\"city\":\"$(telemetry_json_escape "$city")\",\"pollutant\":\"$(telemetry_json_escape "$pollutant")\",\"batch_month\":\"$(telemetry_json_escape "$batch_month")\",\"host\":\"$(telemetry_json_escape "$host")\"}"
}

telemetry_emit_metric_event() {
  local run_id="$1"
  local trace_id="$2"
  local span_id="$3"
  local metric_name="$4"
  local metric_value="$5"
  local unit="${6:-count}"
  local city="${7:-}"
  local pollutant="${8:-}"
  local batch_month="${9:-}"

  local ts host
  ts="$(telemetry_now_utc)"
  host="$(telemetry_hostname)"

  telemetry_append_jsonl "${METRIC_EVENTS_FILE}" \
    "{\"metric_ts\":\"$(telemetry_json_escape "$ts")\",\"run_id\":\"$(telemetry_json_escape "$run_id")\",\"trace_id\":\"$(telemetry_json_escape "$trace_id")\",\"span_id\":\"$(telemetry_json_escape "$span_id")\",\"metric_name\":\"$(telemetry_json_escape "$metric_name")\",\"metric_value\":\"$(telemetry_json_escape "$metric_value")\",\"unit\":\"$(telemetry_json_escape "$unit")\",\"city\":\"$(telemetry_json_escape "$city")\",\"pollutant\":\"$(telemetry_json_escape "$pollutant")\",\"batch_month\":\"$(telemetry_json_escape "$batch_month")\",\"host\":\"$(telemetry_json_escape "$host")\"}"
}

telemetry_emit_error_event() {
  local run_id="$1"
  local trace_id="$2"
  local span_id="$3"
  local step_name="$4"
  local error_type="$5"
  local error_message="$6"

  local ts host
  ts="$(telemetry_now_utc)"
  host="$(telemetry_hostname)"

  telemetry_append_jsonl "${ERROR_EVENTS_FILE}" \
    "{\"error_ts\":\"$(telemetry_json_escape "$ts")\",\"run_id\":\"$(telemetry_json_escape "$run_id")\",\"trace_id\":\"$(telemetry_json_escape "$trace_id")\",\"span_id\":\"$(telemetry_json_escape "$span_id")\",\"step_name\":\"$(telemetry_json_escape "$step_name")\",\"error_type\":\"$(telemetry_json_escape "$error_type")\",\"error_message\":\"$(telemetry_json_escape "$error_message")\",\"host\":\"$(telemetry_json_escape "$host")\"}"
}

telemetry_new_run_id() {
  telemetry_uuid
}

telemetry_new_trace_id() {
  telemetry_uuid
}

telemetry_new_span_id() {
  telemetry_uuid
}
