#!/usr/bin/env bash

telemetry_timestamp_utc() {
  python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
}

telemetry_generate_id() {
  local prefix="${1:?prefix is required}"
  python3 - "$prefix" <<'PY'
import sys, uuid
print(f"{sys.argv[1]}_{uuid.uuid4().hex[:8]}")
PY
}

_telemetry_json_escape() {
  local raw="${1-}"
  python3 - "$raw" <<'PY'
import json, sys
print(json.dumps(sys.argv[1], ensure_ascii=False)[1:-1])
PY
}

telemetry_fail_core_sink() {
  local sink_name="${1:?sink_name is required}"
  local error_message="${2:?error_message is required}"
  printf 'telemetry core sink failure [%s]: %s\n' "$sink_name" "$error_message" >&2
  return 1 2>/dev/null || exit 1
}

telemetry_init() {
  : "${RUN_ID:?RUN_ID must be set before telemetry_init}"
  : "${TRACE_ID:?TRACE_ID must be set before telemetry_init}"
  PIPELINE_NAME="${PIPELINE_NAME:-smart_city_air_quality}"
  TELEMETRY_DIR="${TELEMETRY_DIR:-data/telemetry}"

  export PIPELINE_NAME TELEMETRY_DIR
  export TELEMETRY_RUNTIME_DIR="${TELEMETRY_DIR}/runtime"
  export TELEMETRY_CURATED_DIR="${TELEMETRY_DIR}/curated"
  export TELEMETRY_RUN_EVENTS="${TELEMETRY_RUNTIME_DIR}/run_events.jsonl"
  export TELEMETRY_SPAN_EVENTS="${TELEMETRY_RUNTIME_DIR}/span_events.jsonl"
  export TELEMETRY_METRIC_EVENTS="${TELEMETRY_RUNTIME_DIR}/metric_events.jsonl"
  export TELEMETRY_ERROR_EVENTS="${TELEMETRY_RUNTIME_DIR}/error_events.jsonl"

  mkdir -p "$TELEMETRY_RUNTIME_DIR" "$TELEMETRY_CURATED_DIR" || telemetry_fail_core_sink "telemetry_dir" "unable to create telemetry directories"
  touch "$TELEMETRY_RUN_EVENTS" "$TELEMETRY_SPAN_EVENTS" "$TELEMETRY_METRIC_EVENTS" "$TELEMETRY_ERROR_EVENTS" || telemetry_fail_core_sink "telemetry_sinks" "unable to initialize runtime sink files"
}

_telemetry_append_jsonl() {
  local sink_name="${1:?sink_name is required}"
  local sink_path="${2:?sink_path is required}"
  local payload="${3:?payload is required}"

  if printf '%s\n' "$payload" >> "$sink_path" 2>/dev/null; then
    return 0
  fi

  if printf '%s\n' "$payload" >> "$sink_path" 2>/dev/null; then
    return 0
  fi

  if [[ "$sink_name" != "error_events" && -n "${TELEMETRY_ERROR_EVENTS:-}" ]]; then
    local error_ts sink_name_esc error_message_esc fallback
    error_ts="$(telemetry_timestamp_utc)"
    sink_name_esc="$(_telemetry_json_escape "$sink_name")"
    error_message_esc="$(_telemetry_json_escape "failed to write core telemetry sink after one retry")"
    fallback=$(cat <<EOF
{"event_ts":"${error_ts}","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","pipeline_name":"${PIPELINE_NAME}","step_name":"telemetry","error_type":"telemetry_sink_failure","error_message":"${sink_name_esc}: ${error_message_esc}","span_id":null,"city":null,"pollutant":null,"batch_month":null,"source_name":null,"retryable":false,"severity":"critical"}
EOF
)
    printf '%s\n' "$fallback" >> "$TELEMETRY_ERROR_EVENTS" 2>/dev/null || true
  fi

  telemetry_fail_core_sink "$sink_name" "unable to append runtime JSONL after one retry"
}

telemetry_run_event() {
  local event_type="${1:?event_type is required}"
  local status="${2:?status is required}"
  local error_message="${3-}"
  local artifact_path="${4-}"
  local event_ts event_type_esc status_esc error_message_esc artifact_path_esc payload
  event_ts="$(telemetry_timestamp_utc)"
  event_type_esc="$(_telemetry_json_escape "$event_type")"
  status_esc="$(_telemetry_json_escape "$status")"
  error_message_esc="$(_telemetry_json_escape "$error_message")"
  artifact_path_esc="$(_telemetry_json_escape "$artifact_path")"
  payload=$(cat <<EOF
{"event_ts":"${event_ts}","event_type":"${event_type_esc}","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","pipeline_name":"${PIPELINE_NAME}","status":"${status_esc}","error_message":"${error_message_esc}","artifact_path":"${artifact_path_esc}"}
EOF
)
  _telemetry_append_jsonl "run_events" "$TELEMETRY_RUN_EVENTS" "$payload"
}

telemetry_start_span() {
  local span_id="${1:?span_id is required}"
  local parent_span_id="${2-}"
  local step_name="${3:?step_name is required}"
  local city="${4-}"
  local pollutant="${5-}"
  local batch_month="${6-}"
  local source_name="${7-}"
  local event_ts payload
  event_ts="$(telemetry_timestamp_utc)"
  payload=$(cat <<EOF
{"event_ts":"${event_ts}","event_type":"span_start","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","span_id":"$(_telemetry_json_escape "$span_id")","parent_span_id":"$(_telemetry_json_escape "$parent_span_id")","pipeline_name":"${PIPELINE_NAME}","step_name":"$(_telemetry_json_escape "$step_name")","status":"started","city":"$(_telemetry_json_escape "$city")","pollutant":"$(_telemetry_json_escape "$pollutant")","batch_month":"$(_telemetry_json_escape "$batch_month")","source_name":"$(_telemetry_json_escape "$source_name")","rows_in":null,"rows_out":null,"artifact_path":null,"error_message":null}
EOF
)
  _telemetry_append_jsonl "span_events" "$TELEMETRY_SPAN_EVENTS" "$payload"
}

telemetry_end_span() {
  local span_id="${1:?span_id is required}"
  local parent_span_id="${2-}"
  local step_name="${3:?step_name is required}"
  local status="${4:?status is required}"
  local rows_in="${5-}"
  local rows_out="${6-}"
  local artifact_path="${7-}"
  local error_message="${8-}"
  local city="${9-}"
  local pollutant="${10-}"
  local batch_month="${11-}"
  local source_name="${12-}"
  local event_ts rows_in_json rows_out_json payload
  event_ts="$(telemetry_timestamp_utc)"
  if [[ -n "$rows_in" ]]; then rows_in_json="$rows_in"; else rows_in_json="null"; fi
  if [[ -n "$rows_out" ]]; then rows_out_json="$rows_out"; else rows_out_json="null"; fi
  payload=$(cat <<EOF
{"event_ts":"${event_ts}","event_type":"span_end","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","span_id":"$(_telemetry_json_escape "$span_id")","parent_span_id":"$(_telemetry_json_escape "$parent_span_id")","pipeline_name":"${PIPELINE_NAME}","step_name":"$(_telemetry_json_escape "$step_name")","status":"$(_telemetry_json_escape "$status")","city":"$(_telemetry_json_escape "$city")","pollutant":"$(_telemetry_json_escape "$pollutant")","batch_month":"$(_telemetry_json_escape "$batch_month")","source_name":"$(_telemetry_json_escape "$source_name")","rows_in":${rows_in_json},"rows_out":${rows_out_json},"artifact_path":"$(_telemetry_json_escape "$artifact_path")","error_message":"$(_telemetry_json_escape "$error_message")"}
EOF
)
  _telemetry_append_jsonl "span_events" "$TELEMETRY_SPAN_EVENTS" "$payload"
}

telemetry_emit_metric() {
  local metric_name="${1:?metric_name is required}"
  local metric_value="${2:?metric_value is required}"
  local metric_unit="${3:?metric_unit is required}"
  local step_name="${4-}"
  local city="${5-}"
  local pollutant="${6-}"
  local batch_month="${7-}"
  local source_name="${8-}"
  local event_ts payload
  event_ts="$(telemetry_timestamp_utc)"
  payload=$(cat <<EOF
{"event_ts":"${event_ts}","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","pipeline_name":"${PIPELINE_NAME}","step_name":"$(_telemetry_json_escape "$step_name")","metric_name":"$(_telemetry_json_escape "$metric_name")","metric_value":${metric_value},"metric_unit":"$(_telemetry_json_escape "$metric_unit")","city":"$(_telemetry_json_escape "$city")","pollutant":"$(_telemetry_json_escape "$pollutant")","batch_month":"$(_telemetry_json_escape "$batch_month")","source_name":"$(_telemetry_json_escape "$source_name")"}
EOF
)
  _telemetry_append_jsonl "metric_events" "$TELEMETRY_METRIC_EVENTS" "$payload"
}

telemetry_emit_error() {
  local step_name="${1:?step_name is required}"
  local error_type="${2:?error_type is required}"
  local error_message="${3:?error_message is required}"
  local span_id="${4-}"
  local city="${5-}"
  local pollutant="${6-}"
  local batch_month="${7-}"
  local source_name="${8-}"
  local retryable="${9-false}"
  local severity="${10-error}"
  local event_ts payload
  event_ts="$(telemetry_timestamp_utc)"
  payload=$(cat <<EOF
{"event_ts":"${event_ts}","run_id":"${RUN_ID}","trace_id":"${TRACE_ID}","pipeline_name":"${PIPELINE_NAME}","step_name":"$(_telemetry_json_escape "$step_name")","error_type":"$(_telemetry_json_escape "$error_type")","error_message":"$(_telemetry_json_escape "$error_message")","span_id":"$(_telemetry_json_escape "$span_id")","city":"$(_telemetry_json_escape "$city")","pollutant":"$(_telemetry_json_escape "$pollutant")","batch_month":"$(_telemetry_json_escape "$batch_month")","source_name":"$(_telemetry_json_escape "$source_name")","retryable":${retryable},"severity":"$(_telemetry_json_escape "$severity")"}
EOF
)
  _telemetry_append_jsonl "error_events" "$TELEMETRY_ERROR_EVENTS" "$payload"
}
