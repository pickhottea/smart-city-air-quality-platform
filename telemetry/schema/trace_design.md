# Trace Design

## Goal

This document defines the runtime trace design for Linux ingestion scripts in the Smart City air-quality platform.

The trace model is intentionally simple:

- one run
- one or more spans
- metrics
- error events

The design must support:

- standalone monthly ingestion
- batch historical backfill
- parent-child span relationships
- warning vs failure distinction

---

## Runtime files

### Run events
Path:

`data/telemetry/runtime/run_events.jsonl`

Each line is one JSON object.

### Span events
Path:

`data/telemetry/runtime/span_events.jsonl`

Each line is one JSON object.

### Metric events
Path:

`data/telemetry/runtime/metric_events.jsonl`

Each line is one JSON object.

### Error events
Path:

`data/telemetry/runtime/error_events.jsonl`

Each line is one JSON object.

---

## Run event schema

Required fields:

- `event_ts`
- `event_type`
- `run_id`
- `trace_id`
- `pipeline_name`
- `status`
- `message`
- `host`

### Event types
Allowed values:

- `run_start`
- `run_end`

### Status values
Allowed values:

- `running`
- `success`
- `warning`
- `failed`

---

## Span event schema

Required fields:

- `event_ts`
- `event_type`
- `run_id`
- `trace_id`
- `span_id`
- `parent_span_id`
- `step_name`
- `status`
- `city`
- `pollutant`
- `batch_month`
- `host`

### Event types
Allowed values:

- `span_start`
- `span_end`

### Status values
Allowed values:

- `running`
- `success`
- `warning`
- `failed`

### Parent span rules

#### Root span
Use empty string when the span has no parent:

- standalone monthly run
- batch runner top-level span

#### Child span
Use the batch span id when the monthly pull is launched by the batch runner.

---

## Metric event schema

Required fields:

- `metric_ts`
- `run_id`
- `trace_id`
- `span_id`
- `metric_name`
- `metric_value`
- `unit`
- `city`
- `pollutant`
- `batch_month`
- `host`

### Initial metric catalog

#### `rows_downloaded_total`
- unit: `count`
- meaning: number of data rows in the downloaded monthly result

#### `file_bytes_written`
- unit: `bytes`
- meaning: number of bytes written to the bronze output file

For `empty_result`, `file_bytes_written` should be recorded as `0` when no bronze CSV is persisted.

---

## Error event schema

Required fields:

- `error_ts`
- `run_id`
- `trace_id`
- `span_id`
- `step_name`
- `error_type`
- `error_message`
- `host`

---

## Error type catalog

### `empty_result`
Meaning:

- source request succeeded
- no data rows were returned for the requested partition

Severity:

- warning

Batch behavior:

- continue

### `script_failed`
Meaning:

- the current script failed due to runtime execution error

Severity:

- failed

Batch behavior:

- stop if emitted by child and child exits fatally

### `child_pull_failed`
Meaning:

- batch runner received a fatal child exit code

Severity:

- failed

Batch behavior:

- stop batch

### `batch_failed`
Meaning:

- batch runner itself failed unexpectedly

Severity:

- failed

Batch behavior:

- stop batch

---

## Trace patterns

### Pattern A: standalone monthly pull

#### Run
- pipeline: `crate_ingestion_monthly`

#### Span tree
- root span: `pull_crate_month`

#### IDs
- one `run_id`
- one `trace_id`
- one monthly `span_id`
- `parent_span_id = ""`

---

### Pattern B: batch backfill

#### Run
- pipeline: `crate_ingestion_backfill`

#### Span tree
- root span: `pull_crate_202307_202509`
- child span(s): `pull_crate_month`

#### IDs
- one batch `run_id`
- one batch `trace_id`
- one batch root span id
- many child monthly span ids
- each child span sets:
  - `parent_span_id = batch root span id`

---

## Status transition rules

### Monthly success
- `run_start.status = running`
- `span_start.status = running`
- `span_end.status = success`
- `run_end.status = success`

### Monthly empty result
- `run_start.status = running`
- `span_start.status = running`
- `error_type = empty_result`
- `span_end.status = warning`
- `run_end.status = warning`

### Monthly fatal failure
- `run_start.status = running`
- `span_start.status = running`
- `error_type = script_failed`
- `span_end.status = failed`
- `run_end.status = failed`

### Batch success
- batch root span ends with `success`
- batch run ends with `success`

### Batch warning
- at least one child month ends with `warning`
- no fatal child occurs
- batch root span ends with `warning`
- batch run ends with `warning`

### Batch failure
- one fatal child fails
- batch emits `child_pull_failed`
- batch root span ends with `failed`
- batch run ends with `failed`

---

## Example shape

### Monthly warning case

- run_start: monthly / running
- span_start: pull_crate_month / running
- metric_event: rows_downloaded_total = 0
- metric_event: file_bytes_written = 0
- error_event: empty_result
- span_end: warning
- run_end: warning

### Batch warning case

- run_start: backfill / running
- span_start: batch root / running
- many child monthly success spans
- one or more child monthly warning spans
- batch root span_end: warning
- batch run_end: warning

### Batch failed case

- run_start: backfill / running
- span_start: batch root / running
- some child spans succeed
- one child fatally fails
- error_event: child_pull_failed
- batch root span_end: failed
- batch run_end: failed

---
## Warning-aware trace semantics

The trace model must preserve non-fatal degraded execution. 
Warning states are represented as normal span/run completion events with `warning` status, not as a separate trace event family.

Supported span end statuses:
- `success`
- `warning`
- `failed`
- `skipped`

Supported run end statuses:
- `success`
- `warning`
- `failed`
- `partial_success`
- `incomplete`

Typical warning case:
- monthly extraction returns `empty_result`
- span end status = `warning`
- run end status = `warning`
- structured error event records `error_type=empty_result`
- process exits with code `10`

For batch orchestration:
- child warnings are preserved as child span warnings
- parent trace may still complete
- final batch status may be `warning` when one or more child warnings exist and no fatal failure occurs

## Design constraints

This trace design must remain:

- file-based
- local
- append-only
- easy to inspect with shell tools
- easy to summarize later into rollup metrics

It should not require an external tracing backend for v1.