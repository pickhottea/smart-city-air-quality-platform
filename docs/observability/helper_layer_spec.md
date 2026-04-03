# Helper Layer Specification

Repo: `smart-city-air-quality-platform`
Status: Draft v0.2
Scope: Shared runtime telemetry helper layer for shell and Python execution paths

---

## 1. Purpose

This document defines the helper layer used to emit runtime telemetry during pipeline execution.

The helper layer exists to enforce the runtime observability contract through a small set of shared functions.

From v0.2 onward, helper design follows these principles:

- trace is treated as a span model, not as a log variant
- runtime telemetry is captured as raw append-only events
- curated analytical artifacts are derived from runtime events
- core telemetry failures are operationally significant
- generated telemetry outputs are runtime artifacts and must not be committed to git

This helper layer is not a standalone service.
It is a shared library layer used by existing scripts.

---

## 2. Design principles

### 2.1 Low-intrusion instrumentation

Telemetry must be added without redesigning the whole pipeline.
Existing business logic stays in the current scripts.

### 2.2 Runtime-first emission

Telemetry must be emitted while the pipeline is running.
Post-run reconstruction is allowed only as a secondary rollup mechanism.

### 2.3 Separation of telemetry types

Logs, metrics, and traces answer different questions and must not be conflated.

- logs capture discrete execution events and failures
- metrics capture numeric signals and counters
- traces capture step timing, sequencing, and parent-child execution structure

### 2.4 Two-layer storage model

Runtime events and curated outputs are different layers.

- raw runtime layer = append-only execution evidence
- curated layer = cleaned, rollup-friendly outputs for analysis and review

### 2.5 Shared contract across languages

Shell and Python must emit the same canonical identifiers, vocabularies, and event structure.

### 2.6 Filesystem-first implementation

Initial rollout uses local files only.
No external collector is required in v0.2.

---

## 3. Helper layer scope

The helper layer will be implemented as two thin libraries.

### 3.1 Shell helper

Path:

`scripts/lib/telemetry.sh`

Used by:

- `scripts/pull_crate_month.sh`
- `scripts/pull_crate_202307_202509.sh`

### 3.2 Python helper

Path:

`src/pipeline_observability/telemetry.py`

Used by:

- `scripts/build_silver_table.py`
- `scripts/build_gold_hourly_table.py`
- `scripts/build_telemetry_metrics.py`

### 3.3 Optional support modules

Optional supporting constants/config may be placed in:

- `src/pipeline_observability/constants.py`
- `config/telemetry_fields.yaml`

These are optional in v0.2.
The hard requirement is that shell and Python helpers emit the same contract.

---

## 4. Runtime storage layout

Generated telemetry artifacts are runtime outputs.
They must be ignored by git.

### 4.1 Raw runtime event layer

Append-only execution evidence:

```
data/telemetry/runtime/run_events.jsonl
data/telemetry/runtime/span_events.jsonl
data/telemetry/runtime/metric_events.jsonl
data/telemetry/runtime/error_events.jsonl
```

### 4.2 Curated analytical layer

Rollup-friendly outputs:

```
data/telemetry/curated/pipeline_run_log.csv
data/telemetry/curated/pipeline_trace.csv
data/telemetry/curated/pipeline_metrics.csv
data/telemetry/curated/quality_metrics_daily.csv
data/telemetry/curated/freshness_metrics.csv
data/telemetry/curated/error_log.csv
```

### 4.3 Git rule

All files under `data/telemetry/` are generated artifacts.

They must not be committed.

---

## 5. Required runtime context

Every instrumented script must receive or derive the following runtime context.

### 5.1 Required environment variables

- `RUN_ID`
- `TRACE_ID`
- `PIPELINE_NAME`
- `TELEMETRY_DIR`

### 5.2 Recommended context variables

- `CITY`
- `POLLUTANT`
- `BATCH_MONTH`
- `SOURCE_NAME`

### 5.3 Defaults

If not explicitly passed:

- `PIPELINE_NAME` defaults to `smart_city_air_quality`
- `TELEMETRY_DIR` defaults to `data/telemetry`

`RUN_ID` and `TRACE_ID` must never silently default to empty values.

They must be created before the first instrumented step begins.

---

## 6. Telemetry type model

### 6.1 Run events

Run events describe lifecycle state of the whole pipeline run.

Examples:

- run_initialized
- run_started
- run_completed
- run_failed
- run_incomplete

Primary raw sink:

`data/telemetry/runtime/run_events.jsonl`

### 6.2 Span events

Span events represent step execution within a trace.

Examples:

- span_start
- span_end

Primary raw sink:

`data/telemetry/runtime/span_events.jsonl`

### 6.3 Metric events

Metric events represent runtime numeric signals emitted when they become known.

Examples:

- rows_downloaded_total
- duplicate_rows_total
- freshness_lag_minutes
- spark_stage_duration_ms

Primary raw sink:

`data/telemetry/runtime/metric_events.jsonl`

Metric events are part of the core runtime telemetry contract in this project.

This means metric emission is not optional side output.
If the canonical metric sink cannot be written after one immediate retry, the pipeline run must fail.

### 6.4 Error events

Error events represent structured failures and degraded states.

Examples:

- download_timeout
- parse_failure
- qc_rule_failure
- telemetry_sink_failure

Primary raw sink:

`data/telemetry/runtime/error_events.jsonl`

---

## 7. Trace design

### 7.1 Trace is not a log variant

A trace models execution structure and timing.

A pipeline run corresponds to one `trace_id`.

Each execution step corresponds to one `span_id`.

### 7.2 Span model

Each span contains at minimum:

- `trace_id`
- `span_id`
- `parent_span_id`
- `step_name`
- `started_at`
- `finished_at`
- `duration_ms`
- `status`

### 7.3 Event-sourced trace writing

v0.2 uses event-sourced span capture.

At step start:

- append one `span_start` event

At step end:

- append one `span_end` event

Curated `pipeline_trace.csv` is built later by pairing start/end events.

This design is preferred because:

- it is truly append-only
- it preserves evidence that a step started even if the process crashes
- it fits future migration to OTel or another backend
- it avoids in-place row mutation

### 7.4 Required raw span event fields

Each raw span event must include:

- `event_ts`
- `event_type`
- `run_id`
- `trace_id`
- `span_id`
- `parent_span_id`
- `pipeline_name`
- `step_name`
- `status`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `rows_in`
- `rows_out`
- `artifact_path`
- `error_message`

Rules:

- `event_type` must be `span_start` or `span_end`
- `status` for `span_start` must be `started`
- `status` for `span_end` must be one of `success`, `failed`, `skipped`

### 7.5 Curated trace output

`pipeline_trace.csv` must contain one row per completed or reconstructed span.

Required columns:

- `trace_id`
- `span_id`
- `parent_span_id`
- `run_id`
- `pipeline_name`
- `step_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `started_at`
- `finished_at`
- `duration_ms`
- `status`
- `rows_in`
- `rows_out`
- `artifact_path`
- `error_message`

### 7.6 Span hierarchy guideline

Recommended root span:

- `pipeline_run`

Recommended ingestion spans:

- `download_data`
- `normalize_files`
- `write_bronze`

Recommended silver spans:

- `read_bronze`
- `normalize_schema`
- `cast_types`
- `deduplicate`
- `qc_range_check`
- `qc_stale_check`
- `qc_outlier_check`
- `write_silver`

Recommended gold spans:

- `read_silver`
- `build_gold_hourly`
- `build_city_comparison`
- `build_coverage`
- `write_gold`

Recommended telemetry summary spans:

- `emit_quality_metrics`
- `emit_freshness_metrics`
- `rollup_telemetry`

## 8. Canonical timestamp and duration rules

All timestamps must use UTC ISO8601.

Preferred format:

`YYYY-MM-DDTHH:MM:SS.sssZ`

Examples:

- `2026-04-03T10:45:12.381Z`
- `2026-04-03T10:45:15.004Z`

All duration values must be stored in milliseconds.

---

## 9. Canonical status vocabulary

### 9.1 Run status

Allowed values:

- `running`
- `success`
- `failed`
- `partial_success`
- `incomplete`

### 9.2 Span status

Allowed values:

- `started`
- `success`
- `failed`
- `skipped`

### 9.3 Freshness status

Allowed values:

- `fresh`
- `delayed`
- `stale`
- `unknown`

### 9.4 Quality status

Allowed values:

- `pass`
- `warn`
- `fail`

---

## 10. Helper responsibilities

The helper layer is responsible for:

- generating or validating identifiers
- creating runtime directories if missing
- writing append-only raw event records
- enforcing canonical field names
- enforcing canonical status vocabulary
- normalizing timestamps
- creating curated artifact headers if needed
- exposing a minimal API to scripts
- surfacing telemetry sink failures clearly

The helper layer is not responsible for:

- defining business policy thresholds
- replacing transformation logic
- becoming a distributed tracing platform
- storing long-term telemetry in a database

---

## 11. Shell helper API

The shell helper should expose the following functions.

### 11.1 `telemetry_init`

Purpose:

- validate runtime context
- create telemetry directories
- initialize raw event sinks and curated directories if missing

### 11.2 `telemetry_run_event`

Purpose:

- append one run event row to `run_events.jsonl`

Required arguments:

- `event_type`
- `status`

Optional:

- `error_message`
- `artifact_path`

### 11.3 `telemetry_start_span`

Purpose:

- append one `span_start` event

Required arguments:

- `span_id`
- `parent_span_id`
- `step_name`

Optional:

- `city`
- `pollutant`
- `batch_month`
- `source_name`

Behavior:

- writes one JSON line to `span_events.jsonl`

### 11.4 `telemetry_end_span`

Purpose:

- append one `span_end` event

Required arguments:

- `span_id`
- `parent_span_id`
- `step_name`
- `status`

Optional:

- `rows_in`
- `rows_out`
- `artifact_path`
- `error_message`
- `city`
- `pollutant`
- `batch_month`
- `source_name`

Behavior:

- writes one JSON line to `span_events.jsonl`

### 11.5 `telemetry_emit_metric`

Purpose:

- append one metric event to `metric_events.jsonl`

Required arguments:

- `metric_name`
- `metric_value`
- `metric_unit`

Optional:

- `step_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`

### 11.6 `telemetry_emit_error`

Purpose:

- append one error event to `error_events.jsonl`

Required arguments:

- `step_name`
- `error_type`
- `error_message`

Optional:

- `span_id`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `retryable`
- `severity`

### 11.7 `telemetry_fail_core_sink`

Purpose:

- mark a core telemetry sink failure
- stop execution when a required sink is unavailable

Used for:

- `run_events.jsonl` unavailable
- `span_events.jsonl` unavailable
- `error_events.jsonl` unavailable

---

## 12. Python helper API

The Python helper should expose functions mirroring the shell helper.

### 12.1 `init_telemetry(...)`

Purpose:

- validate identifiers
- initialize runtime directories and sinks
- prepare helper state

Suggested signature:

```
init_telemetry(
run_id:str,
trace_id:str,
pipeline_name:str="smart_city_air_quality",
telemetry_dir:str="data/telemetry",
)
```

### 12.2 `emit_run_event(...)`

Purpose:

- append one run event

Suggested signature:

```
emit_run_event(
event_type:str,
status:str,
error_message:str|None=None,
artifact_path:str|None=None,
)->None
```

### 12.3 `start_span(...)`

Purpose:

- append one `span_start` event

Suggested signature:

```
start_span(
step_name:str,
parent_span_id:str|None=None,
city:str|None=None,
pollutant:str|None=None,
batch_month:str|None=None,
source_name:str|None=None,
)->dict
```

Returns:

- a span context object containing at least:
    - `span_id`
    - `parent_span_id`
    - `step_name`
    - `started_at`

### 12.4 `end_span(...)`

Purpose:

- append one `span_end` event

Suggested signature:

```
end_span(
span_id:str,
step_name:str,
status:str,
parent_span_id:str|None=None,
rows_in:int|None=None,
rows_out:int|None=None,
artifact_path:str|None=None,
error_message:str|None=None,
city:str|None=None,
pollutant:str|None=None,
batch_month:str|None=None,
source_name:str|None=None,
)->None
```

### 12.5 `emit_metric(...)`

Purpose:

- append one metric event immediately when known

Suggested signature:

```
emit_metric(
metric_name:str,
metric_value:float|int,
metric_unit:str,
step_name:str|None=None,
city:str|None=None,
pollutant:str|None=None,
batch_month:str|None=None,
source_name:str|None=None,
)->None
```

### 12.6 `emit_error(...)`

Purpose:

- append one structured error event

Suggested signature:

```
emit_error(
step_name:str,
error_type:str,
error_message:str,
span_id:str|None=None,
city:str|None=None,
pollutant:str|None=None,
batch_month:str|None=None,
source_name:str|None=None,
retryable:bool=False,
severity:str="error",
)->None
```

### 12.7 `fail_core_sink(...)`

Purpose:

- raise or terminate when a core telemetry sink is unavailable

Suggested signature:

```
fail_core_sink(
sink_name:str,
error_message:str,
)->None
```

## 13. Raw event schemas

### 13.1 `run_events.jsonl`

Required fields:

- `event_ts`
- `event_type`
- `run_id`
- `trace_id`
- `pipeline_name`
- `status`
- `error_message`
- `artifact_path`

Allowed `event_type` examples:

- `run_initialized`
- `run_started`
- `run_completed`
- `run_failed`
- `run_incomplete`

### 13.2 `span_events.jsonl`

Required fields:

- `event_ts`
- `event_type`
- `run_id`
- `trace_id`
- `span_id`
- `parent_span_id`
- `pipeline_name`
- `step_name`
- `status`

Recommended additional fields:

- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `rows_in`
- `rows_out`
- `artifact_path`
- `error_message`

### 13.3 `metric_events.jsonl`

Required fields:

- `event_ts`
- `run_id`
- `trace_id`
- `pipeline_name`
- `metric_name`
- `metric_value`
- `metric_unit`

Recommended additional fields:

- `step_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`

### 13.4 `error_events.jsonl`

Required fields:

- `event_ts`
- `run_id`
- `trace_id`
- `pipeline_name`
- `step_name`
- `error_type`
- `error_message`

Recommended additional fields:

- `span_id`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `retryable`
- `severity`

---

## 14. Curated artifact schemas

### 14.1 `pipeline_run_log.csv`

Required columns:

- `run_id`
- `trace_id`
- `pipeline_name`
- `started_at`
- `finished_at`
- `duration_ms`
- `status`
- `rows_in`
- `rows_out`
- `rows_flagged`
- `rows_rejected`
- `error_count`
- `artifact_path`
- `error_message`

Optional columns:

- `city_scope`
- `pollutant_scope`
- `source_name`

### 14.2 `pipeline_trace.csv`

Required columns:

- `trace_id`
- `span_id`
- `parent_span_id`
- `run_id`
- `pipeline_name`
- `step_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `started_at`
- `finished_at`
- `duration_ms`
- `status`
- `rows_in`
- `rows_out`
- `artifact_path`
- `error_message`

### 14.3 `pipeline_metrics.csv`

Required columns:

- `metric_ts`
- `run_id`
- `trace_id`
- `pipeline_name`
- `step_name`
- `metric_name`
- `metric_value`
- `metric_unit`
- `city`
- `pollutant`
- `batch_month`
- `source_name`

### 14.4 `quality_metrics_daily.csv`

Required columns:

- `metric_date`
- `run_id`
- `city`
- `pollutant`
- `total_records`
- `null_rate`
- `duplicate_rate`
- `stale_rate`
- `outlier_rate`
- `flagged_rate`
- `rejected_rate`
- `pass_rate`
- `quality_status`

### 14.5 `freshness_metrics.csv`

Required columns:

- `metric_ts`
- `run_id`
- `city`
- `pollutant`
- `latest_observed_at`
- `latest_ingested_at`
- `freshness_lag_minutes`
- `freshness_status`

### 14.6 `error_log.csv`

Required columns:

- `error_ts`
- `run_id`
- `trace_id`
- `span_id`
- `pipeline_name`
- `step_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `error_type`
- `error_message`
- `retryable`
- `severity`

---

## 15. Validation rules

### 15.1 Hard validation fields

The helper must fail fast if missing:

- `run_id`
- `trace_id`
- `pipeline_name`
- required sink path
- event timestamp
- event type
- status where required
- `step_name` for span/error events

These are identity and correlation fields.

### 15.2 Soft validation fields

The helper may allow null values for:

- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `rows_in`
- `rows_out`
- `artifact_path`

These are contextual enrichment fields and are not always applicable.

### 15.3 Vocabulary validation

Helpers must normalize or reject invalid values for:

- run status
- span status
- freshness status
- quality status

---

## 16. Write model and safety

### 16.1 Raw layer

Raw runtime event sinks must be append-only.

No in-place mutation is allowed in the raw event layer.

### 16.2 Curated layer

Curated CSV artifacts may be written by rollup jobs after runtime events are captured.

### 16.3 Safe write expectation

For shell:

- use one-line append writes
- keep event payload construction simple
- add file locking later if needed

For Python:

- use append mode
- flush after critical writes where practical

### 16.4 Locking

Full distributed locking is not required in v0.2.

However, helper code should be written so later versions can add:

- `flock` on shell side
- file-lock wrappers on Python side

---

## 17. Telemetry failure policy

### 17.1 Core telemetry sinks

The following raw sinks are core:

- `run_events.jsonl`
- `span_events.jsonl`
- `error_events.jsonl`
- `metric_events.jsonl`

If any core sink cannot be written, the pipeline must fail.

Reason:
without runtime logs, traces, errors, and metrics, the run is not operationally valid under this contract.

### 17.2 Core sink failure rule

A core sink failure is defined as failure to write to the canonical sink after one immediate retry.

Allowed behavior:

1. attempt write
2. retry once immediately if the first write fails
3. fail the pipeline if the second write also fails

This keeps the contract strict without making the pipeline brittle to a single transient file write issue.

### 17.3 Non-core telemetry outputs

The following outputs are non-core in execution control terms:

- curated CSV rollups
- quality summary outputs
- freshness summary outputs

If these fail:

- emit an error event where possible
- continue business execution if safe
- mark final run state as `incomplete` or `partial_success`

### 17.4 Telemetry sink failure event

When possible, the helper should emit an error event with:

- `error_type = telemetry_sink_failure`
- sink name
- failure message
- severity

If the failed sink is itself the error sink, the process must fail immediately.

### 17.5 Contract rule

Raw runtime event coverage is mandatory.

If any one of the required raw event types is missing for a run:

- `run_events`
- `span_events`
- `error_events`
- `metric_events`

the run must be treated as failed or operationally invalid under this contract.

Rollup is for summarization only.
Rollup does not repair missing runtime evidence.

---

## 18. Identifier generation rules

### 18.1 `run_id`

Generated once at orchestration start.

Recommended format:

`run_<utc timestamp>_<short suffix>`

Example:

`run_20260403T104512Z_a1f39b`

### 18.2 `trace_id`

Generated once per run.

Recommended format:

`trace_<utc timestamp>_<short suffix>`

### 18.3 `span_id`

Generated per span.

Recommended format:

`span_<short suffix>`

### 18.4 Ownership

Preferred ownership:

- orchestration entrypoint generates `RUN_ID` and `TRACE_ID`
- child scripts inherit them from environment
- each helper generates `span_id` when starting a step unless explicitly passed

---

## 19. Script-level responsibility guidance

### 19.1 Shell side

Good fit for helper use:

- ingestion run start/end
- per-download span start/end
- file path capture
- HTTP/download errors
- bytes downloaded or row counts if known

### 19.2 Python side

Good fit for helper use:

- bronze read metrics
- silver row counts
- dedup metrics
- outlier metrics
- QC metrics
- aggregation span timing
- gold output row counts

### 19.3 Rollup side

Good fit for `build_telemetry_metrics.py`:

- pairing span start/end into curated trace
- building curated run log
- metric compaction
- quality/freshness summaries
- consistency checks across artifacts

`build_telemetry_metrics.py` must not remain the only source of telemetry truth.

---

## 20. Minimal rollout requirement

The helper layer is considered ready for rollout when both shell and Python helpers support:

- run event emission
- span start/end emission
- metric emission
- error emission
- core sink failure handling

before any extra abstraction is added.

---

## 21. Suggested file structure

```
scripts/
  lib/
    telemetry.sh

src/
  pipeline_observability/
    __init__.py
    telemetry.py
    constants.py   # optional

config/
  telemetry_fields.yaml   # optional
```

---

## 22. Final rule

Pipeline scripts must not write ad hoc telemetry layouts directly once this helper layer is adopted.

All new runtime telemetry emission must go through the shared helper interface.

Runtime evidence first.
Curated summaries second.

The required raw runtime evidence for a valid run is:

- `run_events.jsonl`
- `span_events.jsonl`
- `error_events.jsonl`
- `metric_events.jsonl`

If any required raw telemetry stream is missing, the run is invalid under this contract.

Rollup is summarization, not recovery.
No core telemetry, no valid run.