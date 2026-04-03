# Runtime Observability Contract
Repo: `smart-city-air-quality-platform`
Status: Draft v0.1
Scope: Runtime telemetry contract for pipeline execution
Audience: project owner / future contributors

---

## 1. Purpose

This contract defines the minimum runtime observability requirements for the Smart City air-quality pipeline.

From this version onward, telemetry is not treated as a post-run side output only.
Telemetry becomes part of normal pipeline execution.

A pipeline run is considered complete only if it emits:

- runtime logs
- runtime metrics
- runtime trace spans
- run-level completion status

This contract is designed to fit the current project shape:

- Linux shell handles ingestion operations and file hygiene
- Spark handles standardization, quality checks, and analytical aggregation
- telemetry must be collected during execution, not reconstructed only after execution

---

## 2. Non-goals

This contract does not turn the repo into a full observability platform.
This contract does not require distributed tracing infrastructure.
This contract does not require streaming-first architecture.

The main repo narrative remains:

- air-quality ingestion
- standardization
- quality-aware transformation
- analytics-ready cross-city outputs

Telemetry is a required runtime capability that supports this pipeline.

---

## 3. Runtime guarantee

Every pipeline run MUST satisfy the following guarantees.

### 3.1 Run identity guarantee

Every run must have:

- `run_id`
- `trace_id`
- `pipeline_name`
- `started_at`

These identifiers must be created before the first operational step begins.

### 3.2 Event emission guarantee

During execution, the pipeline must emit:

- span start events
- span end events
- metric events
- error events when failures occur
- run finalization event

Telemetry emission must happen during runtime, not only in a later summary job.

### 3.3 Failure guarantee

If a step fails, the pipeline must emit an error event before process exit whenever technically possible.

At minimum, the failure record must contain:

- `run_id`
- `trace_id`
- `step_name`
- `error_type`
- `error_message`
- `error_ts`

### 3.4 Completion guarantee

A run is considered complete only if all of the following exist:

- one run-level record in `pipeline_run_log.csv`
- one or more step-level records in `pipeline_trace.csv`
- one or more metric rows in `pipeline_metrics.csv`
- zero or more error rows in `error_log.csv`
- quality/freshness outputs when downstream steps are executed

If the main pipeline finishes but telemetry is missing, the run is classified as `incomplete`.

---

## 4. Canonical identifiers

The following identifiers are canonical across all telemetry artifacts.

### 4.1 `run_id`

Definition:
A unique identifier for one end-to-end pipeline execution.

Rules:
- generated once per pipeline run
- reused across all scripts participating in the same run
- string format is implementation-defined, but must be filesystem-safe

Example:
- `run_20260403T104512Z_a1f39b`

### 4.2 `trace_id`

Definition:
A unique identifier representing the parent trace of one pipeline run.

Rules:
- one `trace_id` per `run_id`
- shared across all spans in the run

### 4.3 `span_id`

Definition:
A unique identifier for a specific execution step.

Rules:
- unique within a trace
- created for each step start
- paired with a matching span end update or row completion

### 4.4 `parent_span_id`

Definition:
The parent step identifier for nested spans.

Rules:
- nullable for top-level spans
- required for child spans when nesting exists

### 4.5 Context fields

The following business context fields should be carried whenever available:

- `pipeline_name`
- `city`
- `pollutant`
- `batch_month`
- `source_name`
- `step_name`

These fields are not all required for every row, but emitters should populate them whenever the step has that context.

---

## 5. Canonical status vocabulary

To avoid inconsistent status labels, all telemetry emitters must use the following values.

### 5.1 Run status

Allowed values:

- `running`
- `success`
- `failed`
- `partial_success`
- `incomplete`

### 5.2 Span status

Allowed values:

- `started`
- `success`
- `failed`
- `skipped`

### 5.3 Freshness status

Allowed values:

- `fresh`
- `delayed`
- `stale`
- `unknown`

### 5.4 Quality status

Allowed values:

- `pass`
- `warn`
- `fail`

---

## 6. Required telemetry artifacts

Each run must write telemetry under:

`data/telemetry/`

The canonical artifacts are:

- `pipeline_run_log.csv`
- `pipeline_trace.csv`
- `pipeline_metrics.csv`
- `quality_metrics_daily.csv`
- `freshness_metrics.csv`
- `error_log.csv`

### 6.1 Artifact roles

#### `pipeline_run_log.csv`

Run-level execution summary.
One row per run.

#### `pipeline_trace.csv`

Step-level execution spans.
Multiple rows per run.

#### `pipeline_metrics.csv`

Atomic metric events emitted during runtime.
Multiple rows per run.

#### `quality_metrics_daily.csv`

Quality summary derived from runtime-aware pipeline outputs.
May contain multiple rows by date / city / pollutant.

#### `freshness_metrics.csv`

Freshness lag summary for latest available observations and ingestion timing.

#### `error_log.csv`

Structured execution failures and exceptions.

---

## 7. Required schemas

This section defines the minimum schema contract.
Additional columns are allowed if they do not break compatibility.

### 7.1 `pipeline_run_log.csv`

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

Recommended optional columns:

- `city_scope`
- `pollutant_scope`
- `source_name`

Notes:
- one row per run
- `artifact_path` may point to primary downstream output or summary path
- `error_message` should be null for successful runs

### 7.2 `pipeline_trace.csv`

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

Notes:
- one row per execution step
- child steps should populate `parent_span_id`
- if a field does not apply, use null rather than inventing placeholder strings

### 7.3 `pipeline_metrics.csv`

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

Examples of valid metric names:

- `ingestion_runs_total`
- `rows_downloaded_total`
- `rows_bronze_written_total`
- `rows_silver_written_total`
- `rows_gold_written_total`
- `duplicate_rows_total`
- `flagged_records_total`
- `quality_checks_failed_total`
- `freshness_lag_minutes`
- `spark_stage_duration_ms`

### 7.4 `quality_metrics_daily.csv`

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

Notes:
- daily grain
- may be derived after silver or gold generation, but still belongs to runtime telemetry outputs of the run

### 7.5 `freshness_metrics.csv`

Required columns:

- `metric_ts`
- `run_id`
- `city`
- `pollutant`
- `latest_observed_at`
- `latest_ingested_at`
- `freshness_lag_minutes`
- `freshness_status`

### 7.6 `error_log.csv`

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

Notes:
- all failures should be represented as structured rows
- `retryable` is boolean-like and implementation may store it as `true/false`

---

## 8. Emission timing rules

To enforce runtime collection, telemetry must be emitted at the following moments.

### 8.1 Before work begins

Emit:

- run initialization metadata
- root span start
- `running` status for run

### 8.2 At step start

Emit:

- new span row or span start event
- step context fields
- started timestamp

### 8.3 At step end

Emit:

- finished timestamp
- duration
- step status
- rows in / rows out if known
- artifact path if output was produced

### 8.4 When a metric is computed

Emit metric row immediately.

Do not wait until the whole run ends if the metric is known during execution.

### 8.5 When an error occurs

Emit structured error row immediately.

### 8.6 At run end

Emit:

- final run summary row
- final status
- final counts
- final error count

---

## 9. Script responsibility map

### 9.1 `scripts/pull_crate_month.sh`

Responsible for emitting:

- ingestion start/end spans
- download metrics
- file output path
- download errors
- source-level context

### 9.2 `scripts/pull_crate_202307_202509.sh`

Responsible for emitting:

- parent orchestration span
- child spans for each city × pollutant × month
- aggregate success/failure counters
- retry/error events

### 9.3 `scripts/build_silver_table.py`

Responsible for emitting:

- read bronze span
- normalization spans
- type casting span
- dedup span
- quality check spans
- silver write span
- row-count metrics
- quality metrics where available

### 9.4 `scripts/build_gold_hourly_table.py`

Responsible for emitting:

- read silver span
- hourly aggregation span
- city comparison span
- coverage span
- gold write span
- output row metrics

### 9.5 `scripts/build_telemetry_metrics.py`

Responsible for:

- rollup
- compaction
- consistency checks across telemetry artifacts
- summary outputs

Not responsible for being the only source of telemetry truth.

---

## 10. Backward compatibility and migration rule

Existing side-output telemetry may continue to exist during migration.
However, runtime-emitted telemetry becomes the target state.

During migration:

- helper-based runtime emission is the source of truth
- summary/rollup jobs may enrich or compact telemetry
- post-run reconstruction must not remain the only telemetry mechanism

---

## 11. Definition of done

The runtime observability implementation is considered acceptable only when:

1. a normal successful run emits all required artifacts
2. a failed run emits at least run log, trace rows, and error rows
3. identifiers are consistent across shell and Python steps
4. statuses follow canonical vocabulary
5. telemetry is produced during execution rather than reconstructed only after execution

---

## 12. Final rule

No telemetry, no complete run.

A pipeline execution without required runtime logs, metrics, and spans must be treated as operationally incomplete.