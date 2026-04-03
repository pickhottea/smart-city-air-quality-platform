# Single Path Rollout Plan

Repo: `smart-city-air-quality-platform`
Status: Draft v0.1
Scope: First runtime observability rollout on one end-to-end path

---

## 1. Purpose

This document defines the first implementation path for runtime observability rollout.

The goal is to avoid instrumenting the whole pipeline at once.
Instead, one representative path is instrumented fully and validated first.

This rollout is intentionally limited to the canonical main path:

`pull_crate_month.sh -> build_silver_table.py -> build_gold_hourly_table.py`

This path is chosen because it matches the current project structure:

- monthly ingestion is already the stable extraction unit
- Silver already represents Spark standardization
- Gold already represents Spark aggregation outputs

---

## 2. Rollout objective

The single-path rollout is considered successful when one end-to-end run:

- emits runtime logs, metrics, traces, and errors during execution
- writes raw runtime artifacts under `data/telemetry/runtime/`
- writes curated outputs under `data/telemetry/curated/`
- preserves Linux/Spark division of labor
- does not depend on post-run reconstruction as the only telemetry mechanism

---

## 3. Canonical first path

### Step 1

`scripts/linux/pull_crate_month.sh`

### Step 2

`spark/jobs/build_silver_table.py`

### Step 3

`spark/jobs/build_gold_hourly_table.py`

This path is the minimum viable runtime observability implementation.

Do not start rollout with:

- `scripts/linux/pull_crate_202307_202509.sh`
- `spark/jobs/build_telemetry_metrics.py`

Those are phase-2 extensions.

---

## 4. Required directories

The rollout must create the following directories if they do not exist.

```
data/telemetry/runtime/
data/telemetry/curated/
scripts/lib/
src/pipeline_observability/
```

Optional local temp state if needed later:

```
tmp/telemetry_state/
```

---

## 5. Required raw sinks

The following raw sinks are required for a valid run:

```
data/telemetry/runtime/run_events.jsonl
data/telemetry/runtime/span_events.jsonl
data/telemetry/runtime/metric_events.jsonl
data/telemetry/runtime/error_events.jsonl
```

These are core telemetry sinks for this project.

If any one of these cannot be written after one immediate retry, the run must fail.

---

## 6. Required curated outputs for phase 1

During first rollout, curated outputs may be partial, but the target set is:

```
data/telemetry/curated/pipeline_run_log.csv
data/telemetry/curated/pipeline_trace.csv
data/telemetry/curated/pipeline_metrics.csv
data/telemetry/curated/error_log.csv
```

The following may remain phase-1 optional if their logic is not yet implemented:

```
data/telemetry/curated/quality_metrics_daily.csv
data/telemetry/curated/freshness_metrics.csv
```

However, the raw runtime evidence still must exist even if these curated outputs are not yet ready.

---

## 7. Implementation order

### 7.1 First create helper layer

Required files:

```
scripts/lib/telemetry.sh
src/pipeline_observability/telemetry.py
```

Do not instrument scripts directly with ad hoc telemetry writes before helpers exist.

### 7.2 Instrument `pull_crate_month.sh`

Add runtime telemetry first to the shell entry path.

### 7.3 Instrument `build_silver_table.py`

Add runtime telemetry second to the Silver Spark transformation path.

### 7.4 Instrument `build_gold_hourly_table.py`

Add runtime telemetry third to the Gold aggregation path.

### 7.5 Only after that, wire curated rollup

Use rollup logic to turn raw runtime events into curated CSV outputs.

---

## 8. Script-level emission points for phase 1

### 8.1 `scripts/linux/pull_crate_month.sh`

This script must emit:

### Run events

- `run_initialized`
- `run_started`

### Spans

- `pipeline_run` root span start/end if this script is the orchestration entrypoint
- `download_data`
- `write_bronze`

### Metrics

- `ingestion_runs_total`
- `rows_downloaded_total` when known
- `rows_bronze_written_total` when known
- optional `download_duration_ms`

### Errors

- download failure
- proxy timeout
- output file write failure
- telemetry sink failure

### Context fields when available

- `city`
- `pollutant`
- `batch_month`
- `source_name`

### 8.2 `spark/jobs/build_silver_table.py`

This script must emit:

### Spans

- `read_bronze`
- `normalize_schema`
- `cast_types`
- `deduplicate`
- `qc_range_check`
- `qc_stale_check`
- `qc_outlier_check`
- `write_silver`

### Metrics

- `rows_bronze_read_total`
- `rows_silver_written_total`
- `duplicate_rows_total`
- `flagged_records_total`
- `quality_checks_failed_total`
- `spark_stage_duration_ms` where practical

### Errors

- schema parse failure
- timestamp cast failure
- silver write failure
- QC rule execution failure
- telemetry sink failure

### 8.3 `spark/jobs/build_gold_hourly_table.py`

This script must emit:

### Spans

- `read_silver`
- `build_gold_hourly`
- `build_city_comparison`
- `build_coverage`
- `write_gold`

### Metrics

- `rows_silver_read_total`
- `rows_gold_hourly_written_total`
- `rows_gold_comparison_written_total`
- `rows_gold_coverage_written_total`
- `city_hourly_updates_total`

### Errors

- read silver failure
- aggregation failure
- parquet write failure
- telemetry sink failure

---
## 9. Parent-child span guideline for phase 1

Recommended hierarchy:

```
pipeline_run
├── download_data
├── write_bronze
├── read_bronze
├── normalize_schema
├── cast_types
├── deduplicate
├── qc_range_check
├── qc_stale_check
├── qc_outlier_check
├── write_silver
├── read_silver
├── build_gold_hourly
├── build_city_comparison
├── build_coverage
└── write_gold
```

A simpler flat model is acceptable in the first implementation as long as:

- all spans share the same `trace_id`
- each span has a valid `span_id`
- timing and status are recorded
- later nesting remains possible

---

## 10. First-pass metric set

For the single-path rollout, implement only this minimum metric set:

- `ingestion_runs_total`
- `rows_downloaded_total`
- `rows_bronze_written_total`
- `rows_bronze_read_total`
- `rows_silver_written_total`
- `duplicate_rows_total`
- `flagged_records_total`
- `quality_checks_failed_total`
- `rows_gold_hourly_written_total`
- `rows_gold_comparison_written_total`
- `rows_gold_coverage_written_total`

Do not over-expand metric vocabulary in phase 1.

---

## 11. Runtime status behavior

### 11.1 Success case

A run is `success` when:

- all three scripts complete
- all four raw sinks were written
- root run event and root span are complete
- no core telemetry sink failed

### 11.2 Failed case

A run is `failed` when:

- business logic fails in one of the three scripts
- a core telemetry sink fails after one immediate retry
- the run terminates before finalization

### 11.3 Incomplete case

A run is `incomplete` when:

- core runtime evidence exists
- business flow may have succeeded
- curated rollup or summary output is missing or partial

---

## 12. Required orchestration behavior

The first script in the path must own:

- `RUN_ID` creation
- `TRACE_ID` creation
- `PIPELINE_NAME` propagation
- `TELEMETRY_DIR` propagation

Child scripts must inherit these values.

No child script may generate its own new `RUN_ID` for the same run.

---

## 13. Proposed environment contract

```
RUN_ID=run_<utc>_<suffix>
TRACE_ID=trace_<utc>_<suffix>
PIPELINE_NAME=smart_city_air_quality
TELEMETRY_DIR=data/telemetry
CITY=<city>
POLLUTANT=<pollutant>
BATCH_MONTH=<yyyy-mm>
SOURCE_NAME=crate_historical_proxy
```

Not every variable must be present for every step, but the first four are mandatory.

---

## 14. Acceptance test for phase 1

A single-path rollout is accepted only if one test run produces:

### Raw layer

- `run_events.jsonl`
- `span_events.jsonl`
- `metric_events.jsonl`
- `error_events.jsonl`

### Curated layer

- `pipeline_run_log.csv`
- `pipeline_trace.csv`
- `pipeline_metrics.csv`
- `error_log.csv`

### Behavioral checks

- one shared `run_id` across shell and Python
- one shared `trace_id` across shell and Python
- at least one shell span present
- at least one Silver span present
- at least one Gold span present
- at least one metric from each stage present

---

## 15. What not to do in phase 1

Do not:

- instrument every repo script at once
- reframe the repo as an observability platform
- move transformation logic into bash
- wait for full OTel before adding traces
- use rollup as a replacement for runtime emission
- stage generated telemetry artifacts in git

---

## 16. Deliverables at the end of phase 1

Phase 1 is complete only when the repo has:

- helper layer in shell and Python
- runtime event sinks emitting during execution
- one end-to-end instrumented path
- curated rollup outputs generated from raw runtime events
- basic failure handling for core telemetry sink failure

---

## 17. Exit criteria

Phase 1 may proceed to full pipeline expansion only when:

1. single-path execution is stable
2. helper APIs are not changing on every run
3. raw event schemas are stable enough for rollup
4. curated trace and run log outputs are readable and correct
5. the branch can demonstrate runtime observability without relying on post-run guessing