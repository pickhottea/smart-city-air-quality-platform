# Full Pipeline Extension Plan
Repo: `smart-city-air-quality-platform`
Status: Draft v0.1
Scope: Expand runtime observability from one path to all relevant pipeline execution paths

---

## 1. Purpose

This document defines how runtime observability expands from the first instrumented path to the rest of the pipeline.

The purpose is to move from:

- one validated observability path

to:

- end-to-end observability coverage across all operationally relevant scripts

without destabilizing the existing Smart City pipeline.

---

## 2. Guiding rule

The main project remains:

- air quality ingestion
- standardization
- quality-aware transformation
- analytics-ready outputs

Observability becomes a runtime capability of that pipeline.
It does not replace the repo narrative.

---

## 3. Expansion order

The rollout order is:

### Phase A
Already done or in progress:
- `scripts/linux/pull_crate_month.sh`
- `spark/jobs/build_silver_table.py`
- `spark/jobs/build_gold_hourly_table.py`

### Phase B
Extend orchestration coverage to:
- `scripts/linux/pull_crate_202307_202509.sh`

### Phase C
Extend summary/rollup behavior to:
- `spark/jobs/build_telemetry_metrics.py`

### Phase D
Add quality/freshness curated outputs from raw events and Gold/Silver results.

This sequence is mandatory.
Do not start by instrumenting rollup before the runtime path is stable.

---

## 4. Script inventory and observability role

### 4.1 `scripts/linux/pull_crate_month.sh`
Role:
- atomic ingestion unit
- source-level runtime telemetry producer

### 4.2 `scripts/linux/pull_crate_202307_202509.sh`
Role:
- orchestration wrapper over many month-level runs
- fan-out telemetry producer
- aggregate retry/error surface

### 4.3 `spark/jobs/build_silver_table.py`
Role:
- Spark standardization stage
- QC-related telemetry producer

### 4.4 `spark/jobs/build_gold_hourly_table.py`
Role:
- Spark aggregation stage
- downstream analytical output telemetry producer

### 4.5 `spark/jobs/build_telemetry_metrics.py`
Role:
- rollup
- compaction
- consistency checking
- curated telemetry generation

Not role:
- sole source of telemetry truth

---

## 5. Expansion target architecture

### Runtime/raw layer

```text
data/telemetry/runtime/run_events.jsonl
data/telemetry/runtime/span_events.jsonl
data/telemetry/runtime/metric_events.jsonl
data/telemetry/runtime/error_events.jsonl
```

### Curated layer

```
data/telemetry/curated/pipeline_run_log.csv
data/telemetry/curated/pipeline_trace.csv
data/telemetry/curated/pipeline_metrics.csv
data/telemetry/curated/quality_metrics_daily.csv
data/telemetry/curated/freshness_metrics.csv
data/telemetry/curated/error_log.csv
```

## 6. Full script emission map

### 6.1 `scripts/linux/pull_crate_month.sh`

### Emits run events

- yes, if used as entrypoint

### Emits spans

- `pipeline_run` optionally
- `download_data`
- `write_bronze`

### Emits metrics

- `ingestion_runs_total`
- `rows_downloaded_total`
- `rows_bronze_written_total`
- `download_duration_ms`

### Emits errors

- API/proxy failure
- timeout
- malformed response
- file write failure
- telemetry sink failure

### 6.2 `scripts/linux/pull_crate_202307_202509.sh`

### Emits run events

- yes, for orchestration-level wrapper run

### Emits spans

- `pipeline_run`
- `monthly_ingestion_batch`
- child spans for each `city × pollutant × batch_month`
- optional retry span or retry metric

### Emits metrics

- `monthly_pull_attempts_total`
- `monthly_pull_success_total`
- `monthly_pull_failed_total`
- `retry_attempts_total`

### Emits errors

- child run failure
- retry exhaustion
- orchestration-level sink failure

### 6.3 `spark/jobs/build_silver_table.py`

### Emits spans

- `read_bronze`
- `normalize_schema`
- `cast_types`
- `deduplicate`
- `qc_range_check`
- `qc_stale_check`
- `qc_outlier_check`
- `write_silver`

### Emits metrics

- `rows_bronze_read_total`
- `rows_silver_written_total`
- `duplicate_rows_total`
- `flagged_records_total`
- `quality_checks_failed_total`
- `spark_stage_duration_ms`

### Emits errors

- schema error
- type conversion error
- QC rule failure
- silver write failure
- telemetry sink failure

### 6.4 `spark/jobs/build_gold_hourly_table.py`

### Emits spans

- `read_silver`
- `build_gold_hourly`
- `build_city_comparison`
- `build_coverage`
- `write_gold`

### Emits metrics

- `rows_silver_read_total`
- `rows_gold_hourly_written_total`
- `rows_gold_comparison_written_total`
- `rows_gold_coverage_written_total`
- `city_hourly_updates_total`

### Emits errors

- silver read failure
- aggregation failure
- output write failure
- telemetry sink failure

### 6.5 `spark/jobs/build_telemetry_metrics.py`

### Emits spans

- `rollup_run_events`
- `rollup_span_events`
- `rollup_metric_events`
- `build_pipeline_run_log`
- `build_pipeline_trace`
- `build_pipeline_metrics`
- `build_quality_metrics_daily`
- `build_freshness_metrics`
- `build_error_log`

### Emits metrics

- `rollup_rows_processed_total`
- `rollup_duration_ms`
- `rollup_errors_total`

### Emits errors

- corrupted raw event file
- unmatched span pair
- malformed metric event
- curated file write failure

---

## 7. Full span hierarchy

Recommended span tree:

```
pipeline_run
├── monthly_ingestion_batch
│   ├── download_data
│   └── write_bronze
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
├── write_gold
├── build_quality_metrics_daily
├── build_freshness_metrics
└── rollup_telemetry
```

Flat spans remain acceptable where nesting is hard to introduce immediately, but future compatibility with parent-child relationships must be preserved.

---

## 8. Metric vocabulary for full extension

### Ingestion metrics

- `ingestion_runs_total`
- `rows_downloaded_total`
- `rows_bronze_written_total`
- `monthly_pull_attempts_total`
- `monthly_pull_success_total`
- `monthly_pull_failed_total`
- `retry_attempts_total`

### Silver/QC metrics

- `rows_bronze_read_total`
- `rows_silver_written_total`
- `duplicate_rows_total`
- `flagged_records_total`
- `quality_checks_failed_total`
- `qc_range_failures_total`
- `qc_stale_failures_total`
- `qc_outlier_failures_total`

### Gold metrics

- `rows_silver_read_total`
- `rows_gold_hourly_written_total`
- `rows_gold_comparison_written_total`
- `rows_gold_coverage_written_total`
- `city_hourly_updates_total`

### Freshness and reliability metrics

- `freshness_lag_minutes`
- `error_events_total`
- `span_events_total`
- `rollup_errors_total`

Do not add a metric unless it has a clear consumer or review purpose.

---

## 9. Core sink policy across the full pipeline

The following remain core across every stage:

- `run_events.jsonl`
- `span_events.jsonl`
- `metric_events.jsonl`
- `error_events.jsonl`

If a script cannot write a core sink after one immediate retry:

- emit telemetry sink failure where possible
- fail the current run
- propagate failure status upward

This rule applies equally to shell and Python paths.

---

## 10. Context propagation rules

The orchestration layer must propagate:

- `RUN_ID`
- `TRACE_ID`
- `PIPELINE_NAME`
- `TELEMETRY_DIR`

Operational context should be propagated when known:

- `CITY`
- `POLLUTANT`
- `BATCH_MONTH`
- `SOURCE_NAME`

A child script may enrich context.

It must not replace the run or trace identity.

---

## 11. Quality and freshness extension targets

Full pipeline extension should eventually produce curated:

### `quality_metrics_daily.csv`

Target fields:

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

### `freshness_metrics.csv`

Target fields:

- `metric_ts`
- `run_id`
- `city`
- `pollutant`
- `latest_observed_at`
- `latest_ingested_at`
- `freshness_lag_minutes`
- `freshness_status`

These should be derived from runtime evidence plus pipeline outputs, not guessed after the fact.

---

## 12. Failure classification across full extension

### Operational failure

Examples:

- pull failed
- silver transform failed
- gold write failed

Run status:

- `failed`

### Telemetry core failure

Examples:

- cannot write span_events
- cannot write metric_events

Run status:

- `failed`

### Rollup failure with valid raw evidence

Examples:

- curated CSV generation failed
- unmatched span reconstruction issue

Run status:

- `incomplete` or `partial_success`

### Expected skip

Examples:

- a non-applicable step not executed by design

Span status:

- `skipped`

---

## 13. Git and artifact rule

The full extension must preserve the repository rule that generated data artifacts stay out of git.

This includes:

- raw telemetry events
- curated telemetry CSVs
- parquet outputs
- other runtime-generated files

Only these should be committed:

- code
- docs
- helper layer
- `.gitignore`
- optional `.gitkeep`

---

## 14. Operational review checklist

Before full extension is considered done, confirm:

- all relevant scripts emit through helpers only
- no script writes ad hoc telemetry files
- one run can be followed from shell ingestion to Gold aggregation
- metrics exist for ingestion, Silver, and Gold
- trace spans explain timing across stages
- error events identify failures before exit
- rollup does not act as recovery for missing runtime evidence

---

## 15. Exit criteria

Full pipeline extension is complete when:

1. all relevant runtime scripts emit raw telemetry events
2. helper layer is the only emission path
3. run identity is stable across all scripts
4. curated outputs are generated from raw evidence
5. the branch can show one successful run and one failed run with coherent telemetry evidence