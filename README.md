# Smart City Air Quality Platform

Batch-first air-quality data platform for smart-city analytics.

## Project purpose

This repository builds a **batch pipeline** for air-quality data across cities.

The v1 goal is to produce a pipeline that can:

- ingest historical Bronze extracts
- standardize data into a canonical Silver table
- apply row-level quality checks
- build Gold hourly analytics outputs
- emit runtime telemetry and roll it up into curated evidence

This repository is **not** trying to become a full observability platform in v1.
Telemetry exists to make pipeline execution observable, reviewable, and operationally valid.

## Pipeline scope

### Bronze

Raw city-pollutant-month extracts under:

`data/bronze/crate/`

Bronze keeps source values with minimal filtering.

### Silver

Canonical long-format air-quality observations under:

`data/silver/air_quality_long/`

Silver is responsible for:

- schema normalization
- path-derived city and pollutant recovery
- timestamp conversion
- quality checks
- duplicate handling
- QC status and QC flag assignment

### Gold

Analytical hourly outputs under:

- `data/gold/hourly_air_quality/`
- `data/gold/hourly_city_comparison/`
- `data/gold/hourly_coverage/`

Gold is responsible for:

- hourly aggregation
- cross-city comparison
- coverage/completeness style outputs

### Telemetry rollup

Curated runtime evidence under:

- `data/telemetry/curated/pipeline_run_log.csv/`
- `data/telemetry/curated/pipeline_trace.csv/`
- `data/telemetry/curated/pipeline_metrics.csv/`
- `data/telemetry/curated/quality_metrics_daily.csv/`
- `data/telemetry/curated/freshness_metrics.csv/`
- `data/telemetry/curated/error_log.csv/`

## Execution order

Run the pipeline in this order:

1. Bronze ingestion produces files under `data/bronze/crate/`
2. Silver build:
   ```bash
   python spark/jobs/build_silver_table.py
   ```
3. Gold build:
   ```bash
   python spark/jobs/build_gold_hourly_table.py
   ```
4. Telemetry rollup:
   ```bash
   python spark/jobs/build_telemetry_metrics.py
   ```

## Run location rule

**Always run pipeline jobs from the repository root.**

Correct:

```bash
cd ~/smart-city-air-quality-platform
python spark/jobs/build_silver_table.py
python spark/jobs/build_gold_hourly_table.py
python spark/jobs/build_telemetry_metrics.py
```

Do **not** run jobs from `spark/jobs/`.
If jobs are launched from that directory, relative `data/...` paths may be written into `spark/jobs/data/...` by mistake.

## Runtime telemetry policy

Runtime telemetry under:

- `data/telemetry/runtime/`
- `data/telemetry/curated/`

is the **canonical execution evidence** for pipeline runs.

These artifacts are generated outputs.
They must not replace the pipeline’s business outputs, and they should not be committed as source files.

## Batch log policy

Human-readable batch logs are **secondary, operator-facing artifacts**.
They are convenience outputs for people, not the source of truth for runtime evidence.

Store operator logs under:

`logs/backfill/`

Rules:

- one batch run produces one operator log file
- `logs/backfill/latest.log` points to the most recent run
- legacy root-level `batch_run*.log` files should be moved to `logs/backfill/archive/`
- operator logs are convenience outputs and must not replace runtime telemetry artifacts

## Branch policy

- active pipeline development belongs on `feature/smart-city-core-pipeline`
- `main` stays clean until reviewed and merged
- runtime-observability design work defines the telemetry contract and helper model
- the core pipeline branch consumes that telemetry contract during normal execution

## Notes for local development

- use the repository `.venv`
- run from repo root only
- generated Spark outputs and telemetry evidence are expected under `data/`
- if `spark/jobs/data/` appears, it was likely produced by launching jobs from the wrong directory and can be removed

## Minimal validation checklist

After a successful local run, verify:

```bash
find data/silver/air_quality_long -type f | head
find data/gold -maxdepth 2 -type f | head -20
find data/telemetry/curated -maxdepth 2 -type f | head -20
```

A clean working tree plus those outputs means the batch pipeline is in a healthy local state.
