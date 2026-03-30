# Naming conventions

## General
- Use snake_case for directories, files, and canonical field names.
- Use lowercase for pollutant fields: co, no2, o3, so2, pm10, pm25.
- Use UTC timestamps in filenames and run identifiers.

## Cities
- torre_pacheco
- molina_de_segura

## Entity types
- air_quality_observed

## Raw files
- {city}_{entity_type}_{run_ts}.json

## Manifest files
- {city}_{entity_type}_{run_ts}.manifest.json

## Canonical fields
- entity_id
- entity_type
- city
- date_observed
- date_modified
- station_id
- latitude
- longitude
- co
- no2
- o3
- so2
- pm10
- pm25
- air_quality_index
- air_quality_level
- reliability
- fiware_service
- fiware_servicepath
- ingestion_ts
- run_id

## Telemetry
- pipeline_run_log
- pipeline_trace_spans
- quality_metrics_daily
- freshness_metrics_daily
- error_log

## Metrics
- ingestion_runs_total
- rows_ingested_total
- rows_flagged_total
- freshness_lag_minutes
- duplicate_rows_total
- pipeline_duration_seconds
