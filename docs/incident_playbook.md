# Incident Playbook — Auto Ticket MVP

## Purpose

This playbook defines how to review incident candidates generated from Smart City curated telemetry.

This is **not** a full ticketing platform.
It is a lightweight candidate-generation layer for the implementation repository.

## Scope

The MVP only evaluates the **latest pipeline run** and generates candidates from:

- `pipeline_run_log.csv`
- `quality_metrics_daily.csv`
- `error_log.csv`

`freshness_metrics.csv` is intentionally excluded from active ticket generation in this phase because the current pipeline mode is historical backfill.

## Candidate types

### 1. Run failure candidate
Trigger:
- latest run status is `failed` or `incomplete`

Action:
- review the latest run summary
- confirm the primary failure message
- inspect matching `error_log.csv` rows by `run_id`
- decide whether this is a transient dev failure or an actionable pipeline incident

Default owner:
- `technical_owner`

### 2. Quality flagged-rate candidate
Trigger:
- latest run contains rows where `flagged_rate > 0.10`

Action:
- identify affected `city`, `pollutant`, and `metric_date`
- review whether threshold breaches are expected for historical backfill
- decide whether a QC rule review or source-quality investigation is needed

Default owner:
- `data_steward`

### 3. Quality rejected-rate candidate
Trigger:
- latest run contains rows where `rejected_rate > 0.05`

Action:
- identify affected `city`, `pollutant`, and `metric_date`
- review whether rejection is caused by schema issues, duplicates, or value anomalies
- escalate if the breach is widespread or repeated

Default owner:
- `data_steward`

### 4. Critical error candidate
Trigger:
- latest run contains one or more `error_log.csv` rows where `severity = critical`

Action:
- group errors by `step_name` and `error_type`
- review whether the issue is a one-off development failure or a genuine operational weakness
- prefer one candidate per distinct `step_name + error_type` pair

Default owner:
- `technical_owner`

## Freshness rule policy

Freshness is **disabled** in the MVP.

Reason:
- the current Smart City pipeline mode is historical backfill
- stale freshness values in historical backfill do not indicate a live timeliness incident
- enabling freshness ticketing now would create persistent false positives

Freshness can be re-enabled later for incremental or scheduled recurring runs.

## Triage order

When candidates are generated, review in this order:

1. `run_failure`
2. `critical_error`
3. `quality_rejected_rate`
4. `quality_flag_rate`

This keeps the focus on pipeline-break and evidence-break conditions before softer QC drift.

## Candidate status vocabulary

- `open_candidate`
- `reviewed_no_action`
- `converted_to_ticket`
- `deferred`
- `closed`

## Review notes

When reviewing a candidate, capture:

- whether it came from the latest run only
- whether the evidence is historical-backfill-specific
- whether the issue is a true pipeline weakness or expected during development
- suggested owner
- suggested follow-up date
