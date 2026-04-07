# QC Policy

## Scope

This policy applies to Bronze and Silver air-quality data for:

- Torre Pacheco
- APBA
- 2023-2025
- CO, NO2, O3, SO2

## QC rule families

### 1. Required field checks

- `time_index` must be present
- `value` must be present
- `pollutant` must be present
- `city` must be present

### 2. Value range checks

- Bronze extraction does not apply pollutant-specific value cutoffs beyond requiring a non-null measurement
- lower and upper range checks are evaluated in Silver
- extreme values are flagged, not immediately deleted
- negative values are retained in Bronze and classified during Silver QC

### 3. Duplicate checks

- duplicate records are rows with the same city, pollutant, and `time_index`
- duplicates are flagged for Silver deduplication

### 4. Freshness checks

- freshness is not used as a blocking rule for historical backfill
- freshness is used later for monitoring incremental runs

### 5. Outlier checks

- sudden spikes and extreme values are flagged
- flagged values remain available for analysis until reviewed

## QC outcomes

- `pass`
- `flagged`
- `rejected`

## v1 handling policy

- Bronze keeps raw extracted values without threshold-based dropping
- Silver applies flags, normalization, and threshold-based classification
- Gold uses cleaned and deduplicated Silver records
- historical backfill and comparison are limited to the common city overlap window: `2023-07` to `2025-09`

## Historical backfill note

Freshness is not a blocking rule for historical backfill.

For backfill runs, freshness is tracked for observability and later incremental monitoring, not for rejecting otherwise valid historical records.

## Bronze handling rule

Bronze is a preservation layer.

Bronze extraction keeps raw source values with minimal filtering and does not apply threshold-based dropping.

This means:

- null and missing measurements may be represented as incomplete raw extracts
- negative or extreme values are not discarded in Bronze solely because they look suspicious
- duplicate candidates may remain in Bronze for later Silver deduplication

## Silver classification rule

Silver is the first enforcement layer for threshold-based QC and classification.

Silver responsibilities include:

- lower and upper range checks
- duplicate detection and deduplication
- outlier and sudden-spike flagging
- threshold-based classification into `pass`, `flagged`, or `rejected`