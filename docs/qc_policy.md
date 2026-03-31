# QC Policy

## Scope

This policy applies to Bronze and Silver air-quality data for:
- Torre Pacheco
- APBA
- 2023-2025
- CO, NO2, O3, SO2

## QC rule families

### 1. Required field checks
- time_index must be present
- value must be present
- pollutant must be present
- city must be present

### 2. Value range checks
- value must be >= 0
- pollutant-specific upper thresholds will be reviewed during Silver profiling
- extreme values are flagged, not immediately deleted

### 3. Duplicate checks
- duplicate records are rows with the same city, pollutant, and time_index
- duplicates are flagged for Silver deduplication

### 4. Freshness checks
- not used as a blocking rule for historical backfill
- used later for monitoring incremental runs

### 5. Outlier checks
- sudden spikes and extreme values are flagged
- flagged values remain available for analysis until reviewed

## QC outcomes

- pass
- flagged
- rejected

## v1 handling policy

- Bronze keeps raw extracted values
- Silver applies flags and normalization
- Gold uses cleaned and deduplicated Silver records
- Historical backfill and comparison are limited to the common city overlap window: 2023-07 to 2025-09.
