# Project Scope

## Project objective

Build a smart-city air-quality data platform focused on ingestion, standardization, quality control, and analytical comparison across cities.

## Primary cities

- Torre Pacheco
- APBA

## Time window

- 2023-07-01 to 2025-09-30

## Coverage note

The target backfill window is 2023-07-01 to 2025-09-30.

Coverage is not fully symmetric across cities and pollutants in later months.
Validated extraction results show that from 2025-02 to 2025-09:

- Torre Pacheco has coverage for CO, NO2, O3, and SO2
- APBA has coverage for SO2
- APBA returns empty results for CO, NO2, and O3 during that period

These cases are treated as partial coverage gaps rather than fatal extraction failures.

## Primary source

Historical air-quality data is extracted from the CrateDB-backed Grafana proxy endpoint:

- `/api/datasources/proxy/1/_sql`

## Primary pollutants

- CO
- NO2
- O3
- SO2

## Data architecture

### Bronze
Raw city-pollutant-month extracts from CrateDB, stored as JSON and CSV. Bronze keeps source values with minimal filtering so Silver can apply explicit QC later.

### Silver
Normalized long-format air-quality observations with canonical fields. This is where threshold-based quality checks and flagging begin:
- city
- pollutant
- time_index
- observed_at
- value
- source_name
- source_servicepath
- qc_status
- qc_flags

### Gold
Analytical tables for:
- hourly pollutant averages
- daily pollutant averages
- city-to-city comparison
- data completeness
- quality summary metrics

## Non-goals for v1

- real-time streaming
- sensor calibration research
- complex policy simulation
- full telemetry platform
