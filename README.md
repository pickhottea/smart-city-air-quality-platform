# Smart City Air Quality Platform

## Overview

This project builds an end-to-end data platform for air quality data using open smart city datasets.

It covers the full pipeline:

- Data ingestion from CrateDB (FIWARE-based open data APIs)
- Standardization into a unified schema
- Data quality-aware transformation
- Analytical tables for cross-city comparison

The goal is not just to collect data, but to evaluate whether open air quality data is **fit for analysis and comparison across cities**.

---

## Why These Cities?

We selected:

- **APBA**
- **Torre Pacheco**

These two cities exhibit different data characteristics:

- Different sensor coverage density
- Different pollutant availability
- Different temporal continuity

This allows us to test:

- Data completeness
- Cross-city comparability
- Stability of ingestion pipelines

---

## Why 2023–2025?

The selected time range:

**2023-07 to 2025-09**

Reasons:

- Avoid very early historical data with inconsistent schema
- Focus on recent data with stable ingestion patterns
- Ensure sufficient volume for time-series analysis
- Align both cities on overlapping data availability windows

---

## Data Architecture

### Bronze Layer (Raw Ingestion)

Data is ingested from CrateDB using monthly extraction:

data/raw/crate/{city}/{pollutant}/{YYYY-MM}.json
data/bronze/crate/{city}/{pollutant}/{YYYY-MM}.csv


Characteristics:

- Partitioned by city / pollutant / month
- Raw API response preserved
- Minimal transformation

---

### Silver Layer (Standardized Long Format)

All pollutants are normalized into a unified schema:

data/silver/air_quality_long/


Schema:

| column       | description                      |
|-------------|----------------------------------|
| time_index   | epoch timestamp (ms)             |
| observed_at  | timestamp                        |
| city         | city name                        |
| pollutant    | pollutant type (co, no2, o3...)  |
| value        | measurement value                |

Transformations:

- Pivot wide → long
- Remove null pollutant values
- Normalize timestamps
- Union across cities

---

### Gold Layer (Analytical Tables)

#### 1. Hourly Aggregation

data/gold/city_hourly_air_quality/

- Aggregated by hour
- Average pollutant values
- Per city + pollutant

---

#### 2. Cross-City Comparison

data/gold/city_hourly_comparison/

- Align cities on same timestamp
- Enable direct comparison

---

#### 3. Coverage Table

data/gold/city_hourly_coverage/

- Tracks data availability
- Identifies missing data patterns

---

## Key Observations

- Pollutant availability differs significantly across cities
- Some city-pollutant combinations have **zero data in certain periods**
- Data density varies heavily by time
- Multiple records may exist for the same timestamp

---

## What This Project Demonstrates

This project demonstrates how to:

- Build a reproducible ingestion pipeline from open APIs
- Normalize heterogeneous environmental data
- Design scalable data layers (Bronze / Silver / Gold)
- Evaluate real-world data usability beyond basic validity checks

It highlights the gap between:

- **"data exists"**
- vs
- **"data is usable for analysis"**

---

## Tech Stack

- Bash (data ingestion)
- Spark (data transformation)
- Parquet (storage format)
- CrateDB (data source)

---

## Next Steps

- Add data quality metrics (null rate, coverage rate)
- Extend to more cities
- Build dashboard / visualization layer
