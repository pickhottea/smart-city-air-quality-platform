# Source Discovery

## Confirmed historical source

Historical air-quality chart data is retrieved through the Grafana proxy endpoint:

`/api/datasources/proxy/1/_sql`

This endpoint is backed by CrateDB.

## Confirmed source table

`mtairquality.etairqualityobserved`

## Confirmed characteristics

- shared multi-city table
- historical time-series source
- event timestamp field: `time_index`
- pollutant fields include:
  - `co`
  - `no2`
  - `o3`
  - `so2`

## Confirmed city service paths

- `/apba`
- `/torrepacheco`

## Coverage decision for project v1

Common comparable time window across APBA and Torre Pacheco:

- 2023-07 to 2025-09

### APBA
- CO available
- NO2 available
- O3 available
- SO2 available

### Torre Pacheco
- CO available
- NO2 available
- O3 available
- SO2 available

## Project source strategy

The project uses Crate historical data as the primary source for Bronze ingestion.

Current Orion snapshot data remains useful for future extensions, but is not the primary v1 ingestion source.
