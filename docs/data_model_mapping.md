# Data Model Mapping

## Source table

`mtairquality.etairqualityobserved`

## Source-to-canonical mapping

| source field       | canonical field      | notes |
|-------------------|----------------------|-------|
| fiware_servicepath| city_servicepath     | `/apba` or `/torrepacheco` |
| time_index        | time_index           | event timestamp in milliseconds |
| co                | value                | when pollutant = CO |
| no2               | value                | when pollutant = NO2 |
| o3                | value                | when pollutant = O3 |
| so2               | value                | when pollutant = SO2 |

## Canonical silver schema

| field              | type      | description |
|-------------------|-----------|-------------|
| city              | string    | normalized city identifier |
| pollutant         | string    | co, no2, o3, so2 |
| time_index        | long      | original event timestamp in milliseconds |
| observed_at       | timestamp | UTC timestamp derived from time_index |
| value             | double    | pollutant measurement value |
| source_name       | string    | crate_grafana_proxy |
| source_servicepath| string    | original FIWARE service path |
| qc_status         | string    | pass, flagged, rejected |
| qc_flags          | string    | comma-separated rule hits |

## Normalization rule

Bronze files keep sparse pollutant-specific extracts.
Silver converts them into a unified long format:
- one row per city
- one row per pollutant
- one row per timestamped value
