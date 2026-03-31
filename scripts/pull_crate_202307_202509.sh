#!/usr/bin/env bash

set -euo pipefail

cities=("apba" "torrepacheco")
pollutants=("co" "no2" "o3" "so2")

months=()

# 2023-07 → 2023-12
for m in 07 08 09 10 11 12; do
  months+=("2023-$m")
done

# 2024 whole year
for m in $(seq -w 1 12); do
  months+=("2024-$m")
done

# 2025-01 → 2025-09
for m in $(seq -w 1 9); do
  months+=("2025-$m")
done

for city in "${cities[@]}"; do
  for pollutant in "${pollutants[@]}"; do
    for month in "${months[@]}"; do
      echo "===================================="
      echo "$city $pollutant $month"
      bash scripts/pull_crate_month.sh "$city" "$pollutant" "$month"
    done
  done
done
