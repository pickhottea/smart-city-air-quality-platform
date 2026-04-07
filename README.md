## Batch log policy

Runtime telemetry under `data/telemetry/runtime/` and `data/telemetry/curated/` is the canonical execution evidence for pipeline runs.

Human-readable batch logs are secondary operator-facing artifacts and must be stored under:

`logs/backfill/`

Rules:

- one batch run produces one operator log file
- `logs/backfill/latest.log` points to the most recent run
- legacy root-level `batch_run*.log` files should be moved to `logs/backfill/archive/`
- operator logs are convenience outputs and must not replace runtime telemetry artifacts