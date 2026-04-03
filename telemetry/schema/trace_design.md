# Trace Design

- one run = one trace_id
- one step = one span_id
- use metrics + structured logs + run spans first
- raw evidence lives in data/telemetry/runtime/
- curated summaries live in data/telemetry/curated/
