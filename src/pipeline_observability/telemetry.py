from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

RUN_STATUSES = {"running", "success", "failed", "partial_success", "incomplete"}
SPAN_STATUSES = {"started", "success", "failed", "skipped"}
FRESHNESS_STATUSES = {"fresh", "delayed", "stale", "unknown"}
QUALITY_STATUSES = {"pass", "warn", "fail"}

RUN_EVENT_TYPES = {
    "run_initialized",
    "run_started",
    "run_completed",
    "run_failed",
    "run_incomplete",
}

CORE_RUNTIME_SINKS = {
    "run_events": "run_events.jsonl",
    "span_events": "span_events.jsonl",
    "metric_events": "metric_events.jsonl",
    "error_events": "error_events.jsonl",
}


class TelemetryError(RuntimeError):
    """Base telemetry exception."""


class TelemetryValidationError(TelemetryError):
    """Raised when required telemetry state is missing or invalid."""


class TelemetrySinkError(TelemetryError):
    """Raised when a required telemetry sink cannot be written."""


@dataclass(frozen=True)
class SpanContext:
    span_id: str
    parent_span_id: Optional[str]
    step_name: str
    started_at: str
    city: Optional[str] = None
    pollutant: Optional[str] = None
    batch_month: Optional[str] = None
    source_name: Optional[str] = None


class TelemetryClient:
    """Filesystem-first telemetry emitter for runtime JSONL events.

    This helper mirrors the shared shell/Python contract:
    - append-only raw runtime events
    - strict run/trace identifiers
    - one immediate retry for core sink writes
    - canonical status vocabularies
    """

    def __init__(
        self,
        run_id: str,
        trace_id: str,
        pipeline_name: str = "smart_city_air_quality",
        telemetry_dir: str = "data/telemetry",
    ) -> None:
        if not run_id:
            raise TelemetryValidationError("run_id is required")
        if not trace_id:
            raise TelemetryValidationError("trace_id is required")
        if not pipeline_name:
            raise TelemetryValidationError("pipeline_name is required")

        self.run_id = run_id
        self.trace_id = trace_id
        self.pipeline_name = pipeline_name
        self.telemetry_dir = Path(telemetry_dir)
        self.runtime_dir = self.telemetry_dir / "runtime"
        self.curated_dir = self.telemetry_dir / "curated"
        self._init_dirs_and_sinks()

    @staticmethod
    def utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def generate_id(prefix: str) -> str:
        suffix = uuid.uuid4().hex[:8]
        return f"{prefix}_{suffix}"

    def _init_dirs_and_sinks(self) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        for filename in CORE_RUNTIME_SINKS.values():
            path = self.runtime_dir / filename
            path.touch(exist_ok=True)

    def _validate_status(self, status: str, allowed: set[str], label: str) -> None:
        if status not in allowed:
            raise TelemetryValidationError(f"invalid {label}: {status!r}")

    def _runtime_path(self, sink_name: str) -> Path:
        try:
            filename = CORE_RUNTIME_SINKS[sink_name]
        except KeyError as exc:
            raise TelemetryValidationError(f"unknown sink_name: {sink_name!r}") from exc
        return self.runtime_dir / filename

    def _append_jsonl(self, sink_name: str, payload: Dict[str, Any]) -> None:
        path = self._runtime_path(sink_name)
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)

        last_error: Optional[Exception] = None
        for _ in range(2):
            try:
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(serialized)
                    handle.write("\n")
                    handle.flush()
                return
            except OSError as exc:
                last_error = exc

        self._handle_core_sink_failure(sink_name=sink_name, error_message=str(last_error))

    def _handle_core_sink_failure(self, sink_name: str, error_message: str) -> None:
        if sink_name != "error_events":
            fallback_payload = {
                "event_ts": self.utc_now(),
                "run_id": self.run_id,
                "trace_id": self.trace_id,
                "pipeline_name": self.pipeline_name,
                "step_name": "telemetry",
                "error_type": "telemetry_sink_failure",
                "error_message": f"{sink_name}: {error_message}",
                "span_id": None,
                "city": None,
                "pollutant": None,
                "batch_month": None,
                "source_name": None,
                "retryable": False,
                "severity": "critical",
            }
            try:
                error_path = self._runtime_path("error_events")
                with error_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(fallback_payload, ensure_ascii=False, sort_keys=True))
                    handle.write("\n")
                    handle.flush()
            except OSError:
                pass

        raise TelemetrySinkError(
            f"failed to write core telemetry sink {sink_name!r} after one retry: {error_message}"
        )

    def emit_run_event(
        self,
        event_type: str,
        status: str,
        error_message: Optional[str] = None,
        artifact_path: Optional[str] = None,
    ) -> None:
        if event_type not in RUN_EVENT_TYPES:
            raise TelemetryValidationError(f"invalid run event_type: {event_type!r}")
        self._validate_status(status, RUN_STATUSES, "run status")
        payload = {
            "event_ts": self.utc_now(),
            "event_type": event_type,
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "pipeline_name": self.pipeline_name,
            "status": status,
            "error_message": error_message,
            "artifact_path": artifact_path,
        }
        self._append_jsonl("run_events", payload)

    def start_span(
        self,
        step_name: str,
        parent_span_id: Optional[str] = None,
        city: Optional[str] = None,
        pollutant: Optional[str] = None,
        batch_month: Optional[str] = None,
        source_name: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> SpanContext:
        if not step_name:
            raise TelemetryValidationError("step_name is required for span_start")

        context = SpanContext(
            span_id=span_id or self.generate_id("span"),
            parent_span_id=parent_span_id,
            step_name=step_name,
            started_at=self.utc_now(),
            city=city,
            pollutant=pollutant,
            batch_month=batch_month,
            source_name=source_name,
        )
        payload = {
            "event_ts": context.started_at,
            "event_type": "span_start",
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "span_id": context.span_id,
            "parent_span_id": context.parent_span_id,
            "pipeline_name": self.pipeline_name,
            "step_name": context.step_name,
            "status": "started",
            "city": context.city,
            "pollutant": context.pollutant,
            "batch_month": context.batch_month,
            "source_name": context.source_name,
            "rows_in": None,
            "rows_out": None,
            "artifact_path": None,
            "error_message": None,
        }
        self._append_jsonl("span_events", payload)
        return context

    def end_span(
        self,
        span_id: str,
        step_name: str,
        status: str,
        parent_span_id: Optional[str] = None,
        rows_in: Optional[int] = None,
        rows_out: Optional[int] = None,
        artifact_path: Optional[str] = None,
        error_message: Optional[str] = None,
        city: Optional[str] = None,
        pollutant: Optional[str] = None,
        batch_month: Optional[str] = None,
        source_name: Optional[str] = None,
    ) -> None:
        if not span_id:
            raise TelemetryValidationError("span_id is required for span_end")
        if not step_name:
            raise TelemetryValidationError("step_name is required for span_end")
        self._validate_status(status, SPAN_STATUSES - {"started"}, "span status")

        payload = {
            "event_ts": self.utc_now(),
            "event_type": "span_end",
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "pipeline_name": self.pipeline_name,
            "step_name": step_name,
            "status": status,
            "city": city,
            "pollutant": pollutant,
            "batch_month": batch_month,
            "source_name": source_name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "artifact_path": artifact_path,
            "error_message": error_message,
        }
        self._append_jsonl("span_events", payload)

    def emit_metric(
        self,
        metric_name: str,
        metric_value: float | int,
        metric_unit: str,
        step_name: Optional[str] = None,
        city: Optional[str] = None,
        pollutant: Optional[str] = None,
        batch_month: Optional[str] = None,
        source_name: Optional[str] = None,
    ) -> None:
        if not metric_name:
            raise TelemetryValidationError("metric_name is required")
        if metric_unit is None or metric_unit == "":
            raise TelemetryValidationError("metric_unit is required")

        payload = {
            "event_ts": self.utc_now(),
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "pipeline_name": self.pipeline_name,
            "step_name": step_name,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "metric_unit": metric_unit,
            "city": city,
            "pollutant": pollutant,
            "batch_month": batch_month,
            "source_name": source_name,
        }
        self._append_jsonl("metric_events", payload)

    def emit_error(
        self,
        step_name: str,
        error_type: str,
        error_message: str,
        span_id: Optional[str] = None,
        city: Optional[str] = None,
        pollutant: Optional[str] = None,
        batch_month: Optional[str] = None,
        source_name: Optional[str] = None,
        retryable: bool = False,
        severity: str = "error",
    ) -> None:
        if not step_name:
            raise TelemetryValidationError("step_name is required")
        if not error_type:
            raise TelemetryValidationError("error_type is required")
        if not error_message:
            raise TelemetryValidationError("error_message is required")

        payload = {
            "event_ts": self.utc_now(),
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "pipeline_name": self.pipeline_name,
            "step_name": step_name,
            "error_type": error_type,
            "error_message": error_message,
            "span_id": span_id,
            "city": city,
            "pollutant": pollutant,
            "batch_month": batch_month,
            "source_name": source_name,
            "retryable": retryable,
            "severity": severity,
        }
        self._append_jsonl("error_events", payload)

    def fail_core_sink(self, sink_name: str, error_message: str) -> None:
        self._handle_core_sink_failure(sink_name=sink_name, error_message=error_message)


def init_telemetry(
    run_id: str,
    trace_id: str,
    pipeline_name: str = "smart_city_air_quality",
    telemetry_dir: str = "data/telemetry",
) -> TelemetryClient:
    return TelemetryClient(
        run_id=run_id,
        trace_id=trace_id,
        pipeline_name=pipeline_name,
        telemetry_dir=telemetry_dir,
    )
