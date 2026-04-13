from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "PyYAML is required for build_incident_candidates.py. Install it with `pip install pyyaml`."
    ) from exc


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build incident candidates from curated Smart City telemetry."
    )
    parser.add_argument(
        "--thresholds",
        default="configs/incident_thresholds.yaml",
        help="Path to incident threshold YAML.",
    )
    parser.add_argument(
        "--run-log-input",
        default="data/telemetry/curated/pipeline_run_log.csv",
        help="Directory containing pipeline_run_log CSV output.",
    )
    parser.add_argument(
        "--quality-input",
        default="data/telemetry/curated/quality_metrics_daily.csv",
        help="Directory containing quality_metrics_daily CSV output.",
    )
    parser.add_argument(
        "--error-input",
        default="data/telemetry/curated/error_log.csv",
        help="Directory containing error_log CSV output.",
    )
    parser.add_argument(
        "--output",
        default="data/telemetry/curated/incident_candidates.csv",
        help="Directory to write incident candidate CSV output.",
    )
    parser.add_argument(
        "--app-name",
        default="build_incident_candidates",
        help="Spark application name.",
    )
    return parser.parse_args()


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def load_thresholds(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("Threshold file must parse to a dictionary.")
    return data


def read_csv_dir(spark: SparkSession, path: str) -> DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input path not found: {path}")
    return spark.read.option("header", True).csv(path)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_candidate_id(run_id: str, rule_id: str, seq: int) -> str:
    safe_run = (run_id or "unknown").replace(" ", "_")
    return f"cand_{safe_run}_{rule_id}_{seq:03d}"


def candidate_schema() -> StructType:
    fields = [
        "ticket_candidate_id",
        "run_id",
        "trigger_type",
        "severity",
        "summary",
        "city",
        "pollutant",
        "step_name",
        "error_type",
        "metric_date",
        "owner_role",
        "source_table",
        "evidence_path",
        "opened_at",
        "status",
    ]
    return StructType([StructField(name, StringType(), True) for name in fields])


def normalize_candidate(candidate: dict[str, Any]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for key in candidate_schema().fieldNames():
        value = candidate.get(key)
        out[key] = None if value is None else str(value)
    return out


def build_run_failure_candidates(
    latest_run: dict[str, Any],
    rule_cfg: dict[str, Any],
    opened_at: str,
) -> list[dict[str, Any]]:
    statuses = set(rule_cfg.get("statuses", []))
    if latest_run["status"] not in statuses:
        return []

    summary = (
        f"Latest run {latest_run['run_id']} ended with status={latest_run['status']}. "
        f"error_message={latest_run.get('error_message') or 'null'}"
    )

    return [
        {
            "ticket_candidate_id": make_candidate_id(latest_run["run_id"], "run_failure", 1),
            "run_id": latest_run["run_id"],
            "trigger_type": "run_failure",
            "severity": rule_cfg["severity"],
            "summary": summary,
            "city": None,
            "pollutant": None,
            "step_name": None,
            "error_type": None,
            "metric_date": None,
            "owner_role": rule_cfg["owner_role"],
            "source_table": rule_cfg["source_table"],
            "evidence_path": rule_cfg["evidence_path"],
            "opened_at": opened_at,
            "status": "open_candidate",
        }
    ]


def build_quality_candidates(
    quality_rows: list[dict[str, Any]],
    rule_name: str,
    threshold_field: str,
    metric_name: str,
    rule_cfg: dict[str, Any],
    opened_at: str,
    seq_offset: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    threshold = float(rule_cfg[threshold_field])

    seq = seq_offset
    for row in quality_rows:
        raw_value = row.get(metric_name)
        if raw_value in (None, ""):
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if value <= threshold:
            continue

        seq += 1
        summary = (
            f"{metric_name}={value:.6f} exceeded threshold {threshold:.6f} "
            f"for city={row.get('city')} pollutant={row.get('pollutant')} metric_date={row.get('metric_date')}"
        )
        candidates.append(
            {
                "ticket_candidate_id": make_candidate_id(row["run_id"], rule_name, seq),
                "run_id": row["run_id"],
                "trigger_type": rule_name,
                "severity": rule_cfg["severity"],
                "summary": summary,
                "city": row.get("city"),
                "pollutant": row.get("pollutant"),
                "step_name": None,
                "error_type": None,
                "metric_date": row.get("metric_date"),
                "owner_role": rule_cfg["owner_role"],
                "source_table": rule_cfg["source_table"],
                "evidence_path": rule_cfg["evidence_path"],
                "opened_at": opened_at,
                "status": "open_candidate",
            }
        )

    return candidates


def build_critical_error_candidates(
    error_rows: list[dict[str, Any]],
    rule_cfg: dict[str, Any],
    opened_at: str,
) -> list[dict[str, Any]]:
    allowed = set(rule_cfg.get("severity_equals", []))
    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, Any]] = []

    seq = 0
    for row in error_rows:
        severity = row.get("severity")
        if severity not in allowed:
            continue

        step_name = row.get("step_name") or "unknown_step"
        error_type = row.get("error_type") or "unknown_error"
        key = (step_name, error_type)
        if key in seen:
            continue
        seen.add(key)

        seq += 1
        summary = (
            f"Latest run contains critical error at step={step_name} "
            f"error_type={error_type}: {row.get('error_message') or 'null'}"
        )

        candidates.append(
            {
                "ticket_candidate_id": make_candidate_id(row["run_id"], "critical_error", seq),
                "run_id": row["run_id"],
                "trigger_type": "critical_error",
                "severity": rule_cfg["severity"],
                "summary": summary,
                "city": row.get("city"),
                "pollutant": row.get("pollutant"),
                "step_name": step_name,
                "error_type": error_type,
                "metric_date": None,
                "owner_role": rule_cfg["owner_role"],
                "source_table": rule_cfg["source_table"],
                "evidence_path": rule_cfg["evidence_path"],
                "opened_at": opened_at,
                "status": "open_candidate",
            }
        )

    return candidates


def write_header_only_csv(output_path: Path) -> None:
    schema = candidate_schema().fieldNames()
    tmp_file = output_path.parent / "_incident_candidates_header_only.csv"
    with open(tmp_file, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=schema)
        writer.writeheader()


def main() -> int:
    args = parse_args()
    thresholds = load_thresholds(args.thresholds)
    rules = thresholds.get("rules", {})
    opened_at = utc_now_iso()

    spark = spark_session(args.app_name)
    try:
        run_log_df = read_csv_dir(spark, args.run_log_input)
        quality_df = read_csv_dir(spark, args.quality_input)
        error_df = read_csv_dir(spark, args.error_input)
        
        business_run_log_df = run_log_df.filter(
            ~F.col("run_id").startswith("run_rollup_")
        )

        latest_run_row = (
            business_run_log_df
            .withColumn("finished_at_ts", F.to_timestamp("finished_at"))
            .withColumn("started_at_ts", F.to_timestamp("started_at"))
            .orderBy(F.col("finished_at_ts").desc_nulls_last(), F.col("started_at_ts").desc_nulls_last())
            .limit(1)
            .collect()
        )
        
        if not latest_run_row:
            raise ValueError("No eligible business runs found in pipeline_run_log.csv")

        latest_run = latest_run_row[0].asDict(recursive=True)
        latest_run_id = latest_run["run_id"]

        latest_quality_rows = [
            row.asDict(recursive=True)
            for row in quality_df.filter(F.col("run_id") == F.lit(latest_run_id)).collect()
        ]
        latest_error_rows = [
            row.asDict(recursive=True)
            for row in error_df.filter(F.col("run_id") == F.lit(latest_run_id)).collect()
        ]

        candidates: list[dict[str, Any]] = []

        run_failure_cfg = rules.get("run_failure", {})
        if run_failure_cfg.get("enabled", False):
            candidates.extend(build_run_failure_candidates(latest_run, run_failure_cfg, opened_at))

        flagged_cfg = rules.get("quality_flag_rate", {})
        if flagged_cfg.get("enabled", False):
            candidates.extend(
                build_quality_candidates(
                    latest_quality_rows,
                    "quality_flag_rate",
                    "flagged_rate_gt",
                    "flagged_rate",
                    flagged_cfg,
                    opened_at,
                    seq_offset=0,
                )
            )

        rejected_cfg = rules.get("quality_rejected_rate", {})
        if rejected_cfg.get("enabled", False):
            candidates.extend(
                build_quality_candidates(
                    latest_quality_rows,
                    "quality_rejected_rate",
                    "rejected_rate_gt",
                    "rejected_rate",
                    rejected_cfg,
                    opened_at,
                    seq_offset=0,
                )
            )

        critical_cfg = rules.get("critical_error", {})
        if critical_cfg.get("enabled", False):
            candidates.extend(build_critical_error_candidates(latest_error_rows, critical_cfg, opened_at))

        normalized = [normalize_candidate(c) for c in candidates]

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        schema = candidate_schema()

        if not normalized:
            write_header_only_csv(output_path)
            df = spark.read.option("header", True).schema(schema).csv(
                str(output_path.parent / "_incident_candidates_header_only.csv")
            )
            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .option("header", True)
                .csv(str(output_path))
            )
            (output_path.parent / "_incident_candidates_header_only.csv").unlink(missing_ok=True)
            return 0

        df = spark.createDataFrame(normalized, schema=schema)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(output_path))
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())