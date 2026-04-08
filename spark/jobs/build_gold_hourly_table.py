from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline_observability.telemetry import TelemetryClient, init_telemetry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold hourly air-quality aggregates from Silver parquet."
    )
    parser.add_argument("--input", default="data/silver/air_quality_long")
    parser.add_argument("--output", default="data/gold/hourly_air_quality")
    parser.add_argument("--comparison-output", default="data/gold/hourly_city_comparison")
    parser.add_argument("--coverage-output", default="data/gold/hourly_coverage")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--pipeline-name", default="smart_city_air_quality")
    parser.add_argument("--source-name", default="silver_air_quality_long")
    parser.add_argument("--telemetry-dir", default="data/telemetry")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--trace-id", default=None)
    parser.add_argument("--app-name", default="build_gold_hourly_table")
    return parser.parse_args()


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


def build_hourly_gold(silver_df):
    clean_df = silver_df.filter(F.col("qc_status") != F.lit("rejected"))

    return (
        clean_df
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("observed_at")))
        .groupBy("hour_bucket", "city", "pollutant")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count(F.lit(1)).alias("record_count"),
            F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0)).alias("flagged_count"),
        )
        .withColumn(
            "flagged_rate",
            F.when(F.col("record_count") > 0, F.col("flagged_count") / F.col("record_count")).otherwise(F.lit(None))
        )
    )


def build_city_comparison(hourly_df):
    return (
        hourly_df
        .groupBy("hour_bucket", "pollutant")
        .agg(
            F.countDistinct("city").alias("city_count"),
            F.avg("avg_value").alias("cross_city_avg_value"),
            F.min("avg_value").alias("cross_city_min_value"),
            F.max("avg_value").alias("cross_city_max_value"),
        )
        .withColumn(
            "cross_city_range",
            F.col("cross_city_max_value") - F.col("cross_city_min_value")
        )
    )


def build_hourly_coverage(silver_df):
    clean_df = silver_df.filter(F.col("qc_status") != F.lit("rejected"))

    return (
        clean_df
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("observed_at")))
        .groupBy("hour_bucket", "city", "pollutant")
        .agg(
            F.count(F.lit(1)).alias("records_available"),
            F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0)).alias("records_flagged"),
            F.sum(F.when(F.col("qc_status") == "pass", 1).otherwise(0)).alias("records_pass"),
        )
    )


def main() -> int:
    args = parse_args()
    run_id = args.run_id or TelemetryClient.generate_id("run")
    trace_id = args.trace_id or TelemetryClient.generate_id("trace")

    telemetry = init_telemetry(
        run_id=run_id,
        trace_id=trace_id,
        pipeline_name=args.pipeline_name,
        telemetry_dir=args.telemetry_dir,
    )

    telemetry.emit_run_event("run_initialized", "running")
    telemetry.emit_run_event("run_started", "running")

    root_span = telemetry.start_span(
        step_name="build_gold_hourly_table",
        source_name=args.source_name,
    )

    spark = None
    try:
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Silver input not found: {args.input}")

        spark = spark_session(args.app_name)

        read_span = telemetry.start_span(
            "read_silver",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )
        silver_df = spark.read.parquet(args.input)
        telemetry.end_span(
            read_span.span_id,
            "read_silver",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        hourly_span = telemetry.start_span(
            "build_gold_hourly",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )
        hourly_df = build_hourly_gold(silver_df)
        telemetry.end_span(
            hourly_span.span_id,
            "build_gold_hourly",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        comparison_span = telemetry.start_span(
            "build_city_comparison",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )
        comparison_df = build_city_comparison(hourly_df)
        telemetry.end_span(
            comparison_span.span_id,
            "build_city_comparison",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        coverage_span = telemetry.start_span(
            "build_coverage",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )
        coverage_df = build_hourly_coverage(silver_df)
        telemetry.end_span(
            coverage_span.span_id,
            "build_coverage",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        write_span = telemetry.start_span(
            "write_gold",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        (
            hourly_df
            .coalesce(1)
            .write
            .mode(args.mode)
            .parquet(args.output)
        )

        (
            comparison_df
            .coalesce(1)
            .write
            .mode(args.mode)
            .parquet(args.comparison_output)
        )

        (
            coverage_df
            .coalesce(1)
            .write
            .mode(args.mode)
            .parquet(args.coverage_output)
        )

        telemetry.emit_metric(
            "gold_write_completed",
            1,
            "run",
            step_name="write_gold",
            source_name=args.source_name,
        )

        telemetry.end_span(
            write_span.span_id,
            "write_gold",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            artifact_path=args.output,
            source_name=args.source_name,
        )

        telemetry.end_span(
            root_span.span_id,
            "build_gold_hourly_table",
            "success",
            artifact_path=args.output,
            source_name=args.source_name,
        )
        telemetry.emit_run_event("run_completed", "success", artifact_path=args.output)
        return 0

    except Exception as exc:
        message = str(exc)
        telemetry.emit_error(
            step_name="build_gold_hourly_table",
            error_type=type(exc).__name__,
            error_message=message,
            span_id=root_span.span_id,
            source_name=args.source_name,
            retryable=False,
            severity="critical",
        )
        telemetry.end_span(
            root_span.span_id,
            "build_gold_hourly_table",
            "failed",
            artifact_path=args.output,
            error_message=message,
            source_name=args.source_name,
        )
        telemetry.emit_run_event(
            "run_failed",
            "failed",
            error_message=message,
            artifact_path=args.output,
        )
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())