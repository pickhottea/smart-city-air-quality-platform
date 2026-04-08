from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline_observability.telemetry import init_telemetry


DEFAULT_SILVER_INPUT = "data/silver/air_quality_long"
DEFAULT_GOLD_ROOT = "data/gold"
DEFAULT_HOURLY_OUTPUT = f"{DEFAULT_GOLD_ROOT}/hourly_pollutant_averages"
DEFAULT_CITY_COMPARISON_OUTPUT = f"{DEFAULT_GOLD_ROOT}/city_comparison_hourly"
DEFAULT_COVERAGE_OUTPUT = f"{DEFAULT_GOLD_ROOT}/hourly_completeness"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold hourly air-quality tables from Silver observations.")
    parser.add_argument("--input", default=DEFAULT_SILVER_INPUT)
    parser.add_argument("--hourly-output", default=DEFAULT_HOURLY_OUTPUT)
    parser.add_argument("--city-comparison-output", default=DEFAULT_CITY_COMPARISON_OUTPUT)
    parser.add_argument("--coverage-output", default=DEFAULT_COVERAGE_OUTPUT)
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
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def base_gold_input(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("qc_status") != F.lit("rejected"))
        .withColumn("observed_hour", F.date_trunc("hour", F.col("observed_at")))
        .withColumn("metric_date", F.to_date(F.col("observed_at")))
    )


def build_hourly_averages(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("city", "pollutant", "observed_hour")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count(F.lit(1)).alias("record_count"),
            F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0)).alias("flagged_record_count"),
            F.max("run_id").alias("run_id"),
        )
        .withColumn(
            "quality_status",
            F.when(F.col("flagged_record_count") > 0, F.lit("warn")).otherwise(F.lit("pass")),
        )
    )


def build_city_comparison(hourly_df: DataFrame) -> DataFrame:
    pairs = hourly_df.alias("left").join(
        hourly_df.alias("right"),
        on=[
            F.col("left.pollutant") == F.col("right.pollutant"),
            F.col("left.observed_hour") == F.col("right.observed_hour"),
            F.col("left.city") < F.col("right.city"),
        ],
        how="inner",
    )

    return pairs.select(
        F.col("left.observed_hour").alias("observed_hour"),
        F.col("left.pollutant").alias("pollutant"),
        F.col("left.city").alias("city_left"),
        F.col("right.city").alias("city_right"),
        F.col("left.avg_value").alias("avg_value_left"),
        F.col("right.avg_value").alias("avg_value_right"),
        (F.col("left.avg_value") - F.col("right.avg_value")).alias("avg_value_delta"),
        F.col("left.record_count").alias("record_count_left"),
        F.col("right.record_count").alias("record_count_right"),
        F.greatest(F.col("left.run_id"), F.col("right.run_id")).alias("run_id"),
    )


def build_hourly_coverage(df: DataFrame) -> DataFrame:
    flagged_expr = F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0))
    total_expr = F.count(F.lit(1))
    coverage_window = Window.partitionBy("city", "pollutant")

    aggregated = (
        df.groupBy("city", "pollutant", "observed_hour")
        .agg(
            total_expr.alias("observed_records"),
            flagged_expr.alias("flagged_records"),
            F.max("run_id").alias("run_id"),
        )
        .withColumn("expected_records", F.greatest(F.lit(1), F.max("observed_records").over(coverage_window)))
        .withColumn("completeness_ratio", F.col("observed_records") / F.col("expected_records"))
        .withColumn(
            "coverage_status",
            F.when(F.col("completeness_ratio") >= F.lit(0.95), F.lit("pass"))
            .when(F.col("completeness_ratio") >= F.lit(0.75), F.lit("warn"))
            .otherwise(F.lit("fail")),
        )
    )

    return aggregated.select(
        "city",
        "pollutant",
        "observed_hour",
        "observed_records",
        "expected_records",
        "completeness_ratio",
        "flagged_records",
        "coverage_status",
        "run_id",
    )


def emit_output_metrics(telemetry, step_name: str, metric_name: str, df: DataFrame) -> int:
    row_count = df.count()
    telemetry.emit_metric(metric_name, row_count, "rows", step_name=step_name)
    return row_count


def main() -> int:
    args = parse_args()
    run_id = args.run_id or f"run_build_gold_{Path(args.input).name}"
    trace_id = args.trace_id or f"trace_build_gold_{Path(args.input).name}"

    telemetry = init_telemetry(
        run_id=run_id,
        trace_id=trace_id,
        pipeline_name=args.pipeline_name,
        telemetry_dir=args.telemetry_dir,
    )
    telemetry.emit_run_event("run_initialized", "running")
    telemetry.emit_run_event("run_started", "running")
    root_span = telemetry.start_span(step_name="build_gold_hourly_table", source_name=args.source_name)

    spark: SparkSession | None = None
    try:
        spark = spark_session(args.app_name)

        read_span = telemetry.start_span("read_silver", parent_span_id=root_span.span_id, source_name=args.source_name)
        silver_df = spark.read.parquet(args.input)
        silver_count = silver_df.count()
        telemetry.emit_metric("rows_silver_read_total", silver_count, "rows", step_name="read_silver", source_name=args.source_name)
        telemetry.end_span(
            read_span.span_id,
            "read_silver",
            "success",
            parent_span_id=root_span.span_id,
            rows_out=silver_count,
            source_name=args.source_name,
        )

        working_df = base_gold_input(silver_df).cache()
        working_count = working_df.count()
        telemetry.emit_metric("rows_gold_input_total", working_count, "rows", step_name="read_silver", source_name=args.source_name)
        telemetry.emit_metric(
            "flagged_records_total",
            working_df.filter(F.col("qc_status") == "flagged").count(),
            "rows",
            step_name="read_silver",
            source_name=args.source_name,
        )

        hourly_span = telemetry.start_span("build_gold_hourly", parent_span_id=root_span.span_id, source_name=args.source_name)
        hourly_df = build_hourly_averages(working_df).cache()
        hourly_count = emit_output_metrics(telemetry, "build_gold_hourly", "rows_gold_hourly_total", hourly_df)
        telemetry.end_span(
            hourly_span.span_id,
            "build_gold_hourly",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=working_count,
            rows_out=hourly_count,
            source_name=args.source_name,
        )

        comparison_span = telemetry.start_span("build_city_comparison", parent_span_id=root_span.span_id, source_name=args.source_name)
        city_comparison_df = build_city_comparison(hourly_df).cache()
        city_comparison_count = emit_output_metrics(
            telemetry,
            "build_city_comparison",
            "rows_gold_city_comparison_total",
            city_comparison_df,
        )
        telemetry.end_span(
            comparison_span.span_id,
            "build_city_comparison",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=hourly_count,
            rows_out=city_comparison_count,
            source_name=args.source_name,
        )

        coverage_span = telemetry.start_span("build_coverage", parent_span_id=root_span.span_id, source_name=args.source_name)
        coverage_df = build_hourly_coverage(working_df).cache()
        coverage_count = emit_output_metrics(telemetry, "build_coverage", "rows_gold_coverage_total", coverage_df)
        telemetry.end_span(
            coverage_span.span_id,
            "build_coverage",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=working_count,
            rows_out=coverage_count,
            source_name=args.source_name,
        )

        write_hourly_span = telemetry.start_span("write_gold", parent_span_id=root_span.span_id, source_name=args.source_name)
        hourly_df.write.mode(args.mode).parquet(args.hourly_output)
        city_comparison_df.write.mode(args.mode).parquet(args.city_comparison_output)
        coverage_df.write.mode(args.mode).parquet(args.coverage_output)
        total_rows_out = hourly_count + city_comparison_count + coverage_count
        telemetry.emit_metric("rows_gold_written_total", total_rows_out, "rows", step_name="write_gold", source_name=args.source_name)
        telemetry.end_span(
            write_hourly_span.span_id,
            "write_gold",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=working_count,
            rows_out=total_rows_out,
            artifact_path=args.hourly_output,
            source_name=args.source_name,
        )

        telemetry.end_span(
            root_span.span_id,
            "build_gold_hourly_table",
            "success",
            rows_in=silver_count,
            rows_out=total_rows_out,
            artifact_path=args.hourly_output,
            source_name=args.source_name,
        )
        telemetry.emit_run_event("run_completed", "success", artifact_path=args.hourly_output)
        return 0
    except Exception as exc:  # noqa: BLE001
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
            artifact_path=args.hourly_output,
            error_message=message,
            source_name=args.source_name,
        )
        telemetry.emit_run_event("run_failed", "failed", error_message=message, artifact_path=args.hourly_output)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
