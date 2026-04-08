from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline_observability.telemetry import TelemetryClient, init_telemetry

DEFAULT_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "co": {"min": 0.0, "max": 50000.0, "spike_abs": 5000.0, "spike_multiplier": 5.0},
    "no2": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
    "o3": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
    "so2": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver long-format air-quality table from Bronze CSV extracts."
    )
    parser.add_argument("--input", default="data/bronze/crate")
    parser.add_argument("--output", default="data/silver/air_quality_long")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--pipeline-name", default="smart_city_air_quality")
    parser.add_argument("--source-name", default="crate_grafana_proxy")
    parser.add_argument("--telemetry-dir", default="data/telemetry")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--trace-id", default=None)
    parser.add_argument("--app-name", default="build_silver_table")
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


def bronze_schema() -> StructType:
    return StructType(
        [
            StructField("time_index", LongType(), True),
            StructField("value", DoubleType(), True),
        ]
    )


def pollutant_threshold_expr(metric: str) -> F.Column:
    expr = None
    for pollutant, values in DEFAULT_THRESHOLDS.items():
        branch = F.when(F.col("pollutant") == pollutant, F.lit(values[metric]))
        expr = branch if expr is None else expr.when(F.col("pollutant") == pollutant, F.lit(values[metric]))
    return expr.otherwise(F.lit(None).cast("double"))


def count_bronze_files(input_root: str) -> int:
    return len(list(Path(input_root).rglob("*.csv")))


def build_long_format(bronze_df: DataFrame, source_name: str = "crate_grafana_proxy") -> DataFrame:
    df = bronze_df.withColumn("_source_file", F.input_file_name())

    path_col = F.col("_source_file")
    city = F.regexp_extract(path_col, r"/crate/([^/]+)/([^/]+)/([^/]+)\.csv$", 1)
    pollutant = F.regexp_extract(path_col, r"/crate/([^/]+)/([^/]+)/([^/]+)\.csv$", 2)
    batch_month = F.regexp_extract(path_col, r"/crate/([^/]+)/([^/]+)/([^/]+)\.csv$", 3)

    return (
        df.select(
            F.col("_source_file").alias("_source_file"),
            F.lower(city).alias("city"),
            F.lower(pollutant).alias("pollutant"),
            batch_month.alias("batch_month"),
            F.col("time_index").cast("long").alias("time_index"),
            F.col("value").cast("double").alias("value"),
        )
        .withColumn("observed_at", F.to_timestamp(F.from_unixtime(F.col("time_index") / 1000.0)))
        .withColumn("source_name", F.lit(source_name))
        .withColumn("source_servicepath", F.concat(F.lit("/"), F.col("city")))
        .withColumn("qc_status", F.lit("pass"))
        .withColumn("qc_flags", F.lit(""))
    )


def apply_qc_rules(df: DataFrame) -> DataFrame:
    series_window = Window.partitionBy("city", "pollutant").orderBy("time_index")

    duplicate_counts = (
        df.groupBy("city", "pollutant", "time_index")
        .agg(F.count(F.lit(1)).alias("_duplicate_count"))
    )

    df = (
        df.join(
            duplicate_counts,
            on=["city", "pollutant", "time_index"],
            how="left",
        )
        .dropDuplicates(["city", "pollutant", "time_index"])
        .withColumn("_lower_bound", pollutant_threshold_expr("min"))
        .withColumn("_upper_bound", pollutant_threshold_expr("max"))
        .withColumn("_spike_abs", pollutant_threshold_expr("spike_abs"))
        .withColumn("_spike_multiplier", pollutant_threshold_expr("spike_multiplier"))
        .withColumn("_prev_value", F.lag("value").over(series_window))
    )

    required_flag = (
        F.col("city").isNull()
        | F.col("pollutant").isNull()
        | F.col("time_index").isNull()
        | F.col("value").isNull()
    )
    below_range_flag = F.col("_lower_bound").isNotNull() & (F.col("value") < F.col("_lower_bound"))
    above_range_flag = F.col("_upper_bound").isNotNull() & (F.col("value") > F.col("_upper_bound"))
    duplicate_flag = F.col("_duplicate_count") > F.lit(1)
    outlier_flag = (
        F.col("_prev_value").isNotNull()
        & (
            (F.abs(F.col("value") - F.col("_prev_value")) > F.col("_spike_abs"))
            | (
                (F.abs(F.col("_prev_value")) > F.lit(0))
                & (F.abs(F.col("value")) > (F.abs(F.col("_prev_value")) * F.col("_spike_multiplier")))
            )
        )
    )
    stale_flag = F.lit(False)

    flag_array = F.array_remove(
        F.array(
            F.when(required_flag, F.lit("missing_required")),
            F.when(duplicate_flag, F.lit("duplicate_timestamp")),
            F.when(below_range_flag, F.lit("below_range")),
            F.when(above_range_flag, F.lit("above_range")),
            F.when(outlier_flag, F.lit("sudden_spike")),
            F.when(stale_flag, F.lit("stale_data")),
        ),
        F.lit(None),
    )

    return (
        df.withColumn("qc_flags_array", flag_array)
        .withColumn(
            "qc_status",
            F.when(required_flag, F.lit("rejected"))
            .when(F.size(F.col("qc_flags_array")) > 0, F.lit("flagged"))
            .otherwise(F.lit("pass")),
        )
        .withColumn("qc_flags", F.concat_ws(",", F.col("qc_flags_array")))
        .drop(
            "_duplicate_count",
            "_lower_bound",
            "_upper_bound",
            "_spike_abs",
            "_spike_multiplier",
            "_prev_value",
            "qc_flags_array",
            "_source_file",
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
        step_name="build_silver_table",
        source_name=args.source_name,
    )

    spark: SparkSession | None = None
    try:
        bronze_files_total = count_bronze_files(args.input)
        telemetry.emit_metric(
            "bronze_files_total",
            bronze_files_total,
            "files",
            step_name="build_silver_table",
            source_name=args.source_name,
        )

        if bronze_files_total == 0:
            raise FileNotFoundError(f"No Bronze CSV files found under {args.input}")

        spark = spark_session(args.app_name)

        read_span = telemetry.start_span(
            "read_bronze",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        bronze_df = (
            spark.read
            .option("header", True)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.csv")
            .schema(bronze_schema())
            .csv(args.input)
        )

        telemetry.end_span(
            read_span.span_id,
            "read_bronze",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        normalize_span = telemetry.start_span(
            "normalize_schema",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        long_df = build_long_format(bronze_df, source_name=args.source_name)

        telemetry.end_span(
            normalize_span.span_id,
            "normalize_schema",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        qc_span = telemetry.start_span(
            "apply_qc",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        silver_df = (
            apply_qc_rules(long_df)
            .withColumn("run_id", F.lit(run_id))
            .withColumn("ingestion_ts", F.current_timestamp())
        )

        telemetry.end_span(
            qc_span.span_id,
            "apply_qc",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        write_span = telemetry.start_span(
            "write_silver",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        (
            silver_df
            .coalesce(1)
            .write
            .mode(args.mode)
            .parquet(args.output)
        )

        telemetry.emit_metric(
            "silver_write_completed",
            1,
            "run",
            step_name="write_silver",
            source_name=args.source_name,
        )

        telemetry.end_span(
            write_span.span_id,
            "write_silver",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            artifact_path=args.output,
            source_name=args.source_name,
        )

        telemetry.end_span(
            root_span.span_id,
            "build_silver_table",
            "success",
            artifact_path=args.output,
            source_name=args.source_name,
        )
        telemetry.emit_run_event("run_completed", "success", artifact_path=args.output)
        return 0

    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        telemetry.emit_error(
            step_name="build_silver_table",
            error_type=type(exc).__name__,
            error_message=message,
            span_id=root_span.span_id,
            source_name=args.source_name,
            retryable=False,
            severity="critical",
        )
        telemetry.end_span(
            root_span.span_id,
            "build_silver_table",
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