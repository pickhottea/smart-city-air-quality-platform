from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline_observability.telemetry import TelemetryClient, init_telemetry

POLLUTANTS = ["co", "no2", "o3", "so2"]
DEFAULT_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "co": {"min": 0.0, "max": 50000.0, "spike_abs": 5000.0, "spike_multiplier": 5.0},
    "no2": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
    "o3": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
    "so2": {"min": 0.0, "max": 2000.0, "spike_abs": 250.0, "spike_multiplier": 5.0},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Silver long-format air-quality table from Bronze CSV extracts.")
    parser.add_argument("--input", default="data/bronze/crate/*/*/*.csv")
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
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def snake_case(name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.select([F.col(col_name).alias(snake_case(col_name)) for col_name in df.columns])


def pollutant_threshold_expr(metric: str) -> F.Column:
    expr = None
    for pollutant, values in DEFAULT_THRESHOLDS.items():
        branch = F.when(F.col("pollutant") == pollutant, F.lit(values[metric]))
        expr = branch if expr is None else expr.when(F.col("pollutant") == pollutant, F.lit(values[metric]))
    return expr.otherwise(F.lit(None).cast("double"))


def normalize_city_expr(city_col: F.Column, servicepath_col: F.Column, source_file_col: F.Column) -> F.Column:
    raw_city = F.lower(
        F.regexp_replace(
            F.coalesce(city_col, F.lit("")),
            r"[^a-z0-9]+",
            "_",
        )
    )
    raw_servicepath = F.lower(F.coalesce(servicepath_col, F.lit("")))
    path_city = F.lower(F.regexp_extract(source_file_col, r"/crate/([^/]+)/", 1))

    return (
        F.when(raw_servicepath.contains("/apba"), F.lit("apba"))
        .when(raw_servicepath.contains("torrepacheco"), F.lit("torre_pacheco"))
        .when(raw_servicepath.contains("torre_pacheco"), F.lit("torre_pacheco"))
        .when(raw_servicepath.contains("molinadesegura"), F.lit("molina_de_segura"))
        .when(raw_servicepath.contains("molina_de_segura"), F.lit("molina_de_segura"))
        .when(raw_city == "apba", F.lit("apba"))
        .when(raw_city == "torre_pacheco", F.lit("torre_pacheco"))
        .when(raw_city == "torrepacheco", F.lit("torre_pacheco"))
        .when(raw_city == "molina_de_segura", F.lit("molina_de_segura"))
        .when(raw_city == "molinadesegura", F.lit("molina_de_segura"))
        .when(path_city == "apba", F.lit("apba"))
        .when(path_city == "torre_pacheco", F.lit("torre_pacheco"))
        .when(path_city == "torrepacheco", F.lit("torre_pacheco"))
        .when(path_city == "molina_de_segura", F.lit("molina_de_segura"))
        .otherwise(F.when(raw_city != "", raw_city).otherwise(path_city))
    )


def normalize_servicepath_expr(servicepath_col: F.Column, city_col: F.Column) -> F.Column:
    clean_servicepath = F.lower(F.coalesce(servicepath_col, F.lit("")))
    return (
        F.when(clean_servicepath != "", clean_servicepath)
        .when(city_col == "apba", F.lit("/apba"))
        .when(city_col == "torre_pacheco", F.lit("/torrepacheco"))
        .when(city_col == "molina_de_segura", F.lit("/molinadesegura"))
        .otherwise(F.lit(None).cast("string"))
    )


def build_long_format(df: DataFrame, source_name: str) -> DataFrame:
    source_file = F.input_file_name()
    df = normalize_columns(df).withColumn("_source_file", source_file)

    servicepath_col = F.coalesce(
        F.col("source_servicepath") if "source_servicepath" in df.columns else F.lit(None).cast("string"),
        F.col("fiware_servicepath") if "fiware_servicepath" in df.columns else F.lit(None).cast("string"),
        F.col("city_servicepath") if "city_servicepath" in df.columns else F.lit(None).cast("string"),
    )
    city_col = F.col("city") if "city" in df.columns else F.lit(None).cast("string")

    if "value" in df.columns and "pollutant" in df.columns:
        long_df = df.withColumn("pollutant", F.lower(F.col("pollutant"))).withColumn("value", F.col("value").cast("double"))
    else:
        available_pollutants = [pollutant for pollutant in POLLUTANTS if pollutant in df.columns]
        if not available_pollutants:
            raise ValueError(
                "No pollutant columns found. Expected one of co/no2/o3/so2 or explicit value+pollutant fields."
            )
        stack_args = ", ".join([f"'{pollutant}', {pollutant}" for pollutant in available_pollutants])
        long_df = df.selectExpr("*", f"stack({len(available_pollutants)}, {stack_args}) as (pollutant, value)")

    long_df = (
        long_df.withColumn("city", normalize_city_expr(city_col, servicepath_col, F.col("_source_file")))
        .withColumn("source_servicepath", normalize_servicepath_expr(servicepath_col, F.col("city")))
        .withColumn("pollutant", F.lower(F.col("pollutant")))
        .withColumn(
            "time_index",
            F.coalesce(
                F.col("time_index").cast("long") if "time_index" in long_df.columns else F.lit(None).cast("long"),
                (F.unix_timestamp(F.col("date_observed")).cast("long") * F.lit(1000))
                if "date_observed" in long_df.columns
                else F.lit(None).cast("long"),
            ),
        )
        .withColumn("observed_at", F.to_timestamp(F.from_unixtime(F.col("time_index") / F.lit(1000))))
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("source_name", F.lit(source_name))
    )

    return long_df


def apply_qc_rules(df: DataFrame) -> DataFrame:
    dedup_window = Window.partitionBy("city", "pollutant", "time_index")
    dedup_order_window = Window.partitionBy("city", "pollutant", "time_index").orderBy("_source_file")
    series_window = Window.partitionBy("city", "pollutant").orderBy("time_index")

    df = (
        df.withColumn("_duplicate_count", F.count(F.lit(1)).over(dedup_window))
        .withColumn("_duplicate_rank", F.row_number().over(dedup_order_window))
        .filter(F.col("_duplicate_rank") == 1)
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
    )


def emit_quality_metrics(telemetry: TelemetryClient, df: DataFrame, step_name: str) -> None:
    summary_rows = (
        df.groupBy("city", "pollutant")
        .agg(
            F.count(F.lit(1)).alias("rows_total"),
            F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0)).alias("rows_flagged"),
            F.sum(F.when(F.col("qc_status") == "rejected", 1).otherwise(0)).alias("rows_rejected"),
            F.sum(F.when(F.col("qc_flags").contains("duplicate_timestamp"), 1).otherwise(0)).alias("rows_duplicate"),
        )
        .collect()
    )
    for row in summary_rows:
        telemetry.emit_metric("rows_silver_written_total", row.rows_total, "rows", step_name=step_name, city=row.city, pollutant=row.pollutant)
        telemetry.emit_metric("flagged_records_total", row.rows_flagged, "rows", step_name=step_name, city=row.city, pollutant=row.pollutant)
        telemetry.emit_metric("rows_rejected_total", row.rows_rejected, "rows", step_name=step_name, city=row.city, pollutant=row.pollutant)
        telemetry.emit_metric("duplicate_rows_total", row.rows_duplicate, "rows", step_name=step_name, city=row.city, pollutant=row.pollutant)
        telemetry.emit_metric(
            "quality_checks_failed_total",
            row.rows_flagged + row.rows_rejected,
            "rows",
            step_name=step_name,
            city=row.city,
            pollutant=row.pollutant,
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
    root_span = telemetry.start_span(step_name="build_silver_table", source_name=args.source_name)

    spark: SparkSession | None = None
    try:
        spark = spark_session(args.app_name)

        read_span = telemetry.start_span("read_bronze", parent_span_id=root_span.span_id, source_name=args.source_name)
        bronze_df = spark.read.option("header", True).option("inferSchema", True).csv(args.input)
        bronze_count = bronze_df.count()
        telemetry.emit_metric("rows_bronze_read_total", bronze_count, "rows", step_name="read_bronze", source_name=args.source_name)
        telemetry.end_span(read_span.span_id, "read_bronze", "success", parent_span_id=root_span.span_id, rows_out=bronze_count, source_name=args.source_name)

        normalize_span = telemetry.start_span("normalize_schema", parent_span_id=root_span.span_id, source_name=args.source_name)
        long_df = build_long_format(bronze_df, source_name=args.source_name).cache()
        long_count = long_df.count()
        telemetry.emit_metric("rows_normalized_total", long_count, "rows", step_name="normalize_schema", source_name=args.source_name)
        telemetry.end_span(normalize_span.span_id, "normalize_schema", "success", parent_span_id=root_span.span_id, rows_in=bronze_count, rows_out=long_count, source_name=args.source_name)

        qc_span = telemetry.start_span("apply_qc", parent_span_id=root_span.span_id, source_name=args.source_name)
        silver_df = apply_qc_rules(long_df).withColumn("run_id", F.lit(run_id)).withColumn("ingestion_ts", F.current_timestamp()).cache()
        silver_count = silver_df.count()
        telemetry.end_span(qc_span.span_id, "apply_qc", "success", parent_span_id=root_span.span_id, rows_in=long_count, rows_out=silver_count, source_name=args.source_name)

        write_span = telemetry.start_span("write_silver", parent_span_id=root_span.span_id, source_name=args.source_name)
        final_df = silver_df.select(
            "city",
            "pollutant",
            "time_index",
            "observed_at",
            "value",
            "source_name",
            "source_servicepath",
            "qc_status",
            "qc_flags",
            "run_id",
            "ingestion_ts",
        )
        final_df.write.mode(args.mode).parquet(args.output)
        emit_quality_metrics(telemetry, final_df, step_name="write_silver")
        telemetry.end_span(
            write_span.span_id,
            "write_silver",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=silver_count,
            rows_out=silver_count,
            artifact_path=args.output,
            source_name=args.source_name,
        )

        telemetry.end_span(
            root_span.span_id,
            "build_silver_table",
            "success",
            rows_in=bronze_count,
            rows_out=silver_count,
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
        telemetry.emit_run_event("run_failed", "failed", error_message=message, artifact_path=args.output)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
