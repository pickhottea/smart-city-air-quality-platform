from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline_observability.telemetry import TelemetryClient, init_telemetry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Roll up runtime telemetry JSONL into curated CSV outputs."
    )
    parser.add_argument("--runtime-dir", default="data/telemetry/runtime")
    parser.add_argument("--silver-input", default="data/silver/air_quality_long")
    parser.add_argument("--output-dir", default="data/telemetry/curated")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--pipeline-name", default="smart_city_air_quality")
    parser.add_argument("--source-name", default="telemetry_runtime")
    parser.add_argument("--telemetry-dir", default="data/telemetry")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--trace-id", default=None)
    parser.add_argument("--app-name", default="build_telemetry_metrics")
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
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


def parse_ts(col_name: str) -> F.Column:
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(F.col(col_name)),
    )


def runtime_schema(name: str) -> StructType:
    if name == "run":
        return StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("trace_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("artifact_path", StringType(), True),
            ]
        )
    if name == "span":
        return StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("trace_id", StringType(), True),
                StructField("span_id", StringType(), True),
                StructField("parent_span_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("step_name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("city", StringType(), True),
                StructField("pollutant", StringType(), True),
                StructField("batch_month", StringType(), True),
                StructField("source_name", StringType(), True),
                StructField("rows_in", DoubleType(), True),
                StructField("rows_out", DoubleType(), True),
                StructField("artifact_path", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )
    if name == "metric":
        return StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("trace_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("metric_name", StringType(), True),
                StructField("metric_value", DoubleType(), True),
                StructField("metric_unit", StringType(), True),
                StructField("step_name", StringType(), True),
                StructField("city", StringType(), True),
                StructField("pollutant", StringType(), True),
                StructField("batch_month", StringType(), True),
                StructField("source_name", StringType(), True),
            ]
        )
    if name == "error":
        return StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("trace_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("step_name", StringType(), True),
                StructField("error_type", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("span_id", StringType(), True),
                StructField("city", StringType(), True),
                StructField("pollutant", StringType(), True),
                StructField("batch_month", StringType(), True),
                StructField("source_name", StringType(), True),
                StructField("retryable", BooleanType(), True),
                StructField("severity", StringType(), True),
            ]
        )
    raise ValueError(name)


def safe_read_jsonl(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return spark.createDataFrame([], schema)
    return spark.read.schema(schema).json(path)


def write_csv(df: DataFrame, path: str, mode: str) -> None:
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", True)
        .csv(path)
    )


def build_pipeline_metrics(metric_events: DataFrame) -> DataFrame:
    return metric_events.select(
        parse_ts("event_ts").alias("metric_ts"),
        F.col("run_id"),
        F.col("trace_id"),
        F.col("pipeline_name"),
        F.col("step_name"),
        F.col("metric_name"),
        F.col("metric_value").cast("double").alias("metric_value"),
        F.col("metric_unit"),
        F.col("city"),
        F.col("pollutant"),
        F.col("batch_month"),
        F.col("source_name"),
    )


def build_pipeline_trace(span_events: DataFrame) -> DataFrame:
    starts = (
        span_events
        .filter(F.col("event_type") == "span_start")
        .select(
            "trace_id",
            "span_id",
            "parent_span_id",
            "run_id",
            "pipeline_name",
            "step_name",
            "city",
            "pollutant",
            "batch_month",
            "source_name",
            parse_ts("event_ts").alias("started_at"),
        )
    )

    ends = (
        span_events
        .filter(F.col("event_type") == "span_end")
        .select(
            "trace_id",
            "span_id",
            "parent_span_id",
            "run_id",
            "pipeline_name",
            "step_name",
            "city",
            "pollutant",
            "batch_month",
            "source_name",
            parse_ts("event_ts").alias("finished_at"),
            F.col("status"),
            F.col("rows_in").cast("double").alias("rows_in"),
            F.col("rows_out").cast("double").alias("rows_out"),
            F.col("artifact_path"),
            F.col("error_message"),
        )
    )

    joined = starts.alias("s").join(
        ends.alias("e"),
        on=["trace_id", "span_id"],
        how="full_outer",
    )

    return (
        joined.select(
            F.coalesce(F.col("s.trace_id"), F.col("e.trace_id")).alias("trace_id"),
            F.coalesce(F.col("s.span_id"), F.col("e.span_id")).alias("span_id"),
            F.coalesce(F.col("s.parent_span_id"), F.col("e.parent_span_id")).alias("parent_span_id"),
            F.coalesce(F.col("s.run_id"), F.col("e.run_id")).alias("run_id"),
            F.coalesce(F.col("s.pipeline_name"), F.col("e.pipeline_name")).alias("pipeline_name"),
            F.coalesce(F.col("s.step_name"), F.col("e.step_name")).alias("step_name"),
            F.coalesce(F.col("s.city"), F.col("e.city")).alias("city"),
            F.coalesce(F.col("s.pollutant"), F.col("e.pollutant")).alias("pollutant"),
            F.coalesce(F.col("s.batch_month"), F.col("e.batch_month")).alias("batch_month"),
            F.coalesce(F.col("s.source_name"), F.col("e.source_name")).alias("source_name"),
            F.col("s.started_at").alias("started_at"),
            F.col("e.finished_at").alias("finished_at"),
            (
                (F.unix_timestamp(F.col("e.finished_at")) - F.unix_timestamp(F.col("s.started_at"))) * F.lit(1000)
            ).cast("double").alias("duration_ms"),
            F.coalesce(F.col("e.status"), F.lit("started")).alias("status"),
            F.col("e.rows_in").alias("rows_in"),
            F.col("e.rows_out").alias("rows_out"),
            F.col("e.artifact_path").alias("artifact_path"),
            F.col("e.error_message").alias("error_message"),
        )
    )


def build_pipeline_run_log(run_events: DataFrame, error_events: DataFrame) -> DataFrame:
    starts = (
        run_events
        .filter(F.col("event_type") == "run_started")
        .select(
            "run_id",
            "trace_id",
            "pipeline_name",
            parse_ts("event_ts").alias("started_at"),
        )
    )

    ends = (
        run_events
        .filter(F.col("event_type").isin("run_completed", "run_failed", "run_incomplete"))
        .select(
            "run_id",
            "trace_id",
            "pipeline_name",
            parse_ts("event_ts").alias("finished_at"),
            F.col("status"),
            F.col("artifact_path"),
            F.col("error_message"),
        )
    )

    error_counts = (
        error_events.groupBy("run_id", "trace_id")
        .agg(F.count(F.lit(1)).alias("error_count"))
    )

    joined = starts.alias("s").join(
        ends.alias("e"),
        on=["run_id", "trace_id", "pipeline_name"],
        how="full_outer",
    )

    joined = joined.join(error_counts, on=["run_id", "trace_id"], how="left")

    return (
        joined.select(
            "run_id",
            "trace_id",
            "pipeline_name",
            F.col("s.started_at").alias("started_at"),
            F.col("e.finished_at").alias("finished_at"),
            (
                (F.unix_timestamp(F.col("e.finished_at")) - F.unix_timestamp(F.col("s.started_at"))) * F.lit(1000)
            ).cast("double").alias("duration_ms"),
            F.coalesce(F.col("e.status"), F.lit("running")).alias("status"),
            F.lit(None).cast("double").alias("rows_in"),
            F.lit(None).cast("double").alias("rows_out"),
            F.lit(None).cast("double").alias("rows_flagged"),
            F.lit(None).cast("double").alias("rows_rejected"),
            F.coalesce(F.col("error_count"), F.lit(0)).alias("error_count"),
            F.col("e.artifact_path").alias("artifact_path"),
            F.col("e.error_message").alias("error_message"),
        )
    )


def build_error_log(error_events: DataFrame) -> DataFrame:
    return error_events.select(
        parse_ts("event_ts").alias("error_ts"),
        F.col("run_id"),
        F.col("trace_id"),
        F.col("span_id"),
        F.col("pipeline_name"),
        F.col("step_name"),
        F.col("city"),
        F.col("pollutant"),
        F.col("batch_month"),
        F.col("source_name"),
        F.col("error_type"),
        F.col("error_message"),
        F.col("retryable"),
        F.col("severity"),
    )


def build_quality_metrics_daily(silver_df: DataFrame) -> DataFrame:
    null_required = (
        F.col("city").isNull()
        | F.col("pollutant").isNull()
        | F.col("time_index").isNull()
        | F.col("value").isNull()
    )

    total_records = F.count(F.lit(1))
    duplicate_records = F.sum(F.when(F.col("qc_flags").contains("duplicate_timestamp"), 1).otherwise(0))
    outlier_records = F.sum(F.when(F.col("qc_flags").contains("sudden_spike"), 1).otherwise(0))
    stale_records = F.sum(F.when(F.col("qc_flags").contains("stale_data"), 1).otherwise(0))
    flagged_records = F.sum(F.when(F.col("qc_status") == "flagged", 1).otherwise(0))
    rejected_records = F.sum(F.when(F.col("qc_status") == "rejected", 1).otherwise(0))
    pass_records = F.sum(F.when(F.col("qc_status") == "pass", 1).otherwise(0))
    null_records = F.sum(F.when(null_required, 1).otherwise(0))

    agg = (
        silver_df
        .groupBy(F.to_date("observed_at").alias("metric_date"), "run_id", "city", "pollutant")
        .agg(
            total_records.alias("total_records"),
            null_records.alias("null_records"),
            duplicate_records.alias("duplicate_records"),
            stale_records.alias("stale_records"),
            outlier_records.alias("outlier_records"),
            flagged_records.alias("flagged_records"),
            rejected_records.alias("rejected_records"),
            pass_records.alias("pass_records"),
        )
    )

    return (
        agg
        .withColumn("null_rate", F.col("null_records") / F.col("total_records"))
        .withColumn("duplicate_rate", F.col("duplicate_records") / F.col("total_records"))
        .withColumn("stale_rate", F.col("stale_records") / F.col("total_records"))
        .withColumn("outlier_rate", F.col("outlier_records") / F.col("total_records"))
        .withColumn("flagged_rate", F.col("flagged_records") / F.col("total_records"))
        .withColumn("rejected_rate", F.col("rejected_records") / F.col("total_records"))
        .withColumn("pass_rate", F.col("pass_records") / F.col("total_records"))
        .withColumn(
            "quality_status",
            F.when(F.col("rejected_rate") > F.lit(0.05), F.lit("fail"))
            .when(F.col("flagged_rate") > F.lit(0.05), F.lit("warn"))
            .otherwise(F.lit("pass")),
        )
        .select(
            "metric_date",
            "run_id",
            "city",
            "pollutant",
            "total_records",
            "null_rate",
            "duplicate_rate",
            "stale_rate",
            "outlier_rate",
            "flagged_rate",
            "rejected_rate",
            "pass_rate",
            "quality_status",
        )
    )


def build_freshness_metrics(silver_df: DataFrame) -> DataFrame:
    agg = (
        silver_df
        .groupBy("run_id", "city", "pollutant")
        .agg(
            F.max("observed_at").alias("latest_observed_at"),
            F.coalesce(F.max("ingestion_ts"), F.current_timestamp()).alias("latest_ingested_at"),
        )
        .withColumn("metric_ts", F.current_timestamp())
    )

    freshness_minutes = (
        F.unix_timestamp(F.col("latest_ingested_at")) - F.unix_timestamp(F.col("latest_observed_at"))
    ) / F.lit(60.0)

    return (
        agg
        .withColumn("freshness_lag_minutes", F.round(freshness_minutes, 3))
        .withColumn(
            "freshness_status",
            F.when(F.col("freshness_lag_minutes").isNull(), F.lit("unknown"))
            .when(F.col("freshness_lag_minutes") <= F.lit(60), F.lit("fresh"))
            .when(F.col("freshness_lag_minutes") <= F.lit(1440), F.lit("delayed"))
            .otherwise(F.lit("stale")),
        )
        .select(
            "metric_ts",
            "run_id",
            "city",
            "pollutant",
            "latest_observed_at",
            "latest_ingested_at",
            "freshness_lag_minutes",
            "freshness_status",
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
        step_name="build_telemetry_metrics",
        source_name=args.source_name,
    )

    spark = None
    try:
        spark = spark_session(args.app_name)

        read_span = telemetry.start_span(
            "read_runtime_events",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        runtime_dir = Path(args.runtime_dir)

        run_events = safe_read_jsonl(
            spark,
            str(runtime_dir / "run_events.jsonl"),
            runtime_schema("run"),
        )
        span_events = safe_read_jsonl(
            spark,
            str(runtime_dir / "span_events.jsonl"),
            runtime_schema("span"),
        )
        metric_events = safe_read_jsonl(
            spark,
            str(runtime_dir / "metric_events.jsonl"),
            runtime_schema("metric"),
        )
        error_events = safe_read_jsonl(
            spark,
            str(runtime_dir / "error_events.jsonl"),
            runtime_schema("error"),
        )

        telemetry.end_span(
            read_span.span_id,
            "read_runtime_events",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        rollup_span = telemetry.start_span(
            "rollup_telemetry",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        pipeline_run_log = build_pipeline_run_log(run_events, error_events)
        pipeline_trace = build_pipeline_trace(span_events)
        pipeline_metrics = build_pipeline_metrics(metric_events)
        error_log = build_error_log(error_events)

        silver_path = Path(args.silver_input)
        silver_df = None
        quality_df = None
        freshness_df = None

        if silver_path.exists():
            silver_df = spark.read.parquet(args.silver_input)
            quality_df = build_quality_metrics_daily(silver_df)
            freshness_df = build_freshness_metrics(silver_df)

        telemetry.end_span(
            rollup_span.span_id,
            "rollup_telemetry",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            source_name=args.source_name,
        )

        write_span = telemetry.start_span(
            "write_curated_telemetry",
            parent_span_id=root_span.span_id,
            source_name=args.source_name,
        )

        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        write_csv(pipeline_run_log, str(output_dir / "pipeline_run_log.csv"), args.mode)
        write_csv(pipeline_trace, str(output_dir / "pipeline_trace.csv"), args.mode)
        write_csv(pipeline_metrics, str(output_dir / "pipeline_metrics.csv"), args.mode)
        write_csv(error_log, str(output_dir / "error_log.csv"), args.mode)

        if quality_df is not None:
            write_csv(quality_df, str(output_dir / "quality_metrics_daily.csv"), args.mode)

        if freshness_df is not None:
            write_csv(freshness_df, str(output_dir / "freshness_metrics.csv"), args.mode)

        telemetry.emit_metric(
            "telemetry_rollup_completed",
            1,
            "run",
            step_name="write_curated_telemetry",
            source_name=args.source_name,
        )

        telemetry.end_span(
            write_span.span_id,
            "write_curated_telemetry",
            "success",
            parent_span_id=root_span.span_id,
            rows_in=None,
            rows_out=None,
            artifact_path=str(output_dir),
            source_name=args.source_name,
        )

        telemetry.end_span(
            root_span.span_id,
            "build_telemetry_metrics",
            "success",
            artifact_path=str(output_dir),
            source_name=args.source_name,
        )
        telemetry.emit_run_event("run_completed", "success", artifact_path=str(output_dir))
        return 0

    except Exception as exc:
        message = str(exc)
        telemetry.emit_error(
            step_name="build_telemetry_metrics",
            error_type=type(exc).__name__,
            error_message=message,
            span_id=root_span.span_id,
            source_name=args.source_name,
            retryable=False,
            severity="critical",
        )
        telemetry.end_span(
            root_span.span_id,
            "build_telemetry_metrics",
            "failed",
            artifact_path=args.output_dir,
            error_message=message,
            source_name=args.source_name,
        )
        telemetry.emit_run_event(
            "run_failed",
            "failed",
            error_message=message,
            artifact_path=args.output_dir,
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