from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    round as spark_round,
    min as spark_min,
    max as spark_max
)
from datetime import datetime, timezone
import os
import csv
import time

spark = (
    SparkSession.builder
    .appName("build_telemetry_metrics")
    .getOrCreate()
)

silver_path = "data/silver/air_quality_long"
gold_hourly_path = "data/gold/city_hourly_air_quality"
gold_coverage_path = "data/gold/city_hourly_coverage"

telemetry_dir = "data/telemetry"
os.makedirs(telemetry_dir, exist_ok=True)

run_log_path = os.path.join(telemetry_dir, "pipeline_run_log.csv")
trace_path = os.path.join(telemetry_dir, "pipeline_trace.csv")
quality_output = os.path.join(telemetry_dir, "quality_metrics")

pipeline_name = "telemetry_build"
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
trace_id = run_id

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_csv(path: str, header: list[str]) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)

def append_csv(path: str, row: list) -> None:
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

def log_trace(
    step_name: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    status: str,
    rows_in: int,
    rows_out: int,
    error_message: str = ""
) -> None:
    append_csv(
        trace_path,
        [
            trace_id,
            run_id,
            pipeline_name,
            step_name,
            started_at,
            finished_at,
            duration_ms,
            status,
            rows_in,
            rows_out,
            error_message
        ]
    )

ensure_csv(
    run_log_path,
    [
        "run_id",
        "pipeline_name",
        "started_at",
        "finished_at",
        "duration_ms",
        "status",
        "silver_rows",
        "gold_hourly_rows",
        "gold_coverage_rows",
        "quality_metric_rows",
        "error_message"
    ]
)

ensure_csv(
    trace_path,
    [
        "trace_id",
        "run_id",
        "pipeline_name",
        "step_name",
        "started_at",
        "finished_at",
        "duration_ms",
        "status",
        "rows_in",
        "rows_out",
        "error_message"
    ]
)

pipeline_started_at = utc_now_iso()
pipeline_t0 = time.time()

silver_rows = 0
gold_hourly_rows = 0
gold_coverage_rows = 0
quality_metric_rows = 0
pipeline_status = "success"
pipeline_error = ""

try:
    # =========================
    # STEP 1: read silver
    # =========================
    step = "read_silver"
    step_started_at = utc_now_iso()
    step_t0 = time.time()

    silver_df = spark.read.parquet(silver_path)
    silver_rows = silver_df.count()

    step_finished_at = utc_now_iso()
    step_duration_ms = int((time.time() - step_t0) * 1000)
    log_trace(
        step_name=step,
        started_at=step_started_at,
        finished_at=step_finished_at,
        duration_ms=step_duration_ms,
        status="success",
        rows_in=0,
        rows_out=silver_rows
    )

    # =========================
    # STEP 2: read gold hourly
    # =========================
    step = "read_gold_hourly"
    step_started_at = utc_now_iso()
    step_t0 = time.time()

    gold_hourly_df = spark.read.parquet(gold_hourly_path)
    gold_hourly_rows = gold_hourly_df.count()

    step_finished_at = utc_now_iso()
    step_duration_ms = int((time.time() - step_t0) * 1000)
    log_trace(
        step_name=step,
        started_at=step_started_at,
        finished_at=step_finished_at,
        duration_ms=step_duration_ms,
        status="success",
        rows_in=0,
        rows_out=gold_hourly_rows
    )

    # =========================
    # STEP 3: read gold coverage
    # =========================
    step = "read_gold_coverage"
    step_started_at = utc_now_iso()
    step_t0 = time.time()

    coverage_df = spark.read.parquet(gold_coverage_path)
    gold_coverage_rows = coverage_df.count()

    step_finished_at = utc_now_iso()
    step_duration_ms = int((time.time() - step_t0) * 1000)
    log_trace(
        step_name=step,
        started_at=step_started_at,
        finished_at=step_finished_at,
        duration_ms=step_duration_ms,
        status="success",
        rows_in=0,
        rows_out=gold_coverage_rows
    )

    # =========================
    # STEP 4: build quality metrics
    # =========================
    step = "build_quality_metrics"
    step_started_at = utc_now_iso()
    step_t0 = time.time()

    quality_df = (
        coverage_df
        .groupBy("city", "pollutant")
        .agg(
            count("*").alias("total_hour_buckets"),
            count(when(col("low_coverage_flag") == 1, 1)).alias("low_coverage_buckets"),
            spark_round(
                count(when(col("low_coverage_flag") == 1, 1)) / count("*"),
                4
            ).alias("low_coverage_rate"),
            spark_round(spark_min("avg_value"), 4).alias("min_avg_value"),
            spark_round(spark_max("avg_value"), 4).alias("max_avg_value")
        )
        .orderBy("city", "pollutant")
    )

    quality_metric_rows = quality_df.count()

    step_finished_at = utc_now_iso()
    step_duration_ms = int((time.time() - step_t0) * 1000)
    log_trace(
        step_name=step,
        started_at=step_started_at,
        finished_at=step_finished_at,
        duration_ms=step_duration_ms,
        status="success",
        rows_in=gold_coverage_rows,
        rows_out=quality_metric_rows
    )

    # =========================
    # STEP 5: write quality metrics
    # =========================
    step = "write_quality_metrics"
    step_started_at = utc_now_iso()
    step_t0 = time.time()

    quality_df.write.mode("overwrite").csv(quality_output, header=True)

    step_finished_at = utc_now_iso()
    step_duration_ms = int((time.time() - step_t0) * 1000)
    log_trace(
        step_name=step,
        started_at=step_started_at,
        finished_at=step_finished_at,
        duration_ms=step_duration_ms,
        status="success",
        rows_in=quality_metric_rows,
        rows_out=quality_metric_rows
    )

except Exception as e:
    pipeline_status = "failed"
    pipeline_error = str(e)

    failed_finished_at = utc_now_iso()
    failed_duration_ms = int((time.time() - pipeline_t0) * 1000)

    log_trace(
        step_name="pipeline_failed",
        started_at=pipeline_started_at,
        finished_at=failed_finished_at,
        duration_ms=failed_duration_ms,
        status="failed",
        rows_in=0,
        rows_out=0,
        error_message=pipeline_error
    )

finally:
    pipeline_finished_at = utc_now_iso()
    pipeline_duration_ms = int((time.time() - pipeline_t0) * 1000)

    append_csv(
        run_log_path,
        [
            run_id,
            pipeline_name,
            pipeline_started_at,
            pipeline_finished_at,
            pipeline_duration_ms,
            pipeline_status,
            silver_rows,
            gold_hourly_rows,
            gold_coverage_rows,
            quality_metric_rows,
            pipeline_error
        ]
    )

    print(f"saved quality metrics → {quality_output}")
    print(f"appended pipeline run log → {run_log_path}")
    print(f"appended pipeline trace → {trace_path}")
    print(f"pipeline_status={pipeline_status}")

    spark.stop()

if pipeline_status != "success":
    raise RuntimeError(pipeline_error)
