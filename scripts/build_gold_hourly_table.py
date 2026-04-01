from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    count,
    min as spark_min,
    max as spark_max,
    expr,
    round as spark_round
)

spark = (
    SparkSession.builder
    .appName("build_gold_hourly_air_quality")
    .getOrCreate()
)

silver_path = "data/silver/air_quality_long"
gold_base = "data/gold"

df = spark.read.parquet(silver_path)

# 1) dashboard 主表：每小時、每城市、每污染物平均
hourly_df = (
    df
    .filter(col("value").isNotNull())
    .withColumn("hour_bucket", date_trunc("hour", col("observed_at")))
    .groupBy("hour_bucket", "city", "pollutant")
    .agg(
        avg("value").alias("avg_value"),
        count("*").alias("record_count"),
        spark_min("value").alias("min_value"),
        spark_max("value").alias("max_value")
    )
    .select(
        "hour_bucket",
        "city",
        "pollutant",
        spark_round("avg_value", 4).alias("avg_value"),
        "record_count",
        spark_round("min_value", 4).alias("min_value"),
        spark_round("max_value", 4).alias("max_value")
    )
)

# 2) city comparison 主表：同一小時同一污染物的兩城平均並排
comparison_df = (
    hourly_df
    .groupBy("hour_bucket", "pollutant")
    .pivot("city", ["apba", "torrepacheco"])
    .agg(expr("first(avg_value)"))
    .withColumn(
        "abs_diff",
        spark_round(expr("abs(apba - torrepacheco)"), 4)
    )
    .orderBy("hour_bucket", "pollutant")
)

# 3) quality / coverage summary：每小時有哪些聚合是稀疏的
coverage_df = (
    hourly_df
    .withColumn(
        "low_coverage_flag",
        expr("CASE WHEN record_count < 3 THEN 1 ELSE 0 END")
    )
    .orderBy("hour_bucket", "city", "pollutant")
)

# write parquet
hourly_df.write.mode("overwrite").parquet(f"{gold_base}/city_hourly_air_quality")
comparison_df.write.mode("overwrite").parquet(f"{gold_base}/city_hourly_comparison")
coverage_df.write.mode("overwrite").parquet(f"{gold_base}/city_hourly_coverage")

print("saved:")
print(f"  {gold_base}/city_hourly_air_quality")
print(f"  {gold_base}/city_hourly_comparison")
print(f"  {gold_base}/city_hourly_coverage")

print("row counts:")
print("  hourly      =", hourly_df.count())
print("  comparison  =", comparison_df.count())
print("  coverage    =", coverage_df.count())

spark.stop()
