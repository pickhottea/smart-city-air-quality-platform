from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_timestamp, from_unixtime
from functools import reduce

spark = (
    SparkSession.builder
    .appName("build_silver_air_quality")
    .getOrCreate()
)

base_path = "data/bronze/crate"
output_path = "data/silver/air_quality_long"

cities = ["apba", "torrepacheco"]
pollutants = ["co", "no2", "o3", "so2"]

dfs = []

for city in cities:
    for pollutant in pollutants:
        path = f"{base_path}/{city}/{pollutant}/*.csv"

        try:
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(path)
            )

            # 跳過空資料
            if len(df.head(1)) == 0:
                continue

            df = (
                df
                .withColumn("city", lit(city))
                .withColumn("pollutant", lit(pollutant))
                .withColumn("observed_at", to_timestamp(from_unixtime(col("time_index") / 1000)))
                .select(
                    col("time_index").cast("long"),
                    col("observed_at"),
                    col("city"),
                    col("pollutant"),
                    col("value").cast("double")
                )
                .filter(col("value").isNotNull())
            )

            dfs.append(df)

        except Exception as e:
            print(f"skip {path}: {e}")

if not dfs:
    raise ValueError("No bronze CSV data found")

silver_df = reduce(lambda a, b: a.unionByName(b), dfs)

silver_df.write.mode("overwrite").parquet(output_path)

print(f"saved silver table to: {output_path}")
print(f"row count: {silver_df.count()}")

spark.stop()
