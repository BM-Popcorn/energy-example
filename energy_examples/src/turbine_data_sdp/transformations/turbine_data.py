from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, min, max, avg, stddev, to_date, round, lit

# Bronze: Ingest raw CSV files
@dp.table(
    name="dev.bronze.turbine_data_raw",
    comment="Raw energy data ingested from CSV files"
)
def bronze_energy_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .load("/Volumes/dev/raw/files")
        .withColumn("file_path", col('_metadata.file_path'))
    )

# Compute power output statistics for outlier detection
@dp.view(
    name="power_output_stats",
    comment="Power output statistical bounds for outlier detection"
)
def power_output_stats():
    stats = spark.read.table("dev.silver.historical_stats_table")
    return (
        spark.readStream.table("dev.bronze.turbine_data_raw")
        .join(stats, on=["turbine_id"], how="left")
    )

# Row-level quality rules (no subqueries)
rules = {
    "no nulls in turbine_id": "turbine_id IS NOT NULL",
    "no nulls in wind_speed": "wind_speed IS NOT NULL",
    "no nulls in wind_direction": "wind_direction IS NOT NULL",
    "no nulls in power_output": "power_output IS NOT NULL",
    "wind_speed positive": "wind_speed >= 0",
    "wind_direction positive": "wind_direction >= 0",
    "power_output positive": "power_output >= 0",
    "within_statistical_range": "lower_bound IS NULL OR power_output BETWEEN lower_bound AND upper_bound"
}

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

# Silver: Clean energy data with statistical outlier detection via join
@dp.table(
    name="dev.silver.turbine_data",
    partition_cols=["is_quarantined"],
)
@dp.expect_all(rules)
def silver_turbine_data():
    return (
        spark.readStream.table("power_output_stats")
        .withColumn("is_quarantined", expr(quarantine_rules))
    )

@dp.view(
    name="valid_silver_turbine_data",
    comment="Valid turbine data passing all quality checks"
)
def valid_silver_turbine_data():
    return spark.read.table("dev.silver.turbine_data").filter("is_quarantined = false")

@dp.view(
    name="invalid_silver_turbine_data",
    comment="Quarantined turbine data failing quality checks"
)
def invalid_silver_turbine_data():
    return spark.read.table("dev.silver.turbine_data").filter("is_quarantined = true")

# Gold: Daily summary statistics per turbine
@dp.materialized_view(
    name="dev.gold.turbine_summary_statistics",
    comment="Daily summary statistics per turbine"
)
def turbine_summary_statistics():
    df = spark.read.table("valid_silver_turbine_data")
    return (
        df.groupBy("turbine_id", to_date(col("timestamp")).alias("date"))
        .agg(
            min("power_output").alias("min_power_output"),
            max("power_output").alias("max_power_output"),
            round(avg("power_output"), 2).alias("avg_power_output"),
        )
    )
