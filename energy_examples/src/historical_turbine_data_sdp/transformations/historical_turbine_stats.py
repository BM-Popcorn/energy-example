from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, expr, min, max, avg, stddev, to_date, round, lit

@dp.materialized_view(
    name="dev.silver.historical_stats_table",
    comment="Historical power output statistical bounds per turbine"
)
def historical_stats_table():
    try:
        df = spark.read.table("dev.silver.turbine_data").filter("is_quarantined = false")
        return (
            df.groupBy("turbine_id")
            .agg(
                (avg("power_output") - 2 * stddev("power_output")).alias("lower_bound"),
                (avg("power_output") + 2 * stddev("power_output")).alias("upper_bound")
            )
        )
    except Exception:
        # Silver table doesn't exist yet - return empty DataFrame with expected schema
        schema = StructType([
            StructField("turbine_id", StringType(), True),
            StructField("lower_bound", DoubleType(), True),
            StructField("upper_bound", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)