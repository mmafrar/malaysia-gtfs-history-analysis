from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, date_format, when, col, substring, sum as _sum, avg as _avg, expr
)
from pyspark.sql.types import IntegerType

DB_NAME = "workspace.malaysia_gtfs_history_analysis"
DATASET_DIR = "/Volumes/workspace/malaysia_gtfs_history_analysis/dataset/frequencies/*.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("Average Trips By Day Type").getOrCreate()

# Set legacy time parser policy for compatibility
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Load CSV files with header
frequencies_df = spark.read.option("header", "true").csv(DATASET_DIR)

# Add date, day type, and calculate trip duration and number of trips
TIME_FORMAT = "HH:mm:ss"
frequencies_df = (
    frequencies_df
    .withColumn("subdir_date", to_date(col("subdir"), "yyyy-MM-dd"))
    .withColumn("day_of_week", date_format(col("subdir_date"), "u").cast(IntegerType()))
    .withColumn("day_type", when(col("day_of_week") >= 6, "weekend").otherwise("weekday"))
    .withColumn("start_timestamp", expr(f"try_to_timestamp(start_time, '{TIME_FORMAT}')"))
    .withColumn("end_timestamp", expr(f"try_to_timestamp(end_time, '{TIME_FORMAT}')"))
    .withColumn("duration_secs", col("end_timestamp").cast("long") - col("start_timestamp").cast("long"))
    .withColumn("num_trips", (col("duration_secs") / col("headway_secs")).cast(IntegerType()))
)

# Map feed_id to operator names
FEED_ID_DICT = {
    "AGL": "LRT Ampang Line",
    "BRT": "BRT Sunway Line",
    "KGL": "MRT Kajang Line",
    "KJL": "LRT Kelana Jaya Line",
    "MRL": "KL Monorail Line",
    "PYL": "MRT Putrajaya Line",
    "SPL": "LRT Sri Petaling Line",
    "Prasarana_RapidBusKL": "Rapid Bus KL",
    "Prasarana_RapidBusKuantan": "Rapid Bus Kuantan"
}

# Adjust feed_id for RapidRail and assign operator names
frequencies_df = (
    frequencies_df
    .withColumn(
        "feed_id",
        when(col("feed_id") == "Prasarana_RapidRailKL", substring(col("trip_id"), 0, 3))
        .otherwise(col("feed_id"))
    )
    .replace(FEED_ID_DICT, subset=["feed_id"])
    .withColumnRenamed("feed_id", "operator_name")
)

# Aggregate total trips per day by operator and day type
daily_trips_df = (
    frequencies_df
    .groupBy("operator_name", "subdir", "day_type")
    .agg(_sum("num_trips").alias("total_trips"))
    .orderBy("subdir")
)

# Compute average daily trips by operator and day type with partitioning
avg_trips_by_day_type_df = (
    daily_trips_df
    .repartition("operator_name", "day_type")
    .groupBy("operator_name", "day_type")
    .agg(_avg("total_trips").cast(IntegerType()).alias("avg_daily_trips"))
    .orderBy("operator_name", "day_type")
)

# Check if the database exists and set it if it does
if spark.catalog.databaseExists(DB_NAME):
    spark.sql(f"USE {DB_NAME}")
    avg_trips_by_day_type_df.write.mode("overwrite").saveAsTable("avg_trips_by_day_type")
else:
    avg_trips_by_day_type_df.show()
