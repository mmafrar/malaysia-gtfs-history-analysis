from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count, round, concat_ws

DB_NAME = "workspace.malaysia_gtfs_history_analysis"
DATASET_DIR = "/Volumes/workspace/malaysia_gtfs_history_analysis/dataset/stops/*.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("Disconnected Stops Per Feed").getOrCreate()

# Read the dataset and process the latitude and longitude columns
df = (spark.read.option("header", "true").csv(DATASET_DIR)
        .selectExpr("*", "try_cast(stop_lat AS DOUBLE) AS stop_lat_double")
        .selectExpr("*", "try_cast(stop_lon AS DOUBLE) AS stop_lon_double")
        .na.drop(subset=["stop_lat_double", "stop_lon_double"])
        .dropDuplicates(["stop_lat_double", "stop_lon_double"])
        .withColumn("stop_lat_lon", concat_ws(",", round("stop_lat", 2), round("stop_lon", 2)))
)

# Define window specification for partitioning by feed_id and stop_lat_lon
window_spec = Window.partitionBy("feed_id", "stop_lat_lon")

# Filter out rows where stop_lat_lon count is greater than 1
df_filtered = df.withColumn("stop_lat_lon_count", count("*").over(window_spec)) \
                .filter("stop_lat_lon_count = 1").drop("stop_lat_lon_count")

# Group by feed_id and count the occurrences, then order by count in descending order
df_feed_id_count = df_filtered.groupBy("feed_id").count().withColumnRenamed("count", "stop_count").orderBy(col("stop_count").desc())

if spark.catalog.databaseExists(DB_NAME):
    spark.sql(f"USE {DB_NAME}")
    df_feed_id_count.write.mode("overwrite").saveAsTable("disconnected_stops_per_feed")
else:
    df_feed_id_count.show()
