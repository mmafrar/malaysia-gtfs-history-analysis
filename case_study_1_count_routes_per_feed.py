from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, countDistinct

DB_NAME = "workspace.malaysia_gtfs_history_analysis"
DATASET_DIR = "/Volumes/workspace/malaysia_gtfs_history_analysis/dataset/routes/*.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("Count Routes Per Feed").getOrCreate()

# Load the CSV files into a DataFrame
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(DATASET_DIR)

# Filter out rows where feed_id is either "KTMB" or "Prasarana_RapidRailKL"
filtered_df = df.filter((col("feed_id") != "KTMB") & (col("feed_id") != "Prasarana_RapidRailKL"))

# Group by feed_id and count distinct route_long_name, then order by the count in descending order
result = filtered_df.groupBy("feed_id").agg(countDistinct(col("route_long_name")).alias("route_count")).orderBy(desc("route_count"))

# Check if the database exists and set it if it does
if spark.catalog.databaseExists(DB_NAME):
    spark.sql(f"USE {DB_NAME}")
    result.write.mode("overwrite").saveAsTable("route_count_per_feed")
else:
    result.show()
