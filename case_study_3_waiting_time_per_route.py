from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as func

FEED_FILTER = "KTMB"
DB_NAME = "workspace.malaysia_gtfs_history_analysis"
DATASET_DIR = "/Volumes/workspace/malaysia_gtfs_history_analysis/dataset"

# Columns to select from each dataset
COLUMNS = {
    "routes": ["route_id", "route_short_name"],
    "trips": ["route_id", "trip_id", "direction_id"],
    "stop_times": ["trip_id", "departure_time"]
}

# Initialize Spark session
spark = SparkSession.builder.appName("Waiting Time Per Route").getOrCreate()

# Helper function to read and filter datasets
def load_filtered_csv(folder, columns):
    return (
        spark.read.option("header", True).csv(f"{DATASET_DIR}/{folder}/*.csv")
        .filter(func.col("feed_id") == FEED_FILTER)
        .select(*columns)
    )

# Load datasets and remove duplicates based on specified columns
routes_df = load_filtered_csv("routes", COLUMNS["routes"]).dropDuplicates(["route_id"])
trips_df = load_filtered_csv("trips", COLUMNS["trips"]).dropDuplicates(["route_id", "trip_id", "direction_id"])
stop_times_df = load_filtered_csv("stop_times", COLUMNS["stop_times"]).dropDuplicates(["trip_id"])

# Join datasets and create new columns for combined identifiers and timestamp
enriched_df = (
    func.broadcast(routes_df)
    .join(trips_df, on="route_id", how="inner")
    .join(stop_times_df, on="trip_id", how="inner")
    .withColumn("route_direction_id", func.concat_ws("__", "route_id", "direction_id"))
    .withColumn("route_trip_direction_id", func.concat_ws("__", "route_id", "trip_id", "direction_id"))
    .withColumn("departure_date_time", func.to_timestamp(func.concat(func.lit("2025-01-01 "), "departure_time"), "yyyy-MM-dd HH:mm:ss"))
    .drop("route_id", "trip_id", "departure_time")
)

# Get the first departure time for each trip
trip_window = Window.partitionBy("route_trip_direction_id").orderBy("departure_date_time")
first_departures_df = (
    enriched_df
    .withColumn("row_num", func.row_number().over(trip_window))
    .filter(func.col("row_num") == 1)
    .drop("row_num")
    .orderBy("direction_id", "route_trip_direction_id", "departure_date_time")
)

# Calculate time intervals between consecutive departures for each route direction
route_window = Window.partitionBy("route_direction_id").orderBy("departure_date_time")
intervals_df = (
    first_departures_df
    .withColumn("prev_departure_time", func.lag("departure_date_time").over(route_window))
    .withColumn("interval_in_minutes", 
                (func.unix_timestamp("departure_date_time") - func.unix_timestamp("prev_departure_time")) / 60)
    .drop("prev_departure_time")
    .dropna(subset=["interval_in_minutes"])
    .filter(func.col("interval_in_minutes") != 0)
    .select("route_trip_direction_id", "route_direction_id", "route_short_name", "departure_date_time", "interval_in_minutes")
)

# Aggregate statistics (minimum, average, maximum waiting time) per route
stats_df = intervals_df.groupBy("route_short_name").agg(
    func.round(func.min("interval_in_minutes"), 0).alias("minimum_waiting_time"),
    func.round(func.avg("interval_in_minutes"), 0).alias("average_waiting_time"),
    func.round(func.max("interval_in_minutes"), 0).alias("maximum_waiting_time")
)

# Check if the database exists and set it if it does
if spark.catalog.databaseExists(DB_NAME):
    spark.sql(f"USE {DB_NAME}")
    stats_df.write.mode("overwrite").saveAsTable("route_waiting_time_stats")
else:
    stats_df.show()
