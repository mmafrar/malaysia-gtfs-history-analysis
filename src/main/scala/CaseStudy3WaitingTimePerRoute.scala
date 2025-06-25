import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object CaseStudy3WaitingTimePerRoute {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Waiting Time Per Route").getOrCreate()
    
    import spark.implicits._

    // Constants
    val FEED_FILTER = "KTMB"
    val DATASET_DIR = "/Users/mmafrar/malaysia-gtfs-history"

    // Columns to select from each dataset
    val COLUMNS = Map(
      "routes" -> Seq("route_id", "route_short_name"),
      "trips" -> Seq("route_id", "trip_id", "direction_id"),
      "stop_times" -> Seq("trip_id", "departure_time")
    )

    // Helper function to read and filter datasets
    def loadFilteredCsv(folder: String, columns: Seq[String]) = {
      spark.read.option("header", "true").csv(s"$DATASET_DIR/$folder/*.csv")
        .filter(col("feed_id") === FEED_FILTER)
        .select(columns.map(col): _*)
    }

    // Load datasets and remove duplicates based on specified columns
    val routesDf = loadFilteredCsv("routes", COLUMNS("routes")).dropDuplicates("route_id")
    val tripsDf = loadFilteredCsv("trips", COLUMNS("trips")).dropDuplicates("route_id", "trip_id", "direction_id")
    val stopTimesDf = loadFilteredCsv("stop_times", COLUMNS("stop_times")).dropDuplicates("trip_id")

    // Join datasets and create new columns for combined identifiers and timestamp
    val enrichedDf = routesDf
      .join(tripsDf, Seq("route_id"), "inner")
      .join(stopTimesDf, Seq("trip_id"), "inner")
      .withColumn("route_direction_id", concat_ws("__", col("route_id"), col("direction_id")))
      .withColumn("route_trip_direction_id", concat_ws("__", col("route_id"), col("trip_id"), col("direction_id")))
      .withColumn("departure_date_time", to_timestamp(concat(lit("2025-01-01 "), col("departure_time")), "yyyy-MM-dd HH:mm:ss"))
      .drop("route_id", "trip_id", "departure_time")

    // Get the first departure time for each trip
    val tripWindow = Window.partitionBy("route_trip_direction_id").orderBy("departure_date_time")
    val firstDeparturesDf = enrichedDf
      .withColumn("row_num", row_number().over(tripWindow))
      .filter(col("row_num") === 1)
      .drop("row_num")
      .orderBy("direction_id", "route_trip_direction_id", "departure_date_time")

    // Calculate time intervals between consecutive departures for each route direction
    val routeWindow = Window.partitionBy("route_direction_id").orderBy("departure_date_time")
    val intervalsDf = firstDeparturesDf
      .withColumn("prev_departure_time", lag("departure_date_time", 1).over(routeWindow))
      .withColumn("interval_in_minutes", (unix_timestamp(col("departure_date_time")) - unix_timestamp(col("prev_departure_time"))) / 60)
      .drop("prev_departure_time")
      .na.drop("any", Seq("interval_in_minutes"))
      .filter(col("interval_in_minutes") =!= 0)
      .select("route_trip_direction_id", "route_direction_id", "route_short_name", "departure_date_time", "interval_in_minutes")

    // Aggregate statistics (minimum, average, maximum waiting time) per route
    val statsDf = intervalsDf.groupBy("route_short_name").agg(
      round(min("interval_in_minutes"), 0).alias("minimum_waiting_time"),
      round(avg("interval_in_minutes"), 0).alias("average_waiting_time"),
      round(max("interval_in_minutes"), 0).alias("maximum_waiting_time")
    )

    // Display the aggregated statistics
    statsDf.show(false)
  }
}
