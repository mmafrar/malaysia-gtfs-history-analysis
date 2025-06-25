import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object CaseStudy2DisconnectedStopsPerFeed {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Disconnected Stops Per Feed").getOrCreate()

    import spark.implicits._

    val dataDirectory = "/Users/mmafrar/malaysia-gtfs-history/stops/*.csv"

    // Read the dataset and process the latitude and longitude columns
    val df = spark.read
      .option("header", "true")
      .csv(dataDirectory)
      .selectExpr("*", "try_cast(stop_lat AS DOUBLE) AS stop_lat_double")
      .selectExpr("*", "try_cast(stop_lon AS DOUBLE) AS stop_lon_double")
      .na.drop(Seq("stop_lat_double", "stop_lon_double"))
      .dropDuplicates("stop_lat_double", "stop_lon_double")
      .withColumn(
        "stop_lat_lon",
        concat_ws(",", round(col("stop_lat"), 2), round(col("stop_lon"), 2))
      )

    // Define window specification for partitioning by feed_id and stop_lat_lon
    val windowSpec = Window.partitionBy("feed_id", "stop_lat_lon")

    // Filter out rows where stop_lat_lon count is greater than 1
    val dfFiltered = df
      .withColumn("stop_lat_lon_count", count("*").over(windowSpec))
      .filter(col("stop_lat_lon_count") === 1)
      .drop("stop_lat_lon_count")

    // Group by feed_id and count the occurrences, then order by count in descending order
    val dfFeedIdCount = dfFiltered
      .groupBy("feed_id")
      .count()
      .orderBy(desc("count"))

    // Show the result
    dfFeedIdCount.show(false)
  }
}
