import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object CaseStudy1CountRoutesPerFeed {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Count Routes Per Feed").getOrCreate()

    import spark.implicits._

    val dataDirectory = "/Users/mmafrar/malaysia-gtfs-history/routes/*.csv"

    // Read all the data from CSV into DataFrame
    val df = spark.read
      .option("header", "true")
      .csv(dataDirectory)

    // Filter out rows where feed_id is either KTMB or Prasarana_RapidRailKL
    val filteredDf = df.filter(
      !$"feed_id".isin("KTMB", "Prasarana_RapidRailKL")
    )

    // Group by feed_id and count distinct route_long_name, then order by count in descending order
    val resultDf = filteredDf.groupBy("feed_id")
      .agg(
        countDistinct("route_long_name").alias("distinct_route_count")
      )
      .orderBy(desc("distinct_route_count"))

    // Show the result
    resultDf.show(false)
  }
}
