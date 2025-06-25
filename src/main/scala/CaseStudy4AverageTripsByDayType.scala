import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SparkSession, functions => F}

object CaseStudy4AverageTripsByDayType {

  def timeToSeconds = udf((time: String) => {
    val parts = time.split(":").map(_.toInt)
    parts(0) * 3600 + parts(1) * 60 + parts(2)
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Average Trips By Day Type").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    import spark.implicits._
    
    val dataDirectory = "/Users/mmafrar/malaysia-gtfs-history/frequencies/*.csv"

    // Read CSV files
    val df = spark.read.option("header", "true").csv(dataDirectory)

    // Determine if the trip is weekday or weekend
    val dfWithDayType = df
      .withColumn("subdir_date", F.to_date($"subdir", "yyyy-MM-dd"))
      .withColumn("day_of_week", F.date_format($"subdir_date", "u").cast("int")) // 1 = Monday, 7 = Sunday
      .withColumn("day_type",
        F.when($"day_of_week" >= 6, "weekend").otherwise("weekday")
      )

    val timeFormat = "HH:mm:ss"

    // Calculate the number of trips
    val dfWithTrips = dfWithDayType
      .withColumn("start_seconds", timeToSeconds(col("start_time")))
      .withColumn("end_seconds", timeToSeconds(col("end_time")))
      .withColumn("duration_secs", col("end_seconds") - col("start_seconds"))
      .withColumn("num_trips", (col("duration_secs") / col("headway_secs")).cast("int"))

    val feedIdMap = Map(
      "AGL" -> "LRT Ampang Line",
      "BRT" -> "BRT Sunway Line",
      "KGL" -> "MRT Kajang Line",
      "KJL" -> "LRT Kelana Jaya Line",
      "MRL" -> "KL Monorail Line",
      "PYL" -> "MRT Putrajaya Line",
      "SPL" -> "LRT Sri Petaling Line",
      "Prasarana_RapidBusKL" -> "Rapid Bus KL",
      "Prasarana_RapidBusKuantan" -> "Rapid Bus Kuantan"
    )

    val mapFeedIdUDF = udf((fid: String) => feedIdMap.getOrElse(fid, fid))

    // Replace feed_id for RapidRail using trip_id prefix
    val adjustedDf = dfWithTrips
      .withColumn(
        "feed_id",
        when(col("feed_id") === "Prasarana_RapidRailKL", substring(col("trip_id"), 0, 3))
          .otherwise(col("feed_id"))
      )
      .withColumn("operator_name", mapFeedIdUDF(col("feed_id")))

    // Group by subdir to get the daily total number of trips
    val dailyTrips = adjustedDf
      .groupBy("operator_name", "subdir", "day_type")
      .agg(sum("num_trips").alias("total_trips"))
      .orderBy("subdir")

    // Calculate the average number of trips for weekday and weekend grouped by feed_id
    val avgTripsByDayType = dailyTrips
      .groupBy("operator_name", "day_type")
      .agg(avg("total_trips").cast("int").alias("avg_daily_trips"))
      .orderBy("operator_name", "day_type")

    // Pivot so weekday and weekend appear as columns
    val pivotedTrips = avgTripsByDayType
      .groupBy("operator_name")
      .pivot("day_type", Seq("weekday", "weekend"))
      .agg(first("avg_daily_trips"))
      .withColumnRenamed("weekday", "weekday_avg_daily_trips")
      .withColumnRenamed("weekend", "weekend_avg_daily_trips")
      .orderBy("operator_name")

    // Display final output
    pivotedTrips.show(false)
  }
}
