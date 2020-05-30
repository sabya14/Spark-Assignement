package ride_info.aggregator

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, max, min, unix_timestamp, when}
import org.apache.spark.sql.types.DoubleType
import ride_info.aggregator.AggregatorFunctions._

class Aggregator() {
  def aggregate(df: DataFrame): DataFrame = {
    df
      .groupBy("number")
      .agg(
        hourlyAverage("ts") as "hourly_avg",
        dailyAverage("ts") as "daily_avg",
        weeklyAverage("ts") as "weekly_avg",
        monthlyAverage("ts") as "monthly_avg"
      )
  }
}
