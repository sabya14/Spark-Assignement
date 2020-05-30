package ride_info.aggregator

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}


object AggregatorFunctions {

  def maxMinDateDiffAsSeconds(tsColumnName: String): Column = {
    unix_timestamp(max(col(tsColumnName))).minus(unix_timestamp(min(col(tsColumnName))))
  }

  def maxMinDateDiffAsDays(tsColumnName: String): Column = {
    datediff(max(col(tsColumnName)), min(col(tsColumnName)))
  }

  def maxMinDateDiffAsWeeks(tsColumnName: String): Column = {
    val maxDate = max(col(tsColumnName))
    val minDate = min(col(tsColumnName))
    val yearDiff = year(maxDate).minus(year(minDate))
    val maxWeek = weekofyear(maxDate)
    val minWeek = weekofyear(minDate)
    maxWeek.minus(minWeek).plus(yearDiff * 52)
  }

  def maxMinDateDiffAsMonth(tsColumnName: String): Column = {
    val maxDate = max(col(tsColumnName))
    val minDate = min(col(tsColumnName))
    floor(months_between(maxDate, minDate, roundOff = false))
  }

  def hourlyAverage(tsColumnName: String): Column = {
    val secondsToHour = 3600
    val maxMinDateDiffInHour = (maxMinDateDiffAsSeconds(tsColumnName) / secondsToHour).cast(IntegerType)
    count(lit(1))
      .divide(
        when(maxMinDateDiffInHour === 0, lit(1.0).cast(DoubleType))
          .otherwise(maxMinDateDiffInHour.cast(DoubleType))
      )

  }

  def dailyAverage(tsColumnName: String): Column = {
    val maxMinDateDiffInDays = maxMinDateDiffAsDays(tsColumnName)
    avgBasedOn(maxMinDateDiffInDays)

  }

  def weeklyAverage(tsColumnName: String): Column = {
    val maxMinDateDiffInWeeks = maxMinDateDiffAsWeeks(tsColumnName)
    avgBasedOn(maxMinDateDiffInWeeks)
  }


  def monthlyAverage(tsColumnName: String): Column = {
    val maxMinDateDiffInMonths = maxMinDateDiffAsMonth(tsColumnName)
    avgBasedOn(maxMinDateDiffInMonths)

  }

  private def avgBasedOn(column: Column) = {
    count(lit(1))
      .divide(minOfValueOr1(column))
  }


  private def minOfValueOr1(column: Column) = {
    when(column === 0, lit(1.0).cast(DoubleType))
      .otherwise(column.cast(DoubleType) + 1.0)
  }
}
