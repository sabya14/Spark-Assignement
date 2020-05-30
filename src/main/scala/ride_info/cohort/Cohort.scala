package ride_info.cohort

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.time.temporal.IsoFields

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

class Cohort() {

  val TIME_STAMP_COLUMN = "ts"
  val WEEK_NO = "weekNo"
  val RIDES = "Rides"
  val START_WEEK_ALIAS = "StartWeek"
  val END_WEEK_ALIAS = "EndWeek"


  def getStartWeekAndYear(startDate: Timestamp): (Int, Int) = {
    val startZonedDate = startDate.toInstant().atZone(ZoneId.systemDefault()).withFixedOffsetZone()
    val startWeek = startZonedDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
    val startYear = startZonedDate.getYear
    (startWeek, startYear)
  }

  def getWeekColumn(startWeek: Int, startYear: Int): Column = {
    val yearlyWeekAdjustment = year(col(TIME_STAMP_COLUMN)).cast(IntegerType).minus(startYear) * 52.1428571
    val weekColumn: Column = assignWeekOfYear.minus(startWeek - 1) + yearlyWeekAdjustment
    weekColumn
  }

  def assignWeekOfYear = {
    val lastSunday = date_sub(next_day(col(TIME_STAMP_COLUMN), "Sunday"), 7)
    val weekColumn = weekofyear(lastSunday).plus(lit(1))
    weekColumn
  }

  def filterByDateRange(startDate: Timestamp, startWeek: Int, startYear: Int, weekRange: Int)(df: DataFrame): DataFrame = {
    val weekColumn = getWeekColumn(startWeek, startYear)
    df
      .withColumn("weekNo", weekColumn.cast(IntegerType))
      .drop(TIME_STAMP_COLUMN)
      .filter(col(WEEK_NO) <= weekRange)
  }


  def calculateCohort(df: DataFrame, startDate: Timestamp, weekRange: Int): DataFrame = {
    val (startWeek, startYear) = getStartWeekAndYear(startDate)
    val filteredDf = df
      .transform(filterByDateRange(startDate, startWeek, startYear, weekRange))

    val startDf = filteredDf
      .withColumnRenamed("weekNo", START_WEEK_ALIAS)
      .withColumnRenamed("number", "startNumber")
      .distinct()

    val endDf = filteredDf
      .withColumnRenamed("weekNo", END_WEEK_ALIAS)
      .withColumnRenamed("number", "endNumber")
      .distinct()

    val resultDf = startDf
      .join(endDf, col("startNumber") === col("endNumber"), "inner")
      .filter(col(START_WEEK_ALIAS) <= col(END_WEEK_ALIAS))
      .groupBy(START_WEEK_ALIAS, END_WEEK_ALIAS)
      .agg(count("*") as RIDES)

    resultDf
  }

}
