package ride_info.aggregator

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import ride_info.SparkTestSession
import ride_info.aggregator.AggregatorFunctions._

class AggregatorFunctionsTest extends org.scalatest.FunSuite with SparkTestSession with DatasetComparer {

  import spark.implicits._

  test("should return hourly average for a single customer") {
    val timeStampStart = "2018-10-02 18:48:05.123"
    val timeStampEnd = "2018-10-02 21:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId)
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 1.00),
    ).toDF("number", "avg_hour")

    val outputDf = inputDf
      .groupBy("number")
      .agg(hourlyAverage("ts") as "avg_hour")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }

  test("should return hourly average for a single customer when ride is taking in same hour") {
    val timeStampStart = "2018-10-02 18:48:05.123"
    val timeStampEnd = "2018-10-02 18:51:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId)
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 3.00),
    ).toDF("number", "avg_hour")

    val outputDf = inputDf
      .groupBy("number")
      .agg(hourlyAverage("ts") as "avg_hour")

    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }

  test("should return daily average for a single customer") {
    val timeStampStart = "2018-10-02 18:48:05.123"
    val timeStampEnd = "2018-10-03 21:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId),
      (dateTimeEnd, customerId)
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 2.00),
    ).toDF("number", "daily_avg")

    val outputDf = inputDf
      .groupBy("number")
      .agg(dailyAverage("ts") as "daily_avg")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }

  test("should return weekly average for a single customer") {
    val timeStampStart = "2018-10-14 18:48:05.123"
    val timeStampEnd = "2018-10-15 21:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId),
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 1.00),
    ).toDF("number", "weekly_avg")

    val outputDf = inputDf
      .groupBy("number")
      .agg(weeklyAverage("ts") as "weekly_avg")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }

  test("should return weekly average for a single customer if all rides in same week") {
    val timeStampStart = "2018-10-13 18:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 3.00),
    ).toDF("number", "weekly_avg")

    val outputDf = inputDf
      .groupBy("number")
      .agg(weeklyAverage("ts") as "weekly_avg")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }


  test("should return monthly average for a single customer") {
    val timeStampStart = "2018-10-13 18:48:05.123"
    val timeStampEnd = "2018-11-14 21:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId),
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 1.00),
    ).toDF("number", "monthly_avg")

    val outputDf = inputDf
      .groupBy("number")
      .agg(monthlyAverage("ts") as "monthly_avg")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }

  test("should return monthly average for a single customer spread across two years") {
    val timeStampStart = "2019-01-01 18:48:05.123"
    val timeStampEnd = "2020-02-01 21:48:05.123"
    val dateTimeStart = Timestamp.valueOf(timeStampStart);
    val dateTimeEnd = Timestamp.valueOf(timeStampEnd)
    val customerId = 1
    val inputDf = Seq(
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeStart, customerId),
      (dateTimeEnd, customerId),
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 0.42857142857142855),
    ).toDF("number", "monthly_avg")

    val outputDf = inputDf
      .groupBy("number")
      .agg(monthlyAverage("ts") as "monthly_avg")
    assertSmallDatasetEquality(outputDf, expectedDf, ignoreNullable = true)

  }
}
