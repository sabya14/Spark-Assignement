package ride_info.cohort

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.IntegerType
import org.scalatest.Matchers
import ride_info.SparkTestSession

class CohortTest extends org.scalatest.FunSuite with SparkTestSession with DatasetComparer with Matchers {

  import spark.implicits._

  test("should return correct start week and year from startData") {
    val startDate = Timestamp.valueOf("2018-04-07 07:07:17");
    val cohort = new Cohort()
    val tuple = cohort.getStartWeekAndYear(startDate)
    assert(tuple._1 == 14)
    assert(tuple._2 == 2018)

  }


  test("should assign correct week of year") {
    val week1 = Timestamp.valueOf("2018-04-07 10:21:38")
    val week2 = Timestamp.valueOf("2018-04-12 10:21:38")
    val week3 = Timestamp.valueOf("2018-04-18 10:21:38")
    val week4 = Timestamp.valueOf("2018-12-31 00:12:12")

    val customerId = 1
    val inputDf = Seq(
      (week1, customerId),
      (week2, customerId),
      (week3, customerId),
      (week4, customerId)
    ).toDF("ts", "number")

    val cohort = new Cohort()
    val weekColumn = cohort.assignWeekOfYear

    val df = inputDf.withColumn("weekNo", weekColumn.cast(IntegerType))
      .drop("ts")

    val expectedDf = Seq(
      (customerId, 14),
      (customerId, 15),
      (customerId, 16),
      (customerId, 53)
    ).toDF("number", "weekNo")

    assertSmallDatasetEquality(df, expectedDf, ignoreNullable = true)
  }

  test("should create week column") {
    val week1 = Timestamp.valueOf("2018-04-07 10:21:38")
    val week2 = Timestamp.valueOf("2018-04-12 10:21:38")
    val week3 = Timestamp.valueOf("2018-04-18 10:21:38")
    val week4 = Timestamp.valueOf("2018-12-31 00:12:12")


    val customerId = 1
    val inputDf = Seq(
      (week1, customerId),
      (week2, customerId),
      (week3, customerId),
      (week4, customerId)
    ).toDF("ts", "number")

    val cohort = new Cohort()
    val weekColumn = cohort.getWeekColumn(14, 2018)

    val df = inputDf.withColumn("weekNo", weekColumn.cast(IntegerType))
      .drop("ts")

    val expectedDf = Seq(
      (customerId, 1),
      (customerId, 2),
      (customerId, 3),
      (customerId, 40)
    ).toDF("number", "weekNo")

    assertSmallDatasetEquality(df, expectedDf, ignoreNullable = true)
  }


    test("should be able to filter based on date and week range and assign week no") {
      val week1 = Timestamp.valueOf("2020-05-29 18:48:05.123");
      val week2 = Timestamp.valueOf("2020-06-02 21:48:05.123");
      val week3 = Timestamp.valueOf("2020-06-13 18:48:05.123")
      val week4 = Timestamp.valueOf("2020-09-14 18:48:05.123")


      val startDate = Timestamp.valueOf("2020-05-29 18:48:05.123");
      val customerId = 1
      val inputDf = Seq(
        (week1, customerId),
        (week2, customerId),
        (week3, customerId),
        (week4, customerId)
      ).toDF("ts", "number")

      val expectedDf = Seq(
        (customerId, 1),
        (customerId, 2),
        (customerId, 3),
      ).toDF("number", "weekNo")

      val cohort = new Cohort()
      val df = cohort.filterByDateRange(startDate, 22, 2020, 3)(inputDf)
      assertSmallDatasetEquality(df, expectedDf, ignoreNullable = true)
    }


  test("should be able to filter based on date and week range and assign week") {
    val week1 = Timestamp.valueOf("2018-12-30 00:12:12.123");
    val week2 = Timestamp.valueOf("2018-12-30 01:12:12.123");
    val week3 = Timestamp.valueOf("2018-12-30 02:12:12.123")
    val week4 = Timestamp.valueOf("2018-12-31 00:12:12.123")

    val startDate = Timestamp.valueOf("2018-12-30 00:12:12.123");
    val weekRange = 10
    val customerId = 1
    val inputDf = Seq(
      (week1, customerId),
      (week2, customerId),
      (week3, customerId),
      (week4, customerId)
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 4),
      (customerId, 4),
      (customerId, 4),
      (customerId, 4)
    ).toDF("number", "weekNo")

    val cohort = new Cohort()
    val df = cohort.filterByDateRange(startDate, 50, 2018, weekRange)(inputDf)
    assertSmallDatasetEquality(df, expectedDf, ignoreNullable = true)
  }

  test("should be able to calculate week by week cohort when repeated rides by same user") {
    val week1 = Timestamp.valueOf("2020-05-29 18:48:05.123");
    val week2 = Timestamp.valueOf("2020-06-02 21:48:05.123");
    val week3 = Timestamp.valueOf("2020-06-13 18:48:05.123")
    val customerId = 1
    val customerId2 = 2
    val inputDf = Seq(
      (week1, customerId),
      (week1, customerId),
      (week1, customerId),

      (week2, customerId),
      (week2, customerId2),

      (week3, customerId2),
      (week3, customerId),
      (week3, customerId),
      (week3, customerId)
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (1, 1, 1L),
      (1, 2, 1L),
      (1, 3, 1L),
      (2, 2, 2L),
      (2, 3, 2L),
      (3, 3, 2L),
    ).toDF("StartWeek", "EndWeek", "Rides")

    val cohort = new Cohort()
    val df = cohort.calculateCohort(inputDf, week1, 3)
    assertSmallDatasetEquality(df, expectedDf, ignoreNullable = true, orderedComparison = false)
  }

}
