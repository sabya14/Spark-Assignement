package ride_info.aggregator

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.Matchers
import ride_info.SparkTestSession


class AggregatorTest extends org.scalatest.FunSuite with SparkTestSession with DatasetComparer with Matchers{

  import spark.implicits._

  test("should return hourly, daily, weekly, aggregates") {
    val timeStamp1 = "2019-01-01 18:48:05.123"
    val timeStamp2 = "2019-01-02 18:48:05.123"
    val timeStamp3 = "2019-01-03 18:48:05.123"
    val dateTime1 = Timestamp.valueOf(timeStamp1)
    val dateTime2 = Timestamp.valueOf(timeStamp2)
    val dateTime3 = Timestamp.valueOf(timeStamp3)


    val customerId = 1
    val inputDf = Seq(
      (dateTime1, customerId),
      (dateTime2, customerId),
      (dateTime3, customerId),
    ).toDF("ts", "number")

    val expectedDf = Seq(
      (customerId, 0.0625, 1, 3, 3),
    ).toDF("number", "hourly_avg","daily_avg","weekly_avg","monthly_avg")

    val aggregator = new Aggregator()
    val output = aggregator.aggregate(inputDf)
    output.collect() should contain theSameElementsAs expectedDf.collect()
  }
}
