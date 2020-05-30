package ride_info.cohort

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import ride_info.{Reader, SparkTestSession, Writer}

class CohortJobTest extends org.scalatest.FunSuite with SparkTestSession {
  test("should run the actual job with file path") {
    val startDateAsString = "2018-04-07"
    val format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    val date = format.parse(startDateAsString);
    val startDate = new Timestamp(date.getTime)
    val filePath = getClass.getResource("/sample_input.csv").getPath.toString
    val destinationPath = getClass.getResource("/").getPath.toString + "output.csv"
    val weekRange = 3
    val reader = new Reader(spark)
    val writer = new Writer(spark)
    val job = new CohortJob(reader, writer)
    job.run(filePath, startDate, weekRange, destinationPath)

    val outputDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(destinationPath)
    assert(outputDf.count() > 0)

  }
}
