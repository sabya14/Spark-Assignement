package ride_info.aggregator

import ride_info.{Reader, SparkTestSession, Writer}

class AggregatorJobTest extends org.scalatest.FunSuite with SparkTestSession {
  test("should run the actual job with file path") {
    val filePath = getClass.getResource("/sample_input.csv").getPath.toString
    val destinationPath = getClass.getResource("/").getPath.toString + "outputAggregate.csv"
    val reader = new Reader(spark)
    val writer = new Writer(spark)
    val job = new AggregatorJob(reader, writer)
    job.run(filePath, destinationPath)

    val outputDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(destinationPath)
    assert(outputDf.count() > 0)
  }
}
