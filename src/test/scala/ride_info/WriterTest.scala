package ride_info

import com.github.mrpowers.spark.fast.tests.DatasetComparer

class WriterTest extends org.scalatest.FunSuite with SparkTestSession with DatasetComparer {
  test("should write data to csv") {
    val inputPath = getClass.getResource("/test.csv").getPath.toString
    val filePath = getClass.getResource("/").getPath.toString + "testOutput.csv"
    val writer = new Writer(spark)

    val toWriteDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputPath)

    writer.write(toWriteDf, filePath)

    val expected = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

    assertSmallDatasetEquality(toWriteDf, expected, ignoreNullable = true)
  }

}
