package ride_info

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}


class ReaderTest extends org.scalatest.FunSuite with SparkTestSession with DatasetComparer {
  test("should read data from csv") {
    val filePath = getClass.getResource("/test.csv").getPath.toString
    val reader = new Reader(spark)
    val df = reader.read(filePath)

    val schema = StructType(Array(
      StructField("ts", TimestampType, true),
      StructField("number", IntegerType, true),
    ))

    val expected = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(filePath)
    assertSmallDatasetEquality(df, expected, ignoreNullable = true)
  }
};
