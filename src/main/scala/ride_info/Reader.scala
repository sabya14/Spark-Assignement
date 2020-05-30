package ride_info

import org.apache.spark.sql.{DataFrame, SparkSession}

class Reader(spark: SparkSession) {
  def read(filePath: String): DataFrame = {
    import org.apache.spark.sql.types._

    val schema = StructType(Array(
      StructField("ts", TimestampType, true),
      StructField("number", IntegerType, true),
    ))

    spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(filePath)
      .na.drop()
  }
}
