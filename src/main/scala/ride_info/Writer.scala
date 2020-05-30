package ride_info

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Writer(spark: SparkSession) {

  def write(df: DataFrame, destinationPath: String): Unit = {
    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save(destinationPath)
  }
}
