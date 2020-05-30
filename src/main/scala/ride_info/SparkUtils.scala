package ride_info

import org.apache.spark.sql.SparkSession

trait SparkUtils {
  val spark = SparkSession
    .builder
    .appName("Ride Info")
    .getOrCreate()
}
