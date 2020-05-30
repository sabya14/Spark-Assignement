package ride_info

import org.apache.spark.sql.SparkSession

trait SparkTestSession {
  val spark = SparkSession
    .builder
    .appName("Test App")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.default.parallelism", "200")
    .getOrCreate()
}
