package ride_info.aggregator

import ride_info.{Reader, SparkUtils, Writer}

object Runner extends SparkUtils {

  val reader = new Reader(spark)
  val writer = new Writer(spark)

  def main(args: Array[String]): Unit = {
    val filePath = args(0)
    val destinationPath = args(1)
    val job = new AggregatorJob(reader, writer)
    job.run(filePath, destinationPath)
  }
}
