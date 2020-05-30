package ride_info.cohort

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import ride_info.{Reader, SparkUtils, Writer}

object Runner extends SparkUtils {

  val reader = new Reader(spark)
  val writer = new Writer(spark)

  def formatDate(date: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    new Timestamp(format.parse(date).getTime)
  }

  def main(args: Array[String]): Unit = {
    val filePath = args(0)
    val startDate = formatDate(args(1))
    val weekRange = args(2).toInt
    val destinationPath = args(3)
    val job = new CohortJob(reader, writer)
    job.run(filePath, startDate, weekRange, destinationPath)
  }
}
