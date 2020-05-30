package ride_info.cohort

import java.sql.Timestamp

import ride_info.{Reader, Writer}
import org.apache.spark.sql.SparkSession

class CohortJob(reader: Reader, writer: Writer) {
  def run(filePath: String, startDate: Timestamp, weekRange: Int, destinationPath: String): Any = {
    val df = reader.read(filePath)
    val cohort = new Cohort()
    val outputDf = cohort.calculateCohort(df, startDate, weekRange)
    writer.write(outputDf, destinationPath)
  }
}
