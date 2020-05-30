package ride_info.aggregator

import java.sql.Timestamp

import ride_info.cohort.Cohort
import ride_info.{Reader, Writer}

class AggregatorJob(reader: Reader, writer: Writer) {
  def run(filePath: String, destinationPath: String): Any = {
    val aggregator = new Aggregator()
    val df = reader.read(filePath)
    val outputDf = aggregator.aggregate(df)
    writer.write(outputDf, destinationPath)
  }
}
