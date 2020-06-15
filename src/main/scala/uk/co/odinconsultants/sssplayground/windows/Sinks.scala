package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain.Payload

class Sink(format: String) {

  def sink(df: Dataset[Payload], sinkFile: String, processTimeMs: Long): StreamingQuery = {
    val checkpointFilename  = sinkFile + "checkpoint"
    df.writeStream.format(format)
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path",               sinkFile)
      .option("checkpointLocation", checkpointFilename)
      .trigger(Trigger.ProcessingTime(processTimeMs))
      .partitionBy("period")
      .start()
  }

  def readFromHdfs(path: String, session: SparkSession): Dataset[Payload] = {
    import session.implicits._
    session.read.format(format).load(path).as[Payload]
  }
}

sealed trait Format {
  def format: String
}

case object DeltaFormat extends Format {
  override def format: String = "delta"
}

case object ParquetFormat extends Format {
  override def format: String = "parquet"
}

object Sinks {

  def apply(x: Format): Sink = new Sink(x.format)

}
