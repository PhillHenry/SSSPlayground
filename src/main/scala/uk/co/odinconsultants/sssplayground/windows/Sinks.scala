package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag

class Sink(format: String) {

  def writeStream(df: Dataset[_], sinkFile: String, processTimeMs: Long, partitionCol: Option[String] = Some("period")): StreamingQuery = {
    val checkpointFilename  = sinkFile + "checkpoint"
    val stream              = df.writeStream.format(format)
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path",               sinkFile)
      .option("checkpointLocation", checkpointFilename)
      .trigger(Trigger.ProcessingTime(processTimeMs))
    val partitionedStream   = partitionCol.map(p => stream.partitionBy(p)).getOrElse(stream)
    partitionedStream.start()
  }

  def readFromHdfs[T : Encoder : ClassTag](path: String, session: SparkSession): Dataset[T] =
    readDataFrameFromHdfs(path, session).as[T]

  def readDataFrameFromHdfs(path: String, session: SparkSession) =
    session.read.format(format).load(path)
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
