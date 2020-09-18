package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import uk.co.odinconsultants.sssplayground.kafka.Consuming

import scala.reflect.ClassTag

class Sink(format: String) {

  def writeStream[T: Encoder](df:             Dataset[T],
                              sinkFile:       String,
                              trigger:        Option[Trigger],
                              partitionCol:   Option[String] = Some("period")): StreamingQuery = {
    val stream            = Consuming.streamToHDFS(df, sinkFile, format, trigger)
    val partitionedStream = partitionCol.map(p => stream.partitionBy(p)).getOrElse(stream)
    partitionedStream.start()
  }

  def readFromHdfs[T : Encoder : ClassTag](path: String, session: SparkSession): Dataset[T] =
    readDataFrameFromHdfs(path, session).as[T]

  def readDataFrameFromHdfs(path: String, session: SparkSession): DataFrame =
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

case object CSVFormat extends Format {
  override def format: String = "csv"
}

case object TextFormat extends Format {
  override def format: String = "text"
}

object Sinks {

  def apply(x: Format): Sink = new Sink(x.format)

}
