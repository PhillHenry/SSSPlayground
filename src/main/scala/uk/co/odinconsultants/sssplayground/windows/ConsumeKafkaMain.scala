package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import uk.co.odinconsultants.sssplayground.kafka.Consuming._
import uk.co.odinconsultants.sssplayground.spark.Init

object ConsumeKafkaMain {

  def main(args: Array[String]): Unit = {
    val kafkaUrl        = args(0)
    val topicName       = args(1)
    val sinkFile        = args(2)
    val processTimeMs   = args(3).toLong
    val s               = Init.session()
    import s.implicits._
    val stream          = streamStringsFromKafka(s, kafkaUrl, topicName, trivialKafkaParseFn)
    streamingToDelta(stream, sinkFile, processTimeMs)
    s.streams.awaitAnyTermination()
  }

  val trivialKafkaParseFn: KafkaParseFn[Payload] = { case (_, v) => Some(Payload(v, (v.hashCode % 10).toString)) }

  case class Payload(payload: String, period: String)

  val FORMAT = "delta"

  def streamingToDelta(df: Dataset[Payload], sinkFile: String, processTimeMs: Long): StreamingQuery = {
    val checkpointFilename  = sinkFile + "checkpoint"
    df.writeStream.format(FORMAT)
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path",               sinkFile)
      .option("checkpointLocation", checkpointFilename)
      .trigger(Trigger.ProcessingTime(processTimeMs))
      .partitionBy("period")
      .start()
  }

  def readFromHdfs(path: String, session: SparkSession): Dataset[Payload] = {
    import session.implicits._
    session.read.format(FORMAT).load(path).as[Payload]
  }

}
