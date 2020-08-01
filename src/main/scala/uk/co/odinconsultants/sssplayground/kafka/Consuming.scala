package uk.co.odinconsultants.sssplayground.kafka

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object Consuming {

  val logger = Logger.getLogger(this.getClass)

  type KafkaParseFn[T] = (String, String) => Option[T]

  def streamStringsFromKafka[T: Encoder](session: SparkSession, kafkaUrl: String, topicName: String, fn: KafkaParseFn[T]): Dataset[T] = {
    val df = streamFromKafka(session, kafkaUrl, topicName)
    import df.sqlContext.implicits._
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].flatMap { case (key, value) =>
      val x = fn(key, value)
      logger.debug(s"key = ${toTruncatedString(key)}, value = ${toTruncatedString(value)}, x = ${toTruncatedString(x)}")
      x
    }
  }

  def toTruncatedString(x: Any): String = if (x == null) "null" else x.toString.substring(0, math.min(x.toString.length, 100))

  def streamToHDFS[T: Encoder](df: Dataset[T], sinkFile: String, maybeTrigger: Option[Trigger]): DataStreamWriter[T] = {
    val stream = df.writeStream.format("parquet")
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path", sinkFile)
      .option("checkpointLocation", sinkFile + "checkpoint")
    maybeTrigger.map(stream.trigger(_)).getOrElse(stream)
  }

  def streamFromKafka(session: SparkSession, kafkaUrl: String, topicName: String): DataFrame =
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",  kafkaUrl)
      .option("subscribe",                topicName)
      .option("offset",                   "earliest")
      .option("startingOffsets",          "earliest")
      .load()

}
