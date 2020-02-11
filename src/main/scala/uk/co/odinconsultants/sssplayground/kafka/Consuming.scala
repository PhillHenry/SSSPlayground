package uk.co.odinconsultants.sssplayground.kafka

import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object Consuming {

  type KafkaParseFn[T] = (String, String) => Option[T]

  def streamStringsFromKafka[T: Encoder](session: SparkSession, kafkaUrl: String, topicName: String, fn: KafkaParseFn[T]): Dataset[T] = {
    val df = streamFromKafka(session, kafkaUrl, topicName)
    import df.sqlContext.implicits._
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].flatMap { case (key, value) =>
      fn(key, value)
    }
  }


  def streamToHDFS[T: Encoder](df: Dataset[T], sinkFile: String, processTimeMs: Long): DataStreamWriter[T] = {
    df.writeStream.format("parquet")
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path", sinkFile)
      .option("checkpointLocation", sinkFile + "checkpoint")
      .trigger(Trigger.ProcessingTime(processTimeMs))
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
