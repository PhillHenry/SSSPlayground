package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}


object ConsumeKafkaMain {

  case class Payload(payload: String, period: String)

  def main(args: Array[String]): Unit = {
    println("hello")
    val s               = session()
    val kafkaUrl        = args(0)
    val topicNAme       = args(1)
    val sinkFile        = args(2)
    val processTimeMs   = args(3).toLong
    val stream          = streamFromKafka(s, kafkaUrl, topicNAme, trivialKafkaParseFn)
    streamingToHDFS(stream, sinkFile, processTimeMs)
    s.streams.awaitAnyTermination()
  }

  def session(): SparkSession = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).getOrCreate()
  }

  private def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.setAppName("SSSPlayground")
    conf
  }

  def streamingToHDFS(df: Dataset[Payload], sinkFile: String, processTimeMs: Long): StreamingQuery = {
    val checkpointFilename  = sinkFile + "checkpoint"
    val streamingQuery      = df.writeStream.format("parquet")
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path",               sinkFile)
      .option("checkpointLocation", checkpointFilename)
      .trigger(Trigger.ProcessingTime(processTimeMs))
      .partitionBy("period")
      .start()
    streamingQuery
  }

  type KafkaParseFn = (String, String) => Option[Payload]

  val trivialKafkaParseFn: KafkaParseFn = { case (_, v) => Some(Payload(v, (v.hashCode % 10).toString)) }

  def streamFromKafka(session: SparkSession, kafkaUrl: String, topicName: String, fn: KafkaParseFn): Dataset[Payload] = {
    val df = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",  kafkaUrl)
      .option("subscribe",                topicName)
      .option("offset",                   "earliest")
      .option("startingOffsets",          "earliest")
      .load()
    import df.sqlContext.implicits._
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].flatMap { case (key, value) =>
      fn(key, value)
    }
  }

}
