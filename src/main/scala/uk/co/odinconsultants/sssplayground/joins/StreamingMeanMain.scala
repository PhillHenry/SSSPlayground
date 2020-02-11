package uk.co.odinconsultants.sssplayground.joins

import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{RunningMean, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.{streamStringsFromKafka, streamToHDFS}
import uk.co.odinconsultants.sssplayground.spark.Init
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Exception in thread "main" org.apache.spark.sql.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
 * Project [id#32L AS id#43L, count(id)#38L AS count#44L, avg(amount)#39 AS mean#45]
 *
 * Because:
withWatermark must be called on the same column as the timestamp column used in the aggregate.
For example, df.withWatermark("time", "1 min").groupBy("time2").count() is invalid in Append output mode,
as watermark is defined on a different column from the aggregation column. Simply stated, for Append you need WaterMark.
 https://stackoverflow.com/questions/54117961/spark-structured-streaming-exception-append-output-mode-not-supported-without
 */
object StreamingMeanMain {

  def main(args: Array[String]): Unit = {
    val session = Init.session()
    import session.implicits._
    val stream  = streamStringsFromKafka(session, kafkaUrl = args(0), topicName = args(1), parsingDatum).withWatermark("ts", "10 minutes")
    val ds      = stream.groupBy('id).agg(count('id), mean('amount), current_timestamp()).toDF("id", "count", "mean", "ts").withWatermark("ts", "10 minutes")
//    streamToHDFS(ds, sinkFile = args(2), processTimeMs = args(3).toLong).start()
    val query   = ds.writeStream.format("parquet")
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path", args(2))
      .option("checkpointLocation", args(2) + "checkpoint")
      .trigger(Trigger.ProcessingTime(args(3).toLong))
      .start()
    query.awaitTermination()
  }

}
