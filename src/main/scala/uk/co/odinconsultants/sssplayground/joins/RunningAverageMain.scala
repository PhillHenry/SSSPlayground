package uk.co.odinconsultants.sssplayground.joins

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.{KafkaParseFn, streamStringsFromKafka}
import uk.co.odinconsultants.sssplayground.spark.Init

import scala.annotation.tailrec

object RunningAverageMain {

  case class RunningMean(id: Long, count: Long, mean: Double)
  case class Datum(id: Long, amount: Double)

  def main(args: Array[String]): Unit = {
    val s         = Init.session()
    import s.implicits._
    val stream    = streamStringsFromKafka(s, kafkaUrl = args(0), topicName = args(1), parsingDatum)
    val pauseMS   = args(4).toLong

    @tailrec
    def sample(): Unit = {
      val query     = streamingToHDFS(stream, sinkFile = args(2), processTimeMs = args(3).toLong)
      Thread.sleep(pauseMS)
      query.stop()
      sample()
    }

    sample()
  }

  def streamingToHDFS(df: Dataset[Datum], sinkFile: String, processTimeMs: Long): StreamingQuery = {
    val checkpointFilename  = sinkFile + "checkpoint"
    df.writeStream.format("parquet")
      .outputMode(OutputMode.Append()) // Data source parquet does not support Complete output mode;
      .option("path",               sinkFile)
      .option("checkpointLocation", checkpointFilename)
      .trigger(Trigger.ProcessingTime(processTimeMs))
      .start()
  }

  val parsingDatum: KafkaParseFn[Datum] = { case (k, v) => Some(Datum(k.toLong, v.toDouble)) }

  def updating(stream: Dataset[RunningMean], static: Dataset[RunningMean]): Dataset[RunningMean] = {
    import stream.sparkSession.implicits._

    val aggStream:  DataFrame = stream.groupBy('id).agg(count('id), mean('amount)).toDF("id", "count", "mean")
    val joined:     DataFrame = aggStream.join(static, aggStream("id") === static("id"), "outer")

    joined.map(foldMean)
  }

  def toMean(r: Row, offset: Int): RunningMean = RunningMean(r.getLong(0 + offset), r.getLong(1 + offset), r.getDouble(2 + offset))

  val add: (RunningMean, RunningMean) => RunningMean = { case (x, y) =>
    val total: Long = x.count + y.count
    val av = ((x.count * x.mean) + (y.count * y.mean)) / total
    RunningMean(x.id, total, av)
  }

  /** assumes row is:
   * idX|countX|meanX|idY|countY|meanY
   * and that all X values are not null
   */
  def foldMean(r: Row): RunningMean = {
    if (r.isNullAt(3)) {
      toMean(r, 0)
    } else {
      val x = toMean(r, 0)
      val y = toMean(r, 3)
      add(x, y)
    }
  }

}
