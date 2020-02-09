package uk.co.odinconsultants.sssplayground.joins

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object RunningAverageMain {

  def main(args: Array[String]): Unit = ???

  def updating(stream: Dataset[RunningMean], static: Dataset[RunningMean]): Dataset[RunningMean] = {
    import stream.sparkSession.implicits._

    val aggStream:  DataFrame = stream.groupBy('id).agg(count('id), mean('amount)).toDF("id", "count", "mean")
    val joined:     DataFrame = aggStream.join(static, aggStream("id") === static("id"), "outer")

    joined.map(foldMean)
  }

  case class RunningMean(id: Long, count: Long, mean: Double)

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
