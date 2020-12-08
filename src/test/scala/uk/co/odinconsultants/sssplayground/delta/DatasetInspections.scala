package uk.co.odinconsultants.sssplayground.delta

import org.apache.spark.sql.{DataFrame, Row}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

object DatasetInspections {

  def readCached(dir: String): Array[Row] = printSorted(session.read.format("delta").load(dir).cache())

  def printSorted(fromDisk: DataFrame): Array[Row] = {
    val actual = fromDisk.collect().sortBy(_.getLong(0))
    println(s"Contents:\n${actual.mkString("\n")}")
    actual
  }
}
