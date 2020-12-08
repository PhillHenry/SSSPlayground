package uk.co.odinconsultants.sssplayground.delta

import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.delta.DatasetInspections.readCached

object SparkUpdate {
  import SparkMain._

  def main(args: Array[String]): Unit = {
    import session.implicits._
    val dir       = args(0)
    val toUpdate  = 3L
    val df        = session.range(10).map(i => (toUpdate, i)).toDF(INDEX_COL, VALUE_COL)

    readCached(dir)

    df.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", s"$INDEX_COL = $toUpdate")
      .save(dir)

    readCached(dir)
  }

}
