package uk.co.odinconsultants.sssplayground.delta

import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.delta.DatasetInspections._

object DeltaLakeUpdate {
  import DeltaLakeSetup._

  def main(args: Array[String]): Unit = {
    import session.implicits._
    val dir       = args(0)
    val toUpdate  = 3L
    val df        = session.range(10).map(i => (toUpdate, i)).toDF(INDEX_COL, VALUE_COL)

    val before = readCached(dir)

    df.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", s"$INDEX_COL = $toUpdate")
      .save(dir)

    val after = readWithoutCache(dir)

    println(s"Is the data the same after a call to 'replaceWhere'? ${before.deep == after.deep}")
  }

}
