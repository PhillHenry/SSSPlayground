package uk.co.odinconsultants.sssplayground

import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting
import org.apache.spark.sql.SaveMode

class DeltaPersistSpec extends WordSpec with Matchers {

  import DeltaPersistSpec._

  "A typical dataframe" should {
    val filename  = hdfsUri + this.getClass.getSimpleName
    val words:    Seq[String]   = "the quick brown fox jumped over the lazy dog".split(" ")
    "be persisted as a delta frame" in {
      val write1:   Seq[MyRow]    = words.map(x => MyRow(x, 1))
      val written1: Array[MyRow]  = writeBatch(filename, write1)
      written1.toSet shouldBe write1.toSet

      val write2:   Seq[MyRow]    = words.map(x => MyRow(x, 2))
      val written2: Array[MyRow]  = writeBatch(filename, write1)
      written2 should have size (words.size * 2)
    }
  }

  private def writeBatch(filename: String, rows: Seq[MyRow]): Array[MyRow] = {
    val s = SparkForTesting.session
    import s.implicits._

    deltaWrite(s, rows, filename)
    val fromDisk = s.read.parquet(filename).as[MyRow]
    fromDisk.collect()
  }

  private def deltaWrite(s: SparkSession, xs: Seq[MyRow], filename: String): Unit = {
    import s.implicits._
    val df = s.sparkContext.parallelize(xs).toDF
    df.write.format("delta").mode(SaveMode.Overwrite).save(filename)
  }
}

object DeltaPersistSpec {

  case class MyRow(word: String, value: Int)

}
