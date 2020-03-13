package uk.co.odinconsultants.sssplayground

import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting
import org.apache.spark.sql.SaveMode

class DeltaPersistSpec extends WordSpec with Matchers {

  import DeltaPersistSpec._

  "A typical dataframe" should {
    "be persisted as a delta frame" in {
      val s               = SparkForTesting.session
      import s.implicits._
      val filename  = hdfsUri + this.getClass.getSimpleName

      val words: Seq[String]        = "the quick brown fox jumped over the lazy dog".split(" ")
      writeFirstBatch(s, filename, words)

      info(s"Files after first bacth in $filename:\n${list(filename).mkString("\n")}")

      val secondRows: Seq[MyRow] = words.map(x => MyRow(x, 2))
      deltaWrite(s, secondRows, filename)
      val fromDisk = s.read.parquet(filename).as[MyRow]

      info(s"Files after second batch in $filename:\n${list(filename).mkString("\n")}")
      fromDisk.collect should have size (words.size * 2)
    }
  }

  private def writeFirstBatch(s: SparkSession, filename: String, words: Seq[String]): Unit = {
    import s.implicits._
    val firstRows: Seq[MyRow] = words.map(x => MyRow(x, 1))
    deltaWrite(s, firstRows, filename)
    val fromDisk = s.read.parquet(filename).as[MyRow]
    fromDisk.collect().toSet shouldBe firstRows.toSet
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
