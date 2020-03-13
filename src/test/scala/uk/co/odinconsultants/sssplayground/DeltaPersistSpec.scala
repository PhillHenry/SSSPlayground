package uk.co.odinconsultants.sssplayground

import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting

class DeltaPersistSpec extends WordSpec with Matchers {

  import DeltaPersistSpec._

  "A typical dataframe" should {
    "be persisted as a delta frame" in {
      val s               = SparkForTesting.session
      import s.implicits._
      val filename  = hdfsUri + this.getClass.getSimpleName

      val words: Seq[String]        = "the quick brown fox jumped over the lazy dog".split(" ")
      val firstRows: Seq[MyRow] = words.map(x => MyRow(x, 1))
      deltaWrite(s, firstRows, filename)

      val fromDisk = s.read.parquet(filename).as[MyRow]
      fromDisk.collect().toSet shouldBe firstRows.toSet

      val actualFiles = list(filename)
      info(s"Files in $filename:\n${actualFiles.mkString("\n")}")
    }
  }

  private def deltaWrite(s: SparkSession, xs: Seq[MyRow], filename: String): Unit = {
    import s.implicits._
    val df = s.sparkContext.parallelize(xs).toDF
    df.write.format("delta").mode("overwrite").save(filename)
  }
}

object DeltaPersistSpec {

  case class MyRow(word: String, value: Int)

}
