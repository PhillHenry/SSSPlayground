package uk.co.odinconsultants.sssplayground

import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting
import org.apache.spark.sql.SaveMode
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class DeltaPersistSpec extends WordSpec with Matchers {

  import DeltaPersistSpec._

  "A typical dataframe" should {
    val filename                = hdfsUri + this.getClass.getSimpleName
    val words:    Seq[String]   = "the quick brown fox jumped over the lazy dog".split(" ")
    val id1                     = 1
    "be persisted as a delta frame" in {
      val write1:   Seq[MyRow]    = words.map(x => MyRow(x, id1))
      val written1: Array[MyRow]  = writeBatch(filename, write1)
      written1.toSet shouldBe write1.toSet

      val write2:   Seq[MyRow]    = words.map(x => MyRow(x, 2))
      val written2: Array[MyRow]  = writeBatch(filename, write2)
      written2 should have size (words.size * 2)
    }
    "be updated like a SQL table" in {
      import org.apache.spark.sql.functions._
      val deltaTable  = DeltaTable.forPath(filename)
      val newWord     = "wotcha"
      deltaTable.update(col("value") === id1, Map("word" -> lit(newWord)))
      val actual: Array[MyRow] = read(filename)
      actual should have size (words.size * 2)
      withClue(s"Actual:\n${actual.mkString("\n")}") {
        actual.filter(_.word == newWord) should have size words.length
      }
    }
  }

  private def writeBatch(filename: String, rows: Seq[MyRow]): Array[MyRow] = {
    deltaWrite(rows, filename)
    read(filename)
  }

  private def read(filename: String): Array[MyRow] = {
    import session.implicits._
    val fromDisk = session.read.format("delta").load(filename).as[MyRow]
    fromDisk.collect()
  }

  private def deltaWrite(xs: Seq[MyRow], filename: String): Unit = {
    import session.implicits._
    val df = session.sparkContext.parallelize(xs).toDF
    df.write.format("delta").mode(SaveMode.Append).save(filename)
  }
}

object DeltaPersistSpec {

  case class MyRow(word: String, value: Int)

}
