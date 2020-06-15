package uk.co.odinconsultants.sssplayground

import io.delta.tables._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

import scala.collection.mutable.ArrayBuffer

class DeltaPersistSpec extends WordSpec with Matchers with LoggingToLocalFS {

  import DeltaPersistSpec._

  "A typical dataframe" should {
    val filename                = hdfsUri + this.getClass.getSimpleName
    val words:    Seq[String]   = "the quick brown fox jumped over the lazy dog".split(" ")
    val id1                     = 1
    "be persisted as a delta frame" in {
      val toWrite:  Seq[MyRow]    = words.map(x => MyRow(x, id1))
      val state:    Array[MyRow]  = writeBatch(filename, toWrite, SaveMode.Append)
      state.toSet shouldBe toWrite.toSet
      logToDisk(readJsonFiles(filename), "1write")
    }
    "be appended" in {
      val toWrite:  Seq[MyRow]    = words.map(x => MyRow(x, 2))
      val state:    Array[MyRow]  = writeBatch(filename, toWrite, SaveMode.Append)
      state should have size (words.size * 2)
      logToDisk(readJsonFiles(filename), "2write")
    }
    "be updated like a DataFrame" in {
      val toWrite:  Seq[MyRow]    = words.map(x => MyRow(x, id1))
      val state:    Array[MyRow]  = writeBatch(filename, toWrite, SaveMode.Overwrite)
      withClue(s"Actual:\n${state.mkString("\n")}") {
        state should have size words.size
      }
      logToDisk(readJsonFiles(filename), "3overwrite")
    }
    "be updated like a SQL table" in {
      val deltaTable  = DeltaTable.forPath(filename)
      val newWord     = "wotcha"
      deltaTable.update(col("value") === id1, Map("word" -> lit(newWord)))

      val state: Array[MyRow] = read(filename)
      withClue(s"Actual:\n${state.mkString("\n")}") {
        state should have size words.size
        state.filter(_.word == newWord) should have size words.length
      }
      logToDisk(readJsonFiles(filename), "4sql_update")
    }

    val before: ArrayBuffer[Path] = ArrayBuffer.empty

    "have its Parquet files coalesced when vacuumed" in {
      before.append(list(filename).toArray: _*)
      info(s"Before:\n${before.mkString("\n")}")

      session.sessionState.conf.setConfString("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      val deltaTable  = DeltaTable.forPath(session, filename)

      deltaTable.vacuum(0)

      val after: List[Path] = list(filename)
      info(s"After:\n${after.mkString("\n")}")
      withClue(s"Before:\n${before.mkString("\n")}\nAfter:\n${after.mkString("\n")}") {
        after.size should be < (before.size)
      }
      logToDisk(readJsonFiles(filename), "5after_vacuum")
    }

    "but its JSON files stay the same" in {
      val isJsonFile: Path => Boolean = _.toString.endsWith(".json")
      val beforeJson: List[Path]      = before.toList.filter(isJsonFile)
      val afterJson:  List[Path]      = list(filename).filter(isJsonFile)
      withClue(s"Before:\n${before.mkString("\n")}\nAfter:\n${afterJson.mkString("\n")}") {
        beforeJson.size shouldEqual afterJson.size
      }
    }
  }


  def readJsonFiles(dir: String): Map[String, String] = TestingHdfsUtils.contentsOfFilesIn(dir, ".json")

  private def writeBatch(filename: String, rows: Seq[MyRow], saveMode: SaveMode): Array[MyRow] = {
    deltaWrite(rows, filename, saveMode)
    read(filename)
  }

  private def read(filename: String): Array[MyRow] = {
    import session.implicits._
    val fromDisk = session.read.format("delta").load(filename).as[MyRow]
    fromDisk.collect()
  }

  private def deltaWrite(xs: Seq[MyRow], filename: String, saveMode: SaveMode): Unit = {
    import session.implicits._
    val df = session.sparkContext.parallelize(xs).toDF
    df.write.format("delta").mode(saveMode).save(filename)
  }
}

object DeltaPersistSpec {

  case class MyRow(word: String, value: Int)

}
