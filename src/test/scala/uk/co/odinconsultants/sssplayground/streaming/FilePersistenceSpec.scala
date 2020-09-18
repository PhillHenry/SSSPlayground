package uk.co.odinconsultants.sssplayground.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.list
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{Datum, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.waitForAll
import uk.co.odinconsultants.sssplayground.windows.{ParquetFormat, Sinks}

class FilePersistenceSpec extends WordSpec with Matchers with TestUtils {

  import session.implicits._

  "Streaming from Kafka" should {

    val sinkFile      = randomFileName()
    val sink          = Sinks(ParquetFormat)
    val processTimeMs = 2000

    "not create too many files" in {
      val dataSet: Dataset[Datum] = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingDatum)
      val someTrigger             = Some(Trigger.ProcessingTime(processTimeMs))
      val df: DataFrame           = dataSet.toDF()
      val query                   = sink.writeStream(df, sinkFile, someTrigger, None)(RowEncoder(df.schema))

      pauseMs(processTimeMs * 2)
      checkNoFilesInHDFS()

      val nFirst        = 10
      val producer      = kafkaProducer()

      waitForAll(sendDatumMessages(nFirst, producer, processTimeMs / nFirst))
      val files: List[String] = listFiles()
      withClue(s"files:\n${files.mkString("\n")}") {
        files.size shouldBe > (0)
      }
    }

    def checkNoFilesInHDFS(): List[String] = {
      val files: List[String] = listFiles()
      withClue(s"files:\n${files.mkString("\n")}") {
        files should have size 0
      }
      files
    }

    def listFiles(): List[String] = {
      val files: List[Path] = list(sinkFile)
      println(s"files:\n${files.mkString("\n")}")
      files.map(_.toString)
    }
  }

}
