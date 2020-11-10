package uk.co.odinconsultants.sssplayground.streaming

import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.list
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.{TestUtils, TestingKafka}
import uk.co.odinconsultants.sssplayground.TestingKafka.{createTopic, hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{Datum, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.waitForAll
import uk.co.odinconsultants.sssplayground.windows.{DeltaFormat, ParquetFormat, Sinks}

class FilePersistenceSpec extends WordSpec with Matchers with TestUtils {

  import session.implicits._

  "Streaming from Kafka" should {

    val sinkFile      = randomFileName()
    val sink          = Sinks(DeltaFormat)
    val processTimeMs = 2000

    "not create too many files" in {
      val numPartitions           = math.max(5, Runtime.getRuntime.availableProcessors())
      createTopic(topicName, numPartitions)

      val dataSet: Dataset[Datum] = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingDatum)
      val someTrigger             = Some(Trigger.ProcessingTime(processTimeMs))
      val df: DataFrame           = dataSet.toDF()
      val query                   = sink.writeStream(df, sinkFile, someTrigger, None)(RowEncoder(df.schema))

//      checkNoFilesInHDFS()

      val nMessags      = numPartitions * 100
      val producer      = kafkaProducer()

      waitForAll(sendDatumMessages(nMessags, producer, processTimeMs / nMessags))
      pauseMs(processTimeMs * 2)
      val firstBatch = checkMinimumNumberOfFilesIs(numPartitions)

      waitForAll(sendDatumMessages(nMessags, producer, processTimeMs / nMessags))
      pauseMs(processTimeMs * 2)
      checkMinimumNumberOfFilesIs(numPartitions + firstBatch.size)

      query.stop()
    }

    def checkNoFilesInHDFS(): List[String] = {
      val files: List[String] = listParquetFiles()
      withClue(s"files:\n${files.mkString("\n")}") {
        files should have size 0
      }
      files
    }

    def listParquetFiles(): List[String] = {
      val files: List[Path] = list(sinkFile)
      println(s"files:\n${files.mkString("\n")}")
      files.map(_.toString).filter(_.endsWith(".parquet"))
    }

    def checkMinimumNumberOfFilesIs(minNumFiles: Int): List[String] = {
      val files: List[String] = listParquetFiles()
      withClue(s"files:\n${files.mkString("\n")}") {
        files.size should be >= minNumFiles
      }
      files
    }
  }
}
