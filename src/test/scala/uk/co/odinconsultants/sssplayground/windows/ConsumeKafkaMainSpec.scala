package uk.co.odinconsultants.sssplayground.windows

import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.sssplayground.LoggingToLocalFS
import uk.co.odinconsultants.sssplayground.TestingHdfsUtils.contentsOfFilesIn
import uk.co.odinconsultants.sssplayground.TestingKafka._
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.ProducerMain.json
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain._

import scala.util.Try

class ConsumeKafkaMainSpec extends WordSpec with Matchers with LoggingToLocalFS with Eventually {

  implicit override def patienceConfig: PatienceConfig = PatienceConfig(Span(10, Seconds), Span(1, Second))

  "Messages sent through Kafka" should {

    "be persisted to HDFS in Parquet format" in {
      val sink            = Sinks(ParquetFormat)
      val hdfsDir         = hdfsUri + sinkFile
      stream2BatchesTo(sink, hdfsDir)
    }
    "be persisted to HDFS in Delta format" in {
      val sink            = Sinks(DeltaFormat)
      val hdfsDir         = hdfsUri + sinkFile

      stream2BatchesTo(sink, hdfsDir)
      logToDisk(contentsOfFilesIn(hdfsDir, ".json"), "0")

      val actualFiles = list(hdfsDir)
      info(s"Files in $sinkFile:\n${actualFiles.mkString("\n")}")
    }
  }

  val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())

  def stream2BatchesTo(sink: Sink, hdfsDir: String): Unit = {
    import session.implicits._

    val stream          = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, trivialKafkaParseFn)
    val processTimeMs   = 2000
    val query           = sink.sink(stream, hdfsUri + sinkFile, processTimeMs)

    val n = 10
    sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println)
    sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println) // APPEND seems to need more messages before it actually writes to disk...

    try {
      eventually {
        val count = sink.readFromHdfs(hdfsDir, session).count().toInt
        println(s"count = $count")
        count shouldBe (n * 2)
      }
    } finally {
      query.stop()
    }
  }

}
