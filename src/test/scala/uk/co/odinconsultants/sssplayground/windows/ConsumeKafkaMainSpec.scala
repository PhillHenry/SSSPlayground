package uk.co.odinconsultants.sssplayground.windows

import org.apache.kafka.clients.producer.ProducerRecord
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

class ConsumeKafkaMainSpec extends WordSpec with Matchers with LoggingToLocalFS with Eventually {

  implicit override def patienceConfig: PatienceConfig = PatienceConfig(Span(10, Seconds), Span(1, Second))

  "Messages sent through Kafka" should {

    import session.implicits._

    s"be persisted to HDFS in $FORMAT format" in {
      val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())
      val hdfsDir         = hdfsUri + sinkFile
      val stream          = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, trivialKafkaParseFn)
      val processTimeMs   = 2000
      streamingToDelta(stream, hdfsUri + sinkFile, processTimeMs)

      val n = 10
      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println)
      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println) // APPEND seems to need more messages before it actually writes to disk...

      eventually{
        readFromHdfs(hdfsDir, session).count() shouldBe (n.toLong * 2)
      }

      logToDisk(contentsOfFilesIn(hdfsDir, ".json"), "0")
      logToDisk(contentsOfFilesIn(hdfsDir, ".json"), "1")
    }
  }

}
