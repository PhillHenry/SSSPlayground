package uk.co.odinconsultants.sssplayground.windows

import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting
import uk.co.odinconsultants.sssplayground.TestingKafka._
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.ProducerMain.json
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain.{streamingToHDFS, trivialKafkaParseFn}
import uk.co.odinconsultants.sssplayground.{LoggingToLocalFS, TestingHdfsUtils}

class ConsumeKafkaMainSpec extends WordSpec with Matchers with LoggingToLocalFS {

  "Messages sent through Kafka" should {
    "be persisted to HDFS" in {
      val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())
      val hdfsDir         = hdfsUri + sinkFile
      val s               = SparkForTesting.session
      import s.implicits._
      val stream          = streamStringsFromKafka(s, s"$hostname:$kafkaPort", topicName, trivialKafkaParseFn)
      val processTimeMs   = 2000
      streamingToHDFS(stream, hdfsUri + sinkFile, processTimeMs)

      val n = 10
      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println)
      Thread.sleep(processTimeMs * 3)
      logToDisk(TestingHdfsUtils.readFileFrom(hdfsDir, ".json"), "0")

      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println) // APPEND seems to need more messages before it actually writes to disk...
      Thread.sleep(processTimeMs * 3)
      logToDisk(TestingHdfsUtils.readFileFrom(hdfsDir, ".json"), "1")

      val actualFiles = list(hdfsDir)
      info(s"Files in $sinkFile:\n${actualFiles.mkString("\n")}")
      actualFiles should not be empty
    }
  }

}
