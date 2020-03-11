package uk.co.odinconsultants.sssplayground.windows

import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.kafka.{KafkaStarter, ZookeeperSetUp}
import uk.co.odinconsultants.htesting.local.UnusedPort
import uk.co.odinconsultants.htesting.spark.SparkForTesting
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.ProducerMain.json
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.spark.Init
import uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain.{streamingToHDFS, trivialKafkaParseFn}

class ConsumeKafkaMainSpec extends WordSpec with Matchers {

  "Messages sent through Kafka" should {
    "be persisted to HDFS" in {
      val topicName   = "topicName"
      val kafkaPort   = UnusedPort()
      val zkPort      = UnusedPort()
      val hostname    = "localhost"
      val zooKeeper   = ZookeeperSetUp(hostname, zkPort)
      val kafkaEnv    = new KafkaStarter(hostname, kafkaPort, zkPort, topicName)
      val sinkFile    = "tmp_parquet"
      val kafkaServer = kafkaEnv.startKafka()

      val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())

      val s               = SparkForTesting.session
      import s.implicits._
      val stream          = streamStringsFromKafka(s, s"$hostname:$kafkaPort", topicName, trivialKafkaParseFn)
      val processTime = 2000
      streamingToHDFS(stream, hdfsUri + sinkFile, processTime)

      val n = 10
      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println)

      Thread.sleep(processTime * 2)
      sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println) // APPEND seems to need more messages before it actually writes to disk...
      Thread.sleep(processTime * 2)


      val actualFiles = list(hdfsUri + sinkFile)
      info(s"Files in $sinkFile:\n${actualFiles.mkString("\n")}")
      actualFiles should not be empty
    }
  }

}
