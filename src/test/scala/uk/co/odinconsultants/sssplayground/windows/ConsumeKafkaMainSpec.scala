package uk.co.odinconsultants.sssplayground.windows

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.kafka.{KafkaStarter, ZookeeperSetUp}
import uk.co.odinconsultants.htesting.local.UnusedPort

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
      //TODO
    }
  }

}
