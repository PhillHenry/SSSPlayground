package uk.co.odinconsultants.sssplayground

import uk.co.odinconsultants.htesting.kafka.{KafkaStarter, ZookeeperSetUp}
import uk.co.odinconsultants.htesting.local.UnusedPort

object TestingKafka {
  val topicName   = "topicName"
  val kafkaPort   = UnusedPort()
  val zkPort      = UnusedPort()
  val hostname    = "localhost"
  val zooKeeper   = ZookeeperSetUp(hostname, zkPort)
  val kafkaEnv    = new KafkaStarter(hostname, kafkaPort, zkPort, topicName)
  val sinkFile    = "tmp_parquet"
  val kafkaServer = kafkaEnv.startKafka()
}
