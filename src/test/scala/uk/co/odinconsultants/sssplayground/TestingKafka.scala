package uk.co.odinconsultants.sssplayground

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import uk.co.odinconsultants.htesting.kafka.{KafkaStarter, ZookeeperSetUp}
import uk.co.odinconsultants.htesting.local.UnusedPort

object TestingKafka {
  val topicName   = "topicName"
  val kafkaPort   = UnusedPort()
  val zkPort      = UnusedPort()
  val hostname    = "localhost"
  val zooKeeper   = ZookeeperSetUp(hostname, zkPort)
  val kafkaEnv    = new KafkaStarter(hostname, kafkaPort, zkPort, topicName)
  val kafkaServer = kafkaEnv.startKafka()


  def createTopic(topicName: String, numPartitions: Int) = {
    val adminClient = AdminClient.create(kafkaEnv.props)
    val topics = Seq[NewTopic](new NewTopic(topicName, numPartitions, 1.toShort))
    import scala.collection.JavaConverters._
    adminClient.createTopics(topics.asJava)
    adminClient.close()
  }
}
