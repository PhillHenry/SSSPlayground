package uk.co.odinconsultants.sssplayground

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.DatumDelimiter
import uk.co.odinconsultants.sssplayground.kafka.Producing.{ProducerCallback, createProducer}

trait TestUtils {

  val logger                    = Logger.getLogger(this.getClass)

  val topicName                 = this.getClass.getSimpleName

  def randomFileName(): String  = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  def pauseMs(dataWindow: Long): Unit = {
    logger.info(s"Pausing for $dataWindow ms")
    Thread.sleep(dataWindow)
  }

  def sendDatumMessages(n: Int, producer: KafkaProducer[String, String], pauseMS: Long) =
    (1 to n).map { i =>
      val now     = new java.util.Date()
      val payload = s"${now.getTime}$DatumDelimiter${i * Math.PI}"
      val record  = new ProducerRecord[String, String](topicName, i.toString, payload)
      val jFuture = producer.send(record, ProducerCallback)
      logger.info(s"Sent $payload (ts = $now)")
      pauseMs(pauseMS)
      jFuture
    }

  def kafkaProducer(): KafkaProducer[String, String] = createProducer(hostname, kafkaPort)
}
