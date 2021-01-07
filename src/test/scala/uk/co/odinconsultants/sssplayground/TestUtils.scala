package uk.co.odinconsultants.sssplayground

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.DatumDelimiter
import uk.co.odinconsultants.sssplayground.kafka.Producing.{ProducerCallback, createProducer}
import uk.co.odinconsultants.sssplayground.windows.StreamingAssert

import scala.collection.immutable

trait TestUtils {

  val logger                    = Logger.getLogger(this.getClass)

  val topicName                 = this.getClass.getSimpleName

  def randomFileName(): String  = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  def pauseMs(dataWindow: Long): Unit = {
    logger.info(s"Pausing for $dataWindow ms")
    Thread.sleep(dataWindow)
  }

  type CreateMessageFn = (Int, java.util.Date) => String

  val CreateDatumFn: CreateMessageFn = (i, now) => s"${now.getTime}$DatumDelimiter${i * Math.PI}"

  type SendFutures = immutable.Seq[Future[RecordMetadata]]

  def sendMessages(msgFn: CreateMessageFn, n: Int, producer: KafkaProducer[String, String], pauseMS: Long): SendFutures = {
    val xs = (1 to n).map { i =>
      val now     = new java.util.Date()
      i.toString -> msgFn(i, now)
    }
    send(xs, producer, pauseMS)
  }

  def send(xs: Seq[(String, String)], producer: KafkaProducer[String, String], pauseMS: Long): SendFutures = {
    val mutable: Array[Future[RecordMetadata]] = xs.map { case (k, v) =>
      val record  = new ProducerRecord[String, String](topicName, k, v)
      val jFuture = producer.send(record, ProducerCallback)
      logger.info(s"Sent $k -> $v")
      pauseMs(pauseMS)
      jFuture
    }.toArray
    collection.immutable.Seq(mutable: _*)
  }

  def kafkaProducer(): KafkaProducer[String, String] = createProducer(hostname, kafkaPort)
}
