package uk.co.odinconsultants.sssplayground.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.concurrent.{TimeUnit, Future => JFuture}

import scala.collection.immutable

object Producing {

  def toLocalEndPoint(hostname: String, port: Int) = s"$hostname:$port"

  type PayloadFn = Int => ProducerRecord[String, String]

  def sendMessages(producer: KafkaProducer[String, String], n: Int, fn: PayloadFn): immutable.Seq[JFuture[RecordMetadata]] = {
    val jFutures = (1 to n).map { i =>
      val jFuture = producer.send(fn(i), new Callback {
        override def onCompletion(metadata: RecordMetadata, x: Exception): Unit = {
          if (x != null) x.printStackTrace()
        }
      })
      jFuture
    }
    jFutures
  }

  def createProducer(hostname: String, kPort: Int): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", toLocalEndPoint(hostname, kPort))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    new KafkaProducer[String, String](props)
  }

  def sendAndWait(fn: PayloadFn, hostname: String, kPort: Int, n: Int): immutable.Seq[RecordMetadata] = {
    val producer = createProducer(hostname, kPort)
    val jFutures = sendMessages(producer, n, fn)

    println("Waiting for Kafka to consume message")
    jFutures.map(_.get(10, TimeUnit.SECONDS))
  }
}
