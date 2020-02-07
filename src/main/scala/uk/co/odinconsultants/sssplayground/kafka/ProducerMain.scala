package uk.co.odinconsultants.sssplayground.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.Future
import scala.util.Random

object ProducerMain {

  def toLocalEndPoint(hostname: String, port: Int) = s"$hostname:$port"

  def main(args: Array[String]): Unit = {
    val hostname  = args(0)
    val kPort     = args(1).toInt
    val n         = args(2).toInt
    val topicName = args(3)

    val props = new Properties()
    props.put("bootstrap.servers",                toLocalEndPoint(hostname, kPort))
    props.put("key.serializer",                   classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    props.put("value.serializer",                 classOf[org.apache.kafka.common.serialization.StringSerializer].getName)

    val producer  = new KafkaProducer[String, String](props)

    def json(): String = Random.alphanumeric.filter(_.isLetter).take(500000).mkString

    val jFutures = (1 to n).map { i =>
      val jFuture = producer.send(new ProducerRecord[String, String](topicName, json), new Callback {
        override def onCompletion(metadata: RecordMetadata, x: Exception) = {
          if (x != null) x.printStackTrace()
        }
      })
      jFuture
    }

    println("Waiting for Kafka to consume message")
    jFutures.map(_.get(10, TimeUnit.SECONDS))
  }

}
