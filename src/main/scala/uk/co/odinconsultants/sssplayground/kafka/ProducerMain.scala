package uk.co.odinconsultants.sssplayground.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import uk.co.odinconsultants.sssplayground.kafka.Producing._

import scala.util.Random

object ProducerMain {

  def main(args: Array[String]): Unit = {
    def json(): String = Random.alphanumeric.filter(_.isLetter).take(500000).mkString

    val hostname  = args(0)
    val kPort     = args(1).toInt
    val n         = args(2).toInt
    val topicName = args(3)

    val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())

    sendAndWait(payloadFn, hostname, kPort, n).foreach(println)
  }


}
