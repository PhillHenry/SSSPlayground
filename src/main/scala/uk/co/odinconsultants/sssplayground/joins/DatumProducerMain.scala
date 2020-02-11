package uk.co.odinconsultants.sssplayground.joins

import org.apache.kafka.clients.producer.ProducerRecord
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}

object DatumProducerMain {

  def main(args: Array[String]): Unit = {
    val hostname  = args(0)
    val kPort     = args(1).toInt
    val n         = args(2).toInt
    val topicName = args(3)

    val payloadFn: PayloadFn = { i =>
      val key: String   = (i % 10).toString
      val now           = new java.util.Date()
      val value: String = s"${now.getTime}:$i"
      new ProducerRecord[String, String](topicName, key, value)
    }

    sendAndWait(payloadFn, hostname, kPort, n)
  }

}
