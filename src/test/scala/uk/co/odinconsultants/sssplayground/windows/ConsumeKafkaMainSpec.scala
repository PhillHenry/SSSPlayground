package uk.co.odinconsultants.sssplayground.windows

import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.sssplayground.LoggingToLocalFS
import uk.co.odinconsultants.sssplayground.TestingKafka._
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.ProducerMain.json
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.ConsumeKafkaMain._

class ConsumeKafkaMainSpec extends WordSpec with Matchers with LoggingToLocalFS {

  import session.implicits._

  "Messages sent through Kafka" should {
    "be persisted to HDFS in Parquet format" in {
      val sink            = Sinks(ParquetFormat)
      val hdfsDir         = hdfsUri + sinkFile
      stream2BatchesTo(sink, hdfsDir)
    }
    "be persisted to HDFS in Delta format" in {
      val sink            = Sinks(DeltaFormat)
      val hdfsDir         = hdfsUri + sinkFile
      stream2BatchesTo(sink, hdfsDir)
    }
  }

  val payloadFn: PayloadFn = _ => new ProducerRecord[String, String](topicName, json())

  def stream2BatchesTo(sink: Sink, hdfsDir: String): Unit = {
    val stream          = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, trivialKafkaParseFn)
    val processTimeMs   = 2000
    val query           = sink.sink(stream, hdfsUri + sinkFile, processTimeMs)

    val n = 10
    sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println)
    Thread.sleep(processTimeMs * 2)
    sendAndWait(payloadFn, hostname, kafkaPort, n).foreach(println) // APPEND seems to need more messages before it actually writes to disk...

    StreamingAssert.assert(query, {
      val count = sink.readFromHdfs[Payload](hdfsDir, session).count().toInt
      println(s"count = $count")
      count shouldBe (n * 2)
    })
  }

}
