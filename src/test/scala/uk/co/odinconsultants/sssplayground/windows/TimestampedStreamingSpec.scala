package uk.co.odinconsultants.sssplayground.windows

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.functions.{count, mean, window}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort, topicName}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.TimestampedDataFixture.timestampedData

class TimestampedStreamingSpec extends WordSpec with Matchers with Eventually {

  def randomFileName(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  "Aggregated stream" should {

    import session.implicits._
    val sinkFile = randomFileName()

    def dataToString(id2Data: Map[Int, String]): PayloadFn = { id =>
      new ProducerRecord[String, String](topicName, id.toString, id2Data(id))
    }

    "be written to HDFS" ignore {
      val stream        = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingDatum)
        .withWatermark("ts", "1 minute")
      val ds            = stream
        .groupBy('id, window('ts, "1 second", "1 second"))
        .agg('id, count('id), mean('amount))
        .toDF("idX", "timestamp", "id", "count", "mean")

      val sink          = Sinks(DeltaFormat)
      val query         = sink.sink(ds, sinkFile, 1000, None)

      val n             = 100
      val data          = timestampedData(n)
      val dataAsStrings = dataToString(data.map { case (id, ts, x, y) =>
        (id + 1) -> s"${ts.getTime}$DatumDelimiter${x * Math.PI}"
      }.toMap)
      sendAndWait(dataAsStrings, hostname, kafkaPort, n).foreach(println)

      StreamingAssert.assert(query, {
        val count = sink.readDataFrameFromHdfs(sinkFile, session).count().toInt
        println(s"count = $count")
        count shouldBe n
      })
    }
  }

}
