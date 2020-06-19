package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.spark.sql.functions.{count, mean, window}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{hdfsUri, list}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort, topicName}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.TimestampedDataFixture.{midnight11Feb2020UTC, midnight30Dec2019UTC, timestampedData}

import scala.collection.immutable

class TimestampedStreamingSpec extends WordSpec with Matchers with Eventually {

  def randomFileName(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  "Aggregated stream" should {

    import session.implicits._
    val sinkFile = randomFileName()


    "be written to HDFS" ignore {
      val duration      = "2 second"
      val stream        = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingDatum)
        .withWatermark("ts", duration)
      val ds            = stream
        .groupBy('id, window('ts, windowDuration = duration, slideDuration = duration))
        .agg('id, count('id), mean('amount))
        .toDF("idX", "timestamp", "id", "count", "mean")

      val sink          = Sinks(ParquetFormat)
      val processTimeMs = 2000
      val query         = sink.sink(ds, sinkFile, processTimeMs, None)
//      query.awaitTermination(10000L)

      val n             = 100
      sendData(n, processTimeMs * 10)
      Thread.sleep(processTimeMs)
      sendData(n, processTimeMs * 10)

      StreamingAssert.assert(query, {
        query.processAllAvailable()
        withClue(s"${list(sinkFile).mkString("\n")}\n") {
//          val count = session.read.format("parquet").load(sinkFile).count().toInt
          val count = sink.readDataFrameFromHdfs(sinkFile, session).count().toInt
          println(s"count = $count")
          count shouldBe (n * 2)
        }

      })
    }
  }

  private def sendData(n: Int, windowMs: Long): immutable.Seq[RecordMetadata] = {
    val now           = System.currentTimeMillis()
    val startInc      = new Timestamp(now)
    val endExcl       = new Timestamp(now + windowMs)
    val data          = timestampedData(n, startInc = startInc, endExcl = endExcl)

    implicit val order = new Ordering[Timestamp] {
      override def compare(x: Timestamp, y: Timestamp): Int = (x.getTime - y.getTime).toInt
    }

    val minDate = data.map(_._2).min
    val maxDate = data.map(_._2).max
    println(s"Sending $n messages between ${minDate} and ${maxDate}")

    val dataAsStrings = dataToString(data.map { case (id, ts, x, y) =>
      (id + 1) -> s"${ts.getTime}$DatumDelimiter${x * Math.PI}"
    }.toMap)
    sendAndWait(dataAsStrings, hostname, kafkaPort, n)
  }

  def dataToString(id2Data: Map[Int, String]): PayloadFn = { id =>
    new ProducerRecord[String, String](topicName, id.toString, id2Data(id))
  }


}
