package uk.co.odinconsultants.sssplayground.windows

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, ProducerCallback, createProducer, waitForAll}

import scala.util.{Failure, Try}

class TimestampedStreamingSpec extends WordSpec with Matchers with Eventually {

  val topicName = this.getClass.getSimpleName

  val logger = Logger.getLogger(this.getClass)

  def randomFileName(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  val processTimeMs = 2000
  val timeUnit      = "milliseconds"

  import session.implicits._

  s"Aggregated stream over a window of $processTimeMs $timeUnit" should {

    val sinkFile = randomFileName()

    val sink     = Sinks(ParquetFormat)

    "be written to HDFS even if there is data still to process (per SPARK-24156)" in {
      val dataFrame = sourceStream()//.withWatermark("timestamp", s"${processTimeMs / 2} $timeUnit") <-- this watermark means nothing comes through
      val query     = sink.writeStream(dataFrame, sinkFile, processTimeMs, None)(RowEncoder(dataFrame.schema))

      val console: StreamingQuery = dataFrame
        .writeStream
        .format("console")
        .outputMode(OutputMode.Complete())
        .option("truncate", "false")
        .queryName("console")
        .start()

      val n             = 10

      val producer      = createProducer(hostname, kafkaPort)

      waitForAll(sendMessages(n, producer, processTimeMs / n))

//      pauseMs(processTimeMs)

      waitForAll(sendMessages(n, producer, processTimeMs / n))

      pauseMs(processTimeMs * 4)

      waitForAll(sendMessages(1, producer, processTimeMs / n)) // dammit - I still need this to make the test pass!

      logQuery(console)

      Try {
        waitForFileToContain(n * 2)
      } match {
        case Failure(e) =>
          logQuery(query)
          query.processAllAvailable()
          logQuery(query)
          query.stop()
          fromDisk().show(false)
          fromDisk().collect().sortBy(_.getLong(0)).foreach(println)
          fail(e)
        case _ =>
      }
    }

    def fromDisk(): DataFrame = sink.readDataFrameFromHdfs(sinkFile, session)

    def waitForFileToContain(expected: Int): Unit =
      StreamingAssert.assert({
        logger.info(s"Processed. Data should have been written to $sinkFile. ")
        val count = fromDisk().count().toInt
        logger.info(s"count = $count")
        count shouldBe expected
      })
  }


  def sendMessages(n: Int, producer: KafkaProducer[String, String], pauseMS: Long) =
    (1 to n).map { i =>
      val now     = new java.util.Date()
      val payload = s"${now.getTime}$DatumDelimiter${i * Math.PI}"
      val record  = new ProducerRecord[String, String](topicName, i.toString, payload)
      val jFuture = producer.send(record, ProducerCallback)
      logger.info(s"Sent $payload (ts = $now)")
      pauseMs(pauseMS)
      jFuture
    }

  private def logQuery(query: StreamingQuery) = {
    logger.info(s"lastProgress = ${query.lastProgress}")
    logger.info(s"status = ${query.status}")
    query.recentProgress.foreach { progress =>
      logger.info(s"${progress.name}: batch id = ${progress.batchId}: number of input rows = ${progress.numInputRows}")
    }
  }

  private def sourceStream(): DataFrame = {
    val dataSet     = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingDatum)
    // Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
    // Event time must be defined on a window or a timestamp, but id is of type bigint
    val stream      = dataSet.withWatermark("ts", s"${processTimeMs} $timeUnit") // appears we wait {delayThreshold} before processing messages (?)
    val overWindow  = window('ts,
      windowDuration = s"${processTimeMs} $timeUnit"
      //        ,slideDuration   = s"$processTimeMs $timeUnit"
    )
    stream
      .groupBy('id, overWindow)
      .agg(count('id).as("count_id"), mean('amount), last('ts))
      .toDF( "id", "timestamp", "count", "mean", "ts")
  }

  private def pauseMs(dataWindow: Long) = {
    logger.info(s"Pausing for $dataWindow ms")
    Thread.sleep(dataWindow)
  }

  def dataToString(id2Data: Map[Int, String]): PayloadFn = { id =>
    new ProducerRecord[String, String](topicName, id.toString, id2Data(id))
  }


}
