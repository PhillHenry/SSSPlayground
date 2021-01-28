package uk.co.odinconsultants.sssplayground.windows

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, ProducerCallback, createProducer, waitForAll}

import scala.util.{Failure, Try}

class TimestampedStreamingSpec extends WordSpec with Matchers with Eventually with TestUtils {

  val processTimeMs = 2000
  val timeUnit      = "milliseconds"

  import session.implicits._

  s"Aggregated stream over a window of $processTimeMs $timeUnit" should {

    val sinkFile = randomFileName()

    val sink     = Sinks(DeltaFormat)

    s"be written to $sinkFile even if there is data still to process (per SPARK-24156)" in {
//      session.sql("set spark.databricks.delta.autoCompact.enabled = true")

      val dataFrame   = sourceStream()//.withWatermark("ts", s"${processTimeMs / 2} $timeUnit") //<-- this watermark means nothing comes through
      val someTrigger = Some(Trigger.ProcessingTime(processTimeMs))
      val query       = sink.writeStream(dataFrame, sinkFile, someTrigger, None)(RowEncoder(dataFrame.schema))

      val console: StreamingQuery = dataFrame
        .writeStream
        .format("console")
        .outputMode(OutputMode.Append())
        .option("truncate", "false")
        .queryName("console")
        .start()

      val nFirst        = 10
      val producer      = kafkaProducer()

      waitForAll(sendMessages(CreateDatumFn, nFirst, producer, processTimeMs / nFirst))

//      pauseMs(processTimeMs)

      val nSecond       = 9
      waitForAll(sendMessages(CreateDatumFn, nSecond, producer, processTimeMs / nSecond))

      pauseMs(processTimeMs * 4)

      waitForAll(sendMessages(CreateDatumFn, 1, producer, processTimeMs / nFirst)) // dammit - I still need this to make the test pass!
      logQuery(console)

      Try {
        waitForFileToContain(nFirst + nSecond)
      } match {
        case Failure(e) =>
          logQuery(query)
          query.processAllAvailable()
          logQuery(query)
          query.stop()
          fromDisk().show(false)
          fromDisk().collect().sortBy(_.getLong(0)).foreach(println)
          listFiles()
          fail(e)
        case _ =>
          listFiles()
          query.stop()
          console.stop()
      }
    }

    def listFiles(): Unit = println(s"files:\n${list(sinkFile).mkString("\n")}")

    def fromDisk(): DataFrame = sink.readDataFrameFromHdfs(sinkFile, session)

    def waitForFileToContain(expected: Int): Unit =
      StreamingAssert.assert({
        logger.info(s"Processed. Data should have been written to $sinkFile. ")
        val count = fromDisk().count().toInt
        logger.info(s"count = $count")
        count shouldBe expected
      })
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
              ,slideDuration   = s"$processTimeMs $timeUnit"
    )
    stream
      .groupBy('id, overWindow)
      .agg(count('id).as("count_id"), mean('amount), last('ts))
      .toDF( "id", "timestamp", "count", "mean", "ts")
  }

  def dataToString(id2Data: Map[Int, String]): PayloadFn = { id =>
    new ProducerRecord[String, String](topicName, id.toString, id2Data(id))
  }


}
