package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, mean, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.{PayloadFn, sendAndWait}
import uk.co.odinconsultants.sssplayground.windows.TimestampedDataFixture.{TimestampedData, timestampedData}

import scala.collection.immutable
import scala.util.{Failure, Try}

class TimestampedStreamingSpec extends WordSpec with Matchers with Eventually {

  val topicName = this.getClass.getSimpleName

  val logger = Logger.getLogger(this.getClass)

  def randomFileName(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  val processTimeMs = 5000
  val timeUnit      = "milliseconds"

  import session.implicits._

  "Aggregated stream" should {

    val sinkFile = randomFileName()

    "be written to HDFS even if there is data still to process (per SPARK-24156)" in {
      val dataFrame     = sourceStream()

      val sink          = Sinks(ParquetFormat)

      val query         = sink.writeStream(dataFrame, sinkFile, processTimeMs, None)

      val console: StreamingQuery = dataFrame
//        .orderBy('id)
        .writeStream
        .format("console")
        .outputMode(OutputMode.Complete())
        .option("truncate", "false")
//        .trigger(Trigger.Continuous(processTimeMs))
        .start()

      val n             = 10
      val dataWindow    = processTimeMs / 2

      val batch1        = makeTestData(n, dataWindow)
      sendData(batch1)

      pauseMs(dataWindow * 4)

      val batch2        = makeTestData(n, dataWindow)
      sendData(batch2)

      val all           = batch1 ++ batch2
//      all.groupBy(_._2.getTime / processTimeMs).map { case (_, xs) => xs.groupBy(_._1)}

      def fromDisk(): DataFrame = sink.readDataFrameFromHdfs(sinkFile, session)
      Try {
        StreamingAssert.assert(query, {
          logger.info("Processing all available...")
          query.processAllAvailable()
          logger.info(s"Processed. Data should have been written to $sinkFile")
          val count = fromDisk().count().toInt
          logger.info(s"count = $count")
          count shouldBe (n * 2)
        })
      } match {
        case Failure(e) =>
          query.stop()
          fromDisk().show(false)
          fromDisk().collect().sortBy(_.getLong(0)).foreach(println)
          fail(e)
        case _ =>
      }
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
      .agg(count('id).as("count_id"), mean('amount))
      .toDF( "id", "timestamp", "count", "mean")
  }

  private def pauseMs(dataWindow: Int) = {
    logger.info(s"Pausing for $dataWindow ms")
    Thread.sleep(dataWindow)
  }

  def makeTestData(n: Int, windowMs: Long): Seq[TimestampedData] = {
    val now           = System.currentTimeMillis()
    val startInc      = new Timestamp(now - windowMs)
    val endExcl       = new Timestamp(now)
    val data          = timestampedData(n, startInc = startInc, endExcl = endExcl)
    data
  }

  private def sendData(data: Seq[TimestampedData]): immutable.Seq[RecordMetadata] = {

    implicit val order = new Ordering[Timestamp] {
      override def compare(x: Timestamp, y: Timestamp): Int = (x.getTime - y.getTime).toInt
    }

    val minDate = data.map(_._2).min
    val maxDate = data.map(_._2).max
    logger.info(s"Sending ${data.length} messages between ${minDate} and ${maxDate}")

    val dataAsStrings = dataToString(data.map { case (id, ts, x, y) =>
      (id + 1) -> s"${ts.getTime}$DatumDelimiter${x * Math.PI}"
    }.toMap)
    sendAndWait(dataAsStrings, hostname, kafkaPort, data.length)
  }

  def dataToString(id2Data: Map[Int, String]): PayloadFn = { id =>
    new ProducerRecord[String, String](topicName, id.toString, id2Data(id))
  }


}
