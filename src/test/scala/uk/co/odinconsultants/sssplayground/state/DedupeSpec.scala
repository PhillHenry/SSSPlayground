package uk.co.odinconsultants.sssplayground.state

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.scalatest.{Assertions, Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.DatumDelimiter
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka
import uk.co.odinconsultants.sssplayground.kafka.Producing.waitForAll
import uk.co.odinconsultants.sssplayground.windows.{DeltaFormat, Sinks, StreamingAssert}

import scala.util.{Failure, Try}

class DedupeSpec extends WordSpec with Matchers with TestUtils with Assertions {

  import Dedupe._

  val CreateUserFn: CreateMessageFn = (i, now) => s"${now.getTime}$DatumDelimiter$i"

  val firstBatchIds   = Seq(1, 3, 5, 1)
  val secondBatchIds  = Seq(2, 3, 1)

  val producer = kafkaProducer()

  "Deduping streaming Dataset" should {

    val sinkFile = randomFileName()

    val sink     = Sinks(DeltaFormat)

    "remove dupes" in {
      import session.implicits._
      val userStream  = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingUser)
      val dataFrame   = removeDuplicates(userStream).toDF()
      val pauseMS     = 5000L
      val someTrigger = Some(Trigger.ProcessingTime(pauseMS))
      val query       = sink.writeStream(dataFrame, sinkFile, someTrigger, None)(RowEncoder(dataFrame.schema))

      val writingBatch = userStream.writeStream.foreachBatch(writeBatch)

      val console: StreamingQuery = writingBatch
        .format("console")
        .outputMode(OutputMode.Append())
        .option("truncate", "false")
        .queryName("console")
        .start()

      val firstMessages: Seq[(String, String)] = firstBatchIds.map(i => (i.toString, i.toString))
      waitForAll(send(firstMessages, producer, 10L))
      pauseMs(pauseMS * 2)

      waitFor(firstBatchIds.toSet.size)

      val secondMessages: Seq[(String, String)] = secondBatchIds.map(i => (i.toString, i.toString))
      waitForAll(send(secondMessages, producer, 10L))
      pauseMs(pauseMS * 2)

      waitFor((firstBatchIds ++ secondBatchIds).toSet.size)
      console.stop()

    }
    def waitFor(nMessages: Int): Unit = {
      Try {
        waitForFileToContain(nMessages)
      } match {
        case Failure(e) =>
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

  "De-duping static batch Dataset" should {
    import session.implicits._
    "remove dupes from across batches" in {
      val xs        = firstBatchIds.map(x => User("a name", x))
      val df        = xs.toDS
      val deduped   = removeDuplicates(df).collect()
      deduped.map(_.userId).toSet shouldEqual firstBatchIds.toSet
    }
  }

}
