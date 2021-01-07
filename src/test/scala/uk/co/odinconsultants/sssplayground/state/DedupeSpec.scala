package uk.co.odinconsultants.sssplayground.state

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.TestingKafka.{hostname, kafkaPort}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{DatumDelimiter, parsingDatum}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.streamStringsFromKafka

class DedupeSpec extends WordSpec with Matchers with TestUtils {

  import Dedupe._

  val CreateUserFn: CreateMessageFn = (i, now) => s"${now.getTime}$DatumDelimiter$i"

  "De-duping static batch Dataset" should {
    import session.implicits._
    "remove dupes from across batches" in {
      val ids       = Seq(1, 3, 5, 1)
      val xs        = ids.map(x => User("a name", x))
      val df        = xs.toDS
      val deduped   = removeDuplicates(df).collect()
      deduped.map(_.userId).toSet shouldEqual ids.toSet
    }
  }

  "Deduping streaming Dataset" should {
    "remove dupes" in {
      import session.implicits._
      val dataSet = streamStringsFromKafka(session, s"$hostname:$kafkaPort", topicName, parsingUser)
    }
  }

}
