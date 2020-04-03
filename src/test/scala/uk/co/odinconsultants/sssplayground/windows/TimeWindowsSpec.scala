package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class TimeWindowsSpec extends WordSpec with Matchers {

  import TimestampedDataFixture._

  "Timestamped data" should {

    val KEY         = "key"
    val TIMESTAMP   = "ts"
    val ID          = "id"
    val data        = timestampedData(10000)

    import session.implicits._
    val df          = session.sparkContext.parallelize(data).toDF(TIMESTAMP, KEY, ID)

    "have the latest data point when queried" in {
      val mostRecent  = df.groupBy(KEY).agg(max(TIMESTAMP)).collect()

      mostRecent should have size nKeys
      val actualMostRecentTs = mostRecent.map(_.getTimestamp(1))
      val expectedMostRecent = data.map(_._1).takeRight(nKeys).toSet
      expectedMostRecent shouldBe actualMostRecentTs.toSet
    }
  }

}
