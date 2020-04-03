package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class TimeWindowsSpec extends WordSpec with Matchers {

  import TimestampedDataFixture._

  "Timestamped data" should {

    val KEY         = "key"
    val TIMESTAMP   = "ts"
    val ID          = "id"
    val INDEX       = "index"
    val data        = timestampedData(10000)

    import session.implicits._
    val df          = session.sparkContext.parallelize(data).toDF(INDEX, TIMESTAMP, KEY, ID)

    "have the latest data point when queried" in {
      val mostRecent  = df.groupBy(KEY).agg(max(TIMESTAMP)).collect()

      mostRecent should have size nKeys
      val actualMostRecentTs = mostRecent.map(_.getTimestamp(1))
      val expectedMostRecent = data.map(_._2).takeRight(nKeys).toSet
      expectedMostRecent shouldBe actualMostRecentTs.toSet
    }

    "have datapoints no later than a point in the date range" in {
      val subset: DataFrame = df.where(col(TIMESTAMP).lt(lit(midnight13Jan2020UTC)))
      val actual = subset.groupBy(KEY).agg(max(TIMESTAMP)).collect()
      actual should have size nKeys
      actual.foreach { r =>
        r.getTimestamp(1).getTime should be <= (midnight13Jan2020UTC.getTime)
      }
    }
  }

}
