package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class TimeWindowsSpec extends WordSpec with Matchers {

  import TimestampedDataFixture._

  "Timestamped data" should {

    val KEY         = "key"
    val TIMESTAMP   = "ts"
    val ID          = "id"
    val n           = 10000
    val data        = timestampedData(n)

    "have the latest data point" in {
      import session.implicits._

      val df          = session.sparkContext.parallelize(data).toDF(TIMESTAMP, KEY, ID)
      val mostRecent  = df.groupBy(KEY).agg(max(TIMESTAMP)).collect()

      mostRecent should have size nKeys
      val mostRecentTs = mostRecent.map(_.getTimestamp(1))
      data.map(_._1).takeRight(nKeys).toSet shouldBe mostRecentTs.toSet
    }
  }

}
