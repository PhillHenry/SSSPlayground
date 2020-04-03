package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.expressions.Window
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import org.apache.spark.sql.functions._

class TimeWindowsSpec extends WordSpec with Matchers {

  "Data partitioned by date" should {

    "have the latest data point available by using a Window function" in new TimestampedDataFixture {
      private val n     = 10000
      private val nKeys = 7
      private val times = generateTimestamps(n, midnight30Dec2019UTC, midnight11Feb2020UTC)
      private val data  = times.zipWithIndex.map { case (ts, i) => (ts, i % nKeys)}

      import session.implicits._

      private val KEY         = "key"
      private val TIMESTAMP   = "ts"
      private val df          = session.sparkContext.parallelize(data).toDF(TIMESTAMP, KEY)
      private val mostRecent  = df.groupBy(KEY).agg(max(TIMESTAMP)).collect() //.over(Window.orderBy(TIMESTAMP))).collect()

      mostRecent should have size nKeys
      private val mostRecentTs = mostRecent.map(_.getTimestamp(1))
      times.takeRight(nKeys).toSet shouldBe mostRecentTs.toSet
    }
  }

}
