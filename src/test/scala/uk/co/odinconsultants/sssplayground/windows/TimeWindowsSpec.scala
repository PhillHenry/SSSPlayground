package uk.co.odinconsultants.sssplayground.windows

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class TimeWindowsSpec extends WordSpec with Matchers {

  "Data partitioned by date" should {

    "have the latest data point available by using a Window function" in new TimestampedDataFixture {
      private val n     = 10000
      private val nKeys = 19
      private val times = generateTimestamps(n, midnight30Dec2019UTC, midnight11Feb2020UTC)
      private val data  = times.zipWithIndex.map { case (ts, i) => (ts, i % nKeys)}

      import session.implicits._
      private val df = session.sparkContext.parallelize(data).toDF
    }
  }

}
