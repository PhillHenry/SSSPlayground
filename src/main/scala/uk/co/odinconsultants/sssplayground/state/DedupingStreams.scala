package uk.co.odinconsultants.sssplayground.state

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import uk.co.odinconsultants.sssplayground.state.DedupeQuestionCode.{EventTime, User, now, timeoutDuration}

object DedupingStreams {

  def windowedUsers(spark: SparkSession, dsUsers: Dataset[User]): DataFrame = {
    import spark.implicits._

    val overWindow  = window(col(EventTime), timeoutDuration, timeoutDuration)
    val ds          = dsUsers.withWatermark(EventTime, timeoutDuration)
    ds.groupBy('userId, overWindow).agg(count('userId), first('name), first('eventTime))
  }

}
