package uk.co.odinconsultants.sssplayground.state

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.state.DedupeQuestionCode._

class DedupingStreamsSpec extends WordSpec with Matchers {

  import DedupingStreams._

  "Streams" should {
    "show user" in {
      import session.implicits._
      implicit val sqlCtx: SQLContext = session.sqlContext
      val memoryStream  = MemoryStream[User]
      val windowedDF    = windowedUsers(session, memoryStream.toDS())
      val stream2       = windowedDF.writeStream.format("console").outputMode("append").start()
      memoryStream.addData(Seq(User("mark", 111, now())))
      Thread.sleep(3000)
      memoryStream.addData(Seq(User("mark2", 111, now())))
      Thread.sleep(3000)
      memoryStream.addData(Seq(User("mark2", 111, now())))
      Thread.sleep(3000)
      stream2.stop()
      memoryStream.stop()
    }
  }

}
