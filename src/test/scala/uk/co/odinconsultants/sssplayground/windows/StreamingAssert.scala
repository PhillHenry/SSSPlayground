package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}

object StreamingAssert extends Eventually{

  implicit override def patienceConfig: PatienceConfig = PatienceConfig(Span(31, Seconds), Span(1, Second))

  def assert(query: StreamingQuery, f: => Unit): Unit = try {
    eventually {
      f
    }
  } finally {
    query.stop()
  }

}
