package uk.co.odinconsultants.sssplayground.windows

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}

import scala.util.{Failure, Success, Try}

object StreamingAssert extends Eventually {

  val logger = Logger.getLogger(this.getClass)

  implicit override def patienceConfig: PatienceConfig = PatienceConfig(Span(21, Seconds), Span(1, Second))

  def assert(f: => Unit): Unit = {
    eventually {
      Try {
        f
      } match {
        case Failure(e) =>
          logger.error(e.getMessage)
          throw e
        case Success(x) => x
      }
    }
  }

}
