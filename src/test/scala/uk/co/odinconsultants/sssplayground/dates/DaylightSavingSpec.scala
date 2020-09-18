package uk.co.odinconsultants.sssplayground.dates

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.scalatest.{Matchers, WordSpec}

class DaylightSavingSpec extends WordSpec with Matchers {

  val _7October1999:    Timestamp = toRecordTime("07/10/1999 00:00")
  val _17December2005:  Timestamp = toRecordTime("17/12/2005 00:00")

  def assertNoTimeComponentIn(x: java.sql.Date): Unit = {
    val timestamp = new Timestamp(x.getTime)
    withClue(s"$x, timestamp = $timestamp") {
      (timestamp.getTime % 86400) shouldBe 0L
    }
  }

  "Dates to Timestamps" should {
    val sqlDateBST = new java.sql.Date(_7October1999.getTime)
    val sqlDateGMT = new java.sql.Date(_17December2005.getTime)
    "maintain timezone" in {
      assertNoTimeComponentIn(sqlDateBST)
      assertNoTimeComponentIn(sqlDateGMT)
    }
  }

  def toRecordTime(x: String): Timestamp = {
    val ApplicationTimeZone:    TimeZone    = TimeZone.getTimeZone("UTC")
    TimeZone.setDefault(ApplicationTimeZone)

    val parser    = new SimpleDateFormat("dd/MM/yyyy HH:mm")
    val date      = parser.parse(x)
    val timestamp = new Timestamp(date.getTime)

    println(s"date = $date, timestamp = $timestamp")

    timestamp
  }
}
