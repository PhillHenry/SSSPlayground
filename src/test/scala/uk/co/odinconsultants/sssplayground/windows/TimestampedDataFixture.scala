package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp
import java.text.{ParsePosition, SimpleDateFormat}
import java.util.Date

import scala.annotation.tailrec

trait TimestampedDataFixture {

  val dateFormat        = "yyyy-MM-dd'T'HH:mm:ssZZZZ"
  private val formatter = new SimpleDateFormat(dateFormat)

  val midnight30Dec2019UTC: Timestamp = new Timestamp(formatter.parse("2019-12-30T00:00:00UTC", new ParsePosition(0)).getTime)
  val midnight11Feb2020UTC: Timestamp = new Timestamp(formatter.parse("2020-02-11T00:00:00UTC", new ParsePosition(0)).getTime)

  def generateTimestamps(n: Int, startInc: Timestamp, endExcl: Timestamp): List[Timestamp] = {
    val step = (endExcl.getTime - startInc.getTime) / n

    @tailrec
    def timestamps(xs: List[Timestamp], remaining: Int): List[Timestamp] = {
      if (remaining == 0) {
        xs
      } else {
        timestamps(xs :+ new Timestamp(startInc.getTime + (n - remaining) * step), remaining - 1)
      }
    }
    timestamps(List.empty, n)
  }


}
