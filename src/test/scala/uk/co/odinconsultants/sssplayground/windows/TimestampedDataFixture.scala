package uk.co.odinconsultants.sssplayground.windows

import java.sql.Timestamp
import java.text.{ParsePosition, SimpleDateFormat}

import scala.annotation.tailrec

object TimestampedDataFixture {

  val dateFormat        = "yyyy-MM-dd'T'HH:mm:ssZZZZ"
  private val formatter = new SimpleDateFormat(dateFormat)

  val midnight30Dec2019UTC: Timestamp = new Timestamp(formatter.parse("2019-12-30T00:00:00UTC", new ParsePosition(0)).getTime)
  val midnight13Jan2020UTC: Timestamp = new Timestamp(formatter.parse("2020-01-13T00:00:00UTC", new ParsePosition(0)).getTime)
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

  val nKeys = 7
  val nIds  = 11

  type TimestampedData = (Int, Timestamp, Int, Int)

  def timestampedData(n: Int,
                      nKeys:    Int       = nKeys,
                      nIds:     Int       = nIds,
                      startInc: Timestamp = midnight30Dec2019UTC,
                      endExcl:  Timestamp = midnight11Feb2020UTC): List[TimestampedData] = {
    val times = generateTimestamps(n, startInc, endExcl)
    times.zipWithIndex.map { case (ts, i) => (i, ts, i % nKeys, i % nIds)}
  }

}
