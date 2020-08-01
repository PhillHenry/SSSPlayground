package uk.co.odinconsultants.sssplayground.windows

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import uk.co.odinconsultants.sssplayground.kafka.Consuming._
import uk.co.odinconsultants.sssplayground.spark.Init

object ConsumeKafkaMain {

  def main(args: Array[String]): Unit = {
    val kafkaUrl        = args(0)
    val topicName       = args(1)
    val sinkFile        = args(2)
    val processTimeMs   = args(3).toLong
    val s               = Init.session()
    import s.implicits._
    val stream          = streamStringsFromKafka(s, kafkaUrl, topicName, trivialKafkaParseFn)
    val sink            = Sinks(DeltaFormat)
    sink.writeStream(stream, sinkFile, Some(Trigger.ProcessingTime(processTimeMs)))
    s.streams.awaitAnyTermination()
  }

  val trivialKafkaParseFn: KafkaParseFn[Payload] = { case (_, v) => Some(Payload(v, (v.hashCode % 10).toString)) }

  case class Payload(payload: String, period: String)



}
