package uk.co.odinconsultants.sssplayground.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

object Consuming {

  def streamFromKafka(session: SparkSession, kafkaUrl: String, topicName: String): DataFrame =
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",  kafkaUrl)
      .option("subscribe",                topicName)
      .option("offset",                   "earliest")
      .option("startingOffsets",          "earliest")
      .load()

}
