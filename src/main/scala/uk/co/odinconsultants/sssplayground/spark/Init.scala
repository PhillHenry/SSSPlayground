package uk.co.odinconsultants.sssplayground.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Init {

  def session(): SparkSession = {
    val builder = SparkSession.builder()
    builder.config(sparkConf).getOrCreate()
  }

  private def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.setAppName("SSSPlayground")
    conf
  }

}
