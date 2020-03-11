package uk.co.odinconsultants.sssplayground

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting

class DeltaPersistSpec extends WordSpec with Matchers {

  "A typical dataframe" should {
    "be persisted as a delta frame" in {
      val s               = SparkForTesting.session
      import s.implicits._
      val xs: Seq[String]        = "the quick brown fox jumped over the lazy dog".split(" ")
      val df        = s.sparkContext.parallelize(xs).toDF("values")
      val filename  = hdfsUri + this.getClass.getSimpleName
      df.write.format("delta").parquet(filename)

      val fromDisk = s.read.parquet(filename)
      fromDisk.collect().map(_.mkString("")).toSet shouldBe xs.toSet
    }
  }

}
