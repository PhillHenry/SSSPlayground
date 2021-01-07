package uk.co.odinconsultants.sssplayground

import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import org.apache.spark.sql.functions._
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri

object DeleteMeMain {

  def main(args: Array[String]): Unit = {
    import session.implicits._

    val xs        = Seq(1, 2, 3)
    val colName   = "number"
    val df        = xs.toDF(colName)
    val path      = s"$hdfsUri/tmp/xs_parquet"
    df.write.format("delta").save(path)
    val fromDisk  = session.read.format("delta").load(path)
    fromDisk.cache()
    println(fromDisk.agg(sum(colName)).collect().mkString("\n"))
    System.exit(0)
  }

}
