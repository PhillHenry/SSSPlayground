package uk.co.odinconsultants.sssplayground.delta

import org.apache.hadoop.fs.Path
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{distributedFS, hdfsUri}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.delta.DatasetInspections.printSorted

object DeltaLakeSetup extends TestUtils {

  val INDEX_COL = "index"
  val VALUE_COL = "value"

  def main(args: Array[String]): Unit = {
    import session.implicits._
    val dir   = randomFileName()
    val df    = session.range(10).map(i => (i, i)).toDF(INDEX_COL, VALUE_COL)
    df.write.partitionBy(INDEX_COL).format("delta").save(dir)
    println(s"dir = $dir")

    val fromDisk = session.read.format("delta").load(dir).cache()

    printSorted(fromDisk)
    val partitionPruning = fromDisk.where(s"$INDEX_COL == 1")
    println("Explaining...")
    partitionPruning.explain()
    println(s"after partition pruning = ${partitionPruning.collect().mkString("\n")}")
    println("Pausing. Press return to continue")
    scala.io.StdIn.readLine()
    printSorted(fromDisk)
  }

  def randomDirectory(): String = {
    val out       = randomFileName()
    distributedFS.mkdirs(new Path(out))
    out
  }
}
