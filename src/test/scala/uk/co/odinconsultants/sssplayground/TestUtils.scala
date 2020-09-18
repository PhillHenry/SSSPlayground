package uk.co.odinconsultants.sssplayground

import org.apache.log4j.Logger
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri

trait TestUtils {

  val logger                    = Logger.getLogger(this.getClass)

  val topicName                 = this.getClass.getSimpleName

  def randomFileName(): String  = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  def pauseMs(dataWindow: Long): Unit = {
    logger.info(s"Pausing for $dataWindow ms")
    Thread.sleep(dataWindow)
  }

}
